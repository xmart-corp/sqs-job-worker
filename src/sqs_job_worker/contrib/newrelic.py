import contextlib
from collections.abc import Callable

import newrelic.agent
import structlog

from sqs_job_worker.job_middleware import Job, JobMiddleware


class NewRelicMiddleware(JobMiddleware):
    """Propagate and measure jobs with New Relic.

    The producer asks the agent to inject distributed-trace headers. The consumer
    accepts them inside a background transaction, binds its trace and span ids to
    logs, and lets handler exceptions cross the transaction boundary.
    """

    def __init__(self, group: str = "SqsWorker") -> None:
        self._group = group

    def produce(self, job: Job, call_next: Callable[[Job], dict]) -> dict:
        if "traceparent" in job.trace_headers or "newrelic" in job.trace_headers:
            raise ValueError("another middleware already wrote trace-context headers; run only one tracing middleware (New Relic, OTel, ...) per process")
        headers: list[tuple[str, str]] = []
        with contextlib.suppress(Exception):
            newrelic.agent.insert_distributed_trace_headers(headers)
        job.trace_headers.update(headers)
        return call_next(job)

    def consume(self, job: Job, call_next: Callable[[Job], None]) -> None:
        try:
            transaction = newrelic.agent.BackgroundTask(newrelic.agent.application(), name=job.job_type, group=self._group)
            transaction.__enter__()
        except Exception:
            return call_next(job)
        try:
            if job.trace_headers:
                with contextlib.suppress(Exception):
                    newrelic.agent.accept_distributed_trace_headers(job.trace_headers, transport_type="Queue")
            structlog.contextvars.bind_contextvars(**self._linking_ids())
            call_next(job)
        except BaseException as error:
            with contextlib.suppress(Exception):
                transaction.__exit__(type(error), error, error.__traceback__)
            raise
        else:
            with contextlib.suppress(Exception):
                transaction.__exit__(None, None, None)

    @staticmethod
    def _linking_ids() -> dict:
        """Return trace and root-span ids for log correlation when available."""
        try:
            metadata = newrelic.agent.get_linking_metadata()
        except Exception:
            return {}
        ids = {"trace_id": metadata.get("trace.id"), "span_id": metadata.get("span.id")}
        return {key: value for key, value in ids.items() if value}
