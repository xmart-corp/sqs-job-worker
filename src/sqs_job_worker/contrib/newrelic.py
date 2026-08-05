import contextlib
import re
import secrets
from collections.abc import Callable
from typing import Any

import newrelic.agent
import structlog

from sqs_job_worker.job_middleware import Job, JobMiddleware

_TRACE_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
_TRACEPARENT_PATTERN = re.compile(r"^[0-9a-f]{2}-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}$")


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
        if "traceparent" not in job.trace_headers and (trace_id := self._fallback_trace_id()):
            job.trace_headers["traceparent"] = f"00-{trace_id}-{secrets.token_hex(8)}-01"
        return call_next(job)

    def consume_transaction(self, job: Job) -> Any:
        return newrelic.agent.BackgroundTask(newrelic.agent.application(), name=job.job_type, group=self._group)

    def linking_ids(self, job: Job) -> dict:
        """Accept the message's trace headers into the transaction, then return log-correlation ids."""
        if job.trace_headers:
            with contextlib.suppress(Exception):
                newrelic.agent.accept_distributed_trace_headers(job.trace_headers, transport_type="Queue")
        try:
            metadata = newrelic.agent.get_linking_metadata()
            ids = {"trace_id": metadata.get("trace.id"), "span_id": metadata.get("span.id")}
            ids = {key: value for key, value in ids.items() if value}
        except Exception:
            ids = {}
        message_trace_id = self._message_trace_id(job)
        if message_trace_id and ids.get("trace_id") != message_trace_id:
            # The transaction did not continue the message's trace (distributed tracing disabled, agent inactive, ...).
            # Correlate logs by the message's trace id; the span id would name the producer's span, so bind none.
            return {"trace_id": message_trace_id}
        return ids

    @staticmethod
    def _fallback_trace_id() -> str | None:
        """Return the current trace id for header synthesis when the agent injected none."""
        trace_id = None
        with contextlib.suppress(Exception):
            trace_id = newrelic.agent.get_linking_metadata().get("trace.id")
        if not trace_id:
            with contextlib.suppress(Exception):
                trace_id = structlog.contextvars.get_contextvars().get("trace_id")
        if isinstance(trace_id, str) and _TRACE_ID_PATTERN.match(trace_id):
            return trace_id
        return None

    @staticmethod
    def _message_trace_id(job: Job) -> str | None:
        traceparent = job.trace_headers.get("traceparent")
        if not isinstance(traceparent, str):
            return None
        match = _TRACEPARENT_PATTERN.match(traceparent)
        return match.group(1) if match else None
