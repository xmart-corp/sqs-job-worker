import contextlib
from collections.abc import Callable

import sentry_sdk

from sqs_job_worker.job_middleware import Job, JobMiddleware


class SentryMiddleware(JobMiddleware):
    """Propagate and measure jobs with Sentry.

    The producer gets ``sentry-trace`` and ``baggage`` from the SDK. The consumer
    continues that trace and captures handler exceptions before the worker converts
    them into retry or deletion outcomes.
    """

    def __init__(self, op: str = "queue.process") -> None:
        self._op = op

    def produce(self, job: Job, call_next: Callable[[Job], dict]) -> dict:
        if "sentry-trace" in job.trace_headers:
            raise ValueError("the job already holds a sentry-trace header; the Sentry middleware is registered twice")
        with contextlib.suppress(Exception):
            traceparent = sentry_sdk.get_traceparent()
            if traceparent:
                job.trace_headers["sentry-trace"] = traceparent
            baggage = sentry_sdk.get_baggage()
            if baggage:
                existing = job.trace_headers.get("baggage")
                job.trace_headers["baggage"] = f"{existing},{baggage}" if existing else baggage
        return call_next(job)

    def consume(self, job: Job, call_next: Callable[[Job], None]) -> None:
        try:
            transaction = sentry_sdk.continue_trace(job.trace_headers, op=self._op, name=job.job_type)
            started = sentry_sdk.start_transaction(transaction)
            started.__enter__()
        except Exception:
            return call_next(job)
        try:
            call_next(job)
        except BaseException as error:
            if isinstance(error, Exception):
                with contextlib.suppress(Exception):
                    sentry_sdk.capture_exception()
            with contextlib.suppress(Exception):
                started.__exit__(type(error), error, error.__traceback__)
            raise
        else:
            with contextlib.suppress(Exception):
                started.__exit__(None, None, None)
