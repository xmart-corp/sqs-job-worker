import contextlib
from collections.abc import Callable
from typing import Any

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

    def consume_transaction(self, job: Job) -> Any:
        return sentry_sdk.start_transaction(sentry_sdk.continue_trace(job.trace_headers, op=self._op, name=job.job_type))

    def on_handler_error(self) -> None:
        sentry_sdk.capture_exception()
