import contextlib
from collections.abc import Callable
from typing import Any

from opentelemetry import propagate, trace

from sqs_job_worker.job_middleware import Job, JobMiddleware


class OTelMiddleware(JobMiddleware):
    """Propagate and measure jobs with OpenTelemetry.

    The producer uses the configured propagator to inject headers. The consumer
    extracts them into a ``CONSUMER`` span, binds trace and span ids to logs, and
    lets handler exceptions cross the span boundary.
    """

    def __init__(self, tracer: "trace.Tracer | None" = None) -> None:
        self._tracer = tracer or trace.get_tracer("sqs_job_worker")

    def produce(self, job: Job, call_next: Callable[[Job], dict]) -> dict:
        if "traceparent" in job.trace_headers:
            raise ValueError("another middleware already wrote a traceparent header; run only one tracing middleware (OTel, New Relic, ...) per process")
        headers: dict = {}
        with contextlib.suppress(Exception):
            propagate.inject(headers)
        for key, value in headers.items():
            if key == "baggage" and job.trace_headers.get("baggage"):
                job.trace_headers["baggage"] = f"{job.trace_headers['baggage']},{value}"
            else:
                job.trace_headers[key] = value
        return call_next(job)

    def consume_transaction(self, job: Job) -> Any:
        attributes = {"messaging.system": "aws_sqs", "messaging.destination.name": job.queue, "messaging.operation.type": "process"}
        if job.message_id:
            attributes["messaging.message.id"] = job.message_id
        return self._tracer.start_as_current_span(
            job.job_type, context=propagate.extract(job.trace_headers), kind=trace.SpanKind.CONSUMER, attributes=attributes
        )

    def linking_ids(self, _job: Job) -> dict:
        return self._span_ids(trace.get_current_span())

    @staticmethod
    def _span_ids(span) -> dict:
        """Return W3C-formatted trace and span ids for log correlation."""
        try:
            span_context = span.get_span_context()
            if not span_context.is_valid:
                return {}
            return {"trace_id": format(span_context.trace_id, "032x"), "span_id": format(span_context.span_id, "016x")}
        except Exception:
            return {}
