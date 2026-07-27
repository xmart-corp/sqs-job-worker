import contextvars
import json
import time
from collections.abc import Callable, Sequence

import structlog

from sqs_job_worker.job_middleware import Job, JobMiddleware
from sqs_job_worker.sqs_message_attributes import SqsMessageAttributes
from sqs_job_worker.sqs_queue import SqsQueue

logger = structlog.get_logger(__name__)
job_logger = structlog.get_logger("sqs_job_worker.job")


class NonRetryableError(Exception):
    """Signal that retrying a job cannot succeed and its message should be deleted."""


class JobProcessor:
    """Run consumer middleware and handlers with logging and heartbeat protection."""

    def __init__(self, handlers: dict[str, Callable[[dict], None]], middleware: Sequence[JobMiddleware] = ()) -> None:
        self._handlers = handlers
        self._middleware = middleware

    def process(self, queue_name: str, queue: SqsQueue, message: dict) -> None:
        attributes = message["Attributes"]
        message_id = message.get("MessageId")
        receive_count = int(attributes["ApproximateReceiveCount"])

        def process_in_job_context() -> None:
            structlog.contextvars.bind_contextvars(receive_count=receive_count)
            self._process_bound(queue_name, queue, message, attributes, message_id, receive_count)

        contextvars.copy_context().run(process_in_job_context)

    def _process_bound(self, queue_name: str, queue: SqsQueue, message: dict, attributes: dict, message_id: str | None, receive_count: int) -> None:
        receipt_handle = message["ReceiptHandle"]
        group_id = attributes.get("MessageGroupId")
        sent_timestamp_ms = int(attributes["SentTimestamp"])
        queue_wait_ms = max(0, int(time.time() * 1000) - sent_timestamp_ms)
        correlation_fields, trace_headers = SqsMessageAttributes.parse(message)
        try:
            envelope = json.loads(message.get("Body", ""))
            job_type = envelope["job_type"]
            payload = envelope["payload"]
        except (ValueError, KeyError, TypeError) as e:
            logger.error(f"deleting message: failed to parse body: {e}")
            queue.delete_message(receipt_handle)
            return

        if not isinstance(job_type, str):
            logger.error(f"deleting message: job_type is not a string job_type={job_type!r}")
            queue.delete_message(receipt_handle)
            return

        if not isinstance(payload, dict):
            logger.error(f"deleting message: payload is not a dict payload={payload!r}")
            queue.delete_message(receipt_handle)
            return

        if job_type not in self._handlers:
            logger.warning("unknown job_type; leaving for redelivery", job_type=job_type)
            return

        # Do not let application correlation fields overwrite worker-bound log fields.
        bound = {
            key: value for key, value in correlation_fields.items() if key not in {"queue", "message_id", "receive_count", "job_type", "trace_id", "span_id"}
        }
        bound["job_type"] = job_type
        structlog.contextvars.bind_contextvars(**bound)

        job = Job(
            job_type=job_type,
            payload=payload,
            trace_headers=trace_headers,
            queue=queue_name,
            correlation_fields=correlation_fields,
            message_id=message_id,
            receive_count=receive_count,
        )
        self._run(queue, receipt_handle, group_id, queue_wait_ms, job)

    def _run(self, queue: SqsQueue, receipt_handle: str, group_id: str | None, queue_wait_ms: int, job: Job) -> None:
        started_at = time.monotonic()
        heartbeat = queue.start_heartbeat(receipt_handle)
        outcome = "retry"
        try:
            self._compose_chain(group_id, queue_wait_ms)(job)
            outcome = "success"
        except NonRetryableError as e:
            logger.error(f"non-retryable error; marking message for deletion: {e}", exc_info=True)
            outcome = "delete"
        except Exception as e:
            logger.error(f"unexpected error in handler; leaving for redelivery: {e}", exc_info=True)
        finally:
            heartbeat.stop()

        duration_ms = round((time.monotonic() - started_at) * 1000, 2)

        if heartbeat.failed:
            outcome = "heartbeat_failed"

        processing_outcome = outcome
        if outcome in ("success", "delete") and not queue.delete_message(receipt_handle):
            outcome = "delete_failed"

        level = {"success": "info", "retry": "warning"}.get(outcome, "error")
        fields = {"outcome": outcome, "duration_ms": duration_ms}
        if outcome == "delete_failed":
            fields["processing_outcome"] = processing_outcome
        getattr(job_logger, level)("job_finished", **fields)

    def _compose_chain(self, group_id: str | None, queue_wait_ms: int) -> Callable[[Job], None]:
        def start_and_dispatch(job: Job) -> None:
            # Keep job_started inside the APM transaction so it includes middleware-bound trace IDs.
            # Middleware failures before this terminal emit job_finished without job_started.
            started_fields = {"group_id": group_id, "queue_wait_ms": queue_wait_ms}
            job_logger.info("job_started", **{key: value for key, value in started_fields.items() if value is not None})
            self._handlers[job.job_type](job.payload)

        return JobMiddleware.compose_consumer(self._middleware, start_and_dispatch)
