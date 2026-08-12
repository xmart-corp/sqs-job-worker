from urllib.parse import unquote, urlsplit

import structlog

from sqs_job_worker.heartbeat import Heartbeat
from sqs_job_worker.sqs_client import SqsClient
from sqs_job_worker.sqs_message_attributes import SqsMessageAttributes

logger = structlog.get_logger(__name__)
job_logger = structlog.get_logger("sqs_job_worker.job")


class SqsQueue:
    """A single SQS queue with producer and consumer operations and configuration."""

    def __init__(self, url: str, *, boto_client, visibility_timeout: int | None = None, max_runtime_seconds: float = 3600) -> None:
        if not url:
            raise ValueError(f"url must be a non-empty string, got {url!r}")
        self.url = url
        self.name = unquote(urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1])
        self._client = SqsClient(url, boto_client)
        self.visibility_timeout = visibility_timeout
        self.max_runtime_seconds = max_runtime_seconds

    def enqueue(
        self,
        job_type: str,
        payload: dict | None = None,
        *,
        correlation_fields: dict | None = None,
        trace_headers: dict | None = None,
        message_group_id: str | None = None,
        message_deduplication_id: str | None = None,
        message_attributes: dict | None = None,
        delay_seconds: int | None = None,
    ) -> dict:
        payload = payload or {}
        message = {"job_type": job_type, "payload": payload}
        attributes, dropped = SqsMessageAttributes.build(
            trace_headers=trace_headers or {}, correlation_fields=correlation_fields or {}, caller_attributes=message_attributes
        )
        if dropped:
            job_logger.warning("propagation_attributes_dropped", queue_name=self.name, dropped=dropped)
        return self._client.send_message(
            message,
            message_group_id=message_group_id,
            message_deduplication_id=message_deduplication_id,
            message_attributes=attributes or None,
            delay_seconds=delay_seconds,
        )

    def receive_message(self, wait_seconds: int) -> dict | None:
        """Receive one message, or return None when the queue is empty."""
        messages = self._client.receive_message(
            wait_time_seconds=wait_seconds,
            max_number_of_messages=1,
            visibility_timeout=self.visibility_timeout,
            message_system_attribute_names=["ApproximateReceiveCount", "SentTimestamp", "MessageGroupId"],
            message_attribute_names=["All"],
        )
        return messages[0] if messages else None

    def resolve_visibility(self) -> None:
        if self.visibility_timeout is None:
            attributes = self._client.get_queue_attributes(["VisibilityTimeout"])
            self.visibility_timeout = int(attributes["VisibilityTimeout"])
            logger.info("resolved visibility timeout from queue", queue_name=self.name, visibility_timeout=self.visibility_timeout)

    def start_heartbeat(self, receipt_handle: str) -> Heartbeat:
        visibility_timeout = self.visibility_timeout
        if visibility_timeout is None:
            raise RuntimeError("visibility timeout must be resolved before starting a heartbeat")
        heartbeat = Heartbeat(
            self._client,
            receipt_handle,
            interval_seconds=max(1, visibility_timeout) / 3,
            visibility_timeout=visibility_timeout,
            max_runtime_seconds=self.max_runtime_seconds,
        )
        # A zero visibility timeout keeps messages visible, so there is no visibility window to extend.
        if visibility_timeout > 0:
            heartbeat.start()
        return heartbeat

    def release_message(self, receipt_handle: str) -> None:
        """Best-effort release of an unprocessed message for immediate redelivery."""
        try:
            self._client.change_message_visibility(receipt_handle, 0)
        except Exception as e:
            logger.warning(f"failed to release message during shutdown: {e}", queue_name=self.name, exc_info=True)

    def defer_message(self, receipt_handle: str, visibility_timeout: int) -> None:
        """Best-effort defer a message for later redelivery."""
        try:
            self._client.change_message_visibility(receipt_handle, visibility_timeout)
        except Exception as e:
            logger.warning(f"failed to defer message for redelivery: {e}", queue_name=self.name, visibility_timeout=visibility_timeout, exc_info=True)

    def delete_message(self, receipt_handle: str) -> bool:
        """Best-effort message deletion, returning whether the request succeeded."""
        try:
            self._client.delete_message(receipt_handle)
        except Exception as e:
            logger.error(f"failed to delete message: {e}", queue_name=self.name, exc_info=True)
            return False
        return True
