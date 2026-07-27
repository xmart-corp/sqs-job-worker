import json
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class SqsClient:
    def __init__(self, url: str, boto_client) -> None:
        self.url = url
        self._boto_client = boto_client

    def send_message(
        self,
        message: dict,
        message_group_id: str | None = None,
        message_deduplication_id: str | None = None,
        message_attributes: dict | None = None,
        delay_seconds: int | None = None,
    ) -> dict:
        data: dict[str, Any] = {"MessageBody": json.dumps(message), "QueueUrl": self.url}
        if message_group_id:
            data["MessageGroupId"] = message_group_id
        if message_deduplication_id:
            data["MessageDeduplicationId"] = message_deduplication_id
        if message_attributes:
            data["MessageAttributes"] = message_attributes
        if delay_seconds is not None:
            data["DelaySeconds"] = delay_seconds
        return self._boto_client.send_message(**data)

    def receive_message(
        self,
        wait_time_seconds: int = 20,
        max_number_of_messages: int = 1,
        visibility_timeout: int | None = None,
        message_system_attribute_names: list[str] | None = None,
        message_attribute_names: list[str] | None = None,
    ) -> list[dict]:
        """Return received messages, or an empty list when the queue has none."""
        params: dict[str, Any] = {"QueueUrl": self.url, "WaitTimeSeconds": wait_time_seconds, "MaxNumberOfMessages": max_number_of_messages}
        if visibility_timeout is not None:
            params["VisibilityTimeout"] = visibility_timeout
        if message_system_attribute_names:
            params["MessageSystemAttributeNames"] = message_system_attribute_names
        if message_attribute_names:
            params["MessageAttributeNames"] = message_attribute_names
        response = self._boto_client.receive_message(**params)
        return response.get("Messages", [])

    def get_queue_attributes(self, attribute_names: list[str]) -> dict:
        response = self._boto_client.get_queue_attributes(QueueUrl=self.url, AttributeNames=attribute_names)
        return response.get("Attributes", {})

    def change_message_visibility(self, receipt_handle: str, timeout: int) -> None:
        self._boto_client.change_message_visibility(QueueUrl=self.url, ReceiptHandle=receipt_handle, VisibilityTimeout=timeout)
        logger.debug("extended message visibility", timeout=timeout)

    def delete_message(self, receipt_handle: str) -> None:
        self._boto_client.delete_message(QueueUrl=self.url, ReceiptHandle=receipt_handle)
        logger.debug("deleted message")
