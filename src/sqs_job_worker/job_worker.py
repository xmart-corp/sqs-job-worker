import random
import time
from collections.abc import Callable

import structlog

from sqs_job_worker.job_middleware import JobMiddleware
from sqs_job_worker.job_processor import JobProcessor
from sqs_job_worker.queue_group import QueueGroup
from sqs_job_worker.sqs_queue import SqsQueue

logger = structlog.get_logger(__name__)


class JobWorker:
    """Poll multiple SQS queues and process one message at a time.

    Each cycle uses a fresh weighted random permutation. Queues with larger weights
    tend to be polled first, while every queue remains starvation-free. All queues are
    short-polled before the first selected queue is long-polled when none has work.
    The middleware polling phase wraps each cycle before any message is received.
    Delivery is at least once, so handlers must be idempotent.
    """

    def __init__(
        self, queues: QueueGroup, handlers: dict[str, Callable[[dict], None]], should_stop: Callable[[], bool] | None = None, *, idle_wait_seconds: int = 20
    ) -> None:
        self._queues = queues
        if not 0 <= idle_wait_seconds <= 20:
            raise ValueError(f"idle_wait_seconds must be within SQS's long-poll range 0-20, got {idle_wait_seconds}")
        self._handlers = dict(handlers)
        self._should_stop = should_stop or (lambda: False)
        self._processor = JobProcessor(self._handlers, queues.middleware)
        self._poll = JobMiddleware.compose_poll(queues.middleware, self._poll_cycle)
        self._idle_wait_seconds = idle_wait_seconds
        self._consecutive_errors = 0
        self._attempted = 0
        self._stop_requested = False

    def run(self) -> None:
        for queue in self._queues.values():
            queue.resolve_visibility()
        logger.info("worker_started", queues=self._queues.names, job_types=list(self._handlers))
        started_at = time.monotonic()
        while not (self._stop_requested or self._should_stop()):
            self._poll()

        uptime = time.monotonic() - started_at
        logger.info("worker_stopped", uptime_seconds=round(uptime), attempted=self._attempted)

    def request_stop(self) -> None:
        self._stop_requested = True

    def _poll_cycle(self) -> None:
        ordered = self._queues.polling_order()
        for queue_name, queue in ordered:
            if self._receive_and_process(queue_name, queue, wait_seconds=0):
                return
        queue_name, queue = ordered[0]
        self._receive_and_process(queue_name, queue, wait_seconds=self._idle_wait_seconds)

    def _receive_and_process(self, queue_name: str, queue: SqsQueue, wait_seconds: int) -> bool:
        """Receive once, process the result, and report whether this cycle should stop."""
        try:
            message = queue.receive_message(wait_seconds)
        except Exception as e:
            self._handle_receive_error(queue_name, e)
            return False
        self._consecutive_errors = 0
        if message is None:
            return self._stop_requested
        with structlog.contextvars.bound_contextvars(queue=queue_name, message_id=message["MessageId"]):
            if self._stop_requested:
                receipt_handle = message.get("ReceiptHandle")
                if receipt_handle is not None:
                    queue.release_message(receipt_handle)
                return True
            try:
                self._processor.process(queue_name, queue, message)
            except Exception as e:
                logger.error(f"unexpected error while processing message: {e}", exc_info=True)
            self._attempted += 1
        return True

    def _handle_receive_error(self, queue_name: str, error: Exception) -> None:
        self._consecutive_errors += 1
        logger.warning(f"failed to receive messages: {error}", queue=queue_name, attempt=self._consecutive_errors, exc_info=True)
        if self._consecutive_errors >= 10:
            logger.error(f"receive failed {self._consecutive_errors} times in a row; exiting to let the supervisor restart the worker")
            raise SystemExit(1)
        ceiling = min(30, 2 ** (self._consecutive_errors - 1))
        time.sleep(random.uniform(0, ceiling))  # nosec B311: backoff jitter
