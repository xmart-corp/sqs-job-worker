import contextvars
import threading
import time

import structlog

from sqs_job_worker.sqs_client import SqsClient

logger = structlog.get_logger(__name__)


class Heartbeat:
    """Extend SQS visibility from a dedicated thread while a job runs.

    The thread may only use the shared SQS client; it must not touch ORM or
    database connections.
    """

    def __init__(
        self, sqs: SqsClient, receipt_handle: str, interval_seconds: float = 90, visibility_timeout: int = 300, max_runtime_seconds: float = 3600
    ) -> None:
        self._sqs = sqs
        self._receipt_handle = receipt_handle
        self._interval = interval_seconds
        self._visibility_timeout = visibility_timeout
        self._max_runtime = max_runtime_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started_at: float = 0.0
        self.failed = False

    def start(self) -> None:
        # A monotonic clock is immune to NTP corrections and timezone changes.
        self._started_at = time.monotonic()
        context = contextvars.copy_context()
        self._thread = threading.Thread(target=context.run, args=(self._run,), name="sqs-heartbeat", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            elapsed = time.monotonic() - self._started_at
            if elapsed > self._max_runtime:
                # Python cannot stop the handler, so let SQS make the message visible again.
                logger.error(f"heartbeat stopped: job exceeded max runtime ({self._max_runtime}s)")
                self.failed = True
                return
            try:
                self._sqs.change_message_visibility(self._receipt_handle, self._visibility_timeout)
            except Exception as e:
                # The message may already be visible again, so prevent its deletion.
                logger.warning(f"heartbeat stopped: failed to extend visibility timeout: {e}", exc_info=True)
                self.failed = True
                return

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)
            if self._thread.is_alive():
                self.failed = True
                logger.error("heartbeat thread join timed out")
