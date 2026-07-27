import random
import signal
from collections.abc import Callable, Sequence

import structlog

from sqs_job_worker.job_middleware import Job, JobMiddleware
from sqs_job_worker.sqs_queue import SqsQueue

logger = structlog.get_logger(__name__)
job_logger = structlog.get_logger("sqs_job_worker.job")


class QueueGroup:
    """Manage named SQS queues and the relative weights used to poll them."""

    def __init__(self, queues: dict[str, dict[str, object]], *, middleware: Sequence[JobMiddleware] = ()) -> None:
        if not isinstance(queues, dict):
            raise TypeError(f"queues must be a dict, got {type(queues).__name__}")
        if not queues:
            raise ValueError("at least one queue is required")

        allowed_keys = {"queue", "weight"}
        configured_queues: dict[str, tuple[SqsQueue, int]] = {}
        for name, definition in queues.items():
            if not isinstance(name, str) or not name:
                raise ValueError(f"queue name must be a non-empty string, got {name!r}")
            if not isinstance(definition, dict):
                raise TypeError(f"queue definition for {name!r} must be a dict, got {type(definition).__name__}")

            unknown_keys = [key for key in definition if key not in allowed_keys]
            if unknown_keys:
                raise ValueError(f"queue definition for {name!r} contains unknown keys: {unknown_keys!r}")
            if "queue" not in definition:
                raise ValueError(f"queue definition for {name!r} must contain 'queue'")

            queue = definition["queue"]
            if not isinstance(queue, SqsQueue):
                raise TypeError(f"queue for {name!r} must be an SqsQueue, got {type(queue).__name__}")
            weight = definition.get("weight", 1)
            if isinstance(weight, bool) or not isinstance(weight, int) or weight <= 0:
                raise ValueError(f"polling weight for {name!r} must be a positive integer, got {weight!r}")
            configured_queues[name] = (queue, weight)

        self._queues = configured_queues
        self._middleware = tuple(middleware)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._queues)

    @property
    def middleware(self) -> tuple[JobMiddleware, ...]:
        return self._middleware

    def items(self) -> tuple[tuple[str, SqsQueue], ...]:
        return tuple((name, queue) for name, (queue, _weight) in self._queues.items())

    def values(self) -> tuple[SqsQueue, ...]:
        return tuple(queue for queue, _weight in self._queues.values())

    def polling_order(self) -> list[tuple[str, SqsQueue]]:
        """Return every queue in a starvation-free weighted random order."""
        ordered = sorted(
            self._queues.items(),
            key=lambda item: random.expovariate(item[1][1]),  # nosec B311: polling lottery
        )
        return [(name, queue) for name, (queue, _weight) in ordered]

    def enqueue(
        self,
        queue_name: str,
        job_type: str,
        payload: dict | None = None,
        *,
        correlation_fields: dict | None = None,
        message_group_id: str | None = None,
        message_deduplication_id: str | None = None,
        message_attributes: dict | None = None,
        delay_seconds: int | None = None,
    ) -> dict:
        if queue_name not in self._queues:
            raise KeyError(f"unknown queue: {queue_name!r}") from None
        job = Job(job_type=job_type, payload=payload or {}, trace_headers={}, queue=queue_name, correlation_fields=dict(correlation_fields or {}))

        def send(produced_job: Job) -> dict:
            try:
                target_queue = self._queues[produced_job.queue][0]
            except KeyError:
                raise KeyError(f"unknown queue: {produced_job.queue!r}") from None
            response = target_queue.enqueue(
                produced_job.job_type,
                produced_job.payload,
                correlation_fields=produced_job.correlation_fields,
                trace_headers=produced_job.trace_headers,
                message_group_id=message_group_id,
                message_deduplication_id=message_deduplication_id,
                message_attributes=message_attributes,
                delay_seconds=delay_seconds,
            )
            fields = {"queue": produced_job.queue, "job_type": produced_job.job_type, "message_id": response.get("MessageId"), "group_id": message_group_id}
            job_logger.info("job_enqueued", **{name: value for name, value in fields.items() if value is not None})
            return response

        return JobMiddleware.compose_producer(self.middleware, send)(job)

    def run_worker(self, handlers: dict[str, Callable[[dict], None]], *, idle_wait_seconds: int = 20) -> None:
        """Run a worker with SIGTERM and SIGINT handling for graceful shutdown."""
        from sqs_job_worker.job_worker import JobWorker

        if not handlers:
            raise ValueError("at least one handler is required")
        worker = JobWorker(self, handlers, idle_wait_seconds=idle_wait_seconds)

        def request_stop(signum, _frame):
            logger.info(f"received signal {signum}; starting graceful shutdown (send again to force quit)")
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.signal(signal.SIGINT, signal.default_int_handler)
            worker.request_stop()

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        worker.run()

    @classmethod
    def build(cls, *, boto_client, queues: dict[str, dict[str, object]], middleware: Sequence[JobMiddleware] = ()) -> "QueueGroup":
        """Build a group from logical names and physical SQS queue names or URLs."""
        if not isinstance(queues, dict):
            raise TypeError(f"queues must be a dict, got {type(queues).__name__}")
        if not queues:
            raise ValueError("at least one queue is required")

        allowed_keys = {"name", "url", "weight"}
        resolved_queues: dict[str, dict[str, object]] = {}
        for logical_name, definition in queues.items():
            if not isinstance(logical_name, str) or not logical_name:
                raise ValueError(f"queue name must be a non-empty string, got {logical_name!r}")
            if not isinstance(definition, dict):
                raise TypeError(f"queue definition for {logical_name!r} must be a dict, got {type(definition).__name__}")

            unknown_keys = [key for key in definition if key not in allowed_keys]
            if unknown_keys:
                raise ValueError(f"queue definition for {logical_name!r} contains unknown keys: {unknown_keys!r}")
            has_name = "name" in definition
            has_url = "url" in definition
            if has_name == has_url:
                raise ValueError(f"queue definition for {logical_name!r} must contain exactly one of 'name' or 'url'")

            if has_name:
                physical_name = definition["name"]
                if not isinstance(physical_name, str) or not physical_name:
                    raise ValueError(f"physical queue name for {logical_name!r} must be a non-empty string, got {physical_name!r}")
                url = boto_client.get_queue_url(QueueName=physical_name)["QueueUrl"]
            else:
                url = definition["url"]
                if not isinstance(url, str) or not url:
                    raise ValueError(f"queue URL for {logical_name!r} must be a non-empty string, got {url!r}")

            resolved_queues[logical_name] = {"queue": SqsQueue(url, boto_client=boto_client), "weight": definition.get("weight", 1)}

        return cls(resolved_queues, middleware=middleware)
