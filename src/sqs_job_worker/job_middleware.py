import contextlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import structlog


@dataclass
class Job:
    """A job as seen by producer and consumer middleware.

    Producers leave ``message_id`` and ``receive_count`` unset. Consumers populate
    them from SQS. Trace headers stay opaque to the core and correlation fields are
    preserved without filtering reserved log keys.
    """

    job_type: str
    payload: dict
    trace_headers: dict
    queue: str
    correlation_fields: dict = field(default_factory=dict)
    message_id: str | None = None
    receive_count: int | None = None


class JobMiddleware:
    """Wrap job production, queue polling, consumption, or any combination."""

    def produce(self, job: Job, call_next: Callable[[Job], dict]) -> dict:
        return call_next(job)

    def poll(self, call_next: Callable[[], None]) -> None:
        return call_next()

    def consume(self, job: Job, call_next: Callable[[Job], None]) -> None:
        try:
            transaction = self.consume_transaction(job)
            if transaction is None:
                return call_next(job)
            transaction.__enter__()
        except Exception:
            return call_next(job)
        try:
            with contextlib.suppress(Exception):
                if ids := self.linking_ids(job):
                    structlog.contextvars.bind_contextvars(**ids)
            call_next(job)
        except BaseException as error:
            if isinstance(error, Exception):
                with contextlib.suppress(Exception):
                    self.on_handler_error()
            with contextlib.suppress(Exception):
                transaction.__exit__(type(error), error, error.__traceback__)
            raise
        else:
            with contextlib.suppress(Exception):
                transaction.__exit__(None, None, None)

    def consume_transaction(self, job: Job, /) -> Any:
        """Return a vendor context manager to wrap consumption in, or None to skip tracing."""

    def linking_ids(self, job: Job, /) -> dict | None:
        """Return ids to bind to logs while the job runs, inside the started transaction."""

    def on_handler_error(self) -> None:
        """Report the active handler exception (``Exception`` only) to the vendor."""

    @classmethod
    def compose_producer(cls, middleware: Sequence["JobMiddleware"], terminal: Callable[[Job], dict]) -> Callable[[Job], dict]:
        def wrap(item: JobMiddleware, call_next: Callable[[Job], dict]) -> Callable[[Job], dict]:
            def call(job: Job) -> dict:
                return item.produce(job, call_next)

            return call

        return cls._compose(middleware, wrap, terminal)

    @classmethod
    def compose_consumer(cls, middleware: Sequence["JobMiddleware"], terminal: Callable[[Job], None]) -> Callable[[Job], None]:
        def wrap(item: JobMiddleware, call_next: Callable[[Job], None]) -> Callable[[Job], None]:
            def call(job: Job) -> None:
                return item.consume(job, call_next)

            return call

        return cls._compose(middleware, wrap, terminal)

    @classmethod
    def compose_poll(cls, middleware: Sequence["JobMiddleware"], terminal: Callable[[], None]) -> Callable[[], None]:
        def wrap(item: JobMiddleware, call_next: Callable[[], None]) -> Callable[[], None]:
            def call() -> None:
                return item.poll(call_next)

            return call

        return cls._compose(middleware, wrap, terminal)

    @staticmethod
    def _compose(middleware: Sequence["JobMiddleware"], wrap: Callable, terminal: Callable) -> Callable:
        chain = terminal
        for item in reversed(middleware):
            chain = wrap(item, chain)
        return chain
