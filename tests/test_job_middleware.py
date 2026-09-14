from unittest import TestCase, mock

from sqs_job_worker import Job, JobMiddleware


class FailingTransactionStart(JobMiddleware):
    def consume_transaction(self, job):
        raise RuntimeError("vendor unavailable")


class FailingTransactionEnter(JobMiddleware):
    def consume_transaction(self, job):
        transaction = mock.MagicMock()
        transaction.__enter__.side_effect = RuntimeError("vendor unavailable")
        return transaction


class JobMiddlewareConsumeTests(TestCase):
    def test_handler_exception_propagates_without_rerunning_the_handler(self):
        job = Job(job_type="example", payload={}, trace_headers={}, queue="default")

        for middleware in (JobMiddleware(), FailingTransactionStart(), FailingTransactionEnter()):
            with self.subTest(middleware=type(middleware).__name__):
                call_next = mock.Mock(side_effect=ValueError("handler failed"))

                with self.assertRaisesRegex(ValueError, "handler failed"):
                    middleware.consume(job, call_next)

                call_next.assert_called_once_with(job)
