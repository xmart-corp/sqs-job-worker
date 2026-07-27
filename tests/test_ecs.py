from unittest import TestCase, mock

from sqs_job_worker.contrib.ecs import EcsDrainMiddleware

FAMILY = "example-worker"


def _arn(family, revision):
    return f"arn:aws:ecs:ap-northeast-1:123456789012:task-definition/{family}:{revision}"


class FakeEcs:
    def __init__(self, tasks=(), error=None):
        self.tasks = tasks
        self.error = error

    def list_tasks(self, **kwargs):
        if self.error:
            raise self.error
        return {"taskArns": [_arn(family, revision) for revision, _, family in self.tasks]}

    def describe_tasks(self, **kwargs):
        return {"tasks": [{"taskDefinitionArn": _arn(family, revision), "lastStatus": status} for revision, status, family in self.tasks]}


def _middleware(tasks=(), error=None):
    return EcsDrainMiddleware(ecs_client=FakeEcs(tasks, error), cluster="cluster", family=FAMILY, revision=26, recheck_interval_seconds=1)


class EcsDrainMiddlewareTests(TestCase):
    def test_drains_only_for_a_newer_running_revision_in_the_same_family(self):
        cases = [
            ([(27, "RUNNING", FAMILY)], True),
            ([(27, "STOPPED", FAMILY)], False),
            ([(99, "RUNNING", "other-worker")], False),
            ([(26, "RUNNING", FAMILY)], False),
        ]

        for tasks, expected in cases:
            with self.subTest(tasks=tasks), mock.patch("sqs_job_worker.contrib.ecs.time.sleep"):
                call_next = mock.Mock()
                _middleware(tasks).poll(call_next)
                self.assertEqual(call_next.called, not expected)

    def test_ecs_errors_fail_open(self):
        call_next = mock.Mock()
        _middleware(error=RuntimeError("throttled")).poll(call_next)
        call_next.assert_called_once_with()

    def test_transient_metadata_failure_is_retried_on_a_later_poll(self):
        middleware = EcsDrainMiddleware(ecs_client=FakeEcs([(27, "RUNNING", FAMILY)]), cluster="cluster", recheck_interval_seconds=1)
        fetches = [None, {"cluster": "cluster", "family": FAMILY, "revision": 26}]
        with (
            mock.patch.object(middleware, "_fetch_metadata", side_effect=fetches),
            mock.patch.object(EcsDrainMiddleware, "MIN_QUERY_INTERVAL_SECONDS", 0),
            mock.patch("sqs_job_worker.contrib.ecs.time.sleep"),
        ):
            before_resolution, after_resolution = mock.Mock(), mock.Mock()
            middleware.poll(before_resolution)
            middleware.poll(after_resolution)

        before_resolution.assert_called_once_with()
        after_resolution.assert_not_called()

    def test_missing_cluster_is_resolved_from_task_metadata(self):
        ecs = mock.Mock()
        ecs.list_tasks.return_value = {"taskArns": []}
        middleware = EcsDrainMiddleware(ecs_client=ecs, family=FAMILY, revision=26)

        with mock.patch.object(middleware, "_fetch_metadata", return_value={"cluster": "production", "family": FAMILY, "revision": 26}) as fetch_metadata:
            middleware.poll(mock.Mock())

        fetch_metadata.assert_called_once_with()
        ecs.list_tasks.assert_called_once_with(family=FAMILY, desiredStatus="RUNNING", cluster="production")

    def test_zero_recheck_interval_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "recheck_interval_seconds must be positive"):
            EcsDrainMiddleware(ecs_client=mock.Mock(), recheck_interval_seconds=0)
