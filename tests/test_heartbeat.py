import threading
from unittest import TestCase

from sqs_job_worker.heartbeat import Heartbeat


class FakeSqs:
    def __init__(self, error=None):
        self.calls = []
        self.error = error
        self.lock = threading.Lock()
        self.called = threading.Event()

    def change_message_visibility(self, receipt_handle, timeout):
        with self.lock:
            self.calls.append((receipt_handle, timeout))
        self.called.set()
        if self.error:
            raise self.error


class HeartbeatTests(TestCase):
    def test_extends_visibility_until_stopped(self):
        sqs = FakeSqs()
        heartbeat = Heartbeat(sqs, "rh", interval_seconds=0.02, visibility_timeout=300)

        heartbeat.start()
        extended = sqs.called.wait(timeout=10)
        heartbeat.stop()

        self.assertTrue(extended)
        self.assertFalse(heartbeat.failed)
        self.assertEqual(sqs.calls[0], ("rh", 300))

    def test_extension_failure_marks_the_heartbeat_failed(self):
        sqs = FakeSqs(RuntimeError("invalid receipt handle"))
        heartbeat = Heartbeat(sqs, "rh", interval_seconds=0.02)

        heartbeat.start()
        # The thread returns on its own after a failed extension.
        heartbeat._thread.join(timeout=10)
        heartbeat.stop()

        self.assertTrue(heartbeat.failed)
        self.assertEqual(len(sqs.calls), 1)

    def test_runtime_limit_stops_without_extending_visibility(self):
        sqs = FakeSqs()
        heartbeat = Heartbeat(sqs, "rh", interval_seconds=0.02, max_runtime_seconds=0)

        heartbeat.start()
        heartbeat._thread.join(timeout=10)
        heartbeat.stop()

        self.assertTrue(heartbeat.failed)
        self.assertEqual(sqs.calls, [])
