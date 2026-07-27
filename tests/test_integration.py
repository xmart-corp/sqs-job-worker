import json
import os
from unittest import TestCase, mock

import boto3
import structlog
from moto import mock_aws

from sqs_job_worker import JobMiddleware, JobWorker, NonRetryableError, QueueGroup, SqsClient, SqsQueue
from sqs_job_worker.contrib.newrelic import NewRelicMiddleware
from sqs_job_worker.contrib.otel import OTelMiddleware
from sqs_job_worker.contrib.sentry import SentryMiddleware

os.environ.setdefault("AWS_DEFAULT_REGION", "ap-northeast-1")
REGION = "ap-northeast-1"


class _FailedHeartbeat:
    failed = True

    def __init__(self, sqs, receipt_handle, **kwargs):
        pass

    def start(self):
        pass

    def stop(self):
        pass


def _stop_after(count):
    calls = 0

    def should_stop():
        nonlocal calls
        calls += 1
        return calls > count

    return should_stop


def _queue(client, name, *, fifo=False, visibility_timeout=None):
    attributes = {}
    if fifo:
        attributes.update({"FifoQueue": "true", "ContentBasedDeduplication": "true"})
    if visibility_timeout is not None:
        attributes["VisibilityTimeout"] = str(visibility_timeout)
    return client.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]


def _remaining(client, url):
    response = client.receive_message(QueueUrl=url, MaxNumberOfMessages=10, VisibilityTimeout=0, WaitTimeSeconds=0)
    return response.get("Messages", [])


def _worker(queues, handlers, *, should_stop=None):
    return JobWorker(queues, handlers, should_stop=should_stop or _stop_after(1), idle_wait_seconds=0)


class QueueIntegrationTests(TestCase):
    def setUp(self):
        structlog.contextvars.clear_contextvars()
        self.addCleanup(structlog.contextvars.clear_contextvars)

    def test_enqueue_process_context_and_delete_round_trip(self):
        with mock_aws():
            client = boto3.client("sqs", region_name=REGION)
            url = _queue(client, "jobs-standard")
            jobs = []

            class CaptureMiddleware(JobMiddleware):
                def produce(self, job, call_next):
                    job.trace_headers["traceparent"] = "00-trace"
                    job.correlation_fields.setdefault("tenant_id", "t-1")
                    return call_next(job)

                def consume(self, job, call_next):
                    jobs.append(job)
                    return call_next(job)

            queues = QueueGroup({"default": {"queue": SqsQueue(url, boto_client=client, visibility_timeout=0)}}, middleware=[CaptureMiddleware()])
            queues.enqueue("default", "greet", {"name": "Ada"}, correlation_fields={"request_id": "r-1"})

            seen = {}

            def handler(payload):
                seen.update(payload=payload, context=structlog.contextvars.get_contextvars())

            _worker(queues, {"greet": handler}).run()

            self.assertEqual(seen["payload"], {"name": "Ada"})
            self.assertEqual(seen["context"]["request_id"], "r-1")
            self.assertEqual(seen["context"]["tenant_id"], "t-1")
            self.assertEqual(seen["context"]["queue"], "default")
            self.assertEqual(jobs[0].trace_headers, {"traceparent": "00-trace"})
            self.assertEqual(jobs[0].correlation_fields, {"request_id": "r-1", "tenant_id": "t-1"})
            self.assertEqual(_remaining(client, url), [])

    def test_middleware_may_implement_only_one_phase(self):
        with mock_aws():
            client = boto3.client("sqs", region_name=REGION)
            url = _queue(client, "jobs-standard")
            events = []

            class ProducerOnly(JobMiddleware):
                def produce(self, job, call_next):
                    events.append("produce")
                    job.correlation_fields["from_producer"] = True
                    return call_next(job)

            class ConsumerOnly(JobMiddleware):
                def consume(self, job, call_next):
                    events.append(("consume", job.correlation_fields["from_producer"]))
                    return call_next(job)

            class PollOnly(JobMiddleware):
                def poll(self, call_next):
                    events.append("poll")
                    return call_next()

            queues = QueueGroup(
                {"default": {"queue": SqsQueue(url, boto_client=client, visibility_timeout=0)}}, middleware=[ProducerOnly(), PollOnly(), ConsumerOnly()]
            )
            queues.enqueue("default", "greet")

            _worker(queues, {"greet": lambda payload: None}).run()

            self.assertEqual(events, ["produce", "poll", ("consume", True)])

    def test_middleware_order_is_rack_style_in_all_phases(self):
        with mock_aws():
            client = boto3.client("sqs", region_name=REGION)
            url = _queue(client, "jobs-standard")
            events = []

            class RecordingMiddleware(JobMiddleware):
                def __init__(self, name):
                    self.name = name

                def produce(self, job, call_next):
                    events.append(f"produce:{self.name}:before")
                    response = call_next(job)
                    events.append(f"produce:{self.name}:after")
                    return response

                def poll(self, call_next):
                    events.append(f"poll:{self.name}:before")
                    call_next()
                    events.append(f"poll:{self.name}:after")

                def consume(self, job, call_next):
                    events.append(f"consume:{self.name}:before")
                    call_next(job)
                    events.append(f"consume:{self.name}:after")

            queues = QueueGroup(
                {"default": {"queue": SqsQueue(url, boto_client=client, visibility_timeout=0)}},
                middleware=[RecordingMiddleware("outer"), RecordingMiddleware("inner")],
            )
            queues.enqueue("default", "greet")
            _worker(queues, {"greet": lambda payload: None}).run()

            self.assertEqual(
                events,
                [
                    "produce:outer:before",
                    "produce:inner:before",
                    "produce:inner:after",
                    "produce:outer:after",
                    "poll:outer:before",
                    "poll:inner:before",
                    "consume:outer:before",
                    "consume:inner:before",
                    "consume:inner:after",
                    "consume:outer:after",
                    "poll:inner:after",
                    "poll:outer:after",
                ],
            )

    def test_direct_queue_definitions_keep_queue_and_weight_together(self):
        first = SqsQueue("https://example.com/first", boto_client=mock.Mock())
        second = SqsQueue("https://example.com/second", boto_client=mock.Mock())
        queues = QueueGroup({"first": {"queue": first, "weight": 3}, "second": {"queue": second}})

        with mock.patch("sqs_job_worker.queue_group.random.expovariate", side_effect=[0.2, 0.1]) as lottery:
            ordered = queues.polling_order()

        self.assertEqual(queues.items(), (("first", first), ("second", second)))
        self.assertEqual(ordered, [("second", second), ("first", first)])
        self.assertEqual(lottery.call_args_list, [mock.call(3), mock.call(1)])

        with self.assertRaisesRegex(ValueError, "must be a positive integer"):
            QueueGroup({"default": {"queue": first, "weight": 0}})

    def test_fifo_jobs_from_multiple_queues_are_all_processed(self):
        with mock_aws():
            client = boto3.client("sqs", region_name=REGION)
            first = _queue(client, "first.fifo", fifo=True)
            second = _queue(client, "second.fifo", fifo=True)
            queues = QueueGroup.build(boto_client=client, queues={"first": {"name": "first.fifo", "weight": 3}, "second": {"url": second}})
            queues.enqueue("first", "greet", {"queue": "first"}, message_group_id="g1")
            queues.enqueue("second", "greet", {"queue": "second"}, message_group_id="g1")

            received = []
            handlers = {"greet": lambda payload: received.append(payload["queue"])}
            _worker(queues, handlers, should_stop=_stop_after(4)).run()

            self.assertEqual(sorted(received), ["first", "second"])
            self.assertEqual(_remaining(client, first), [])
            self.assertEqual(_remaining(client, second), [])

    def test_delay_seconds_is_honored_by_sqs(self):
        with mock_aws():
            client = boto3.client("sqs", region_name=REGION)
            url = _queue(client, "jobs-standard")
            queues = QueueGroup({"default": {"queue": SqsQueue(url, boto_client=client)}})
            queues.enqueue("default", "later", delay_seconds=900)
            queues.enqueue("default", "now")

            messages = SqsClient(url, client).receive_message(wait_time_seconds=0, max_number_of_messages=10)

            self.assertEqual(len(messages), 1)
            self.assertEqual(json.loads(messages[0]["Body"])["job_type"], "now")

    def test_propagation_attributes_are_dropped_only_beyond_the_sqs_limit(self):
        trace_headers = {"traceparent": "00-trace", "tracestate": "vendor=1", "baggage": "k=v", "newrelic": "payload"}

        class TraceHeadersMiddleware(JobMiddleware):
            def produce(self, job, call_next):
                job.trace_headers.update(trace_headers)
                return call_next(job)

        cases = [
            (5, {"correlation_fields", "traceparent", "tracestate", "baggage", "newrelic"}),
            (6, {"correlation_fields", "traceparent", "tracestate", "baggage"}),
        ]

        for caller_count, expected_propagation in cases:
            with self.subTest(caller_count=caller_count), mock_aws():
                client = boto3.client("sqs", region_name=REGION)
                url = _queue(client, "jobs-standard")
                queues = QueueGroup({"default": {"queue": SqsQueue(url, boto_client=client)}}, middleware=[TraceHeadersMiddleware()])
                caller_attributes = {f"attr_{i}": {"DataType": "String", "StringValue": str(i)} for i in range(caller_count)}

                queues.enqueue("default", "greet", correlation_fields={"user_id": "u-1"}, message_attributes=caller_attributes)

                message = SqsClient(url, client).receive_message(wait_time_seconds=0, message_attribute_names=["All"])[0]
                attributes = message["MessageAttributes"]
                self.assertEqual(set(attributes), expected_propagation | set(caller_attributes))
                self.assertEqual(json.loads(attributes["correlation_fields"]["StringValue"]), {"user_id": "u-1"})

    def test_exceptions_are_retried_and_permanent_failures_are_deleted(self):
        cases = [(RuntimeError, 1), (NonRetryableError, 0)]

        for error_type, remaining_count in cases:
            with self.subTest(error_type=error_type.__name__), mock_aws():
                client = boto3.client("sqs", region_name=REGION)
                url = _queue(client, "jobs.fifo", fifo=True)
                queues = QueueGroup({"default": {"queue": SqsQueue(url, boto_client=client, visibility_timeout=0)}})
                queues.enqueue("default", "fail", message_group_id="g1")

                def handler(payload, error_type=error_type):
                    raise error_type("failed")

                _worker(queues, {"fail": handler}).run()

                self.assertEqual(len(_remaining(client, url)), remaining_count)

    def test_invalid_messages_are_deleted_and_unknown_jobs_are_retained(self):
        cases = [("not-json", 0), (json.dumps({"job_type": "unknown", "payload": {}}), 1)]

        for body, remaining_count in cases:
            with self.subTest(body=body), mock_aws():
                client = boto3.client("sqs", region_name=REGION)
                url = _queue(client, "jobs.fifo", fifo=True)
                client.send_message(QueueUrl=url, MessageBody=body, MessageGroupId="g1")
                queues = QueueGroup({"default": {"queue": SqsQueue(url, boto_client=client, visibility_timeout=0)}})

                _worker(queues, {}).run()

                self.assertEqual(len(_remaining(client, url)), remaining_count)

    def test_poll_middleware_may_stop_messages_from_being_received(self):
        with mock_aws():
            client = boto3.client("sqs", region_name=REGION)
            url = _queue(client, "jobs.fifo", fifo=True)

            class PausePolling(JobMiddleware):
                def poll(self, call_next):
                    pass

            queues = QueueGroup({"default": {"queue": SqsQueue(url, boto_client=client, visibility_timeout=0)}}, middleware=[PausePolling()])
            queues.enqueue("default", "greet", message_group_id="g1")
            received = []

            _worker(queues, {"greet": lambda payload: received.append(payload)}, should_stop=_stop_after(2)).run()

            self.assertEqual(received, [])
            self.assertEqual(len(_remaining(client, url)), 1)

    def test_heartbeat_failure_prevents_message_deletion(self):
        with mock_aws(), mock.patch("sqs_job_worker.sqs_queue.Heartbeat", _FailedHeartbeat):
            client = boto3.client("sqs", region_name=REGION)
            url = _queue(client, "jobs.fifo", fifo=True)
            queues = QueueGroup({"default": {"queue": SqsQueue(url, boto_client=client, visibility_timeout=0)}})
            queues.enqueue("default", "greet", message_group_id="g1")
            received = []

            _worker(queues, {"greet": lambda payload: received.append(payload)}).run()

            self.assertEqual(received, [{}])
            self.assertEqual(len(_remaining(client, url)), 1)

    def test_visibility_timeout_is_resolved_from_the_queue(self):
        with mock_aws():
            client = boto3.client("sqs", region_name=REGION)
            url = _queue(client, "jobs.fifo", fifo=True, visibility_timeout=123)
            queues = QueueGroup({"default": {"queue": SqsQueue(url, boto_client=client)}})
            worker = JobWorker(queues, {}, should_stop=_stop_after(0))

            worker.run()

            queue = worker._queues.items()[0][1]
            self.assertEqual(queue.visibility_timeout, 123)


class ApmIntegrationTests(TestCase):
    def _run(self, middleware):
        with mock_aws():
            client = boto3.client("sqs", region_name=REGION)
            url = _queue(client, "jobs-standard")
            queues = QueueGroup.build(boto_client=client, queues={"default": {"url": url}}, middleware=[middleware])
            queues.enqueue("default", "greet")
            handled = []

            _worker(queues, {"greet": lambda payload: handled.append(payload)}).run()

            self.assertEqual(handled, [{}])
            self.assertEqual(_remaining(client, url), [])

    @mock.patch("sqs_job_worker.contrib.newrelic.newrelic.agent")
    def test_new_relic_headers_reach_the_consumer_middleware(self, agent):
        agent.insert_distributed_trace_headers.side_effect = lambda headers: headers.extend([("newrelic", "nr"), ("traceparent", "00-trace")])
        agent.get_linking_metadata.return_value = {}

        self._run(NewRelicMiddleware())

        agent.accept_distributed_trace_headers.assert_called_once_with({"newrelic": "nr", "traceparent": "00-trace"}, transport_type="Queue")

    @mock.patch("sqs_job_worker.contrib.sentry.sentry_sdk")
    def test_sentry_headers_reach_the_consumer_middleware(self, sdk):
        sdk.get_traceparent.return_value = "sentry-trace"
        sdk.get_baggage.return_value = "environment=test"

        self._run(SentryMiddleware())

        sdk.continue_trace.assert_called_once_with({"sentry-trace": "sentry-trace", "baggage": "environment=test"}, op="queue.process", name="greet")

    @mock.patch("sqs_job_worker.contrib.otel.propagate")
    def test_otel_headers_reach_the_consumer_middleware(self, propagate):
        propagate.inject.side_effect = lambda carrier: carrier.update({"traceparent": "00-trace"})
        tracer = mock.MagicMock()
        tracer.start_as_current_span.return_value.__enter__.return_value.get_span_context.return_value.is_valid = False

        self._run(OTelMiddleware(tracer=tracer))

        propagate.extract.assert_called_once_with({"traceparent": "00-trace"})


class RunnerIntegrationTests(TestCase):
    @mock.patch("sqs_job_worker.queue_group.signal.signal")
    @mock.patch("sqs_job_worker.job_worker.JobWorker.run", autospec=True)
    def test_run_worker_uses_supplied_queues_and_handlers(self, run_mock, signal_mock):
        with mock_aws():
            client = boto3.client("sqs", region_name=REGION)
            url = _queue(client, "jobs-standard")
            handler = mock.Mock()

            queue = SqsQueue(url, boto_client=client)
            queues = QueueGroup({"default": {"queue": queue}})
            queues.run_worker(handlers={"greet": handler}, idle_wait_seconds=7)

            worker = run_mock.call_args.args[0]
            self.assertIs(worker._queues.items()[0][1], queue)
            self.assertEqual(worker._queues.names, ("default",))
            self.assertIs(worker._handlers["greet"], handler)
            self.assertEqual(worker._idle_wait_seconds, 7)
            self.assertEqual(signal_mock.call_count, 2)
