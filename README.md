# sqs-job-worker

[English](https://github.com/xmart-corp/sqs-job-worker/blob/main/README.md) | [日本語](https://github.com/xmart-corp/sqs-job-worker/blob/main/README.ja.md)

A framework-agnostic, long-running worker for processing Amazon SQS jobs.
Hand it a boto3 client and it works; the core depends on no web framework, APM SDK, or deployment platform.

## Features

- A simple long-running worker that receives and processes one message at a time; concurrency scales with the number of processes (containers)
- Polls multiple queues in weighted random order, prioritizing some queues without starving the low-weight ones
- A dedicated thread keeps extending the visibility timeout while a job runs, so even jobs with unpredictable runtimes are unlikely to be delivered twice
- Graceful shutdown on SIGTERM / SIGINT: in-flight jobs run to completion and unprocessed messages are returned to the queue immediately
- Rack-style middleware with three hooks: produce / poll / consume
- New Relic, OpenTelemetry, and Sentry trace propagation, plus draining for ECS rolling deploys, bundled as `contrib`
- Structured logging via structlog; correlation fields travel in SQS message attributes and are attached to consumer-side logs automatically
- Works with both standard and FIFO queues

## Installation

```console
$ pip install sqs-job-worker
```

To use an APM integration, install the corresponding extra:

```console
$ pip install "sqs-job-worker[newrelic]"
$ pip install "sqs-job-worker[otel]"
$ pip install "sqs-job-worker[sentry]"
```

Requires Python 3.11 or later.

## Quickstart

Create a `QueueGroup` that maps logical names to physical queues. Share the same definition between producers and consumers.

```python
import boto3

from sqs_job_worker import QueueGroup

sqs = boto3.client("sqs")
queues = QueueGroup.build(boto_client=sqs, queues={"default": {"name": "my-app-jobs"}})
```

Enqueue a job:

```python
queues.enqueue("default", "send_welcome_email", {"user_id": 42})
```

In the worker process, start the loop with a handler for each `job_type`:

```python
def send_welcome_email(payload: dict) -> None:
    ...

queues.run_worker({"send_welcome_email": send_welcome_email})
```

`run_worker()` is a blocking loop with signal handling built in.
On ECS or Kubernetes, run this process as a long-running service as is.

Messages are delivered at least once.
The same message may arrive more than once, so implement handlers idempotently.

## Message format

The message body is the following JSON.
Any producer that follows this format can enqueue jobs, including producers written in other languages.

```json
{"job_type": "send_welcome_email", "payload": {"user_id": 42}}
```

- When the body cannot be parsed as JSON, `job_type` is not a string, or `payload` is not an object, the message is deleted, because no retry can fix it
- When no handler is registered for the `job_type`, the message is left undeleted, on the assumption that it will be redelivered to another worker responsible for that job

Correlation fields and trace headers travel in SQS message attributes, not in the body.

`enqueue()` also accepts SQS send options such as `message_group_id` / `message_deduplication_id` for FIFO queues.
For standard queues you can also delay delivery with `delay_seconds`.
FIFO queues do not support per-message delays.

```python
queues.enqueue(
    "default",
    "recalculate_balance",
    {"account_id": "a-1"},
    message_group_id="a-1",
    message_deduplication_id="job-1",
)
```

## Retries and failures

- When a handler returns normally, the message is deleted
- When a handler raises, the message is not deleted and is redelivered after the visibility timeout expires. Configure retry limits and dead-letter queues through the SQS redrive policy
- When retrying cannot possibly succeed, raise `NonRetryableError`. The message is deleted immediately without redelivery

```python
from sqs_job_worker import NonRetryableError

def sync_user(payload: dict) -> None:
    user = find_user(payload["user_id"])
    if user is None:
        raise NonRetryableError("user was deleted; retrying cannot succeed")
```

## Multiple queues and weighted polling

```python
queues = QueueGroup.build(
    boto_client=sqs,
    queues={
        "critical": {"name": "my-app-jobs-critical", "weight": 5},
        "default": {"name": "my-app-jobs", "weight": 1},
    },
)
```

- Each cycle, the worker draws a random queue order based on the weights, short-polls each queue in that order, and processes the first message it finds
- `weight` is a relative value used in this ordering draw. In the example above, `critical` is more likely to come first, but every queue is checked every cycle, so low-weight queues are never left behind
- When every queue is empty, the worker long-polls the queue that came first in the order for `idle_wait_seconds` (default 20 seconds), waiting for a message to arrive

Instead of `name`, you can specify the queue URL directly with `url`. For per-queue tuning, construct an `SqsQueue` yourself and pass it to `QueueGroup`.

```python
from sqs_job_worker import QueueGroup, SqsQueue

heavy = SqsQueue(queue_url, boto_client=sqs, max_runtime_seconds=7200)
queues = QueueGroup({"heavy": {"queue": heavy, "weight": 1}})
```

## Long-running jobs and the visibility timeout

- When no visibility timeout is specified, the worker fetches the queue's configured value at startup
- While a job runs, a dedicated heartbeat thread keeps extending the visibility timeout at intervals of one third of the timeout. You don't need to raise the queue's visibility timeout to match the job's maximum runtime
- When a job runs longer than `max_runtime_seconds` (default 3600 seconds), the heartbeat stops and the message returns to SQS for redelivery
- When extending the visibility timeout fails, the message may already have been redelivered to another worker, so it is not deleted even if the job finishes successfully

## Graceful shutdown

On SIGTERM / SIGINT, `run_worker()` stops receiving new messages, finishes the in-flight job, then exits.
A second signal exits immediately.
Messages received after the stop request are not processed; their visibility timeout is reset to 0 so they become available for redelivery right away.

If receive errors persist, the worker retries with jittered exponential backoff capped at 30 seconds. After 10 consecutive failures it exits with code 1, leaving the restart to the supervisor.

## Middleware

Middleware wraps enqueueing, polling, and processing so you can inject cross-cutting behavior such as trace propagation and instrumentation.
Pass the middleware to use as a list via `QueueGroup`'s `middleware`.

```python
from sqs_job_worker.contrib.otel import OTelMiddleware

queues = QueueGroup.build(
    boto_client=sqs,
    queues={"default": {"name": "my-app-jobs"}},
    middleware=[OTelMiddleware()],
)
```

Like Rack and WSGI, middleware is applied from the outside in, with the first list entry outermost.
Share the same `QueueGroup` definition between producers and consumers and the same middleware applies to both enqueueing and processing.

### Middleware in contrib

The core depends only on boto3 and structlog; integrations that touch APM vendor SDKs or platform APIs are kept in `contrib`.
Install the corresponding extra to use an APM integration.

- `contrib.newrelic.NewRelicMiddleware` — propagates New Relic distributed traces, instruments jobs as `BackgroundTask`s, and binds `trace_id` / `span_id` to logs
- Where the agent cannot link the trace, it falls back to a synthesized W3C traceparent so producer and consumer logs stay correlated by one `trace_id`
- `contrib.otel.OTelMiddleware` — propagates traces with the propagator configured in your application, instruments jobs with `CONSUMER` spans carrying `messaging.*` attributes, and binds `trace_id` / `span_id` to logs
- `contrib.sentry.SentryMiddleware` — propagates `sentry-trace` / `baggage`, reports handler exceptions to Sentry, and leaves the retry decision to the worker
- `contrib.ecs.EcsDrainMiddleware` — during an ECS rolling deploy, pauses polling once a task from a newer revision of the same task definition family is RUNNING, preventing previous-generation workers from picking up new jobs. If the new generation fails and stops, polling resumes automatically
  - Pass an ECS client, e.g. `EcsDrainMiddleware(boto3.client("ecs"))`. The task role needs the `ecs:ListTasks` and `ecs:DescribeTasks` permissions
  - The worker identifies its own task via the ECS Task Metadata Endpoint V4; outside ECS, or on ECS API errors, it fails open and keeps polling as usual

Register only one trace-context-injecting middleware per process. Duplicates are detected as a `ValueError` when a job is enqueued.

### Writing your own middleware

Subclass `JobMiddleware` and override only the hooks you need.

- `produce(job, call_next)` wraps `enqueue()`. Use it to inject correlation fields and trace headers
- `poll(call_next)` wraps one polling cycle. Use it to pause receiving (draining) or for per-cycle instrumentation
- `consume(job, call_next)` wraps handler execution. Use it to set up APM transactions or tenant scopes

```python
from sqs_job_worker import Job, JobMiddleware

class TenantMiddleware(JobMiddleware):
    def produce(self, job: Job, call_next):
        job.correlation_fields.setdefault("tenant_id", current_tenant_id())
        return call_next(job)

    def consume(self, job: Job, call_next):
        with tenant_scope(job.correlation_fields["tenant_id"]):
            return call_next(job)
```

## Correlation fields and trace propagation

- The dict passed as `enqueue(..., correlation_fields={"request_id": "r-1"})` is stored as JSON in a single message attribute named `correlation_fields`. On the consumer side it is bound to the structlog context automatically and attached to every log line the job emits. Keys reserved by the worker, such as `queue` and `message_id`, are never overwritten
- Trace headers (such as `traceparent`) travel as individual message attributes. The core never interprets their contents; injection and extraction are handled by the tracing middleware in contrib
- SQS allows up to 10 message attributes per message. When the limit would be exceeded, attributes given explicitly via `message_attributes` take precedence: propagation attributes are dropped from the end and a `propagation_attributes_dropped` warning is logged

## Logging

Logs are structured with structlog. Configure the output destination and formatting in your application.

Job lifecycle events are emitted to the `sqs_job_worker.job` logger.

- `job_enqueued` — on enqueue (`queue`, `job_type`, `message_id`)
- `job_started` — when execution starts (`group_id`, `queue_wait_ms`)
- `job_finished` — on completion (`outcome`, `duration_ms`)

The `outcome` of `job_finished` is one of:

- `success` — completed normally; the message is deleted
- `retry` — the handler raised; the message is redelivered after the visibility timeout expires
- `delete` — the message is deleted due to `NonRetryableError`
- `heartbeat_failed` — extending the visibility timeout failed, or `max_runtime_seconds` was exceeded; the message is left for redelivery rather than deleted
- `delete_failed` — processing finished but the delete request failed; the result of the processing itself is recorded in `processing_outcome`

Logs emitted while a job is being processed automatically carry `queue` / `message_id` / `receive_count` / `job_type`, the correlation fields, and `trace_id` / `span_id` when an APM middleware is in use.

## Development

```console
$ uv sync --all-extras --all-groups
$ uv run pytest
$ uv run ruff check . && uv run ruff format --check .
$ uv run ty check src
```

## License

[MIT](https://github.com/xmart-corp/sqs-job-worker/blob/main/LICENSE)
