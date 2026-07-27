# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-27

Initial release.

### Added

- Unified `SqsQueue` for one physical queue's producer and consumer operations.
- `QueueGroup` for logical queue names, name-based enqueue, and per-queue polling
  `weight`, with `QueueGroup.build()` resolving physical queue names or accepting queue URLs.
- Single-threaded SQS consumer with starvation-free weighted polling across multiple queues.
- Explicitly injected boto3 SQS and ECS clients; the library never creates them internally.
- Per-message `delay_seconds` on enqueue (standard queues only — SQS rejects
  per-message delays on FIFO queues).
- Heartbeat visibility extension for jobs longer than the visibility timeout.
- Retry semantics for ordinary handler exceptions and explicit deletion through `NonRetryableError`.
- Graceful shutdown on SIGTERM/SIGINT via `QueueGroup.run_worker()`, which builds the
  consumer from a handler mapping.
- ECS blue/green drain control through `EcsDrainMiddleware` in `contrib.ecs`.
- Log correlation: producer-supplied `correlation_fields` propagate into the job's logs.
- One Rack-style `JobMiddleware` stack on `QueueGroup` for production, polling, and consumption,
  with pass-through defaults so middleware may implement any phase independently.
- Distributed tracing via opaque trace headers in message attributes: New Relic, Sentry,
  and OpenTelemetry middleware in `contrib` propagate on the producer and measure on the consumer.

[Unreleased]: https://github.com/xmart-corp/sqs-job-worker/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/xmart-corp/sqs-job-worker/releases/tag/v0.1.0
