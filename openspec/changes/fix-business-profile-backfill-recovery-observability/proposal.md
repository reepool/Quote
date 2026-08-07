## Why

Long-running business-profile backfills can requeue stale-scope work while retaining a checkpoint whose logical knowledge cutoff no longer matches the current invocation. The recovered work then fails again as terminal work, while the task emits no useful progress logs between start and its final degraded report.

## What Changes

- Bind each durable work item to a stable knowledge cutoff and reuse that cutoff throughout all pipeline stages and later resumptions.
- Repair stale-scope work from the persisted checkpoint when possible; rotate an unreadable or incompatible checkpoint and rebuild automatically when it cannot be reused safely.
- Add structured lifecycle, discovery, recovery, enqueue, stage, batch, heartbeat, failure, writer, and final queue-health logs for both single-batch and continuous backfills.
- Preserve bounded asynchronous parse and semantic concurrency and the existing serialized SQLite writer channel.
- Distinguish resumable discovery backlog from worker failures in final operational telemetry.

## Capabilities

### New Capabilities

- `business-profile-backfill-resilience`: Durable cross-run scope binding, automated stale-checkpoint recovery, and progress observability for company-profile backfills.

### Modified Capabilities

## Impact

- `research/business_profile_async_production.py` queue recovery and worker orchestration.
- `data_manager.py` work-stage scope construction and high-level backfill logging.
- `scheduler/tasks.py` task lifecycle and final progress logging.
- Business-profile async-production and scheduler unit tests; no database schema migration or public command change.
