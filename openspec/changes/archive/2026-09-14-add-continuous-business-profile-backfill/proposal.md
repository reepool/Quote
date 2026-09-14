## Why

The market-wide latest-annual bootstrap is durable and resumable, but each bounded batch currently requires another manual `/run` invocation and exposes only its final report. A genuinely long-running rollout needs one-start continuous draining, cooperative interruption, restart-safe progress, and a read-only operator status path without weakening queue, promotion, or database-write controls.

## What Changes

- Add an opt-in `continuous=true` mode to `business_profile_backfill`; the default remains one bounded batch for initial validation and specialist runs.
- Repeatedly execute the existing bounded discovery and queue workers until the active rollout phase is complete, a cooperative stop is requested, or the pipeline cannot make automatic progress.
- Persist a restart-safe progress snapshot with run identity, heartbeat, batch counters, stage throughput, queue health, rollout readiness, failure reasons, and terminal state.
- Add a separate manual control task for `status` and `stop`, so a running backfill can be observed or asked to stop without starting a competing backfill instance.
- Make worker draining observe cooperative stop requests between claimed batches while preserving leases, retry state, checkpoints, immutable annual-report assets, and the single-writer contract.
- Keep daily scheduling, field-family activation, promotion manifests, and approved-write gates unchanged.

## Capabilities

### New Capabilities

- `continuous-business-profile-backfill`: Restart-safe continuous bootstrap control, cooperative stopping, and persistent progress reporting for the existing asynchronous business-profile queues.

### Modified Capabilities


## Impact

- Runtime: business-profile async worker stop checks, DataManager backfill forwarding, scheduler task loop and control task.
- Configuration: manual backfill continuous-control defaults and a read-only/control manual job.
- Operations: stable start, status, stop, and resume commands plus progress documentation.
- Storage: small atomic JSON control/progress files under the existing business-profile checkpoint root; no production fact schema migration.
- Tests: focused continuous-loop, stop, restart, progress, parser, and scheduler tests.
