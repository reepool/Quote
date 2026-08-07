## 1. Durable Scope Recovery

- [x] 1.1 Persist and backfill a stable knowledge cutoff in durable work metadata
- [x] 1.2 Run every semantic pipeline stage with the work-bound knowledge cutoff
- [x] 1.3 Repair stale-scope checkpoints in place when safe and rotate unrecoverable checkpoints

## 2. Long-Run Observability

- [x] 2.1 Add backfill lifecycle, discovery, recovery, enqueue, and final queue-health logs
- [x] 2.2 Add per-stage start/end, batch progress, periodic heartbeat, writer-pressure, and safe failure logs
- [x] 2.3 Add scheduler single-batch and continuous run start/end logs with real elapsed time

## 3. Verification

- [x] 3.1 Add queue tests for stable cutoff metadata and automatic stale-checkpoint recovery
- [x] 3.2 Add async service and scheduler tests for progress and degraded-run logging
- [x] 3.3 Run focused tests, strict OpenSpec validation, and uncommitted-change review
