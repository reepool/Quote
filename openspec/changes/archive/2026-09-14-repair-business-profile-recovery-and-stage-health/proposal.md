## Why

Production evidence shows that stale business-profile checkpoints are requeued and preserved when only their knowledge cutoff is readable, even when the immutable processing identities have changed. The same items then fail terminally again, consume entire semantic and publish budgets, and the run is still reported as successful despite publishing nothing.

## What Changes

- Require full logical-scope compatibility before preserving a semantic checkpoint; rotate incompatible checkpoints and restart from the earliest safe stage while reusing durable annual-report assets.
- Make stale-scope recovery idempotent so a checkpoint rejected after recovery cannot be requeued into the same incompatible state on every invocation.
- Prevent recovered failures from starving ordinary pending work within a bounded stage run.
- Propagate stage retries, configuration blocks, and terminal failures into the top-level operational status and reason codes without treating resumable discovery backlog as failure.
- Add regression tests based on the observed old-model/new-route identity transition and mixed recovered/fresh queues.

## Capabilities

### New Capabilities

- `business-profile-recovery-health`: Logical-scope-safe checkpoint recovery, bounded queue fairness, and truthful end-to-end health reporting for business-profile production runs.

### Modified Capabilities


## Impact

- `research/business_profile_async_production.py` checkpoint inspection, recovery, queue claiming, and report status aggregation.
- `research/business_profile_semantic_pipeline.py` shared logical-scope comparison helpers.
- Business-profile async-production tests and operational documentation.
- No database schema migration, public command change, LLM payload change, or destructive rewrite of existing annual-report assets.
