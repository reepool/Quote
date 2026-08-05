## Why

The first production backfill discovered 3,188 latest-annual work items but the semantic acquisition stage ignored each work item's already selected frontier announcement and attempted a second instrument search with an invalid half-bounded date scope. The first 20 items therefore produced no PDF, no manifest, and no semantic input while still advancing to completed, and the operator report mislabeled enqueued work as completed work.

## What Changes

- Bind acquisition to the active frontier row carried by each durable work item and archive that exact official PDF without rediscovering the issuer's announcements.
- Require a usable, identity-matching business-profile source manifest before an acquire-stage work item may advance.
- Treat missing or invalid frontier assets as retryable work rather than successful semantic completion.
- Add an idempotent repair that requeues only completed latest-annual work proven to have no usable bound manifest and resolves stale missing-document machine-rework exceptions when recovery succeeds.
- Make single-batch and control reports read authoritative nested results and distinguish enqueued, worker-completed, pending, and terminal counts.

## Capabilities

### New Capabilities

- `business-profile-frontier-bound-processing`: Durable frontier-bound PDF acquisition, manifest completion gates, defect recovery, and truthful long-running backfill telemetry.

### Modified Capabilities

None.

## Impact

- Affects business-profile work repository lifecycle, async stage wiring, immutable PDF archive integration, semantic runtime input handoff, backfill control snapshots, scheduler reporting, and focused production tests.
- Uses existing frontier, work, exception, and financial source-file schemas; no destructive migration or archive rewrite is required.
- Existing valid PDFs and manifests remain reusable. The repair targets only work items with positive evidence of the known missing-manifest defect.
