## Why

The CNInfo corporate-action and three-source canonical factor rollout now produces a complete production factor chain, but the operational path still loads unbounded audit reports on quote requests, depends on an ignored local activation file, retains every staging snapshot, and exposes inconsistent quality metadata. These defects create immediate API latency and memory risk and make future daily maintenance harder to audit safely.

## What Changes

- Separate lightweight canonical series readiness from paged per-instrument selection decisions so quote reads never parse or cache a full-market report.
- Make production factor activation durable and fail-safe, while retaining an explicit audited rollback to the BaoStock/Sina composite path.
- Recompute canonical coverage and reconciliation summaries consistently after full builds, promotions, and targeted daily merges.
- Add protected, previewable retention maintenance for obsolete staging, benchmark, endpoint-status, and report-detail records.
- Qualify the BaoStock/Sina composite by factor-path coverage and continuity rather than claiming unavailable XDXR event completeness.
- Require a fresh successful predecessor watermark for the quote/Sina input used by the CNInfo daily canonical update, with a visible deferred outcome when it is unavailable.
- Align canonical quality APIs, configuration examples, operations documentation, task names, and source terminology with the promoted production series.
- Deprecate ambiguous legacy rebuild entry points and classify one-off operator scripts as replayable manifests or archived migrations.
- Isolate tests from production activation state and split canonical orchestration into focused services without changing raw source tables or factor economics.

## Capabilities

### New Capabilities
- `a-share-canonical-factor-operations`: Defines lightweight production reads, durable activation, factor-path source qualification, consistent quality summaries, protected retention, and operational entry-point lifecycle.

### Modified Capabilities
- `quote-api-query-semantics`: Requires adjusted quote reads and factor-quality queries to resolve the active canonical series without loading full-market audit payloads.
- `scheduler`: Requires CNInfo canonical daily maintenance to verify the freshness and success of its quote/Sina predecessor watermark.

## Impact

- Affects `data_manager.py`, canonical selection helpers, database models/operations and migrations, quote/corporate-action API routes, scheduler configuration/tasks, factor activation handling, tests, and operations documentation.
- Adds normalized canonical decision/report storage and lightweight status access while preserving existing CNInfo, TDX, BaoStock/Sina, canonical, document, LLM, and review evidence.
- Existing production factor values and raw source records are not rewritten by this change. Destructive retention actions remain manual, previewable, protected, and separately confirmed.
