## Why

Business-profile annual-report discovery still spends the full 240-page allowance before splitting a filing-season window, then restarts each child window at page one. This preserves correctness but repeatedly reads already-seen metadata and makes unattended backfill converge much more slowly than necessary.

## What Changes

- Capture CNInfo's reported total-page count and pagination diagnostics in the source-neutral scan result.
- Preflight multi-day annual-report windows and split them after the first page when the reported result set exceeds the configured page allowance.
- Resume an incomplete, already-ended single-day historical window from a persisted next-page checkpoint because it cannot be split further.
- Keep current-day/fresh discovery watermark-based and restartable; do not trust a mutable page offset for a live announcement stream.
- Preserve selected preflight records, local title filtering, frontier deduplication, correction precedence, bounded work, and provider fallback rules.
- Expose preflight splits and page-resume state in discovery telemetry and logs.

## Capabilities

### New Capabilities

- `business-profile-discovery-pagination`: Total-page-aware discovery window planning and safe historical single-day page continuation for annual-report production.

### Modified Capabilities

- None.

## Impact

- Affects the CNInfo announcement provider, announcement acquisition page-bound classification, business-profile resumable-window planner, persisted operation-state payloads, and focused provider/discovery tests.
- Existing frontier rows, work items, annual-report assets, committed publication watermarks, and persisted window payloads remain compatible.
- No database migration or new dependency is required.
