## Why

The business-profile bootstrap currently scans every market announcement from a fixed historical date, so concentrated filing seasons exhaust the page bound after reading thousands of unrelated disclosures. The incomplete CNInfo result then falls through to exchange providers that cannot satisfy the same market scope, discarding useful discovery work and leaving the durable queue empty.

## What Changes

- Discover annual reports with the official upstream annual-report category plus bounded date and market filters while retaining strict local full-document classification.
- Bootstrap the current filing season first, then reconcile active-company coverage and use bounded instrument-scoped lookback only for companies whose latest available annual report remains missing.
- Keep daily discovery category-filtered and overlap-bounded; leave semiannual and exceptional disclosure scans manual-only.
- Persist partial CNInfo discoveries and split incomplete date windows instead of routing `max_pages_exhausted` into an incompatible fallback provider.
- Prefer the newest active corrected or revised full annual report for an issuer and report period. Do not enqueue, download, or parse an earlier original when the revised full report is already known; supersede an unstarted original if the revision appears later.
- Recognize official annual-report abbreviations such as `2025年年报`, including BSE titles, without admitting summaries or annual-report-related notices as full reports.
- Add source-specific annual/semiannual category mappings for CNInfo, SSE, and SZSE so exchange sources can be used for compatible reconciliation without leaking provider parameters into business callers.
- Expose discovery filter, coverage, split-window, targeted-repair, and supersession counts in operational reports.

## Capabilities

### New Capabilities

- `business-profile-annual-report-discovery`: Category-filtered, resumable latest-annual discovery, coverage repair, and corrected-full-report preference for unattended business-profile production.

### Modified Capabilities

- None.

## Impact

- Affects the source-neutral announcement query/provider layer, business-profile production discovery and queue selection, rollout configuration, document-title classification, scheduler-facing telemetry, and focused unit/integration tests.
- Existing immutable annual-report assets and published facts remain readable; no destructive schema or PDF migration is required.
- Automatic processing remains latest-annual-only. Semiannual and specialist documents remain available only through explicit manual backfill scope.
