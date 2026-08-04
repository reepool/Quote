## Why

The daily financial disclosure maintenance task currently re-enqueues large numbers of already accepted lifecycle gaps, can retry pending records after their retry horizon, and may treat performance forecast announcements as formal periodic reports. This creates avoidable source requests, consumes the bounded candidate budget, and reports a primary-source outage as `success` when only fallback repair succeeded.

## What Changes

- Keep accepted lifecycle and disclosure-explained gaps out of the daily incremental candidate pool; retain them for bounded reconciliation.
- Restrict pending recheck candidates to records whose retry horizon has not expired, and persist an explicit terminal outcome when the horizon is exhausted.
- Expand non-primary financial announcement filtering to exclude performance forecasts, pre-increase/pre-decrease notices, and other non-filing result announcements unless they carry an explicit delayed-disclosure or listing-risk signal.
- Preserve formal half-year/annual/quarterly report announcements as candidates, including newly available report periods outside the rolling readiness display window.
- Make maintenance status `degraded` when the configured official CNInfo route has complete failure or unresolved pending fallback work, while retaining successful fallback writes and detailed source-routing diagnostics.
- Add focused unit and scheduler-report tests for candidate selection, retry expiry, noise filtering, source degradation, and candidate-limit behavior.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `financial-operations-scheduler`: tighten financial disclosure candidate eligibility, pending-recheck lifecycle, source-routing status semantics, and operator-facing diagnostics.

## Impact

- Affects `research/financial_disclosure_incremental_sync.py`, `research/financial_disclosure_events.py`, financial disclosure state storage, scheduler status/report formatting, and related unit tests.
- Does not change financial fact schemas, canonical fact names, source priority, or the configured report-period history window.
- Existing accepted disclosure state remains auditable in `data/financials.db`; only its daily candidate eligibility changes.
