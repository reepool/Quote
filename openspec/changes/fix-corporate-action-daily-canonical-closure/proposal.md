## Why

The A-share corporate-action daily task can report `partial` even when BSE discovery, CNInfo factor derivation, and TDX reference derivation all succeed. The current canonical closure depends on quote/composite watermarks that are not yet guaranteed to exist, then places every affected instrument into the factor retry queue; title classification also treats several non-XDXR uses of words such as "补偿" or share cancellation as semantic corporate-action blockers.

## What Changes

- Make the BaoStock/Sina quote-composite predecessor watermark a durable output of the normal A-share quote/factor update path, with explicit per-exchange coverage through the latest usable trading session.
- Make canonical daily maintenance distinguish predecessor unavailability from factor-path defects and retry only instruments that have a real unresolved factor or merge failure.
- Preserve a successful empty BSE official-announcement window as success and keep BSE evidence isolated from CNInfo and canonical source tables.
- Tighten deterministic title classification so financing statements mentioning compensation and ordinary repurchase/restricted-share cancellations do not enter XDXR semantic governance, while genuine distribution implementation announcements remain eligible.
- Add bounded report fields that state the exact canonical merge decision, predecessor status, retry count, and unresolved announcement samples behind a `partial` result.
- Add regression tests for the observed weekend run and for subsequent trading-day closure.

## Capabilities

### New Capabilities

- `a-share-corporate-action-daily-closure`: Governs predecessor readiness, BSE empty-window semantics, canonical incremental merge/retry behavior, special-announcement classification, and operator-facing failure reporting for the daily A-share corporate-action workflow.

### Modified Capabilities

None.

## Impact

- Affected code: `data_manager.py`, A-share daily quote/factor completion logic, CNInfo corporate-action title classification, scheduler report formatting, and their unit tests.
- Affected storage: existing `operational_watermarks`, canonical staging/status tables, and the governed daily factor retry status. No schema migration is expected.
- Source isolation remains unchanged: BSE, CNInfo, TDX, and BaoStock/Sina source observations are not overwritten by canonical selection.
- API contracts remain backward compatible; reports gain explicit diagnostic fields.
