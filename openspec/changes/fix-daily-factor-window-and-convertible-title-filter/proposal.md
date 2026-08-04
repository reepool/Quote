## Why

The normal A-share quote/factor update can mark an exchange-level BaoStock/Sina predecessor watermark `partial` even when the factor source is healthy. Ex-dividend discovery is performed over a union of all instrument fetch dates, but the resulting symbols are not matched back to each instrument's own factor request window; events outside that window then return zero rows and are incorrectly counted as download failures. The corporate-action title trigger also treats the substring `债转股` inside ordinary convertible-bond conversion-price adjustment notices as a restructuring event.

## What Changes

- Preserve ex-dividend event dates during discovery and select a stock for factor synchronization only when at least one discovered event falls inside that stock's own inclusive factor request window.
- Count an empty factor response as a known-event failure only when the same instrument has an in-window discovered event; out-of-window events are excluded rather than reported as source failures.
- Keep exchange-specific BaoStock/Sina predecessor watermarks fail-closed for real factor transport, parsing, coverage, or persistence failures, while preventing cross-instrument date-window contamination from holding the watermark stale.
- Add bounded factor-sync diagnostics containing selected, excluded-out-of-window, empty-known-event, and failed instrument samples so a stale watermark can be explained from the daily report or logs.
- Deterministically exclude repurchase-cancellation notices that only adjust a convertible-bond conversion price from XDXR anomaly governance unless the title also contains a genuine distribution implementation pattern.
- Add regression coverage for the seven false SZSE factor failures observed on 2026-08-03 and the `300707.SZ` convertible-bond title false positive.

## Capabilities

### New Capabilities

- `a-share-corporate-action-daily-input-integrity`: Governs per-instrument date alignment for BaoStock/Sina daily factor synchronization, predecessor watermark failure semantics, diagnostic samples, and deterministic exclusion of convertible-bond conversion-price notices from XDXR semantic governance.

### Modified Capabilities

None.

## Impact

- Affected code: A-share daily quote/factor synchronization in `data_manager.py`, ex-dividend discovery result normalization, corporate-action title classification, daily task diagnostics/report formatting, and focused unit tests.
- Affected operations: `daily_data_update` produces the exchange-specific BaoStock/Sina predecessor watermarks consumed by `a_share_cninfo_corporate_action_daily_sync`.
- Affected storage: existing `operational_watermarks` and factor/source tables only; no schema migration or historical source-data rewrite is expected.
- Source and canonical policies remain unchanged: CNInfo, TDX, and BaoStock/Sina observations stay isolated, and stale or genuinely failed predecessor watermarks continue to block Canonical promotion.
