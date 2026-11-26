## Why

Run 741 blocked the whole overseas daily commodity sync because the configured primary metadata request for the canonical oil chain timed out. LME prices were already collected, and the chain's configured fallback can validate the same series, but a primary request failure currently removes the series before any price fetch or fallback metadata check runs.

## What Changes

- Keep each source profile's request budget in its own `timeout_seconds` configuration, shared by that profile's metadata and observation requests. The EIA official profile used by the canonical oil chain is 60 seconds. Adapters SHALL read this value and SHALL NOT hardcode a timeout.
- When a configured source chain's primary master governance fails for a request-class reason, validate that series with the chain's configured fallback governance adapters, in chain order, and then continue the existing price-fetch chain.
- Report that outcome as success with an operator note identifying the primary failure and the fallback profile that completed validation. Do not turn the note into a warning or blocker.
- Block the series only when every configured chain profile fails with a request-class outcome. A frequency or unit mismatch from a profile that actually answered remains a blocker and SHALL NOT be overridden by another profile.
- Keep price selection in `ConfiguredSourceChainProvider.fetch`, and make one narrow addition: a primary `missing_api_key` blocker is downgraded to a per-series note when fallback covers that series without a value conflict. A series that stays uncovered keeps a blocker carrying `series_id` and `api_key_env`. Any provider blocker that still has no series id excludes the whole provider batch from observation persistence.
- Redact secret query values before request errors are logged or stored in master evidence, blockers, or operator notes.
- Preserve the canonical series venue. Use `official_primary_with_governed_fallback` only for primary verification, `fallback_master_verified` for fallback verification, and `unverified` for a blocked record.

## Capabilities

### New Capabilities

### Modified Capabilities

- `special-commodity-market-data`: Configured source-chain master governance can complete on fallback metadata after a primary request failure, while request budgets stay in source-profile configuration and semantic mismatches still block.

## Impact

- `research/special_commodity_market_data.py`: configured source-chain master governance, the price chain's primary `missing_api_key` downgrade, redacted request errors, master-result notes, and note propagation.
- `config/11_futures.json`: only `eia_official_api.timeout_seconds` changes to 60. Unrelated AkShare edits already in that file stay outside this change.
- Unit tests for mixed-series fallback calls, price-key downgrade, secret redaction, and the success-plus-note report. No scheduler entry or storage schema is added.
- The running service keeps its startup configuration until restart; this change does not add a reload mechanism.
