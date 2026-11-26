## Context

`ConfiguredSourceChainGovernanceAdapter.govern_master` validates only the first profile in `source_chain`. For `eia_fred_oil_chain` that profile is EIA. A timeout or HTTP failure becomes `master_metadata_request_failed`, the governance pipeline removes the series from the fetch set, and the existing price chain never runs. Price fallback already lives in `ConfiguredSourceChainProvider.fetch`: a primary `provider_request_failed` warning covered by a later profile becomes a note. A primary `missing_api_key` is different: `EiaCommodityProvider.fetch` returns it as a blocker without `series_id`, and the chain copies blockers through unchanged. The pipeline then keeps fallback observations eligible because it only excludes blockers that carry `series_id`, while the series-less blocker still marks the run and date governance blocked.

Run 741 showed the operational gap. EIA metadata reads exhausted three 30-second attempts for both canonical oil series. LME completed. Direct checks afterward showed FRED series metadata for the configured fallback symbols returning Daily and Dollars per Barrel within one second, while EIA date-window calls received an Akamai HTTP 504 at about 61 seconds. `requests` includes the full URL in `HTTPError`; for these APIs that URL contains `api_key`. A read timeout does not, but any HTTP 504 stored with `str(exc)` would persist the key. `_redact_url` already removes those query values from lineage URLs and is not applied to exception text.

The agreed request budget for the EIA official profile is 60 seconds. The authoritative owner remains the configured source-chain adapter inside `research/special_commodity_market_data.py`. Scheduler and data-manager entry points stay forwarders. This is not a framework-program change and does not add a second pipeline.

## Goals / Non-Goals

**Goals:**

- Read every profile timeout from `timeout_seconds` and set the EIA official profile to 60 seconds.
- After a primary request-class master failure, call the next configured governance adapter only with the unresolved series, then continue the existing price fetch when one fallback validates.
- Downgrade a primary price-stage `missing_api_key` to a per-series note when fallback covers that series without a value conflict. Keep a series-scoped blocker, including `api_key_env`, when it does not.
- Treat a provider blocker that still has no `series_id` as applying to every series in that provider call, so observations from that call are not persisted under a blocked run.
- Emit the structured master note and leave run status at success when that note is the only diagnostic.
- Preserve canonical `venue` and chain `source_profile`. Use the three quality flags defined below.
- Redact secret query values in retry logs and in every stored or reported request error.
- Make the accept/block decision a pure function over governance results. The adapter decides which subset to call next.

**Non-Goals:**

- Hardcoding EIA, FRED, WTI, Brent, series symbols, or a 60-second literal in adapter code.
- A new governance framework, registry, or task-level oil branch.
- Changing LME primary/fallback behavior or the existing observation conflict rule.
- Changing retry counts, adding a config reload, or restarting the running service.
- Treating an uncovered price failure as success because metadata fallback succeeded.
- Overriding a semantic mismatch with another profile.
- Teaching `_aggregate_special_commodity_scope_results` to merge notes. The overseas daily task uses one sync call and does not use that aggregator. Industrial indicator scope aggregation is unchanged.

## Decisions

1. **Split calling from folding.** `ConfiguredSourceChainGovernanceAdapter.govern_master` calls the primary governance adapter for the batch. A pure function returns the subset whose blockers are request-class and not semantic. The adapter calls the next fallback governance adapter with only that subset, using `ConfiguredSourceChainGovernanceAdapter.GOVERNANCE` and `mapped_series`. The same pure function then folds that fallback result. It does not receive fallback results for series that were not sent. Alternative considered: call every fallback with the whole batch and let the function ignore extra rows. Rejected because a mixed batch would query the fallback for a series whose primary already succeeded or whose primary returned a semantic mismatch.

2. **Request-class reasons stay a closed pair.** `missing_api_key` and `master_metadata_request_failed` are request-class. A blocker without `series_id` applies to every series in that call. Every other blocker, including `master_frequency_mismatch` and `master_unit_mismatch`, is semantic. The pure function does not inspect exception text and does not reimplement unit or frequency comparison.

3. **Walk fallbacks in configured order and stop on an answer.** Primary semantic failure does not call fallback. A fallback request-class failure, a missing chain governance adapter, or a missing `source_chain_sources` mapping skips that profile for that series and tries the next one. The first fallback that returns a valid master record supplies the evidence. A semantic mismatch blocks immediately. If no profile validates, the series keeps one blocked record and the request-class blockers, including `api_key_env` when the reason is `missing_api_key`.

4. **Record canonical identity and who verified the series.** Every written chain record uses the canonical series `venue` and the chain `source_profile`. Quality flags are:
   - primary verification: `official_primary_with_governed_fallback`, with `master_verified_by` set to the primary profile;
   - fallback verification: `fallback_master_verified`, with `master_verified_by` set to the fallback profile, plus redacted `primary_error` in metadata;
   - blocked: `governance_status` `blocked` and quality flag `unverified`.
   The existing persistence rule still refuses to replace a successful row with a blocked row. A later primary success replaces fallback evidence through the normal success update. Warnings returned by a profile that was called stay warnings and are not folded into the note.

5. **Reuse the report note channel with fixed fields.** Master note reason is `primary_master_request_failed_fallback_verified`, with `series_id`, `primary_source_profile`, `fallback_source_profile`, and redacted `primary_error`. Price-stage key downgrade reason is `missing_api_key_fallback_covered`, with `series_id`, `chain_source_profile`, `api_key_env`, and `fallback_filled_dates`. Add notes to the master-governance result and append them in the existing pipeline. The scheduler report already prints notes under 说明. Per-source note counts include these notes.

6. **Extend the price chain only for the key blocker.** `ConfiguredSourceChainProvider.fetch` keeps its current observation selection. In addition to downgrading a covered primary `provider_request_failed` warning, it downgrades a primary `missing_api_key` blocker to `missing_api_key_fallback_covered` for each series that fallback covered without a value conflict. An uncovered series gets a blocker with `series_id` and `api_key_env`. The pipeline treats any remaining provider blocker without `series_id` as blocking every series in that call. Other price warnings and blockers keep their current status effect.

7. **Redact before logging or storage.** Add one helper beside `_redact_url` and use it for retry-log exception text and for request errors copied into master metadata, blockers, and notes. The helper redacts the same query names `_redact_url` already redacts. Callers do not persist raw `str(exc)` for these HTTP clients.

## Risks / Trade-offs

- [Risk] A fallback success replaces the previous primary evidence row for that series. → Metadata names the verifying profile and the redacted primary error; the next primary success writes primary evidence again. Blocked refreshes still preserve the last successful row.
- [Risk] EIA can still consume the full 60-second budget and then return HTTP 504 from the edge at about 61 seconds, so the price chain may rely on fallback observations. → That is the existing price fallback. If fallback observations also fail, the run stays a warning or blocked rather than a success.
- [Risk] Two oil series that both exhaust EIA metadata and then EIA price requests cost about 2 × 3 × 60 seconds plus another 2 × 3 × 60 seconds, roughly 12 minutes, before fallback work. → `special_commodity_overseas_daily_price_sync` allows 3600 seconds. Keep the configured attempt count. Do not add an oil-specific short circuit.
- [Risk] `master_metadata_request_failed` today also covers an empty parsed payload that raises while selecting the first row. → Treat the existing reason code consistently. Do not add an empty-payload special case in the chain.
- [Risk] `config/11_futures.json` already contains unrelated AkShare edits beside the timeout line. → Commit only the `eia_official_api.timeout_seconds` hunk for this change.

## Migration Plan

1. Confirm `eia_official_api.timeout_seconds` is 60 and that both EIA metadata and observation calls read it from profile configuration. Leave every other hunk in that file untouched.
2. Add redaction, the pure chain decision, subset fallback calls, canonical venue and quality flags, the price-key downgrade, and the series-less provider-blocker backstop.
3. Verify with unit tests that mock governance and provider results for the configured chain, including mixed batches and a secret-bearing HTTP error. No production backfill is required.
4. Rollback by restoring the previous chain behavior and returning the EIA profile timeout to 30 seconds. Stored fallback evidence remains readable; the next successful primary run replaces it. No schema migration is involved.

The running process keeps configuration loaded at startup. Observing the 60-second budget in production requires a normal process restart outside this change.
