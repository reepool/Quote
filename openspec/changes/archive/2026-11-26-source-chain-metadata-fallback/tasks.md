## 1. Profile request budget

- [x] 1.1 Confirm `eia_official_api.timeout_seconds` is 60 and that EIA metadata and observation requests both read that profile value through the existing retry helper. Do not stage the unrelated AkShare edits in `config/11_futures.json`.
- [x] 1.2 Add a focused test that a chain profile request uses its configured timeout and does not depend on a numeric literal in the adapter.

## 2. Chain master decision

- [x] 2.1 Add a pure function that folds one called profile's master result into the running records, blockers, and notes, and returns only the series that still need another profile. Request-class reasons are only `missing_api_key` and `master_metadata_request_failed`. A semantic mismatch blocks that series and removes it from the next call set.
- [x] 2.2 From `ConfiguredSourceChainGovernanceAdapter.govern_master`, call the primary adapter for the batch, then call each later profile's governance adapter only with the unresolved subset. Resolve adapters from `ConfiguredSourceChainGovernanceAdapter.GOVERNANCE` and map series with the existing mapping. A missing adapter or missing source mapping skips that profile for that series. Do not add a commodity, series, venue, or provider-name branch.
- [x] 2.3 Write canonical `venue` and chain `source_profile` on every record. Use `official_primary_with_governed_fallback` plus `master_verified_by` for primary success, `fallback_master_verified` plus `master_verified_by` and redacted `primary_error` for fallback success, and `blocked` / `unverified` for a final blocker. Preserve `api_key_env` on a missing-key blocker and do not replace an existing successful master row with a blocked row.
- [x] 2.4 Retain warnings from profiles that were called. Do not convert those warnings into the metadata-fallback note.

## 3. Price chain and secret redaction

- [x] 3.1 In `ConfiguredSourceChainProvider.fetch`, downgrade a primary `missing_api_key` blocker to note reason `missing_api_key_fallback_covered` for each series fallback covered without a value conflict. The note includes `series_id`, `chain_source_profile`, `api_key_env`, and `fallback_filled_dates`. An uncovered series keeps a blocker with `series_id` and `api_key_env`.
- [x] 3.2 In the governance pipeline, treat a provider blocker that still has no `series_id` as blocking every series in that provider call, so their observations are not persisted.
- [x] 3.3 Redact secret query values with one helper shared by the retry logger and by request errors copied into master metadata, blockers, and notes. Do not persist raw HTTP exception text.

## 4. Reporting

- [x] 4.1 Carry master-governance notes through the existing pipeline into run notes and the per-source note count. The master note reason is `primary_master_request_failed_fallback_verified`, with `series_id`, `primary_source_profile`, `fallback_source_profile`, and redacted `primary_error`.
- [x] 4.2 Confirm the current scheduler report shows those notes under 说明 on a success status and does not render them as warnings or blockers.

## 5. Verification

- [x] 5.1 Test primary request failure plus valid fallback metadata, both profiles failing by request, primary semantic mismatch, fallback semantic mismatch, primary success, and a missing primary key. Include a two-series batch where only one series is sent to fallback, and a batch where a semantic mismatch is not sent while a timeout is.
- [x] 5.2 Test that a primary price-stage `missing_api_key` becomes a note when fallback covers the series, and stays a series-scoped blocker when it does not. Test that a remaining series-less provider blocker prevents observation persistence.
- [x] 5.3 Test that an `HTTPError` whose URL contains `api_key=SECRET_VALUE` produces stored evidence and report text without `SECRET_VALUE`.
- [x] 5.4 Test that a later uncovered price failure remains a warning after metadata fallback succeeds.
- [x] 5.5 Run the focused special-commodity governance tests and review the diff for hardcoded symbols, timeouts, a second fetch path, or unrelated AkShare config hunks.
