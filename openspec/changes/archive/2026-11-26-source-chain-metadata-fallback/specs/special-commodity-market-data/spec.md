## MODIFIED Requirements

### Requirement: Secrets are not persisted in tracked artifacts

The system SHALL read API keys and login credentials from environment variables or gitignored local runtime configuration and SHALL NOT persist them in tracked configuration, OpenSpec files, docs, logs, or Telegram reports.

#### Scenario: FRED provider initializes

- **WHEN** the FRED provider is enabled
- **THEN** it SHALL read the key from the configured environment variable
- **AND** logs SHALL report only configured/missing status, not the key value.

#### Scenario: Key is missing

- **WHEN** a required API key is absent for a profile governed directly
- **THEN** the task SHALL fail closed for that provider with `missing_api_key`
- **AND** the report SHALL include the environment variable name to configure.
- **WHEN** the missing key belongs to the primary profile of a configured source chain
- **THEN** chain master governance SHALL treat `missing_api_key` as a request-class failure and apply configured fallback metadata validation before blocking the chain series.

#### Scenario: Request errors are logged or stored

- **WHEN** a source request exception is written to a retry log, master-evidence metadata, blocker, or operator note
- **THEN** the text SHALL redact query values named `api_key`, `apikey`, `key`, `token`, `access_token`, `appkey`, or `secret`
- **AND** the stored or reported text SHALL NOT contain the credential value.

### Requirement: Operator reports are compact on normal success

Special commodity sync and backfill reports SHALL summarize normal success compactly and expand details only when failures, source gaps, unit warnings, pending credentials, or data quality issues exist. A success produced by configured source-chain metadata fallback SHALL still include the fallback explanation note.

#### Scenario: Daily sync succeeds normally

- **WHEN** all enabled daily commodity series sync without failures or warnings
- **THEN** the Telegram report SHALL include venue list, inserted/changed/unchanged counts, stale series count, and source-gap count
- **AND** it SHALL omit long per-series details.

#### Scenario: Source issue occurs

- **WHEN** a provider fails, an API key is missing, a unit is ambiguous, or a source gap is detected
- **THEN** the report SHALL switch to detailed mode grouped by venue.

#### Scenario: Long-running provider call remains observable

- **WHEN** a special-commodity provider call runs longer than its configured progress interval
- **THEN** the system SHALL emit periodic heartbeat logs with source, series, requested range, and elapsed time
- **AND** it SHALL emit a completion log with total provider-call duration.
- **AND** provider/governance progress SHALL use the task-domain logger and appear in `log/task.log`, not `log/sys.log`.

#### Scenario: Long-range source observations are audited

- **WHEN** a long-range dry-run returns observations
- **THEN** diagnostics SHALL retain first/latest date, annual counts, numeric range, nonpositive values, duplicate dates, largest absolute changes, currency, and unit per series
- **AND** a series with configured exchange-calendar evidence SHALL report expected-date coverage, missing samples, and longest missing run from persisted governed dates
- **AND** the system SHALL NOT synthesize expected dates from weekdays.

#### Scenario: Metadata fallback completes successfully

- **WHEN** a run finishes with status `success` and a configured source-chain metadata-fallback note
- **THEN** the operator report SHALL show success
- **AND** SHALL include the fallback explanation under the existing note section
- **AND** SHALL NOT present that note as a blocker or warning.

### Requirement: Special commodity scheduling is isolated from domestic futures

The system SHALL schedule non-domestic special commodity observations independently from domestic exchange futures while reusing the common task, database-channel, reporting, and governance infrastructure.

#### Scenario: LME daily sync is enabled

- **WHEN** the first production LME scope is activated
- **THEN** `special_commodity_overseas_daily_price_sync` SHALL run `lme_nonferrous` Tuesday-Saturday at 08:00 Asia/Shanghai
- **AND** `futures_market_data_sync` SHALL remain limited to the five configured domestic exchanges.

#### Scenario: Governed FRED oil spot series are enabled

- **WHEN** WTI and Brent FRED series have completed full-range master governance, source-observed date governance, and historical backfill
- **THEN** `special_commodity_overseas_daily_price_sync` SHALL include canonical `eia_energy_oil` in the same Tuesday-Saturday 08:00 Asia/Shanghai run as `lme_nonferrous`
- **AND** the common governance-first pipeline SHALL remain mandatory for both scopes.

#### Scenario: Canonical oil source chain resolves a date

- **WHEN** EIA and FRED both provide a WTI or Brent value for the same observation date
- **THEN** the canonical series SHALL select EIA and record any numerical conflict in diagnostics
- **WHEN** EIA lacks a date and FRED provides it
- **THEN** the canonical series SHALL use FRED with `primary_date_missing` lineage
- **AND** raw FRED series SHALL remain independently queryable for audit.

#### Scenario: Official API request fails transiently

- **WHEN** a FRED or EIA master-data or observation request fails with a transient TLS, connection, timeout, or HTTP error
- **THEN** the shared official-API request helper SHALL retry using that source profile's configured timeout, bounded attempts, and backoff
- **AND** a profile governed outside a configured source chain SHALL block the series only after all attempts fail
- **AND** a configured source chain SHALL apply configured fallback master validation after the primary profile's master-data attempts are exhausted, instead of blocking at that primary boundary.

#### Scenario: Governed monthly metal benchmarks are scheduled

- **WHEN** FRED/IMF copper and aluminum have completed historical governance and backfill
- **THEN** `special_commodity_international_monthly_price_sync` SHALL run twice per month on the configured schedule with a bounded multi-month lookback
- **AND** master and source-period governance SHALL run before observations are persisted
- **AND** the lookback SHALL allow delayed publication and historical revisions to update existing months idempotently.

#### Scenario: World Bank metals are compared with IMF/FRED

- **WHEN** World Bank Pink Sheet copper or aluminum is introduced for validation
- **THEN** it SHALL remain an independently identified series with its own source profile, unit, and observation lineage
- **AND** validation SHALL report overlap coverage, missing months, unit compatibility, level differences, monthly-return correlation, and revision behavior
- **AND** it SHALL NOT overwrite, average with, or act as an automatic fallback for IMF/FRED before explicit source-policy approval.

#### Scenario: Scheduled sync has no explicit dates

- **WHEN** the scheduler invokes special commodity daily sync without start/end dates
- **THEN** the task SHALL derive a configured rolling calendar-day lookback ending on the current local date
- **AND** the common master/date governance pipeline SHALL execute before observations are persisted.

#### Scenario: Another task occupies the same minute

- **WHEN** an enabled maintenance task is already configured at the LME start minute
- **THEN** the schedules SHALL be deconflicted before production enablement to avoid concurrent writes or resource contention.

## ADDED Requirements

### Requirement: Configured source chains validate master data through fallback profiles

Configured source-chain master governance SHALL own primary-to-fallback metadata validation. It SHALL resolve each chain profile's governance adapter from the chain governance map keyed by that profile's configured `governance_adapter`, and SHALL pass only the series still unresolved for that profile. The accept-or-continue decision SHALL be a callable function of the current governance outcome. Production code SHALL NOT branch on commodity id, series id, venue, or a named provider to select this behavior. The price chain SHALL keep its existing observation selection and SHALL additionally downgrade a covered primary `missing_api_key` as specified below.

A request-class failure means the profile did not return a usable metadata payload after its own configured retries. The request-class reasons are `missing_api_key` and `master_metadata_request_failed`, including transport timeout, connection failure, HTTP error, and non-JSON body. A blocker without a series id applies to every series in that governance call. Frequency mismatch, unit mismatch, and every other blocker produced from a parsed payload are semantic failures.

#### Scenario: Primary metadata request fails and fallback validates

- **WHEN** primary master governance for a chain series ends in a request-class failure
- **AND** a later configured chain profile returns metadata whose frequency and unit match the series
- **THEN** the chain SHALL accept that fallback result as the series master evidence
- **AND** SHALL record the verifying source profile and the primary request error on the evidence
- **AND** SHALL NOT describe that evidence as verification of the primary route
- **AND** SHALL continue the existing configured price-fetch chain for that series
- **AND** the run SHALL have status `success` when no other warnings or blockers remain
- **AND** the operator note SHALL use reason `primary_master_request_failed_fallback_verified`
- **AND** SHALL include `series_id`, `primary_source_profile`, `fallback_source_profile`, and redacted `primary_error`.

#### Scenario: Every configured profile fails the metadata request

- **WHEN** every configured profile in the chain ends in a request-class failure for a series
- **THEN** the series SHALL be blocked
- **AND** no observations for that series SHALL be written
- **AND** an existing successful `commodity_master_governance` record SHALL be retained.

#### Scenario: An answered profile reports a semantic mismatch

- **WHEN** a chain profile returns a parsed metadata payload whose frequency or unit does not match the series
- **THEN** the series SHALL be blocked for that mismatch
- **AND** the chain SHALL NOT ask a subsequent profile to override that mismatch.

#### Scenario: Primary metadata validates

- **WHEN** primary master governance succeeds for a series
- **THEN** the chain SHALL NOT call fallback governance adapters for that series.

#### Scenario: Metadata fallback does not hide a failed price fetch

- **WHEN** fallback metadata validates a series
- **AND** the existing price chain then produces a warning or blocker for that series
- **THEN** the run status SHALL follow that price outcome
- **AND** SHALL NOT be forced to success because metadata fallback succeeded.

#### Scenario: Primary price fetch lacks its API key and fallback covers the series

- **WHEN** the primary profile returns `missing_api_key` during price fetch
- **AND** a later chain profile supplies that series' dates without a value conflict
- **THEN** the chain SHALL replace the primary blocker for that series with a note
- **AND** the note reason SHALL be `missing_api_key_fallback_covered`
- **AND** the note SHALL include `series_id`, `chain_source_profile`, `api_key_env`, and `fallback_filled_dates`
- **AND** SHALL NOT include the credential value.

#### Scenario: Primary price fetch lacks its API key and a series stays uncovered

- **WHEN** primary `missing_api_key` applies to a series that fallback did not cover without conflict
- **THEN** the chain SHALL retain a blocker for that series
- **AND** the blocker SHALL include `series_id` and `api_key_env`.

#### Scenario: A provider blocker still has no series id

- **WHEN** a provider blocker remains without `series_id` after chain classification
- **THEN** observation persistence SHALL exclude every series in that provider call
- **AND** the run SHALL remain blocked.

#### Scenario: Master evidence keeps canonical identity

- **WHEN** the chain writes a master record
- **THEN** `venue` SHALL be the canonical series venue
- **AND** `source_profile` SHALL be the chain profile
- **AND** primary verification SHALL use quality flag `official_primary_with_governed_fallback` and `master_verified_by` equal to the primary profile
- **AND** fallback verification SHALL use quality flag `fallback_master_verified` and `master_verified_by` equal to the verifying profile
- **AND** a blocked record SHALL keep `governance_status` `blocked` and quality flag `unverified`.

#### Scenario: Only unresolved series call the next fallback

- **WHEN** a batch contains one series with successful primary governance and another with a request-class failure
- **THEN** the next fallback governance adapter SHALL receive only the request-class series
- **WHEN** a batch contains one semantic mismatch and one request-class failure
- **THEN** the semantic series SHALL NOT be sent to a fallback adapter
- **AND** the request-class series SHALL.

#### Scenario: A fallback profile cannot govern a series

- **WHEN** a fallback profile has no governance adapter in the chain governance map or no source mapping for a series
- **THEN** the chain SHALL skip that profile for that series and try the next configured profile
- **AND** SHALL block the series, retaining the profile and reason, when no later profile validates.

#### Scenario: Called profiles return warnings

- **WHEN** a profile that was actually called returns warnings
- **THEN** the chain SHALL retain those warnings
- **AND** SHALL NOT convert them into the metadata-fallback note.

#### Scenario: Full chain request failure preserves blocker detail

- **WHEN** every configured profile fails by request, including a missing key
- **THEN** the blocked master record SHALL use `governance_status` `blocked` and quality flag `unverified`
- **AND** a missing-key blocker SHALL retain `api_key_env`
- **AND** an existing successful master record SHALL be retained.

#### Scenario: Request budget remains profile configuration

- **WHEN** the canonical oil chain uses the EIA official source profile
- **THEN** that profile's configured `timeout_seconds` SHALL be 60
- **AND** the profile's metadata and observation requests SHALL both use that configured budget
- **AND** adapter code SHALL NOT embed a numeric timeout for the chain.
