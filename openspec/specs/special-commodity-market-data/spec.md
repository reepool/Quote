## Purpose

Define requirements for non-domestic-futures commodity market data such as Brent/WTI, LME metals, thermal-coal spot or long-term contract references, chemical spot indices, inventory indicators, and other DCF-facing commodity inputs.
## Requirements
### Requirement: Special commodity data uses venue-scoped governance

The system SHALL model special commodity data using a venue/category/commodity/series scope that generalizes the domestic futures exchange/category/instrument/series pattern.

#### Scenario: Venue scope is resolved

- **WHEN** a run is requested with `venues=["FRED"]` and `categories=["energy"]`
- **THEN** the system SHALL resolve the scope to enabled FRED energy commodity series
- **AND** the run metadata SHALL preserve both requested selectors and resolved `commodity_ids` and `series_ids`.

#### Scenario: Explicit series overrides venue scope

- **WHEN** a run is requested with explicit `series_ids`
- **THEN** the system SHALL operate only on those series
- **AND** broader `venues`, `categories`, or `commodity_ids` SHALL NOT expand the target set.

#### Scenario: Scope resolves empty

- **WHEN** a requested special commodity scope resolves no enabled series
- **THEN** the run SHALL stop before provider requests
- **AND** it SHALL report `empty_special_commodity_scope`.

### Requirement: Special commodity storage is isolated from futures bars

The system SHALL store special commodity master data, series, observations, policy events, and source manifests in isolated `commodity_*` tables and SHALL NOT write non-futures observations into `futures_price_bars`.

#### Scenario: WTI spot observation is stored

- **WHEN** FRED WTI spot data is synchronized
- **THEN** rows SHALL be written to `commodity_price_observations`
- **AND** no rows SHALL be written to `futures_price_bars` for the WTI spot series.

#### Scenario: Futures and commodity tables coexist

- **WHEN** futures storage initializes
- **THEN** `commodity_*` table creation SHALL NOT modify existing futures table semantics
- **AND** existing domestic futures API responses SHALL remain compatible.

### Requirement: Provider outputs are normalized and source-labeled

Every special commodity provider SHALL normalize rows to the canonical observation contract and preserve the real source profile.

#### Scenario: Official API row is normalized

- **WHEN** FRED or EIA returns a price observation
- **THEN** the normalized row SHALL include `series_id`, `observation_date`, `value`, `currency`, `unit`, `raw_value`, `raw_unit`, `source_profile`, `source_url`, `quality_flag`, and metadata.

#### Scenario: AkShare wraps a public-web source

- **WHEN** AkShare is used to fetch 100ppi spot or basis data
- **THEN** the stored `source_profile` SHALL identify the real source as 100ppi public web data
- **AND** the row SHALL NOT be labeled as official merely because AkShare returned it.

#### Scenario: Unit is unclear

- **WHEN** a provider cannot verify a source unit, specification, region, tax basis, or conversion basis
- **THEN** the row SHALL preserve the raw unit and mark quality as partial or lower
- **AND** diagnostics SHALL NOT use the row as clean comparable input without a warning.

### Requirement: Commodity currency and unit metadata is preserved

The system SHALL preserve original commodity observation currency and unit metadata and SHALL NOT maintain FX rates or perform currency conversion inside the commodity data layer.

#### Scenario: USD commodity is stored

- **WHEN** a USD-denominated commodity series such as Brent, WTI, LME copper, or FRED/IMF copper is synchronized
- **THEN** the stored commodity observation SHALL preserve the USD currency and original unit
- **AND** the commodity layer SHALL NOT create or write an FX-derived CNY observation.

#### Scenario: A target currency is needed

- **WHEN** DCF or diagnostics require a commodity value in a target currency different from the stored commodity currency
- **THEN** the commodity read or diagnostics layer SHALL request a governed local conversion from the independent FX module
- **AND** it SHALL include source currency, target currency, observation date, source profile, unit, FX series id, FX date, conversion policy, and lineage metadata when conversion succeeds.

#### Scenario: Local FX conversion is unavailable

- **WHEN** a target-currency conversion is required but the independent FX module has not provided a governed conversion result, or the result is stale, disabled, or below quality policy
- **THEN** DCF readiness SHALL report a dependency gap such as `requires_fx_conversion`
- **AND** commodity providers SHALL NOT perform remote FX lookup as a workaround.

#### Scenario: Unit conversion needs assumptions

- **WHEN** a requested comparison or DCF input requires commodity unit conversion such as barrel-to-metric-ton
- **THEN** the system SHALL require a configured commodity-specific conversion rule
- **AND** it SHALL preserve the conversion assumption and source in metadata.

#### Scenario: Mixed currency comparison is requested

- **WHEN** diagnostics, spreads, or DCF inputs attempt to combine commodity series with incompatible currencies
- **THEN** the system SHALL use a governed local FX conversion result or fail closed with a readiness blocker.

### Requirement: Calendar governance supports observation and publication periods

The system SHALL support source-appropriate calendar governance for daily observations, monthly observations, source publication days, and policy effective periods.

#### Scenario: Monthly benchmark is synced

- **WHEN** a World Bank or FRED/IMF monthly copper price is synced
- **THEN** the system SHALL store it with a governed monthly observation date
- **AND** DCF lookup SHALL use the latest locally available observation on or before the valuation date.

#### Scenario: Policy event is synced

- **WHEN** a thermal-coal long-term contract or policy-price event is imported
- **THEN** the system SHALL store effective start and end dates
- **AND** it SHALL NOT fabricate daily prices unless an explicit derived series rule is configured.
- **AND** configured events SHALL validate commodity identity, source profile, effective dates, currency, unit, price-range ordering, and evidence URL before persistence.
- **AND** an official reasonable range SHALL preserve its lower and upper bounds without fabricating a midpoint or labeling the range as an observed transaction price.

#### Scenario: NBS thermal-coal ten-day benchmark is synced

- **WHEN** the official NBS Shanxi premium-blend 5500 kcal market-price series is synchronized
- **THEN** the system SHALL govern the product name, 5500 kcal specification, CNY-per-ton unit, and official article lineage before persistence
- **AND** it SHALL use the ten-day period end as `observation_date` while preserving period start, period end, and publication date separately
- **AND** period ends SHALL follow the source convention of days 10, 20, and 30, with February using its actual final day, rather than assuming every lower period ends on the calendar month end
- **AND** it SHALL identify the value as a wholesale/sales market-price reference rather than a futures price or long-term-contract price.
- **AND** an eligible historical ten-day period without a discovered official article SHALL be reported as an unresolved source-period warning rather than silently omitted or inferred.
- **AND** the NBS adapter SHALL normalize historical title variants such as `1-10`, `1日-10日`, and Chinese dash forms before period governance.
- **AND** official schedule cancellations SHALL be represented as configured observation exceptions with a reason and evidence URL.
- **AND** the report SHALL distinguish theoretical periods, governed exceptions, discovered periods, and unresolved periods.
- **AND** governed exceptions SHALL NOT downgrade the task, while unresolved periods SHALL remain warnings.
- **AND** an upstream business rejection or anomalous empty broad search SHALL stop exact-period requests and SHALL be reported as a source failure rather than a legitimate empty dataset.

#### Scenario: Public-web daily page is missing

- **WHEN** a 100ppi daily page is missing for a requested date
- **THEN** the system SHALL record a source gap or warning
- **AND** it SHALL NOT fill the date with a weekday estimate.

#### Scenario: Daily source has no exchange calendar

- **WHEN** a FRED, EIA, or 100ppi daily series is governed
- **THEN** the calendar SHALL contain only source-observed dates or explicitly published source dates
- **AND** weekdays without source evidence SHALL NOT be marked as verified expected observations.

#### Scenario: Exchange-traded overseas source is enabled

- **WHEN** an LME or another exchange-traded overseas series is enabled
- **THEN** its adapter SHALL provide exchange-calendar evidence or source-observed trading-date evidence with an explicit quality label
- **AND** absent source dates SHALL remain unresolved rather than being inferred as weekdays or exchange closures.

### Requirement: Every series has source-backed master governance

Every active special commodity series SHALL pass a concrete source adapter that validates its identity and available name, frequency, currency, unit, lifecycle, and source lineage before observations are persisted.

#### Scenario: FRED series is governed

- **WHEN** a FRED commodity series is synchronized
- **THEN** its adapter SHALL query FRED series metadata and validate the configured symbol, frequency, and unit
- **AND** the governance evidence SHALL be stored separately from the static configuration seed.

#### Scenario: Public-web series is governed

- **WHEN** a 100ppi series is synchronized
- **THEN** its adapter SHALL validate configured mapping fields against actual source rows
- **AND** unavailable name/specification/unit evidence SHALL remain explicitly partial rather than being promoted to official quality.

#### Scenario: Governance adapter is missing

- **WHEN** an active series has no registered master or date governance adapter
- **THEN** backfill and sync SHALL stop that series with `missing_commodity_governance_adapter`
- **AND** task code SHALL NOT bypass the gate with a source-specific exception.

### Requirement: Backfill and sync enforce governance before persistence

Backfill, daily sync, and monthly sync SHALL execute the same governance-first pipeline for the exact requested scope and date range.

#### Scenario: Governance succeeds

- **WHEN** master governance and date governance succeed for a series
- **THEN** only observations whose dates are present in the governed source calendar SHALL be eligible for persistence.

#### Scenario: Governance is blocked

- **WHEN** master governance or date governance is blocked
- **THEN** no observations for that series SHALL be written
- **AND** the report SHALL identify the venue, series, governance stage, and blocker.

#### Scenario: Source payload includes dates and values together

- **WHEN** the source exposes observation dates only in the value payload
- **THEN** the provider MAY fetch that payload once and share it with the date-governance adapter
- **AND** the date gate SHALL complete before observation rows are persisted.

### Requirement: Secrets are not persisted in tracked artifacts

The system SHALL read API keys and login credentials from environment variables or gitignored local runtime configuration and SHALL NOT persist them in tracked configuration, OpenSpec files, docs, logs, or Telegram reports.

#### Scenario: FRED provider initializes

- **WHEN** the FRED provider is enabled
- **THEN** it SHALL read the key from the configured environment variable
- **AND** logs SHALL report only configured/missing status, not the key value.

#### Scenario: Key is missing

- **WHEN** a required API key is absent
- **THEN** the task SHALL fail closed for that provider with `missing_api_key`
- **AND** the report SHALL include the environment variable name to configure.

### Requirement: LME aggregated daily data has governed primary and fallback sources

The system SHALL collect the AkShare-supported LME copper, aluminium, zinc, lead, nickel, and tin 3-month daily market-proxy series through a configuration-driven provider chain using Sina as primary and Eastmoney as fallback. Because AkShare identifies the Sina instruments as CFDs rather than exchange futures, the system SHALL preserve that classification and SHALL NOT label the rows as official LME futures or Closing Prices.

#### Scenario: Sina primary succeeds

- **WHEN** Sina returns a valid mapped LME 3-month payload for the requested series
- **THEN** the provider SHALL use the Sina rows and SHALL NOT request Eastmoney when the requested dates are covered
- **AND** lineage SHALL identify Sina, its source symbol, returned columns, lifecycle coverage, and OHLCV payload.

#### Scenario: Sina has isolated date gaps

- **WHEN** Sina succeeds but a series is missing dates observed for peer LME products in the requested range
- **THEN** the adapter SHALL request the configured Eastmoney fallback only for affected series and fill only matching missing dates
- **AND** every filled row SHALL preserve the actual Eastmoney source profile, symbol, fallback reason, and attempts
- **AND** dates absent from both sources SHALL remain unresolved unless a configured source-evidenced market-disruption exception applies.

#### Scenario: Daily primary data is complete

- **WHEN** a normal daily run finds every requested LME series covered by Sina
- **THEN** it SHALL make no Eastmoney request
- **AND** historical cross-source auditing SHALL remain an explicit backfill/quality operation rather than a mandatory daily full download.

#### Scenario: Cross-source quality differs

- **WHEN** source rows have missing dates or a close outside the reported intraday low/high range
- **THEN** reports SHALL disclose primary rows, fallback-filled rows, unresolved gaps, governed exceptions, and OHLC consistency counts
- **AND** the adapter SHALL preserve raw values and SHALL NOT rewrite source OHLC fields to manufacture consistency.

#### Scenario: Sina primary request fails

- **WHEN** the Sina request fails, returns an empty full payload, or lacks required date/close fields
- **THEN** the provider SHALL try the configured Eastmoney symbol through the same provider contract
- **AND** the task SHALL report `primary_provider_failed_fallback_used` when fallback succeeds or block the series when all candidates fail.

#### Scenario: LME product master data is governed

- **WHEN** an LME series is synchronized or backfilled
- **THEN** governance SHALL validate AkShare contract detail, CFD/market-proxy type, product name, primary and fallback codes, LME exchange identity, 3-month tenor, USD currency, metric-ton unit, multiplier, tick size, payload schema, and source-observed lifecycle
- **AND** unknown or unmapped AkShare LME products SHALL be reported as master-data discovery warnings rather than silently ignored.

#### Scenario: LME trading dates are governed

- **WHEN** an LME source returns daily rows
- **THEN** only the returned observation dates SHALL be persisted as source-observed trading dates before price persistence
- **AND** the system SHALL NOT create closed-day rows or expected weekdays without additional source evidence.

### Requirement: Special commodity diagnostics are DCF-ready

The system SHALL compute local diagnostics for special commodity series and expose them for DCF readiness without remote fetches during valuation.

#### Scenario: DCF requests Brent input

- **WHEN** DCF readiness needs Brent price assumptions
- **THEN** it SHALL read local `commodity_*` data and diagnostics
- **AND** it SHALL NOT invoke FRED, EIA, AkShare, LME, or any remote provider.

#### Scenario: Required commodity input is stale

- **WHEN** the latest local observation exceeds configured staleness limits
- **THEN** readiness SHALL report a commodity input gap with series id, latest date, source profile, and required freshness.

### Requirement: Operator reports are compact on normal success

Special commodity sync and backfill reports SHALL summarize normal success compactly and expand details only when failures, source gaps, unit warnings, pending credentials, or data quality issues exist.

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

### Requirement: Special commodity scheduling is isolated from domestic futures

The system SHALL schedule non-domestic special commodity observations independently from domestic exchange futures while reusing the common task, database-channel, reporting, and governance infrastructure.

#### Scenario: LME daily sync is enabled

- **WHEN** the first production LME scope is activated
- **THEN** `special_commodity_price_sync` SHALL run `lme_nonferrous` Tuesday-Saturday at 08:00 Asia/Shanghai
- **AND** `futures_market_data_sync` SHALL remain limited to the five configured domestic exchanges.

#### Scenario: Governed FRED oil spot series are enabled

- **WHEN** WTI and Brent FRED series have completed full-range master governance, source-observed date governance, and historical backfill
- **THEN** `special_commodity_price_sync` SHALL include canonical `eia_energy_oil` in the same Tuesday-Saturday 08:00 Asia/Shanghai run as `lme_nonferrous`
- **AND** the common governance-first pipeline SHALL remain mandatory for both scopes.

#### Scenario: Canonical oil source chain resolves a date

- **WHEN** EIA and FRED both provide a WTI or Brent value for the same observation date
- **THEN** the canonical series SHALL select EIA and record any numerical conflict in diagnostics
- **WHEN** EIA lacks a date and FRED provides it
- **THEN** the canonical series SHALL use FRED with `primary_date_missing` lineage
- **AND** raw FRED series SHALL remain independently queryable for audit.

#### Scenario: Official API request fails transiently

- **WHEN** a FRED or EIA master-data or observation request fails with a transient TLS or connection error
- **THEN** the shared official-API request helper SHALL retry using configured bounded attempts and backoff
- **AND** the series SHALL be blocked only after all attempts fail.

#### Scenario: Governed monthly metal benchmarks are scheduled

- **WHEN** FRED/IMF copper and aluminum have completed historical governance and backfill
- **THEN** `special_commodity_price_monthly_sync` SHALL run twice per month on the configured schedule with a bounded multi-month lookback
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

### Requirement: Commodity Observations Emit Change Records
Special commodity daily and monthly observation syncs SHALL emit changelog records for inserted and materially changed observations.

#### Scenario: Daily commodity observation changes
- **WHEN** a domestic or overseas commodity observation is refetched with a changed semantic hash
- **THEN** the commodity storage path SHALL append a commodity-domain change record
- **AND** the record SHALL identify series id, observation date or period, source, and source profile

### Requirement: Commodity Policy Discovery Is Domain-Isolated
Special commodity policy discovery, candidate review, and policy-event promotion SHALL emit policy-domain changes separately from price-observation changes.

#### Scenario: Policy candidate is promoted
- **WHEN** a policy candidate is approved and promoted into an event record
- **THEN** the changelog SHALL classify the change under the policy/event domain
- **AND** price-only commodity change queries SHALL NOT include the policy event

### Requirement: Commodity Dry Runs Do Not Emit Persistent Changes
Special commodity syncs running in dry-run mode SHALL report would-write counters but SHALL NOT persist changelog rows.

#### Scenario: Dry run finds changed rows
- **WHEN** a dry-run commodity sync detects observations that would be changed
- **THEN** the task result SHALL report would-write or changed estimates
- **AND** no persistent change watermark SHALL be advanced

