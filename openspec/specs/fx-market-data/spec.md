# fx-market-data Specification

## Purpose
Define the local-first FX market-data domain, including storage isolation, source governance, publication calendars, derivations, quality checks, APIs, scheduler tasks, and downstream consumer contracts.
## Requirements
### Requirement: FX Data Uses Dedicated Local Storage
The system SHALL store FX-domain currencies, instruments, series, observations, derivations, calendars, source manifests, quality issues, and readiness snapshots in a dedicated local `data/fx.db` database.

#### Scenario: FX schema initializes independently
- **WHEN** the FX storage layer is initialized
- **THEN** it SHALL create FX-domain tables in `data/fx.db`
- **AND** it SHALL NOT require `quotes.db`, `research.db`, `valuation.db`, or `futures.db` to exist

#### Scenario: FX writes stay isolated
- **WHEN** FX rates, indices, source manifests, or derivations are written
- **THEN** the system SHALL write them only to FX-domain tables
- **AND** it SHALL NOT insert FX observations into stock quote, futures, valuation, financial, or general research tables

### Requirement: FX Configuration Is Domain Scoped
The system SHALL load FX-domain storage, source profiles, scope definitions, rate limits, derivation policies, and readiness thresholds from a dedicated FX configuration surface while exposing runtime settings through `ResearchConfig.modules["fx_market_data"]`.

#### Scenario: FX config is loaded
- **WHEN** application configuration is loaded
- **THEN** the FX module SHALL expose `enabled`, storage, source, universe, derivation, quality, and scheduler settings under `ResearchConfig.modules["fx_market_data"]`
- **AND** the configured storage path SHALL default to `data/fx.db`

#### Scenario: FX module is disabled
- **WHEN** `fx_market_data.enabled` is false
- **THEN** FX sync jobs and readiness SHALL return disabled status before provider or database write work starts

### Requirement: FX Master Data Preserves Currency And Instrument Semantics
The FX module SHALL maintain governed master data for currencies, currency pairs, currency indices, effective exchange-rate indices, forward points, and swap points.

#### Scenario: First-phase currencies are seeded
- **WHEN** FX master sync runs for the first-phase scope
- **THEN** it SHALL seed or update `CNY`, `CNH`, `USD`, `EUR`, and `JPY` currency rows with active status and metadata

#### Scenario: Currency pair direction is explicit
- **WHEN** a currency-pair instrument is stored
- **THEN** it SHALL include `base_currency`, `quote_currency`, `quote_multiplier`, `market_scope`, `instrument_type`, `category`, and active status

#### Scenario: Dollar index is not stored as a currency pair
- **WHEN** a dollar-index instrument such as `FXI.DXY` or a trade-weighted dollar index is stored
- **THEN** it SHALL use `instrument_type=currency_index`
- **AND** it SHALL NOT be stored as a two-currency exchange rate

### Requirement: FX Series Preserves Source Profile And Rate Type
The FX module SHALL distinguish series by instrument, source profile, rate type, frequency, timezone, publication lag, lifecycle, and quality policy.

#### Scenario: First-phase series are seeded
- **WHEN** FX master sync runs
- **THEN** it SHALL create configured first-phase series for official CNY fixings, CNH spot or fallback series, derived CNH cross-rates, and dollar-index series

#### Scenario: Same pair from different sources is stored separately
- **WHEN** two sources publish values for the same instrument and observation date
- **THEN** the system SHALL preserve separate series or source profiles
- **AND** it SHALL NOT silently overwrite official data with aggregated public data

### Requirement: FX Observations Preserve Quote Unit And Lineage
Every stored FX observation SHALL preserve observation date, value, base currency, quote currency, quote multiplier, source profile, source reference, quality flag, revision id, and metadata.

#### Scenario: Official JPY fixing uses 100 JPY quote unit
- **WHEN** an official source publishes JPY/CNY as 100 JPY equals a CNY amount
- **THEN** the stored observation SHALL preserve `quote_multiplier=100`
- **AND** conversions SHALL account for that multiplier instead of treating the value as 1 JPY

#### Scenario: Observation uniqueness is enforced
- **WHEN** an FX observation is written for a series, observation date, and source profile
- **THEN** repeated runs SHALL update the same logical row idempotently
- **AND** the storage layer SHALL NOT create duplicate observations for the same logical source value

#### Scenario: Fallback source writes are labeled
- **WHEN** an aggregated public provider writes an FX observation
- **THEN** the observation SHALL carry `quality_flag=aggregated_public` or a more specific non-official fallback flag
- **AND** it SHALL NOT be labeled as official

### Requirement: FX Source Adapters Use A Common Gateable Contract
FX providers SHALL implement a common adapter contract that emits normalized observations, source manifest metadata, parser version, quality flags, raw payload references, and structured errors.

#### Scenario: Official source adapter succeeds
- **WHEN** an official FX source adapter returns observations
- **THEN** each observation SHALL include source profile, source interface, source URL or payload reference, parser version, fetched time, and quality flag

#### Scenario: Source dependency is unavailable
- **WHEN** a provider dependency, API key, official endpoint, or parser requirement is unavailable
- **THEN** the adapter SHALL fail explicitly with a structured blocker or warning
- **AND** the scheduler report SHALL expose the source-profile failure

#### Scenario: Disabled source is requested
- **WHEN** a sync requests a configured source profile that is disabled
- **THEN** the run SHALL skip that profile before external requests
- **AND** it SHALL report the disabled source profile in run metadata

### Requirement: FX Sync Supports Scoped Universe Selection
The FX module SHALL support scopes that select targets by currency, instrument type, market scope, instrument id, series id, source profile, rate type, and frequency.

#### Scenario: Default RMB core scope is resolved
- **WHEN** an FX sync runs without explicit targets
- **THEN** the system SHALL resolve the configured default first-phase RMB scope
- **AND** the run summary SHALL include the requested scope and concrete series ids used for execution

#### Scenario: Explicit series override broader filters
- **WHEN** an FX sync request supplies explicit `series_ids`
- **THEN** the system SHALL operate only on those series
- **AND** broader currency or source filters SHALL NOT expand the target set

#### Scenario: Scope resolves to no series
- **WHEN** the requested FX scope cannot resolve any enabled series
- **THEN** the run SHALL stop before provider requests
- **AND** it SHALL report `empty_fx_download_scope`

### Requirement: FX Publication Calendar Governance Is Independent
The FX module SHALL govern observation dates, publication dates, source business days, and source lag without relying on futures trading calendars.

#### Scenario: Official fixing is missing on a holiday
- **WHEN** an official fixing source has no observation on a source holiday or non-publication date
- **THEN** quality checks SHALL distinguish expected non-publication from source failure

#### Scenario: CNH market series publishes on different dates
- **WHEN** a CNH market source publishes on a date that is not a mainland official fixing publication day
- **THEN** the FX calendar governance SHALL evaluate it using the source profile's own calendar policy

### Requirement: FX Derivations Are Auditable
The FX module SHALL support inverse-rate and cross-rate derivations with formula lineage, source series ids, date matching policy, maximum source lag, quality policy, and input hashes.

#### Scenario: Inverse conversion is requested
- **WHEN** a caller requests `CNY/USD` and only `USD/CNY` is stored for the requested date
- **THEN** the FX service SHALL return the inverse rate with derivation metadata
- **AND** it SHALL identify the source series and source observation date

#### Scenario: CNH cross-rate is derived
- **WHEN** a configured `EUR/CNH` or `JPY/CNH` derived series is generated
- **THEN** the system SHALL record the source series ids, formula, input observation dates, source lag, and derived quality flag

#### Scenario: Required source rate is unavailable
- **WHEN** a derivation cannot find source observations within the configured date policy and lag limit
- **THEN** it SHALL not write a derived observation
- **AND** it SHALL emit a derivation gap in readiness or quality issues

### Requirement: FX Quality Checks Detect Missing And Suspicious Data
The FX module SHALL provide quality checks for missing observations, stale series, abnormal jumps, source conflicts, cross-market basis monitoring, invalid quote multipliers, and derivation gaps.

#### Scenario: Series is stale
- **WHEN** a required daily FX series has no acceptable observation within the configured stale threshold
- **THEN** readiness SHALL include a stale-series blocker or warning with the affected series id

#### Scenario: Source values conflict
- **WHEN** official and fallback observations for the same instrument and date differ beyond the configured tolerance
- **THEN** quality checks SHALL record a source-conflict issue
- **AND** official observations SHALL remain the preferred production source unless configured otherwise

#### Scenario: Direct and derived cross-market values diverge
- **WHEN** a direct market CNH series and its derived cross-rate series differ beyond the configured basis tolerance
- **THEN** quality checks SHALL record `cross_market_basis_monitoring`
- **AND** it SHALL NOT classify this as a same-instrument source conflict
- **AND** the issue SHALL be warning severity unless explicitly configured otherwise

### Requirement: FX Read APIs Are Local Only
The system SHALL expose FX dictionary, series, rates, conversion, indices, and readiness through `/api/v1/research/fx/*` read APIs backed by local FX storage.

#### Scenario: Rate read is requested
- **WHEN** a caller requests FX rates by series id and date range
- **THEN** the API SHALL return local observations with source profile, quality flag, quote direction, quote multiplier, and metadata
- **AND** it SHALL NOT fetch remote data during the request

#### Scenario: Currency conversion is requested
- **WHEN** a caller requests currency conversion for amount, source currency, target currency, and date
- **THEN** the API SHALL use a local direct, inverse, or configured derived rate as of the requested date
- **AND** it SHALL return the FX series id, FX date, conversion policy, and lineage

#### Scenario: Currency-index observations are requested
- **WHEN** a caller requests a currency-index series such as the trade-weighted dollar index by series id and date range
- **THEN** the API SHALL return local index observations with source profile, quality flag, index metadata, and lineage
- **AND** it SHALL reject non-index FX series for the index-observation endpoint
- **AND** it SHALL NOT treat currency-index values as exchange rates for conversion

#### Scenario: Local rate is missing
- **WHEN** no acceptable local FX rate exists for a conversion request
- **THEN** the API SHALL return a structured missing-rate response or error
- **AND** it SHALL NOT perform an external provider call as a fallback

### Requirement: FX Scheduler Tasks Are Explicit And Reportable
The scheduler SHALL expose explicit FX tasks for master sync, calendar governance, rate backfill, rate sync, derivation sync, and quality checks.

#### Scenario: Historical backfill lacks dates
- **WHEN** `fx_rate_backfill` is requested without required start and end dates
- **THEN** the task SHALL fail before provider requests with a clear validation error

#### Scenario: Daily sync completes
- **WHEN** `fx_rate_sync` completes
- **THEN** the scheduler and Telegram report SHALL include status, scope, source profiles, inserted/updated/unchanged counts, quality issues, derivation gaps, and readiness summary

#### Scenario: Quality gate blocks production write
- **WHEN** a non-dry-run FX task fails a configured source or quality gate
- **THEN** the scheduler SHALL report blocked status
- **AND** it SHALL not continue to downstream derivation or consumer refresh steps that depend on the failed data

### Requirement: FX Consumers Use Local Availability-Aware Rates
Downstream commodity, DCF, macro, and portfolio consumers SHALL use local FX rates with explicit date, cutoff, source, and conversion policy.

#### Scenario: Commodity conversion needs FX
- **WHEN** a commodity observation in USD is converted to CNY for downstream analysis
- **THEN** the consumer SHALL request a local FX conversion for the commodity observation date or configured cutoff date
- **AND** it SHALL record FX series id, FX date, rate, source profile, and conversion policy

#### Scenario: FX is missing for downstream conversion
- **WHEN** a downstream consumer cannot find an acceptable local FX rate
- **THEN** it SHALL return a readiness gap or partial diagnostic
- **AND** it SHALL NOT perform an ad hoc remote FX fetch inside the consumer calculation

### Requirement: FX Observations Emit Change Records
FX rate sync SHALL emit changelog records for inserted or materially changed direct and derived FX observations.

#### Scenario: Direct FX observation value changes
- **WHEN** a stored FX observation is refetched with a changed value or semantic hash
- **THEN** the FX storage path SHALL append an FX-domain change record
- **AND** the record SHALL include series id, observation date, source, and revision id where available

### Requirement: Derived FX Changes Preserve Lineage
Derived FX observations SHALL record source lineage so callers can understand why a derived cross-rate changed.

#### Scenario: Derived CNH cross-rate changes
- **WHEN** a derived FX observation changes because one source leg changed
- **THEN** the change record or row metadata SHALL expose lineage or input hash information for the derivation

### Requirement: FX Changelog Does Not Replace Calendar Governance
FX changelog emission SHALL NOT replace FX publication calendar governance or quality checks.

#### Scenario: No observation expected
- **WHEN** FX calendar governance marks a date as an expected non-publication date
- **THEN** FX sync SHALL NOT emit a missing-data change record solely because no observation was written

