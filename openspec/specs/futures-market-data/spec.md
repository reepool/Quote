## Purpose
Define the futures market-data domain requirements for scoped universe selection, master-data discovery, governance, and readiness.
## Requirements
### Requirement: Futures downloads support scoped universe selection
The futures market-data domain SHALL support download and update scopes that select targets by exchange, category, instrument, series, and series type.

#### Scenario: Exchange and category scope is resolved
- **WHEN** a futures run is requested with `exchanges=["GFEX"]` and `categories=["all"]`
- **THEN** the system SHALL resolve the scope to all enabled GFEX futures instruments and requested series types
- **AND** the run summary SHALL include the resolved instrument and series identifiers

#### Scenario: Explicit series overrides broader scope
- **WHEN** a futures run is requested with explicit `series_ids`
- **THEN** the system SHALL operate only on those series
- **AND** broader exchange or category selectors SHALL NOT expand the target set

### Requirement: Futures scope supports all macros
The futures scope selector SHALL support `all` in exchange and category selectors as a macro that is expanded before execution.

#### Scenario: All exchanges are requested
- **WHEN** `exchanges=["all"]` is configured
- **THEN** the selector SHALL expand it to currently enabled futures exchanges
- **AND** it SHALL NOT include future-extension markets that are configured but disabled

#### Scenario: All categories are requested for an exchange
- **WHEN** `categories=["all"]` is configured with a bounded exchange list
- **THEN** the selector SHALL expand it to the enabled categories present under those exchanges

#### Scenario: Scope resolves to no instruments
- **WHEN** a scope cannot resolve any enabled futures instrument
- **THEN** the run SHALL stop before provider requests and report `empty_futures_download_scope`

### Requirement: Futures run metadata preserves requested and resolved scope
Every scoped futures run SHALL record both the operator-requested scope and the fully resolved execution target.

#### Scenario: Scope contains all macro
- **WHEN** a run is requested with `all`
- **THEN** run metadata SHALL preserve the requested `all` value
- **AND** separately include concrete exchanges, categories, instrument ids, and series ids used for execution

### Requirement: Futures master discovery shall persist unknown exchange varieties
The system SHALL persist unknown futures variety symbols observed in official exchange payloads as structured discovery candidates.

#### Scenario: Unknown variety appears in official daily rows
- **WHEN** official exchange daily data contains a variety symbol that is not mapped to an active `futures_instruments` row for that exchange
- **THEN** the system SHALL create or update a discovery candidate
- **AND** the candidate SHALL include exchange, variety symbol, first seen trade date, last seen trade date, observed contracts, source profile, source interface, and raw evidence references.

#### Scenario: Existing candidate is seen again
- **WHEN** the same unknown exchange variety is observed on a later verified trading day
- **THEN** the system SHALL update last seen date and observed contract samples
- **AND** it SHALL NOT create duplicate discovery candidates.

### Requirement: Futures daily sync shall enrich unknown products before price writes
The futures daily-sync and backfill flows SHALL run product master discovery and enrichment for newly observed unknown varieties before writing price bars for the affected exchange scope.

#### Scenario: Unknown product appears during daily sync
- **WHEN** a daily sync or backfill run observes official daily rows for a variety that is not mapped to an active futures instrument
- **THEN** the system SHALL create or update a discovery candidate
- **AND** it SHALL invoke the configured exchange product enrichment adapter before deciding whether the affected rows can be written.

#### Scenario: Product is auto-promoted in the same run
- **WHEN** enrichment supplies all required production master fields with acceptable quality
- **THEN** the system SHALL promote the candidate to `futures_instruments` and `futures_series`
- **AND** it SHALL reprocess the current run's official rows for that product without requiring a second manual run.

#### Scenario: Existing product master is refreshed by enrichment
- **WHEN** master governance runs for an exchange with active root instruments
- **THEN** the system SHALL use the same exchange adapter contract to refresh existing root product fields before contract discovery
- **AND** it SHALL update `futures_instruments` and `futures_series` only when core fields change or official product-spec evidence is available
- **AND** it SHALL NOT rewrite existing master data merely because a built-in seed repeats already stored values.

#### Scenario: Product remains pending
- **WHEN** enrichment cannot supply a required field or detects conflicting evidence
- **THEN** the system SHALL keep the candidate out of production master data
- **AND** it SHALL skip price writes only for the unresolved product unless strict exchange-level blocking is configured
- **AND** the run report SHALL list missing fields, affected contracts, and skipped row counts.

### Requirement: Futures product enrichment shall preserve source evidence
The system SHALL preserve auditable source evidence for every automatically enriched or auto-promoted futures product.

#### Scenario: Adapter supplies enriched fields
- **WHEN** an exchange adapter supplies `name`, `category`, `currency`, `unit`, or optional product fields
- **THEN** the system SHALL record each field's source type, source interface, source URL or payload reference, and quality flag in discovery metadata and promoted instrument metadata where applicable.

#### Scenario: Governed local rule metadata is used
- **WHEN** an adapter uses governed local rule metadata to fill fields not directly exposed by the official interface
- **THEN** the system SHALL mark those fields as governed rule evidence
- **AND** it SHALL NOT label them as raw official values.

### Requirement: Futures master discovery shall use exchange adapters for enrichment
The system SHALL expose a standard adapter interface for enriching discovery candidates from exchange product-rule pages, announcements, official product-spec endpoints, official daily-row payloads, governed local rule metadata, or other configured evidence sources. Every enabled exchange SHALL use the same enrichment result contract, and exchange-specific implementations SHALL emit normalized product-master evidence before master governance merges it into discovery candidates.

#### Scenario: Adapter returns standard critical fields
- **WHEN** an unknown variety candidate is discovered for any enabled exchange
- **THEN** the exchange adapter SHALL attempt to enrich `name`, `category`, `currency`, and `unit`
- **AND** it MAY enrich `contract_multiplier`, `tick_size`, listing metadata, delisting metadata, lifecycle lineage, and source evidence.

#### Scenario: GFEX adapter enriches candidate
- **WHEN** a GFEX unknown variety candidate is discovered
- **THEN** the GFEX adapter SHALL attempt to enrich name, category, quote unit, currency, contract multiplier, tick size, listing evidence, and source URL from official GFEX daily rows, product-rule pages, announcements, or governed local rule metadata where available.

#### Scenario: GFEX adapter enriches candidate from official listed-product pages
- **WHEN** GFEX product-spec enrichment is enabled for root varieties observed in official rows
- **THEN** the system SHALL discover matching official listed-product page URLs from the configured GFEX listed-products entry page
- **AND** explicitly configured GFEX `product_rule_pages` SHALL override or supplement auto-discovered URLs
- **AND** it SHALL fetch those official pages through the GFEX official-source request path with the same retry/backoff/challenge diagnostics used by GFEX daily data
- **AND** it SHALL parse product name, product code, quote unit, trading unit, tick size, source interface, source URL, and evidence into a standard product-master enrichment record
- **AND** it SHALL merge those fields with governed local rule metadata through the same source-priority rules used by other exchanges.

#### Scenario: DCE adapter enriches candidate from official contract info
- **WHEN** a DCE unknown variety candidate is discovered and DCE product-spec enrichment is enabled
- **THEN** the system SHALL fetch DCE official `contractInfo` through the browser-assisted official source path
- **AND** it SHALL normalize product name, contract multiplier, tick size, source interface, source URL, and evidence into a standard product-master enrichment record.

#### Scenario: DCE adapter enriches candidate from official listed-product pages
- **WHEN** DCE product-spec enrichment is enabled for root varieties observed in official rows or `contractInfo`
- **THEN** the system SHALL discover matching official listed-product page URLs from the configured DCE listed-products page
- **AND** explicitly configured DCE `product_rule_pages` SHALL override or supplement auto-discovered URLs
- **AND** it SHALL fetch those official pages through the browser-assisted DCE source path
- **AND** it SHALL parse product name, product code, quote unit, trading unit, tick size, source interface, source URL, and evidence into a standard product-master enrichment record
- **AND** it SHALL merge those fields with `contractInfo` and governed local rule metadata through the same source-priority rules used by other exchanges.

#### Scenario: DCE contract unit is parsed
- **WHEN** DCE official `contractInfo` returns a `unit` field
- **THEN** the system SHALL treat it as contract trading unit or multiplier evidence
- **AND** it SHALL NOT use it as the candidate quote unit unless a separate official or governed quote-unit source confirms that meaning.

#### Scenario: Legacy product lineage is discovered
- **WHEN** an exchange product is a historical or replaced root variety such as DCE `S`
- **THEN** the adapter SHALL preserve it as an independent historical instrument
- **AND** it SHALL record successor-family or lineage metadata instead of remapping historical contracts to a newer product.

#### Scenario: Product lifecycle is governed by master data
- **WHEN** master governance enriches or promotes a historical, replaced, delisted, or otherwise inactive root product
- **THEN** the system SHALL record a normalized lifecycle payload in `futures_instruments.metadata.lifecycle`
- **AND** the payload SHALL include a status, valid date window where known, lifecycle source, lifecycle reason, and lineage evidence when applicable
- **AND** the system SHALL NOT rely on symbol-specific market-data skip rules to represent product lifecycle.

#### Scenario: Exchange adapter is not implemented
- **WHEN** an unknown variety is discovered for an exchange without a concrete adapter
- **THEN** the system SHALL persist the candidate with `discovered_unverified` quality
- **AND** it SHALL report that enrichment is unsupported for that exchange.

### Requirement: Futures master discovery shall classify confidence and review status
The system SHALL assign each discovery candidate a confidence/quality state and review status before it can affect production master data.

#### Scenario: High confidence candidate
- **WHEN** official evidence or governed rule evidence confirms exchange, variety symbol, product name, category, quote currency, and quote unit without conflict
- **THEN** the system MAY auto-promote the candidate according to configuration
- **AND** it SHALL record evidence and mark the promoted master data lineage.

#### Scenario: Product spec is partial
- **WHEN** enrichment evidence confirms only a subset of critical fields such as name, contract multiplier, or tick size
- **THEN** the system SHALL preserve those fields on the discovery candidate
- **AND** it SHALL keep the candidate out of production master data until product name, category, quote currency, and quote unit are complete.

#### Scenario: Daily-row name fallback is available
- **WHEN** official daily rows contain a product name for an otherwise unknown variety
- **THEN** the system MAY use that daily-row name as `name` evidence
- **AND** it SHALL NOT infer category, currency, or quote unit from the name alone.

#### Scenario: Incomplete candidate
- **WHEN** official evidence is incomplete or ambiguous
- **THEN** the system SHALL keep the candidate out of production master data
- **AND** it SHALL mark review status as `pending` or quality as `discovered_unverified`.

#### Scenario: Conflicting candidate
- **WHEN** candidate evidence conflicts with an existing active instrument
- **THEN** the system SHALL NOT overwrite the existing instrument automatically
- **AND** it SHALL report `conflict` for manual review.

### Requirement: Futures master discovery shall integrate with master governance reports
The system SHALL include discovery, enrichment, promotion, and pending-review outcomes in futures master-governance, daily-sync, readiness, and Telegram reports.

#### Scenario: Unknown varieties are discovered during master governance
- **WHEN** futures master governance observes unknown varieties
- **THEN** the report SHALL include candidate counts, symbols, first/last seen dates, sample contracts, confidence, review status, enrichment status, attempted sources, missing fields, and promotion result
- **AND** known varieties SHALL continue to be governed unless strict blocking is configured.

#### Scenario: Product-spec enrichment source is unavailable
- **WHEN** an official product-spec enrichment source fails during master governance
- **THEN** the report SHALL include an `official_product_spec_enrichment_unavailable` warning with exchange, target symbols where available, and the error text
- **AND** the warning SHALL be visible in task and Telegram reports, not only in local logs.

#### Scenario: Existing master data is refreshed
- **WHEN** master governance refreshes existing root instruments or their series metadata
- **THEN** the report SHALL include initial, final, and refreshed counts for instruments and series.

#### Scenario: Unknown variety requires configuration maintenance
- **WHEN** a discovery candidate is reported, auto-promoted, or left pending
- **THEN** the report SHALL state that runtime tasks write `futures.db` but do not rewrite configuration files automatically
- **AND** it SHALL include the configuration file, JSON path, and suggested `known_products` entry needed to persist the governed rule metadata after operator review.

#### Scenario: Unknown varieties are handled during daily sync
- **WHEN** daily sync or backfill observes unknown varieties
- **THEN** the report SHALL include `discovered`, `enriched`, `auto_promoted`, `pending`, and `skipped_price_writes` counts by exchange
- **AND** long per-series or per-candidate details SHALL be grouped by exchange to stay within notification limits.

#### Scenario: Target date is outside product lifecycle
- **WHEN** daily sync or backfill targets a date range outside a series instrument's governed lifecycle window
- **THEN** the system SHALL mark that series as `lifecycle_skip`
- **AND** it SHALL NOT request official or fallback providers for that series and target window
- **AND** the skip reason SHALL reference the master-data lifecycle window rather than an exchange-specific or symbol-specific exception.

#### Scenario: Pending discovery exists for enabled scope
- **WHEN** readiness is requested for a scope containing pending discovery candidates
- **THEN** readiness SHALL include `needs_master_review`
- **AND** it SHALL list affected exchange and variety symbols.

### Requirement: Futures master discovery promotion shall be auditable and idempotent
The system SHALL promote approved or high-confidence candidates through existing futures master-data upsert paths without deleting or renaming existing instruments.

#### Scenario: Candidate is promoted
- **WHEN** a candidate is approved or auto-promoted
- **THEN** the system SHALL upsert the corresponding root instrument and default research series
- **AND** rerunning promotion SHALL be idempotent.

#### Scenario: Candidate is rejected
- **WHEN** a candidate is rejected
- **THEN** the system SHALL preserve the discovery record with rejection metadata
- **AND** it SHALL NOT write the candidate to active futures instruments.

### Requirement: Futures market-data tasks can skip calendar backfill without skipping trading-day governance

Futures market-data sync and backfill tasks SHALL support an operator option that skips official trading-calendar backfill while preserving trading-day governance over the task's requested date range.

#### Scenario: Historical backfill reuses verified stored calendar

- **Given** a futures market-data backfill is requested for an exchange with already backfilled official calendar rows
- **And** the request includes `skip_trading_calendar_backfill`
- **When** the task starts
- **Then** it SHALL NOT call the official calendar backfill preflight
- **And** it SHALL still call trading-day governance for the requested `start/end` range
- **And** price downloads SHALL use the governed target trading dates.

#### Scenario: Trading-day governance is explicitly skipped

- **Given** a futures market-data task includes `skip_trading_day_governance`
- **When** the task starts
- **Then** the task MAY skip both official calendar backfill and trading-day governance
- **And** this mode SHALL remain a diagnostic override, not the production default.

#### Scenario: Operator report distinguishes the two calendar steps

- **Given** a futures market-data task is started manually
- **When** the task start acknowledgement is sent
- **Then** it SHALL show whether official calendar backfill is enabled
- **And** it SHALL separately show whether trading-day governance is enabled.

### Requirement: Futures Data Uses Dedicated Local Storage
The system SHALL store futures-domain data in a dedicated local `data/futures.db` database and SHALL NOT persist futures market history in stock quote tables.

#### Scenario: Futures schema initializes independently
- **WHEN** the futures storage layer is initialized
- **THEN** it SHALL create the futures-domain tables in `data/futures.db`
- **AND** existing stock quote, valuation, research, and financial databases SHALL NOT be required for the schema creation to succeed

#### Scenario: Futures bars are not stock-adjusted
- **WHEN** a futures price series is returned by the futures read API
- **THEN** the response SHALL NOT apply stock forward or backward adjustment factors
- **AND** the response SHALL identify the futures series type and price lineage instead

### Requirement: Futures Instruments And Series Are Versioned
The system SHALL maintain futures instrument metadata separately from concrete or constructed price series.

#### Scenario: Instrument and series are distinct
- **WHEN** a futures instrument has a main continuous series and an index continuous series
- **THEN** both series SHALL reference the same instrument identity
- **AND** each series SHALL preserve its own `series_id`, `series_type`, source profile, unit, currency, and construction method

#### Scenario: Continuous series lineage is exposed
- **WHEN** a caller queries a continuous futures series
- **THEN** the response SHALL identify whether the series is source-native, exchange-official, or project-constructed
- **AND** it SHALL expose roll or construction warnings when the rule is not fully auditable locally

### Requirement: Futures Providers Preserve Source Hierarchy And Lineage
The system SHALL prefer first-hand free official sources where implemented and use AkShare-style free aggregator sources as fallback providers.

#### Scenario: Provider result carries lineage
- **WHEN** a futures provider normalizes external bars
- **THEN** every row SHALL preserve source, source mode, source profile, source interface, parser version, unit, currency, raw payload hash, and quality flag

#### Scenario: Aggregator fallback is explicit
- **WHEN** a futures series is fetched from an AkShare-style aggregator
- **THEN** the stored source profile SHALL identify the aggregator source
- **AND** readiness SHALL NOT report the row as first-hand official coverage

### Requirement: Futures Sync Is Idempotent And Audited
The system SHALL support explicit futures history backfill and daily update runs with ingestion audit metadata.

#### Scenario: Existing identical bars are skipped
- **WHEN** a sync run sees an existing bar with the same series, trade date, source, source mode, and raw payload hash
- **THEN** the write path SHALL skip rewriting the row
- **AND** the sync summary SHALL count it as unchanged

#### Scenario: Changed source row is updated with audit
- **WHEN** a sync run sees an existing bar whose normalized values or raw payload hash changed
- **THEN** the write path SHALL update the current row
- **AND** the ingestion run metadata SHALL report the changed row count

### Requirement: Futures History Targets At Least Ten Years When Available
The system SHALL target the longest available free history for enabled futures series and SHALL require at least 10 years of coverage unless the instrument listing history or source availability makes that impossible.

#### Scenario: Ten-year coverage is available
- **WHEN** a P0 futures series has at least 10 years of upstream history
- **THEN** the backfill SHALL be able to populate enough local bars for 10-year diagnostics

#### Scenario: Ten-year coverage is not available
- **WHEN** a P0 futures series cannot reach 10-year coverage because of listing date or free-source limitation
- **THEN** readiness SHALL report `insufficient_history`
- **AND** it SHALL include observed coverage, target coverage, and the limiting reason

### Requirement: Futures Cycle Diagnostics Are Persisted
The system SHALL calculate and persist futures cycle diagnostics for enabled series.

#### Scenario: Diagnostics are computed
- **WHEN** a futures series has enough local bars for a configured lookback window
- **THEN** diagnostics SHALL include latest price, mean, median, percentile, mean deviation, volatility, high/low position, coverage ratio, cycle state, calc version, and input hash

#### Scenario: Diagnostics lack enough history
- **WHEN** a futures series lacks enough bars for a lookback window
- **THEN** diagnostics SHALL mark that window as `insufficient_history`
- **AND** readiness SHALL expose the missing observation count

### Requirement: Futures Spreads Are Defined And Recomputed Locally
The system SHALL support versioned futures spread definitions and derived spread values.

#### Scenario: Spread definition is versioned
- **WHEN** a futures spread is configured
- **THEN** the definition SHALL include spread id, formula version, legs, weights, unit conversion notes, currency conversion notes, and validity dates

#### Scenario: Spread value is derived
- **WHEN** all required spread legs have local prices for a trade date
- **THEN** the system SHALL calculate and store the spread value with source lineage pointing to the leg series and calculation version

### Requirement: Futures Exposure Mappings Support DCF Inputs
The system SHALL store industry and instrument exposure mappings between listed companies, revenue-side futures series, cost-side futures series, and spread diagnostics.

#### Scenario: Company exposure is resolved
- **WHEN** a caller asks for a company's futures exposure mapping
- **THEN** the response SHALL include mapped revenue series, cost series, spread ids, direction, transmission strength, lag days, confidence, source, and validity period

#### Scenario: Exposure is unavailable
- **WHEN** a company has no futures exposure mapping
- **THEN** the response SHALL return a structured input gap instead of guessing a commodity from the company name

### Requirement: Futures Read APIs Are Local-Only
The system SHALL expose local futures data through read-only research APIs and SHALL NOT perform remote provider calls inside those API requests.

#### Scenario: Price API reads local bars
- **WHEN** a caller requests futures prices through the API
- **THEN** the API SHALL read local `futures.db` rows
- **AND** it SHALL return an empty or not-found response if local data is unavailable

#### Scenario: Readiness API explains gaps
- **WHEN** futures readiness is requested
- **THEN** the API SHALL return enabled series counts, P0 coverage, stale series, source fallback usage, history coverage, blockers, and warnings

### Requirement: GFEX futures master governance shall be runnable independently

The system SHALL provide a GFEX-scoped futures master-governance task that refreshes root instruments, default research series, and discovered real contracts before GFEX price downloads are enabled.

#### Scenario: GFEX root instruments are governed
- **WHEN** GFEX master governance runs for `exchange=GFEX`
- **THEN** the system SHALL upsert active root instruments for `CNF.SI.GFEX`, `CNF.LC.GFEX`, `CNF.PS.GFEX`, `CNF.PT.GFEX`, and `CNF.PD.GFEX`
- **AND** each root instrument SHALL include exchange, category, currency, unit, active flag, source profiles, and metadata.

#### Scenario: GFEX main series are governed
- **WHEN** GFEX root instruments are governed
- **THEN** the system SHALL upsert the default `main_continuous` series for each active GFEX root instrument
- **AND** those series SHALL be usable by existing futures price sync scope resolution.

### Requirement: GFEX real contracts shall be discovered from official daily payloads

The system SHALL discover GFEX real exchange contracts from official daily market-data payloads over governed trading dates and persist them as first-class contract master records.

#### Scenario: Contract is discovered from official GFEX row
- **WHEN** an official GFEX daily payload contains a parsed contract such as `LC2407`
- **THEN** the system SHALL persist a contract id such as `CNF.LC.GFEX.LC2407`
- **AND** the contract SHALL link to root instrument `CNF.LC.GFEX`
- **AND** the row SHALL preserve official source lineage and partial-quality metadata.

#### Scenario: Unsupported GFEX variety appears
- **WHEN** the official GFEX payload contains a variety that cannot be mapped to a governed root instrument
- **THEN** the system SHALL skip that contract
- **AND** the task report SHALL include a warning sample for the unmapped variety.

### Requirement: GFEX master governance shall depend on verified trading calendar coverage

GFEX master governance SHALL use verified GFEX trading calendar rows to determine which dates are eligible for official contract discovery.

#### Scenario: Calendar coverage exists
- **WHEN** GFEX has `backfilled_verified` trading calendar rows for the requested range
- **THEN** the task SHALL request only verified trading days from the official GFEX source.

#### Scenario: Calendar coverage is missing
- **WHEN** the requested GFEX date range has no verified trading days
- **THEN** the task SHALL stop before official provider requests
- **AND** it SHALL report a blocked status explaining the missing calendar coverage.

### Requirement: GFEX master governance shall be safe for staged production rollout

The GFEX master-governance task SHALL be idempotent, manually runnable, and disabled for automatic scheduling until the operator explicitly enables GFEX futures operations.

#### Scenario: Dry-run mode
- **WHEN** GFEX master governance runs with `dry_run=True`
- **THEN** it SHALL return would-write counts for instruments, series, and contracts
- **AND** it SHALL NOT modify `data/futures.db`.

#### Scenario: Write mode
- **WHEN** GFEX master governance runs with `write`
- **THEN** it SHALL upsert instruments, series, and contracts
- **AND** rerunning the same task SHALL be safe and idempotent.

### Requirement: Production writes repair then require verified trading calendars

Commodity futures market-data production writes MUST attempt official trading-calendar backfill when target-date expansion includes calendar evidence below the configured production quality threshold, and MUST NOT fetch or write price bars if the calendar remains below threshold after that repair attempt.

#### Scenario: Market-data sync does not persist estimated calendars by default

- **Given** a futures market-data sync or backfill task is requested for a date range with missing calendar rows
- **When** the task expands target dates
- **Then** it SHALL NOT persist weekday-derived `estimated` trading-calendar rows unless an explicit development or offline seed option is enabled
- **And** it SHALL use official calendar backfill or calendar quality gates to decide whether the run may continue.

#### Scenario: Production backfill includes an estimated calendar date and repair succeeds

- **Given** a futures market-data backfill is requested with `dry_run=False`
- **And** the expanded date range contains an `estimated` calendar row
- **When** the task validates trading-day governance
- **Then** the task runs official calendar backfill for the low-quality exchange/date range
- **And** it re-expands target dates after the calendar repair
- **And** it may continue if the repaired calendar reaches the production threshold.

#### Scenario: Production backfill includes an estimated calendar date and repair fails

- **Given** a futures market-data backfill is requested with `dry_run=False`
- **And** the expanded date range contains an `estimated` calendar row
- **When** the task attempts official calendar backfill
- **And** the calendar remains below the production threshold
- **Then** the task returns `blocked`
- **And** no provider requests are made
- **And** no price bars are written
- **And** the result includes a calendar quality blocker.

#### Scenario: Dry-run includes an estimated calendar date

- **Given** a futures market-data backfill is requested with `dry_run=True`
- **And** the expanded date range contains an `estimated` calendar row
- **When** the task validates trading-day governance
- **Then** the task may continue
- **And** the result includes a warning.

### Requirement: GFEX challenge retry is independent from generic request retry

The GFEX official daily payload provider MUST honor the configured GFEX challenge retry budget even when prior attempts include generic network errors.

#### Scenario: Mixed generic error and GFEX 567 challenge

- **Given** `retry_attempts=2`
- **And** `challenge_retry_attempts_by_exchange.GFEX=3`
- **When** a GFEX request first fails with a generic retryable network error
- **And** a later attempt receives a GFEX 567 challenge response
- **Then** the provider retries the challenge according to the GFEX challenge retry budget
- **And** it does not stop solely because the generic retry attempt count has been reached.

### Requirement: Futures Market Data Writes Emit Shared Change Records
Futures market-data sync SHALL map existing inserted, changed, and unchanged bar classifications into the shared daily-sync changelog contract.

#### Scenario: Contract bar hash changes
- **WHEN** a futures contract price bar exists and the incoming bar has a different semantic hash
- **THEN** the futures storage path SHALL update the bar
- **AND** it SHALL append a futures-domain change record with the contract id, trade date, source, and source mode

#### Scenario: Contract bar hash matches
- **WHEN** a futures contract price bar exists and the incoming bar hash matches the stored hash
- **THEN** the futures storage path SHALL count the row as unchanged
- **AND** it SHALL NOT append a change record

### Requirement: Futures Continuous Series Changes Are Separately Identified
Futures continuous-series observations SHALL emit change records using series-level keys instead of only contract-level keys.

#### Scenario: Continuous series row changes
- **WHEN** a continuous futures series observation changes after roll or source repair
- **THEN** the changelog SHALL identify the series id and trade date affected

### Requirement: Product-page browser fallback is exchange scoped
The system SHALL keep browser-assisted product enrichment scoped to the exchange whose official page or business endpoint is being accessed.

#### Scenario: DCE contract information is enriched
- **WHEN** DCE `contractInfo` or a DCE product page is requested during master governance
- **THEN** the system SHALL use the same validated DCE browser route and session used by the authoritative DCE provider

#### Scenario: Another exchange product page needs fallback
- **WHEN** a CZCE, INE, SHFE, or GFEX product page needs browser-assisted fallback
- **THEN** the fallback SHALL NOT initialize, wait for, or circuit-break on the DCE browser challenge
- **AND** a DCE access failure SHALL NOT delay that exchange's master governance

### Requirement: DCE browser request lifecycle is not orphaned by caller timeout
The market-data sync SHALL let the bounded DCE browser client own timeout and cleanup for official DCE exchange payload requests.

#### Scenario: A DCE route takes longer than the generic source timeout
- **WHEN** a DCE exchange payload request is still rotating bounded browser routes after the generic official-source timeout elapses
- **THEN** the sync SHALL continue awaiting that request until the DCE client succeeds or reaches its own bounded terminal result
- **AND** it SHALL NOT start fallback or queue another DCE date while the prior browser request remains active

### Requirement: Scheduled futures phases preserve DCE route continuity and exchange isolation
The scheduled futures daily task SHALL reuse one provider-scoped DCE browser route across calendar repair, master governance, and price synchronization without invoking its event loop from multiple worker threads.

#### Scenario: Calendar repair validates a DCE route
- **WHEN** scheduled calendar repair completes a DCE business request on a validated browser route
- **THEN** subsequent DCE master governance and price synchronization in that task SHALL borrow the same official provider
- **AND** all synchronous DCE calls SHALL execute on the provider-owned single-worker executor
- **AND** the provider SHALL be closed exactly once after the scheduled task exits

#### Scenario: One exchange's master governance is blocked
- **WHEN** master governance blocks one exchange while another requested exchange remains runnable
- **THEN** the scheduler SHALL exclude only the blocked exchange from price synchronization
- **AND** it SHALL continue the runnable exchange or exchanges
- **AND** the final ingestion result SHALL be `partial`
- **AND** reports and persisted metadata SHALL retain the blocked result's real date range, calendar coverage, counts, route metrics, warnings, and blockers

#### Scenario: Every requested exchange is blocked by master governance
- **WHEN** no requested exchange remains runnable after master governance
- **THEN** price synchronization SHALL NOT start
- **AND** the scheduler SHALL report each original blocked governance result without replacing its diagnostics with zero-valued synthetic data

### Requirement: Incomplete master evidence cannot shorten futures lifecycles
The system SHALL require successful official contract discovery for every verified trading date in the governance window before persisting master or lifecycle changes.

#### Scenario: A verified DCE date remains unresolved after bounded retries
- **WHEN** DCE contract discovery succeeds for some verified dates but one or more verified dates remain unresolved
- **THEN** DCE master governance SHALL return `blocked`
- **AND** it SHALL retain the real failed dates, partial contract count, route metrics, warnings, and blockers
- **AND** it SHALL NOT write instruments, series, discoveries, or contracts from that incomplete scan
- **AND** the scheduler SHALL exclude DCE while continuing other runnable exchanges

#### Scenario: A previous outage produced a recent weak lifecycle boundary
- **WHEN** an observed lifecycle boundary falls inside the current target window but lacks evidence that official discovery was complete through the target end
- **THEN** market-data synchronization SHALL NOT use that boundary to remove target dates
- **AND** the completeness result SHALL continue to expose the missing date until authoritative data is written

#### Scenario: Contract discovery completes for the whole governance window
- **WHEN** every verified trading date is successfully discovered
- **THEN** lifecycle inference MAY update the observed window
- **AND** any inferred inactive boundary SHALL record the official evidence-complete-through date

### Requirement: DCE route expiry has one bounded retry owner
The system SHALL treat explicit proxy expiry and proxy-route throttling as failures of the current DCE route and SHALL keep bounded retry ownership inside the DCE browser client.

#### Scenario: A validated proxy reports that its IP expired
- **WHEN** a DCE business response reports `HTTP 403 ip expired`
- **THEN** the client SHALL invalidate that browser/proxy route immediately
- **AND** it SHALL NOT retry the same browser session
- **AND** it SHALL rotate to a fresh lease within the configured run-wide bounds
- **AND** diagnostics SHALL expose only a credential-free proxy-expiry classification

#### Scenario: A proxy route is throttled
- **WHEN** a DCE business response reports that the request is too frequent for the current route
- **THEN** the client SHALL invalidate and rotate that route immediately
- **AND** the outer official provider SHALL NOT repeat the browser request or apply its generic rate-limit backoff

### Requirement: Scheduled DCE phases reuse official daily payloads
The task-scoped official provider SHALL cache each successful DCE `dayQuotes` payload by normalized trade date for the lifetime of that provider instance.

#### Scenario: Calendar and master request the same DCE date
- **WHEN** calendar probing has already fetched a successful DCE daily payload and master governance requests the same date
- **THEN** master governance SHALL reuse the cached payload
- **AND** no additional browser business request SHALL be issued for that date
- **AND** request and cache-hit metrics SHALL distinguish the two events

### Requirement: Routine DCE governance avoids stable product-page refresh
The system SHALL allow routine master governance to skip official product-spec refresh for already governed exchange products without disabling discovery enrichment.

#### Scenario: Existing DCE products are governed during a daily task
- **WHEN** DCE existing-product refresh is disabled by production configuration
- **THEN** governance SHALL scan official daily contract rows without first requesting DCE contract-info or product pages

#### Scenario: A new DCE variety appears in daily rows
- **WHEN** daily contract discovery observes an unknown DCE variety
- **THEN** governance SHALL still request targeted official product enrichment for that variety

### Requirement: Publication-eligible futures bars must be finalized after cutoff
The futures sync SHALL distinguish provisional same-day observations from finalized daily observations using the target exchange's publication timezone and cutoff. Persisted row presence alone MUST NOT satisfy post-cutoff completeness when the required date is represented only by provisional evidence.

#### Scenario: Same-day bar is fetched before cutoff
- **WHEN** an official or fallback provider returns a bar for the exchange-local current trading date before the configured publication cutoff
- **THEN** the stored row SHALL be marked provisional with acquisition time and cutoff context
- **AND** the row MAY remain available for diagnostics
- **AND** it SHALL NOT be represented as finalized settlement coverage

#### Scenario: Nightly run receives changed final data
- **WHEN** the publication-eligible target date has a provisional row and a post-cutoff provider fetch returns changed final values
- **THEN** the sync SHALL update or source-replace the row through the existing priority rules
- **AND** it SHALL record final verification time and ingestion run id
- **AND** the finalized date SHALL satisfy completeness when no other blocker remains

#### Scenario: Nightly run verifies identical final data
- **WHEN** a post-cutoff official fetch returns values semantically identical to an existing provisional row
- **THEN** the sync SHALL persist that final verification advanced
- **AND** it SHALL count the price values as unchanged
- **AND** it SHALL NOT report a same-source price correction solely for the finality metadata update

#### Scenario: Nightly final fetch fails
- **WHEN** publication is due and the provider cannot verify or finalize a required date that has only provisional stored rows
- **THEN** the exchange SHALL remain partial or blocked
- **AND** the result SHALL list the stale provisional date and provider blocker
- **AND** the provisional row SHALL be retained rather than deleted

#### Scenario: Current date is not publication-eligible
- **WHEN** a run occurs before the configured cutoff
- **THEN** the exchange-local current date SHALL NOT be required for final completeness
- **AND** any provisional current-date row SHALL NOT make an earlier completed-date run fail

### Requirement: Futures write summaries must distinguish business transitions
Futures series and contract write results SHALL classify business-semantic outcomes independently from physical insert/update/delete operations while preserving existing aggregate counters.

#### Scenario: First observation for a series and date is written
- **WHEN** no observation exists for a series, trade date, and source mode before the write
- **THEN** the result SHALL count a `new_business_date` row
- **AND** it SHALL include the newly covered trade date in the business-date summary

#### Scenario: Official row supersedes aggregator fallback
- **WHEN** an official row replaces an existing lower-priority fallback row for the same series, trade date, and source mode
- **THEN** the result SHALL count a `source_upgrade`
- **AND** it SHALL NOT count that transition as new business-date coverage
- **AND** existing delete and insert change-log evidence SHALL remain auditable

#### Scenario: Same source publishes corrected values
- **WHEN** an existing source key receives changed normalized values or a changed semantic hash
- **THEN** the result SHALL count a `same_source_correction`
- **AND** it SHALL preserve the existing `changed` aggregate counter

#### Scenario: Rerun requires no value or finality change
- **WHEN** the stored row already matches the incoming values and has sufficient final verification evidence
- **THEN** the result SHALL count it as unchanged
- **AND** it SHALL NOT emit a business transition or semantic change record

### Requirement: Futures completeness must expose provisional reconciliation
Each exchange completeness result SHALL expose finalized latest coverage, stale provisional dates, dates finalized by the current run, and post-cutoff verification blockers in addition to actual persisted latest date.

#### Scenario: Persisted latest date is provisional
- **WHEN** `actual_latest_price_date` reaches the expected latest date but that date has only provisional evidence after cutoff
- **THEN** the exchange SHALL NOT report success
- **AND** the completeness result SHALL distinguish persisted latest date from finalized latest date

#### Scenario: Rolling run finalizes a provisional date
- **WHEN** the daily rolling run successfully finalizes a recent provisional date
- **THEN** the completeness result SHALL include that date under finalized or reconciled dates
- **AND** it SHALL remove the date from remaining stale provisional dates
