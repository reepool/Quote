# research-data-engine Specification

## Purpose
The Research Data Engine provides a local-first research data service layer on
top of the Quote System. It owns research-domain storage, ingestion, derived
analytics, readiness gates, and `/api/v1/research/*` read APIs for A-share
research workflows while keeping production market-data tables isolated.
## Requirements
### Requirement: Research Data Engine Execution Mainline
The project SHALL maintain a dedicated execution document for the Research Data Engine that is separate from the requirement document and defines the implementation mainline, delivery order, and current rollout status.

#### Scenario: Execution document is linked from the requirement document
- **WHEN** a maintainer opens the Research Data Engine requirement document
- **THEN** the document SHALL link to the execution document and the associated OpenSpec change package

#### Scenario: Execution document records completed and pending domains
- **WHEN** a maintainer reviews the execution document
- **THEN** the document SHALL distinguish already delivered domains from domains that remain in backlog

### Requirement: Research Module Enablement Alignment
The runtime research configuration SHALL only mark a module as enabled when the corresponding domain has an implemented storage, sync or calculation path, and a stable read interface or service path.

#### Scenario: Current baseline modules remain enabled
- **WHEN** the runtime configuration is loaded for the current Research Data Engine baseline
- **THEN** `company_profile`, `industry`, `shareholders`, `financial_summary`,
  `financial_statements`, `technical`, and `risk` SHALL be enabled

#### Scenario: Shareholder snapshot API is open after rollout
- **WHEN** the shareholder domain has completed full local snapshot import and
  readiness has no rollout blockers
- **THEN** the runtime configuration SHALL allow local shareholder snapshot
  reads with `delivery_mode=paid_high_availability`

#### Scenario: Deferred or guarded modules remain disabled by default
- **WHEN** the runtime configuration is loaded before the guarded research domains are explicitly rolled out
- **THEN** `valuation`, `analyst_forecasts`, `research_reports`, and
  `sentiment_events` SHALL remain disabled by default

### Requirement: Research Delivery Order
The Research Data Engine execution mainline SHALL define the next delivery sequence after the current Phase 0 / Phase 1 baseline.

#### Scenario: Next mainline is explicitly defined
- **WHEN** a maintainer reviews the execution document
- **THEN** the next delivery order SHALL identify `valuation` rollout
  readiness and quality hardening before external research metadata rollout

#### Scenario: Completed domains remain in maintenance
- **WHEN** strict Shenwan industry and shareholder domains have completed their
  current rollout
- **THEN** the execution mainline SHALL treat them as maintenance domains for
  readiness checks, scheduled refresh, and targeted gap repair rather than as
  blockers for starting valuation rollout work

#### Scenario: Domain implementation follows the shared template
- **WHEN** a new research domain enters implementation
- **THEN** it SHALL be tracked through schema, provider, sync, scheduler, read API, tests, and rollout steps

### Requirement: Local Research Data Must Be API-Queryable
Every local research dataset persisted or materialized by the Research Data Engine MUST have a read API or an explicit gated/disabled API contract.

#### Scenario: Persisted research table has a read path
- **WHEN** a research domain writes local normalized tables or materialized
  snapshots
- **THEN** callers SHALL be able to query those rows through the API layer or
  see an explicit disabled/gated response explaining why the read API is not
  available

#### Scenario: API work follows storage rollout
- **WHEN** a new research storage table is added before the API surface is
  implemented
- **THEN** the OpenSpec tasks and engineering execution document SHALL record
  the pending API endpoint before the change can be considered fully complete

### Requirement: Strict Shenwan Industry Taxonomy
The system SHALL distinguish raw industry reference fields from the authoritative Shenwan level-1, level-2, and level-3 taxonomy used for research, peer grouping, and valuation.

#### Scenario: Raw source fields remain reference-only before mapping
- **WHEN** a source only provides coarse industry or board fields without a reliable Shenwan level-1, level-2, and level-3 mapping
- **THEN** those fields SHALL only be stored as reference information and SHALL NOT be treated as authoritative industry memberships

#### Scenario: Authoritative industry library is versioned
- **WHEN** the authoritative Shenwan taxonomy is maintained
- **THEN** the system SHALL keep dedicated taxonomy and membership tables with taxonomy versioning and stock-level linkage

#### Scenario: Industry memberships support level-1, level-2, and level-3 outputs
- **WHEN** a stock has a valid authoritative industry mapping
- **THEN** its current membership SHALL expose Shenwan level-1, level-2, and level-3 classification information

#### Scenario: Shenwan industry assets are queryable by API
- **WHEN** strict Shenwan taxonomy, stock memberships, or leaf component sets
  are persisted locally
- **THEN** the API layer SHALL expose stock-level Shenwan level-1/level-2/level-3
  membership, taxonomy nodes, and Shenwan leaf/third-level component sets

### Requirement: Relative Valuation Benchmark Uses Shenwan Level 2
The system SHALL use the authoritative Shenwan level-2 industry peer set as the default comparison center for relative valuation outputs.

#### Scenario: Default peer group for relative valuation
- **WHEN** the relative valuation module computes comparable PE, PB, or PS benchmarks for an A-share stock
- **THEN** the default peer group SHALL be the stock's authoritative Shenwan level-2 industry membership

### Requirement: Research AkShare Mode-Aware Loading
The research domain SHALL give `AkShare` providers explicit runtime semantics for `direct` and `proxy_patch` modes instead of treating `source_mode` as metadata only.

#### Scenario: Direct mode loads AkShare without requesting proxy patch installation
- **WHEN** a research-domain `AkShare` provider runs in `direct` mode
- **THEN** it SHALL load `akshare` without actively installing the proxy patch for that request path

#### Scenario: Proxy mode requests proxy patch installation
- **WHEN** a research-domain `AkShare` provider runs in `proxy_patch` mode
- **THEN** it SHALL attempt to install the configured `akshare` proxy patch before importing `akshare`
- **AND** if the patch is unavailable or disabled, the provider SHALL fail explicitly instead of silently behaving like `direct`

### Requirement: Industry Standard Fetch Hardening
The authoritative Shenwan membership fetch path SHALL degrade gracefully and expose diagnostics when third-level constituent-page fetches fail.

#### Scenario: Single constituent page fails
- **WHEN** one or more `sw_index_third_cons` third-level constituent-page requests fail
- **THEN** the provider SHALL continue attempting other third-level pages
- **AND** the provider SHALL record diagnostics about attempted and failed third-level codes

#### Scenario: Membership fetch returns partial matches
- **WHEN** only a subset of target instruments can be matched from successful third-level constituent pages
- **THEN** the sync result SHALL expose membership diagnostics including matched and missing instruments

### Requirement: Industry Standard Rollout Diagnostics
The authoritative industry sync SHALL report taxonomy progress and membership progress separately so rollout validation can distinguish partial taxonomy success from authoritative membership success.

#### Scenario: Taxonomy succeeds but memberships fail
- **WHEN** taxonomy nodes are fetched and written but authoritative memberships cannot be produced
- **THEN** the sync result SHALL report the taxonomy write count
- **AND** the run metadata SHALL include diagnostics explaining the membership failure path

#### Scenario: Proxy fallback requires actual upstream hook coverage
- **WHEN** `industry_standard` relies on an upstream host outside the currently hooked domains
- **THEN** the proxy-patch configuration SHALL include that host before the `proxy_patch` route is treated as a real availability fallback

### Requirement: Official Shenwan Stock Membership Source
The research system SHALL support an official Shenwan stock-classification history source as an authoritative membership input instead of relying only on third-level constituent-page crawling.

#### Scenario: Official stock-history source is available
- **WHEN** the system ingests Shenwan official stock-classification history
- **THEN** it SHALL be able to resolve the latest stock-to-industry membership for a stock from that source

#### Scenario: Official source coexists with legacy page crawling
- **WHEN** the official stock-history source is introduced
- **THEN** the existing page-crawling path MAY remain as validation or fallback
- **AND** the page-crawling path SHALL NOT remain the only primary authoritative membership strategy

### Requirement: Official Industry Code Mapping Layer
The research system SHALL maintain a mapping layer between official Shenwan stock-history industry codes and the taxonomy nodes used by the research read model.

#### Scenario: Official six-digit code is normalized into research taxonomy
- **WHEN** the official stock-history source returns a six-digit industry code for a stock
- **THEN** the system SHALL map that code into the corresponding research taxonomy node before exposing the membership as authoritative

#### Scenario: Mapping is incomplete
- **WHEN** the system cannot confidently map an official code into the research taxonomy
- **THEN** it SHALL record the raw official code and mark the normalization result as degraded or unmapped rather than silently guessing

### Requirement: Financial Statements Must Use Official Structured Sources First
The Research Data Engine SHALL prefer official exchange, filing, XBRL, or equivalent structured disclosure artifacts for full financial statements before AkShare, BaoStock, PyTDX, or other third-party aggregators. A reachable official manifest or disclosure page SHALL NOT by itself count as official structured coverage unless discovery resolves a parseable structured artifact for the filing.

#### Scenario: Official structured artifact is available for a filing
- **WHEN** a target instrument has an official parseable structured financial filing artifact available for a report period
- **THEN** the financial statement sync SHALL ingest that official source before attempting third-party fallback sources

#### Scenario: Only manifest evidence is available
- **WHEN** an official manifest or disclosure page is reachable but no parseable structured artifact can be resolved for a target instrument and report period
- **THEN** the sync SHALL record the manifest evidence and unresolved structured-endpoint reason without treating official structured coverage as ready

#### Scenario: Official source is not available
- **WHEN** no official structured filing can be fetched or parsed for a target instrument and report period
- **THEN** the sync MAY use configured fallback sources and SHALL record the fallback source, source mode, and missing official-source reason

### Requirement: Financial Source Parameters Must Be Configurable
Financial data providers SHALL read endpoints, report-period windows, rate limits, retries, concurrency, parser versions, field aliases, and fallback policy from configuration or versioned provider parameters rather than hard-coded business logic.

#### Scenario: Provider configuration is loaded
- **WHEN** a financial sync or probe starts
- **THEN** it SHALL resolve source candidates, URLs or manifest endpoints, timeout, retry, request interval, concurrency, baseline period, rolling period floor, parser version, and fallback policy from the research configuration or an explicit runtime override

#### Scenario: Runtime overrides are used
- **WHEN** an operator narrows exchanges, instruments, report periods, or concurrency for a validation run
- **THEN** the override SHALL apply only to that run and SHALL be recorded in ingestion metadata without modifying the persisted default configuration

### Requirement: Financial Statement Backfill Must Cover Multiple Report Periods
The Research Data Engine SHALL support configurable multi-period financial statement backfill with a default baseline from 2024Q1 through the latest disclosed report period and a rolling minimum of at least eight quarters.

#### Scenario: Initial financial backfill runs
- **WHEN** the financial statement backfill is executed without a narrower override
- **THEN** it SHALL target all report periods from 2024Q1 through the latest disclosed report period for each target company

#### Scenario: Rolling period floor is configured
- **WHEN** the configured rolling history window is greater than the calendar baseline
- **THEN** the backfill SHALL include enough report periods to satisfy the rolling minimum of at least eight quarters

### Requirement: Financial Ingestion Must Preserve All Numeric Facts
The Research Data Engine SHALL persist every numeric fact parsed from official structured financial filings in an auditable long-form fact store while also maintaining a curated core fact layer for valuation and screening.

#### Scenario: XBRL numeric facts are parsed
- **WHEN** an official XBRL or equivalent structured filing contains numeric facts
- **THEN** the system SHALL store each numeric fact with fact name, namespace or taxonomy, context, unit, decimals or precision, period metadata, dimensions, value, source file reference, and parser version

#### Scenario: Core valuation facts are derived
- **WHEN** the all-fact layer contains recognized revenue, net income, equity, assets, liabilities, cash flow, or share-count facts
- **THEN** the system SHALL derive normalized core facts without discarding the original all-fact rows

### Requirement: Financial Storage Must Preserve Source Lineage And Be Backend Portable
Financial storage SHALL expose repository or storage-layer operations that preserve source lineage while avoiding business-layer dependence on SQLite-only behavior.

#### Scenario: Source file is stored
- **WHEN** a filing payload or structured source file is downloaded or discovered
- **THEN** the system SHALL persist a manifest row with exchange, instrument, report period, report type, official filing id or URL, archive path when available, content hash, parser version, parse diagnostics, source, source mode, and ingestion run id

#### Scenario: Storage backend is replaced
- **WHEN** financial data is moved from `research.db` to `financials.db`, DuckDB, PostgreSQL, or another supported backend
- **THEN** API responses, readiness semantics, source lineage, and valuation calculations SHALL remain unchanged except for documented operational configuration

#### Scenario: Backend-specific SQL is needed
- **WHEN** upsert, JSON querying, pagination, or bulk insert requires backend-specific syntax
- **THEN** that syntax SHALL be isolated inside the storage adapter and SHALL NOT leak into providers, sync services, API routes, or valuation calculations

### Requirement: Financial Facts Should Support Configurable Hot Cold Tiers
Financial fact and indicator storage SHALL support a configurable hot/cold tier so common reads use a bounded recent-window dataset while long-history research remains available.

#### Scenario: Common financial read uses the hot tier
- **WHEN** a caller requests latest financial statements, latest financial indicators, company overview, or default valuation inputs without a long-history override
- **THEN** the read path SHALL query the hot tier first and SHALL NOT scan historical facts by default

#### Scenario: Long-history read is requested
- **WHEN** a caller explicitly requests a period range, lookback quarters, or history flag that exceeds the configured hot window
- **THEN** the storage layer SHALL combine hot and history tiers and return a single logical result without exposing physical table names to API or valuation code

#### Scenario: New filings move the hot window
- **WHEN** daily catch-up or backfill ingests newer report periods
- **THEN** tier maintenance SHALL move facts and derived indicators outside the configured hot window into the history tier while preserving source lineage, uniqueness, and idempotency

#### Scenario: Hot window is configured
- **WHEN** financial storage is initialized
- **THEN** the hot quarter window, defaulting to 12 quarters, and any anchor-period policy for TTM or YoY calculations SHALL come from configuration rather than hard-coded query logic

### Requirement: Financial Facts Must Carry Availability Metadata
Financial facts used by valuation SHALL include report-period and availability metadata so historical valuation cannot use facts before they were publicly available.

#### Scenario: Valuation history is rebuilt for a trade date
- **WHEN** the system computes valuation metrics for a trade date
- **THEN** it SHALL only use financial facts whose data available date is on or before that trade date

#### Scenario: Publish date is missing
- **WHEN** a financial source lacks a reliable publish date for a report period
- **THEN** the system SHALL mark the fact as unavailable for historical valuation unless a configured conservative availability date can be derived and recorded

### Requirement: Broker Regulatory Ingestion Must Use Confirmed Listed Broker Scope
The Research Data Engine SHALL gate broker regulatory fact ingestion by an auditable listed-broker scope and SHALL NOT rely on Shenwan securities membership alone.

#### Scenario: Confirmed listed broker is ingested
- **WHEN** an instrument has a valid `listed_broker_dealer_scope` entry linked to CSRC securities-company registry evidence
- **THEN** broker regulatory fact ingestion SHALL include that instrument in historical backfill and incremental update scopes

#### Scenario: Shenwan-only candidate is skipped
- **WHEN** an instrument has Shenwan `证券` membership but no confirmed listed-broker scope
- **THEN** broker regulatory fact ingestion SHALL skip the instrument and record the scope-gate reason

### Requirement: Broker Regulatory Facts Remain In The Financial Fact Chain
Broker annual/semiannual embedded regulatory facts SHALL use the existing financial source manifest, numeric fact, hot/history tier, and readiness repository APIs.

#### Scenario: Broker embedded table fact is written
- **WHEN** a broker annual or semiannual report embedded risk-control table is parsed
- **THEN** the parsed facts SHALL be written through the financial numeric fact APIs and SHALL be queryable by canonical fact name across hot/history tiers

#### Scenario: Broker regulatory fact is read by downstream code
- **WHEN** DCF or readiness code reads broker regulatory facts
- **THEN** it SHALL use the existing local financial fact read path without querying remote annual reports or AkShare during valuation calculation

### Requirement: PE PB PS Metrics Must Distinguish Static TTM And Forward Variants
The valuation engine SHALL compute and expose static, TTM, and dynamic or forward PE/PB/PS variants as distinct metrics with explicit numerator, denominator, source period, and availability provenance.

#### Scenario: TTM valuation is computed
- **WHEN** the system has enough disclosed quarterly or annual facts to compute trailing twelve month revenue or net income
- **THEN** it SHALL compute PE TTM and PS TTM separately from static and forward valuation metrics

### Requirement: Research ingestion uses governed current universe
The Research Data Engine SHALL run instrument master governance before current research or financial ingestion jobs resolve active stock instruments.

#### Scenario: Current financial summary sync starts
- **WHEN** `financial_summary_shadow_sync` starts for supported current A-share exchanges
- **THEN** the system SHALL run or reuse instrument master governance before selecting `is_active` stock instruments
- **AND** the sync SHALL build its target universe from the post-governance local master state

#### Scenario: Current financial statements sync starts
- **WHEN** `financial_statements_shadow_sync` starts for supported current A-share exchanges
- **THEN** the system SHALL run or reuse instrument master governance before selecting `is_active` stock instruments
- **AND** the sync SHALL include the governance result in its structured scheduler result

#### Scenario: Current shareholder sync starts
- **WHEN** `shareholder_shadow_sync` starts for supported current A-share exchanges
- **THEN** the system SHALL run or reuse instrument master governance before selecting `is_active` stock instruments
- **AND** newly listed active instruments discovered by governance SHALL be eligible for the same sync run

#### Scenario: Current research metadata sync starts
- **WHEN** company profile, industry, analyst forecast, research report, or sentiment event sync starts for supported current A-share exchanges
- **THEN** the system SHALL run or reuse instrument master governance before selecting active stock instruments

### Requirement: Derived research rebuilds declare universe semantics
The Research Data Engine SHALL make universe semantics explicit for derived rebuilds that use quote, financial, or research inputs.

#### Scenario: Latest technical snapshot refresh starts
- **WHEN** `technical_snapshot_refresh` runs as a current latest-snapshot job
- **THEN** the system SHALL run or reuse instrument master governance before selecting active stock instruments

#### Scenario: Risk snapshot rebuild starts
- **WHEN** `risk_snapshot_rebuild` runs as a current snapshot job
- **THEN** the system SHALL run or reuse instrument master governance before selecting active stock instruments

#### Scenario: Historical valuation rebuild starts
- **WHEN** `valuation_history_rebuild` runs for historical trade dates or a backfill range
- **THEN** the system SHALL NOT silently use current master refresh as a point-in-time universe substitute
- **AND** the rebuild result SHALL record whether current master governance was skipped or explicitly forced

### Requirement: Research reports include master governance diagnostics
The Research Data Engine SHALL expose instrument master governance diagnostics in guarded research and financial job reports.

#### Scenario: Guarded research job completes
- **WHEN** a guarded research or financial job completes successfully
- **THEN** its scheduler report SHALL include the master-governance status and any warnings separately from domain-specific success counts

#### Scenario: Master governance degrades but domain sync continues
- **WHEN** a guarded research or financial job continues after degraded master governance
- **THEN** the job report SHALL state that the domain result used an existing or fallback-governed universe

### Requirement: Research configuration controls governance participation
The Research Data Engine SHALL allow configuration or explicit runtime overrides to control which research and financial jobs participate in instrument master governance.

#### Scenario: Module governance is enabled
- **WHEN** a research module has master governance enabled in configuration
- **THEN** its scheduler entry point SHALL call the shared governance guard before invoking the domain sync service

#### Scenario: Module governance is disabled
- **WHEN** a research module has master governance disabled in configuration
- **THEN** the scheduler result SHALL identify that master governance was skipped by configuration if report payloads include governance details

#### Scenario: PB valuation is computed
- **WHEN** the system has a latest available balance-sheet equity or book-value fact
- **THEN** it SHALL compute PB using the latest report-period balance-sheet denominator and record the report period used

#### Scenario: Forward valuation input is unavailable
- **WHEN** analyst forecast or forward financial data is not enabled or not available for an instrument
- **THEN** dynamic or forward PE/PS SHALL return an explicit unavailable status instead of reusing static or TTM denominators

### Requirement: Relative Valuation Must Use Authoritative Peer Groups And Clean Multiples
Relative valuation SHALL use authoritative Shenwan peer groups by default and SHALL exclude invalid or unsupported multiples from benchmark statistics.

#### Scenario: Peer benchmark is computed
- **WHEN** relative valuation computes industry PE/PB/PS benchmarks
- **THEN** it SHALL use the configured authoritative Shenwan peer level and return peer count, mean, median, quartiles, percentile rank, and premium or discount to median

#### Scenario: Invalid peer multiples exist
- **WHEN** peer valuation rows contain negative denominators, zero denominators, missing values, or unsupported metric variants
- **THEN** the benchmark calculation SHALL exclude those values and report the valid peer count used for each metric

### Requirement: Financial Updates Must Separate Daily Catchup From Weekly Reconciliation
The scheduler SHALL support a daily incremental financial statement catch-up job and a separate weekly reconciliation or repair job so financial updates do not depend only on the Saturday maintenance window.

#### Scenario: Daily catchup runs
- **WHEN** the daily financial catch-up job runs
- **THEN** it SHALL discover newly disclosed or changed financial filings since the previous successful checkpoint and process only affected instruments and report periods

#### Scenario: Weekly reconciliation runs
- **WHEN** the weekly financial reconciliation job runs
- **THEN** it SHALL verify coverage, retry missing or failed filings, compare source hashes, and emit readiness blockers without colliding with the shareholder full-refresh window

### Requirement: Financial And Valuation Readiness Must Expose Coverage Blockers
The Research Data Engine SHALL expose readiness summaries for financial statement coverage and relative valuation coverage before valuation rollout can be enabled.

#### Scenario: Financial coverage is incomplete
- **WHEN** required report periods or core valuation facts are missing for target instruments
- **THEN** readiness SHALL report missing instruments, missing periods, missing core facts, source distribution, parser version distribution, and rollout blockers

#### Scenario: Valuation coverage is incomplete
- **WHEN** PE/PB/PS valuation metrics do not cover the configured target universe
- **THEN** valuation readiness SHALL report metric-level coverage and prevent production rollout until blockers are resolved or explicitly accepted

### Requirement: Official Financial Backfill Production Writes Must Be Gated
The Research Data Engine SHALL NOT enable SSE official structured financial statement production writes or valuation rollout until dry-run evidence and readiness blockers are explicitly reported.

#### Scenario: Production backfill is requested without dry-run evidence
- **WHEN** an operator attempts to run write-enabled SSE official financial statement backfill without recorded dry-run evidence for the target exchange, report periods, and sample scope
- **THEN** the command SHALL refuse write-enabled execution and SHALL report the missing gate evidence

#### Scenario: Dry-run evidence is summarized
- **WHEN** SSE official structured JSON dry-run completes for a larger multi-period sample
- **THEN** readiness output SHALL include exchange, report periods, storage target, checkpoint path, request policy, parser version, source manifest count, numeric fact count, core fact coverage, failed instrument-period pairs, and fallback requirements

#### Scenario: Valuation rollout depends on financial readiness
- **WHEN** valuation rollout readiness is evaluated
- **THEN** it SHALL treat missing financial statement dry-run evidence, missing required core facts, parser failures, or unresolved fallback lists as rollout blockers unless explicitly accepted by configuration or operator override

#### Scenario: Non-SSE markets require separate evidence
- **WHEN** SSE official structured JSON dry-run passes
- **THEN** CNInfo/SZSE and BSE SHALL remain separate official-source readiness dimensions until concrete parseable structured artifacts or equivalent official structured endpoints, parser coverage, unit normalization, and bounded dry-run evidence are validated for those markets

### Requirement: SSE Official Financial Backfill Command Must Be Production Safe
The Research Data Engine SHALL provide an SSE official structured financial statement backfill command that is non-destructive by default and requires explicit operator intent before writing to production storage.

#### Scenario: Backfill runs without write intent
- **WHEN** the SSE official financial backfill command is executed without an explicit write-enabled option
- **THEN** it SHALL run against an isolated or explicitly selected non-production storage target and SHALL NOT mutate production financial facts

#### Scenario: Backfill scope is missing
- **WHEN** the SSE official financial backfill command is executed without bounded exchange, instrument scope, report-period scope, batch size, timeout, throttle, or checkpoint policy
- **THEN** it SHALL refuse execution and report the missing operational parameters

#### Scenario: Production write is requested
- **WHEN** an operator requests write-enabled SSE official financial backfill
- **THEN** the command SHALL report storage target, exchange, report periods, instrument scope, source configuration keys, request policy, parser version, checkpoint path, and write gate status before processing instruments

### Requirement: SSE Official Financial Backfill Writes Must Be Checkpointed And Auditable
SSE official financial statement backfill SHALL record checkpoint and lineage evidence at instrument-period granularity so production writes are resumable and auditable.

#### Scenario: Instrument period succeeds
- **WHEN** a write-enabled SSE official backfill successfully persists source manifests, numeric facts, core facts, and tier placement for an instrument and report period
- **THEN** the checkpoint SHALL mark only that instrument-period as completed and SHALL preserve source-file lineage, parser diagnostics, parser version, and ingestion run linkage

#### Scenario: Instrument period fails
- **WHEN** one instrument-period fails, times out, returns no parseable official facts, or lacks required core facts
- **THEN** the command SHALL preserve completed instrument-periods, record the failed instrument-period pair with reason, and keep it retryable without rolling back unrelated successes

#### Scenario: Production run is resumed
- **WHEN** a production backfill is restarted with an existing checkpoint
- **THEN** it SHALL skip only completed instrument-periods that match the current exchange, report period set, parser/source configuration, and storage target policy

### Requirement: Financial Backfill Readiness Evidence Must Gate Production Promotion
The Research Data Engine SHALL gate SSE official financial production writes and valuation rollout with structured dry-run evidence and post-run readiness blockers.

#### Scenario: Dry run evidence is missing
- **WHEN** write-enabled SSE official financial backfill is requested without matching dry-run evidence or an explicit recorded operator override
- **THEN** the command SHALL refuse production writes and report the missing or mismatched evidence fields

#### Scenario: Dry run evidence is accepted
- **WHEN** matching dry-run evidence exists for the requested exchange, report periods, source configuration, parser version, request policy, and required core fact coverage
- **THEN** the command SHALL allow the write-enabled run to proceed and SHALL include the accepted evidence summary in the run output

#### Scenario: Backfill completes
- **WHEN** a write-enabled SSE official financial backfill completes
- **THEN** readiness output SHALL report source manifest count, numeric fact count, core fact coverage, hot/cold tier coverage, parser failures, failed instrument-periods, fallback requirements, and valuation rollout blockers

#### Scenario: Non-SSE official sources require independent promotion gates
- **WHEN** SSE official financial backfill readiness passes
- **THEN** CNInfo/SZSE and BSE official structured source readiness SHALL still require their own validated parseable artifacts or equivalent structured endpoints, parser coverage, dry-run evidence, and production write gates before promotion

### Requirement: Financial Statements Must Expose Layered Service Sources
The Research Data Engine SHALL separate financial statement service sources into a local core layer, an official structured maintenance layer, an official summary validation layer, and a remote extension layer.

#### Scenario: Local core layer is used for routine reads
- **WHEN** a financial statement read requests facts covered by the approved Sina/THS local core mapping
- **THEN** the system SHALL serve those facts from local storage using canonical fact names, canonical units, report period metadata, source lineage, and mapping version

#### Scenario: CNInfo data20 is used as official maintenance source
- **WHEN** financial disclosure incremental sync or reconciliation sync repairs a missing or incomplete instrument-period
- **AND** CNInfo data20 is configured for the exchange, instrument, report period, and requested canonical fact
- **THEN** the system SHALL attempt CNInfo data20 structured JSON first, normalize `CNY_10K` values to CNY, preserve official source lineage, and only mark facts ready when semantic and canonical mapping checks pass

#### Scenario: CNInfo data20 cannot satisfy a required fact
- **WHEN** CNInfo data20 is unavailable, returns no parseable statement, lacks a requested canonical fact, or returns a semantically ambiguous value such as total equity where parent equity is required
- **THEN** the system SHALL record the official-source failure or ambiguity reason
- **AND** it MAY attempt the configured Sina/THS field-level fallback for only the missing or invalid canonical facts

#### Scenario: CNInfo is used as official summary validation
- **WHEN** CNInfo data20 is used for financial statement validation
- **THEN** the system SHALL treat it as an official summary validation source, normalize `CNY_10K` values to CNY, and SHALL NOT promote it as complete statement coverage

#### Scenario: Remote extension is required
- **WHEN** a request asks for a financial field not covered by the local core layer
- **THEN** the system MAY invoke the configured Eastmoney remote extension path and SHALL return canonicalized values, canonical units, source lineage, and `is_remote=true` without writing those facts into L1 local core storage by default

### Requirement: Sina And THS Core Fields Must Be Strictly Audited
The local core financial layer SHALL only include Sina/THS field pairs that pass strict semantic, unit, sample, and accounting identity validation under a versioned mapping profile.

#### Scenario: Candidate fields are exactly equivalent
- **WHEN** a Sina field and THS metric have matching statement family, field meaning, ownership semantics, period/value type, and units or explicit unit conversion
- **THEN** the mapping audit MAY mark the relationship as `exact_equivalent` or `equivalent_after_unit` and set `approved_for_core=true`

#### Scenario: Candidate fields are only related
- **WHEN** two fields are related but one contains the other, is a subcomponent, has different ownership semantics, has different period/value type, or differs by industry template
- **THEN** the mapping audit SHALL classify the relationship as `broader_than`, `narrower_than`, `related_only`, `rejected`, or `unknown_candidate` and SHALL NOT allow the pair into the local core layer

#### Scenario: Bank and nonbank fields differ
- **WHEN** a source field belongs to a bank-specific statement template
- **THEN** the mapping audit SHALL use a bank profile and SHALL NOT reuse nonbank mappings unless the mapping is explicitly approved for the bank profile

### Requirement: Financial Mapping Decisions Must Be Versioned And Auditable
Financial source-field mapping SHALL preserve enough evidence to explain why a field is or is not part of the local core layer.

#### Scenario: Mapping audit emits a decision
- **WHEN** a candidate mapping is evaluated
- **THEN** the result SHALL include canonical fact, statement family, Sina field when present, THS metric when present, relationship, source unit, canonical unit, unit multiplier, value type, sample pass count, sample fail count, max relative difference, accounting identity status, approved-for-core flag, mapping version, and rejection reason when applicable

#### Scenario: Upstream fields change
- **WHEN** Sina or THS adds, removes, renames, or changes a source field
- **THEN** the system SHALL update the source-field catalog or audit output and require mapping-version review before changing the local core approved field set

#### Scenario: Storage model remains stable
- **WHEN** the approved Sina/THS intersection changes
- **THEN** the system SHALL manage the change through canonical fact catalog and mapping version metadata rather than dynamically adding or dropping physical financial fact columns

### Requirement: Sina THS Local Core Promotion Must Require Live Audit Evidence
The Research Data Engine SHALL require bounded live audit evidence before enabling Sina/THS local-core financial facts for production updates or routine reads.

#### Scenario: Live audit evidence is generated
- **WHEN** an operator runs a bounded Sina/THS local-core live audit
- **THEN** the system SHALL record instruments, report periods, mapping version, profile, source interfaces, request timing, source errors, field counts, approved mapping checks, unit checks, accounting identity checks, status, and promotion blockers

#### Scenario: Live evidence fails
- **WHEN** any required source cannot be fetched, approved mappings mismatch beyond configured tolerance, required local-core fields are missing, or identity checks fail
- **THEN** the promotion summary SHALL report blockers and SHALL NOT mark the mapping version as promotable

#### Scenario: Missing local-core fields have source candidates
- **WHEN** live evidence reports `approved_local_core_fact_missing`
- **THEN** the blocker SHALL include source-field candidates by canonical fact with source name, statement type, field name, value, canonical semantic, and canonical unit
- **AND** these candidates SHALL remain review evidence only until a mapping-version change explicitly approves them

#### Scenario: Remote extension is observed
- **WHEN** Eastmoney remote extension is included in the live audit
- **THEN** the evidence SHALL keep Eastmoney results separate from L1 local-core facts and SHALL NOT treat Eastmoney-only fields as local-core coverage

#### Scenario: Production enablement is requested
- **WHEN** local-core production enablement is considered
- **THEN** the operator SHALL have recent passing live audit evidence for the target mapping version, profiles, representative bank/nonbank instruments, and requested report periods

### Requirement: THS Financial Statements Must Be Supported As A Local Source
The AkShare financial statement provider SHALL support THS new financial statement interfaces as a source for local core financial facts and mapping audit.

#### Scenario: THS statement frames are fetched
- **WHEN** the provider is configured to use `ths_report`
- **THEN** it SHALL fetch balance sheet, income statement, and cash flow data through `stock_financial_debt_new_ths`, `stock_financial_benefit_new_ths`, and `stock_financial_cash_new_ths`

#### Scenario: THS long rows are normalized
- **WHEN** THS statement data contains `metric_name`, `value`, `single`, `yoy`, `mom`, or `single_yoy`
- **THEN** the provider or parser SHALL preserve source metric names and value-type fields so mapping audit can distinguish point-in-time balances, cumulative amounts, single-period amounts, ratios, and growth fields

### Requirement: Eastmoney Remote Extension Must Be Explicit And Bounded
The Eastmoney financial statement source SHALL be used as a bounded remote extension for fields outside the local core layer rather than as an implicit fallback that changes local read semantics.

#### Scenario: Remote extension is invoked
- **WHEN** a caller explicitly allows remote extension for missing local financial fields
- **THEN** the system SHALL fetch the required Eastmoney statements within configured timeout, retry, throttle, and cache policy and return converted canonical facts with remote lineage

#### Scenario: Remote extension is not allowed
- **WHEN** a caller does not allow remote extension and requested fields are absent from L1 local core facts
- **THEN** the read service SHALL return a structured missing-field response rather than silently calling Eastmoney

#### Scenario: Eastmoney value is returned
- **WHEN** Eastmoney supplies a requested remote field
- **THEN** the result SHALL include source field name, source unit, canonical unit, conversion metadata, report period, and remote/cache status

### Requirement: Maintenance Repair Source Routing Must Be Fact Level
Financial maintenance repair SHALL decide source priority per canonical fact so official structured facts can be used without forcing all facts for an instrument-period to come from the same source.

#### Scenario: Maintenance jobs use a shared repair router
- **WHEN** full import preparation, disclosure incremental sync, or reconciliation sync needs targeted financial statement repair
- **THEN** the job SHALL pass canonical instrument-period targets and required canonical facts into a shared maintenance repair routing abstraction
- **AND** provider-specific CNInfo data20, Sina, THS, and future source fallback rules SHALL NOT be duplicated inside scheduler task implementations

#### Scenario: CNInfo partially satisfies a candidate
- **WHEN** CNInfo data20 supplies some required canonical facts for a candidate instrument-period and other required facts remain missing
- **THEN** the repair path SHALL persist or accept the official facts with CNInfo lineage
- **AND** it SHALL attempt Sina/THS fallback only for the remaining missing facts
- **AND** readiness SHALL be evaluated against the combined canonical fact set after all accepted source attempts

#### Scenario: Fallback values are used
- **WHEN** Sina or THS supplies a fallback value for a fact that CNInfo data20 could not satisfy
- **THEN** the stored or returned fact SHALL include source name, source field, mapping version, canonical unit, fallback reason, and source-attempt lineage

#### Scenario: Official ambiguity is not silently filled
- **WHEN** CNInfo data20 provides a value with a broader, narrower, or ambiguous semantic relationship to the requested canonical fact
- **THEN** the system SHALL NOT mark the requested fact ready from that value
- **AND** it SHALL expose the ambiguity in diagnostics before trying fallback sources

### Requirement: Financial Disclosure Events Drive Targeted Maintenance
The Research Data Engine SHALL support financial statement maintenance driven by CNInfo periodic-report and disclosure-anomaly announcements.

#### Scenario: Announcement event maps to report period
- **WHEN** a financial announcement event references an annual report, semiannual report, first-quarter report, or third-quarter report
- **THEN** the Research Data Engine SHALL normalize the event to a specific `report_period`
- **AND** it SHALL preserve the announcement ID, title, time, market, symbol, and selection reasons as evidence

#### Scenario: Announcement event has no report period
- **WHEN** a financial announcement event cannot be mapped to a report period
- **THEN** the Research Data Engine SHALL record it as review-only evidence
- **AND** it SHALL NOT relax financial readiness gates for any instrument-period

### Requirement: Financial Local Core Completeness Remains Strict
The Research Data Engine SHALL keep local-core financial completeness rules independent from announcement-driven gap explanations.

#### Scenario: Accepted disclosure anomaly exists
- **WHEN** an instrument-period has an accepted lifecycle or disclosure-anomaly classification
- **THEN** the Research Data Engine MAY allow the batch to continue
- **AND** it SHALL still record the missing required facts and accepted classification

#### Scenario: No accepted explanation exists
- **WHEN** required local-core financial facts are missing and there is no lifecycle, delisting, or disclosure-anomaly evidence
- **THEN** the Research Data Engine SHALL keep the instrument-period as a blocking data-quality item

### Requirement: Financial Maintenance Writes To Financial Database
Financial full import, incremental sync, and reconciliation tasks SHALL write production financial facts to the configured financial database rather than to the research database.

#### Scenario: Production maintenance runs
- **WHEN** a financial maintenance task writes source manifests, raw statements, numeric facts, core facts, mapping audits, or run metadata
- **THEN** it SHALL use the configured `financials_db_path`
- **AND** it SHALL report the actual database path in structured results

### Requirement: Financial Statements History API
The Research Data Engine SHALL expose a company-level financial statements history API that returns multiple report periods using the same canonical field semantics as the single-period financial statements API.

#### Scenario: Latest rolling periods are returned
- **WHEN** a caller requests `/api/v1/research/company/{instrument_id}/financial-statements/history` with `period_window=latest` and `rolling_quarters=N`
- **THEN** the API SHALL return up to `N` locally available report periods for that company ordered by report period descending
- **AND** each item SHALL include the same normalized facts and indicators shape as the single-period financial statements API

#### Scenario: Explicit report periods are returned
- **WHEN** a caller supplies `report_periods` as a comma-separated list
- **THEN** the API SHALL return only matching local financial statement periods for the company
- **AND** missing requested periods SHALL NOT trigger network fetches or remote extension by default

#### Scenario: Raw statements are optional
- **WHEN** `include_statements=false`
- **THEN** the API SHALL omit raw statement payloads for each period while preserving core facts and indicators

#### Scenario: Service-layer diagnostics are requested
- **WHEN** `include_local_core=true` is supplied
- **THEN** the API SHALL attach local-core service-layer diagnostics per returned report period using the same mapping/profile semantics as the single-period endpoint

### Requirement: Numeric Coverage Gate For Financial Production Promotion

Production-oriented official financial statement backfill MUST require dry-run
evidence that includes a bounded `financial_numeric_facts` coverage audit.

#### Scenario: Missing coverage audit blocks production write
- **WHEN** write-enabled production backfill validates dry-run evidence
- **AND** the evidence has no `numeric_fact_coverage` object
- **THEN** the gate MUST reject the evidence unless an explicit operator override is supplied

#### Scenario: Missing required canonical fields blocks promotion
- **WHEN** the coverage audit reports missing required canonical facts for any selected instrument-period
- **THEN** the production gate MUST reject the evidence

#### Scenario: Unit conflicts block promotion
- **WHEN** the coverage audit reports conflicting units for the same canonical fact
- **THEN** the production gate MUST reject the evidence

#### Scenario: Unmapped fields remain visible without automatic rejection
- **WHEN** the coverage audit reports unmapped source-native fields
- **AND** all required canonical facts are present with no unit conflicts
- **THEN** the gate MAY accept the evidence while preserving the unmapped field count for catalog maintenance

### Requirement: Source Profile Comparisons Include Long-Form Coverage

Same-sample official financial source-profile comparisons MUST include numeric
fact coverage summaries in addition to speed, stability, and core-fact
consistency metrics.

#### Scenario: Coverage warning is surfaced
- **WHEN** any compared profile has non-passing numeric coverage
- **THEN** the comparison assessment MUST surface a numeric coverage warning for that profile

### Requirement: Financial Numeric Facts Must Use A Unified Catalog
The Research Data Engine SHALL standardize structured financial statement rows through a versioned fact catalog shared by official sources and fallback sources.

#### Scenario: Source-native field is known
- **WHEN** SSE coded fields, CNInfo row labels, XBRL tags, or fallback provider fields match a catalog alias
- **THEN** the numeric fact row SHALL retain the source-native `fact_name` and also persist canonical name, statement family, semantic, unit, and catalog version metadata

#### Scenario: Source-native field is unknown
- **WHEN** a source returns a numeric row that is not in the catalog
- **THEN** the row SHALL still be stored with source lineage and SHALL be reported as unmapped by coverage diagnostics rather than silently dropped

#### Scenario: Share capital is reported as a balance-sheet amount
- **WHEN** a source provides `S2010_0700`, `实收资本（或股本）`, or equivalent paid-in capital amount rows
- **THEN** the row SHALL map to `share_capital_amount` with CNY unit and SHALL NOT populate `shares_outstanding`

### Requirement: Fallback Sources Must Share Long-Form Storage
Fallback financial statement providers SHALL write numeric statement rows through the same long-form fact storage model as official structured sources.

#### Scenario: AkShare fallback bundle is accepted
- **WHEN** AkShare fallback supplies balance sheet, income statement, or cash-flow rows for an instrument-period
- **THEN** the sync SHALL persist those rows as `financial_numeric_facts` with source file lineage, statement family, parser version, source mode, raw value, and canonical metadata when available

#### Scenario: Official facts already exist
- **WHEN** a higher-priority official source fact exists for the same instrument-period
- **THEN** fallback facts SHALL NOT overwrite official core facts unless a later explicit repair mode permits it

### Requirement: Financial Field Coverage Must Be Auditable
The system SHALL provide a bounded audit path for validating field completeness and cross-source compatibility before production promotion.

#### Scenario: Coverage audit is run
- **WHEN** an operator audits one or more instrument-periods
- **THEN** the result SHALL report source-native field counts, canonical field counts, unmapped fields, missing required canonical facts, unit conflicts, semantic warnings, and source distribution

### Requirement: Official Financial Backfill Must Support Source Profiles
The Research Data Engine SHALL support production-safe official financial statement backfill for multiple configured official source profiles rather than only SSE.

#### Scenario: CNInfo dry-run is requested
- **WHEN** an operator runs a non-write official financial backfill or validation for `SSE`, `SZSE`, or `BSE` with source profile `cninfo_data20`
- **THEN** the command SHALL use temporary or explicitly selected non-production storage by default, process bounded instrument-period batches, and emit source-profile evidence without mutating production financial facts

#### Scenario: CNInfo production write is requested
- **WHEN** an operator requests write-enabled CNInfo data20 official financial backfill for `SSE`, `SZSE`, or `BSE`
- **THEN** the command SHALL require explicit write intent, production storage target, matching dry-run evidence, profile-aware checkpoint metadata, bounded scope, request policy, and readiness gates before writing production financial facts

#### Scenario: Source profile evidence mismatches
- **WHEN** write-enabled backfill evidence was generated for a different exchange, source, source mode, parser profile, request policy, report period set, storage target, or instrument scope
- **THEN** the command SHALL refuse production writes unless an explicit operator override is supplied and recorded in output metadata

### Requirement: Market Readiness Must Remain Source Scoped
Financial readiness SHALL report each market/source profile independently so success in one official source profile does not promote another market by implication.

#### Scenario: SSE readiness passes
- **WHEN** SSE `commonQuery.do` readiness passes
- **THEN** readiness SHALL still require separate CNInfo data20 evidence and gates for SZSE and BSE, and SHALL report any SSE/CNInfo evidence as a separate alternate source profile rather than as the default SSE source profile

#### Scenario: CNInfo readiness passes for SZSE
- **WHEN** CNInfo data20 readiness passes for SZSE
- **THEN** readiness SHALL still require separate CNInfo or equivalent official evidence and gates for BSE

#### Scenario: CNInfo readiness passes for BSE
- **WHEN** CNInfo data20 readiness passes for BSE
- **THEN** readiness SHALL report BSE as covered by `cninfo:direct` and SHALL NOT imply that BSE-hosted XBRL/XML/ZIP artifacts have been found

### Requirement: Official Source Promotion Must Report Fallback And Access Risk
Official financial source promotion SHALL include fallback and access-risk evidence before valuation rollout or scheduler enablement can treat the source as production-ready.

#### Scenario: Fallback is used during validation
- **WHEN** AkShare or another fallback source fills missing official facts during a validation or backfill run
- **THEN** readiness SHALL report fallback share, fallback source distribution, fallback reasons, and fallback-filled fields as rollout blockers unless the operator accepts them explicitly

#### Scenario: Official endpoint throttling is detected
- **WHEN** an official endpoint shows timeout, HTTP blocking, empty payload, malformed JSON, or parser failures during bounded validation
- **THEN** readiness SHALL report those as source-profile blockers and SHALL keep production scheduling disabled for that profile

### Requirement: Official financial backfill selects source profiles by period availability
The Research Data Engine SHALL resolve the official financial source profile from exchange, requested report periods, explicit operator source selection, and configured source-period availability metadata.

#### Scenario: SSE annual period is beyond commonQuery max year
- **WHEN** an operator runs a dry-run or production-gated financial backfill for `SSE`
- **AND** no explicit official source is provided
- **AND** all requested report periods exceed the configured `sse_commonquery` max year for their report type
- **THEN** the command SHALL select `cninfo_data20` as the official source profile
- **AND** the evidence SHALL record the source-selection reason and affected periods
- **AND** production write gates SHALL still require matching dry-run evidence and numeric coverage gates.

#### Scenario: Operator explicitly selects SSE
- **WHEN** an operator runs financial backfill for `SSE`
- **AND** `--official-source sse` is provided
- **THEN** the command SHALL use `sse_commonquery`
- **AND** it SHALL NOT auto-switch to CNInfo even if the requested period is configured as beyond the SSE max year.

#### Scenario: CNInfo has long-form facts but strict core semantics are incomplete
- **WHEN** CNInfo data20 writes numeric facts for an SSE period
- **AND** required canonical facts such as `equity_parent` are missing while only `equity_total` is present
- **THEN** the numeric coverage gate SHALL report the semantic gap
- **AND** the production gate SHALL remain blocked unless an explicit operator override or required-fact policy change is supplied.

### Requirement: Financial Numeric Coverage Gaps Must Be Classified
Financial numeric coverage evidence SHALL classify the cause of every missing or incompatible required field instead of reporting only a generic degraded status.

#### Scenario: Instrument-period has no numeric facts
- **WHEN** coverage audit finds zero stored numeric facts for an expected instrument and report period
- **THEN** the audit SHALL include `missing_numeric_rows` in that instrument-period's `gap_reasons`
- **AND** the aggregate summary SHALL count the reason in `gap_reason_counts`

#### Scenario: Required canonical fact is missing
- **WHEN** a required canonical fact is absent from an instrument-period
- **THEN** the audit SHALL include `missing_required_canonical_fact`
- **AND** it SHALL list the missing canonical fact names

#### Scenario: Semantically related field is present but not equivalent
- **WHEN** a required parent-attributable fact is missing but a total-company canonical fact is present, such as `equity_total` for missing `equity_parent`
- **THEN** the audit SHALL classify the condition as `semantic_gap`
- **AND** it SHALL NOT treat the total-company fact as satisfying the required parent-attributable fact

#### Scenario: Required fact can only be derived with missing components
- **WHEN** a required fact could be derived from configured components but at least one component is absent
- **THEN** the audit SHALL classify the condition as `derivation_component_gap`
- **AND** it SHALL identify the present and missing component canonical facts

#### Scenario: Unmapped field may be a required alias
- **WHEN** a source-native unmapped field appears to be a candidate alias for a missing required canonical fact
- **THEN** the audit SHALL classify the condition as `alias_gap_candidate`
- **AND** catalog maintainers SHALL review semantic meaning and unit before mapping it

#### Scenario: Unmapped fields are not required blockers
- **WHEN** unmapped source-native fields exist but they are not candidates for missing required facts
- **THEN** the audit SHALL classify them as `unmapped_nonrequired_fields`
- **AND** those fields SHALL remain catalog backlog rather than production-ready evidence

#### Scenario: Dry-run evidence summarizes gap reasons
- **WHEN** financial statement dry-run evidence attaches numeric fact coverage
- **THEN** the compact evidence summary SHALL include aggregate gap reason counts so operators can distinguish source availability, alias, semantic, and unit problems.

### Requirement: Industry Financial Fact Packs Must Be Profile Scoped
The Research Data Engine SHALL support optional industry-specific financial fact packs for `bank`, `securities`, and `insurance` profiles without changing the global L1 common-core required fact set.

#### Scenario: Common facts remain stable
- **WHEN** a financial update evaluates L1 common-core readiness for any company profile
- **THEN** the readiness gate SHALL use the existing common required facts and SHALL NOT require industry-specific facts unless a future configuration explicitly promotes an industry pack to required status

#### Scenario: Industry pack applies only to matching profile
- **WHEN** a company resolves to `bank`, `securities`, or `insurance`
- **THEN** only the matching profile's industry fact pack SHALL be considered applicable
- **AND** the system SHALL NOT apply bank-specific facts to securities, insurance, or nonbank companies

#### Scenario: Industry pack is missing
- **WHEN** an applicable industry pack field is missing from local data
- **THEN** the system SHALL report an `industry_pack_missing` diagnostic with profile, pack version, canonical fact, source coverage, and report period
- **AND** the missing industry field SHALL NOT be recorded as a common `blocking_gap`

### Requirement: Industry Financial Fields Must Be Versioned And Audited
Industry-specific financial fields SHALL be admitted through a versioned, profile-aware mapping catalog with explicit semantic and unit evidence.

#### Scenario: Industry field is approved
- **WHEN** a candidate industry field has matching profile, statement family, field meaning, ownership semantics, period/value type, unit or explicit unit conversion, and multi-period sample evidence
- **THEN** the mapping catalog MAY approve it for the industry pack and SHALL record profile, pack version, canonical fact, source fields, relationship, canonical unit, value type, evidence, and approval status

#### Scenario: Industry field is ambiguous
- **WHEN** a candidate field has only related semantics, broader/narrower scope, unclear unit, different value type, or insufficient evidence
- **THEN** the mapping catalog SHALL mark it as rejected, related-only, or review-required and SHALL NOT expose it as an approved L1.5 industry fact

#### Scenario: Profile-specific correction exists
- **WHEN** an industry profile has a source field whose name resembles a common-field mapping but differs in financial meaning
- **THEN** the profile-specific decision SHALL override generic inheritance and SHALL preserve the rejection or corrected mapping in the versioned catalog

### Requirement: Industry Financial Facts Must Be Exposed Separately
The financial read layer and API SHALL expose industry-specific facts separately from common financial facts and indicators.

#### Scenario: API returns company financial statements
- **WHEN** a company has approved industry-specific facts for the requested report period
- **THEN** the response SHALL include those values in a separate `industry_facts` section or equivalent profile-scoped service layer with canonical units, profile, pack version, and source lineage

#### Scenario: Caller requests unsupported industry field
- **WHEN** a caller asks for an industry-specific field that is not available locally
- **THEN** the response SHALL return structured missing-field diagnostics and MAY use an explicitly allowed remote extension path without writing the remote-only value into L1.5 local storage by default

### Requirement: Industry Pack Rollout Must Be Evidence Bounded
Industry-specific fact packs SHALL be introduced through bounded evidence, tests, and documentation before routine use.

#### Scenario: Initial rollout is run
- **WHEN** a new industry pack version is proposed
- **THEN** validation SHALL include representative companies for the target profile, multiple report periods where practical, source field counts, approved mapping checks, unit checks, and promotion blockers

#### Scenario: Production maintenance runs
- **WHEN** daily incremental or weekly reconciliation financial tasks process a company with an industry profile
- **THEN** industry pack coverage SHALL be reported separately from common readiness and SHALL NOT degrade the whole task solely because optional industry facts are absent

### Requirement: Valuation History Must Use Local Auditable Inputs
The Research Data Engine SHALL rebuild daily valuation history only from local quotes, local financial facts, and local market-cap or share-count inputs with explicit source and availability metadata.

#### Scenario: Market-cap input is available locally
- **WHEN** valuation history is rebuilt for an instrument and trade date
- **THEN** the rebuild SHALL use a locally stored market-cap value or a locally stored share-count value multiplied by local close price
- **AND** the valuation row SHALL record the input source, source mode, input date, and calculation version in details or equivalent lineage

#### Scenario: Share-count input is ambiguous
- **WHEN** only an amount-denominated capital fact such as share capital amount is available
- **THEN** the rebuild SHALL NOT treat that amount as shares outstanding
- **AND** it SHALL classify the instrument-date as unavailable with a market-cap or share-count input missing reason

#### Scenario: External source is needed for valuation inputs
- **WHEN** market-cap or share-count data must be fetched from a third-party source
- **THEN** the fetch SHALL occur in a configured sync or rebuild step that writes local auditable inputs
- **AND** valuation read APIs SHALL NOT fetch that external source synchronously

#### Scenario: Valuation input full backfill is requested
- **WHEN** operators run valuation input sync in full/history mode for A-share instruments
- **THEN** the sync SHALL fetch per-symbol CNInfo capital-change events through a configured provider
- **AND** it SHALL convert 10k-share source values into raw shares before writing `valuation_inputs`
- **AND** it SHALL preserve both the capital effective date and announcement date in lineage

#### Scenario: Valuation input daily update is requested
- **WHEN** operators run valuation input sync in incremental mode
- **THEN** the sync SHALL use an efficient all-market CNInfo capital snapshot when configured
- **AND** it SHALL upsert normalized share-count inputs into `valuation.db` before valuation history rebuild consumes them

#### Scenario: Valuation input sync is registered for operations
- **WHEN** scheduler configuration is loaded
- **THEN** valuation input daily sync SHALL be present but disabled until rollout approval
- **AND** valuation history daily rebuild SHALL be present but disabled until rollout approval
- **AND** valuation input daily sync and valuation history rebuild SHALL be scheduled after A-share daily quote update so same-day local close prices are available before valuation rows are calculated
- **AND** valuation input full backfill SHALL be available as an enabled manual-only job
- **AND** both paths SHALL log source, mode, exchange, requested instruments, written rows, missing counts, and elapsed time for progress tracking

### Requirement: Valuation History Storage Must Be Isolated Bounded And Per Instrument Date
The Research Data Engine SHALL store daily valuation metrics in a dedicated valuation database as per-instrument, per-trade-date rows rather than materializing all peer comparison combinations.

#### Scenario: Daily valuation history is persisted
- **WHEN** valuation history is rebuilt for a configured window
- **THEN** the system SHALL write to `data/valuation.db` at most one row per instrument, trade date, calculation method, calculation version, and parameter hash
- **AND** the row SHALL include market cap, supported PE/PB/PS variants, source lineage, and metric diagnostics

#### Scenario: Current valuation history is read
- **WHEN** valuation history, readiness coverage, or relative valuation reads stored rows
- **THEN** the read path SHALL filter by the current `calc_method`, `calc_version`, and `parameter_hash` unless an explicit audit query requests another identity
- **AND** prior bounded-window or prior-parameter rows SHALL NOT affect latest API values, readiness coverage, or peer statistics

#### Scenario: Valuation history identity is calculated
- **WHEN** the system computes `parameter_hash` for valuation history
- **THEN** the hash SHALL include parameters that can change a single date's PE/PB/PS or market-cap result
- **AND** it SHALL exclude task-window and scheduler parameters such as `lookback_days`, `window_mode`, `quote_limit_days`, `write_policy`, progress logging, and runtime timeout
- **AND** daily, weekly, and past-12-quarter updates SHALL share the same hash when their single-date valuation calculation semantics are the same
- **AND** migration code MAY temporarily recognize compatible legacy hashes for missing-only checks and reads until storage maintenance rewrites or removes them

#### Scenario: Total and float market cap are calculated
- **WHEN** a valuation input row contains explicit total and float share counts
- **THEN** valuation history SHALL calculate total market cap from local close times total shares
- **AND** it SHALL calculate float market cap from local close times float shares
- **AND** it SHALL retain the valuation input lineage used for both calculations

#### Scenario: Valuation database is isolated
- **WHEN** valuation storage is initialized
- **THEN** valuation inputs, valuation history, valuation ingestion audit, and valuation lineage SHALL be physically stored outside `research.db`
- **AND** `research.db` SHALL remain the source of light research dimensions such as authoritative industry membership

#### Scenario: Relative valuation is requested
- **WHEN** a caller requests relative valuation for a stock
- **THEN** the service SHALL compute peer statistics from stored valuation history and authoritative industry membership at request time or from a bounded aggregate cache
- **AND** it SHALL NOT require a persisted full matrix of subject-peer-date comparisons

#### Scenario: Rebuild window is configured
- **WHEN** valuation history rebuild runs without an explicit override
- **THEN** the lookback window SHALL come from configuration
- **AND** the rebuild SHALL NOT default to an unbounded full-history write

#### Scenario: Daily or weekly valuation history update is requested
- **WHEN** valuation history rebuild runs for a daily or weekly trading-day window
- **THEN** the default write policy SHALL be `missing_only`
- **AND** the rebuild SHALL only persist rows missing for the full key `instrument_id + as_of_date + calc_method + calc_version + parameter_hash`
- **AND** instruments that already have complete candidate valuation dates for the selected window SHALL skip full metric calculation
- **AND** operators SHALL be able to explicitly request `overwrite` for controlled repair of the selected window

#### Scenario: Past 12-quarter valuation history update is requested
- **WHEN** operators run the manual past-12-quarter valuation history update
- **THEN** the rebuild SHALL derive each instrument's quote start boundary from the earliest locally available financial fact `data_available_date` in the latest 12-quarter window
- **AND** it SHALL default to `missing_only` rather than rewriting existing rows
- **AND** it SHALL skip full metric calculation for instruments whose selected 12-quarter candidate dates are already complete
- **AND** it SHALL NOT be described as an unbounded full-history rebuild

#### Scenario: Newly listed instrument lacks financial facts
- **WHEN** a listed instrument has local quotes and valuation inputs but no available local financial facts
- **THEN** valuation history SHALL persist partial market-cap rows for eligible quote dates
- **AND** PE/PB/PS variants SHALL remain unavailable with `financial_facts_not_available` diagnostics
- **AND** this lifecycle condition SHALL be reported separately from missing valuation inputs

#### Scenario: Backup policy is loaded
- **WHEN** the database backup task discovers project data databases
- **THEN** `data/valuation.db` SHALL be included in the backup policy with the other production databases

### Requirement: Valuation Metrics Must Expose Variant Level Diagnostics
The valuation engine SHALL expose PE/PB/PS variants with metric-level availability, denominator provenance, and invalid-value diagnostics.

#### Scenario: TTM valuation is computed
- **WHEN** enough available financial periods exist for TTM revenue or net income
- **THEN** PE TTM and PS TTM SHALL record all report periods used and their data availability dates

#### Scenario: Denominator is invalid
- **WHEN** a denominator is missing, zero, negative, or unavailable as of the trade date
- **THEN** the metric SHALL be marked unavailable for peer statistics
- **AND** the unavailable reason SHALL be recorded without substituting another metric variant silently

#### Scenario: Forward input is unavailable
- **WHEN** forecast inputs are disabled or unavailable locally
- **THEN** forward PE and PS SHALL be explicitly unavailable rather than copied from static or TTM values

#### Scenario: Daily metric lineage is stored
- **WHEN** valuation history writes `details_json`
- **THEN** details SHALL use compact lineage containing source, key report periods, availability dates, denominators, metric status, and missing reasons
- **AND** it SHALL NOT duplicate full upstream payloads or large diagnostics in every daily row
- **AND** the physical storage representation MAY use short keys if read APIs expand it back to the documented detail fields
- **AND** storage maintenance SHALL be able to compact previously written verbose details and vacuum the dedicated valuation database after explicit confirmation

### Requirement: Valuation Readiness Must Gate Production Rollout
The Research Data Engine SHALL keep valuation production rollout gated until valuation history, valuation inputs, financial readiness, and authoritative peer readiness pass configured checks.

#### Scenario: Valuation history is empty
- **WHEN** `/api/v1/research/valuation/readiness` is requested and no valuation history rows exist
- **THEN** readiness SHALL report `no_valuation_history`
- **AND** valuation SHALL NOT be ready for rollout

#### Scenario: Market-cap inputs are incomplete
- **WHEN** target instruments lack local market-cap or share-count inputs for the configured rebuild window
- **THEN** readiness SHALL report input coverage and blockers by exchange and missing reason

#### Scenario: Financial or industry prerequisites are incomplete
- **WHEN** financial statement readiness or authoritative Shenwan membership readiness blocks valuation use
- **THEN** valuation readiness SHALL include those dependencies in its blockers rather than reporting valuation as independently ready

#### Scenario: Valuation rollout is approved
- **WHEN** valuation readiness has no blockers or only explicitly accepted blockers
- **THEN** operators MAY enable valuation reads through configuration
- **AND** the readiness response SHALL show module enabled state separately from rollout readiness

### Requirement: Valuation Rollout Readiness Must Be Queryable
The research system SHALL expose a read-only readiness summary for the valuation domain.

#### Scenario: Readiness summary exposes valuation coverage and gate status
- **WHEN** the caller requests valuation readiness
- **THEN** the system SHALL return target-universe coverage, valuation-history coverage, source distribution, mode distribution, module gate status, relative-valuation prerequisite status, and rollout blockers

### Requirement: Valuation Readiness Must Require Valuation History Coverage
The research system SHALL report when valuation history rows do not yet cover the current target stock universe.

#### Scenario: Valuation history coverage is incomplete
- **WHEN** persisted latest valuation history rows are fewer than the current target stock universe
- **THEN** the readiness summary SHALL mark rollout as not ready and include an explicit valuation-history coverage blocker

### Requirement: Relative Valuation Readiness Must Use Current Authoritative Industry Membership
The research system SHALL use current authoritative industry memberships as the default prerequisite for relative valuation readiness.

#### Scenario: Relative valuation industry prerequisite is incomplete
- **WHEN** current authoritative industry membership coverage is incomplete and `require_authoritative=true`
- **THEN** the readiness summary SHALL mark relative valuation as not ready and include explicit relative valuation blockers

### Requirement: Valuation History Percentile Is Queryable
The Research Data Engine SHALL provide an on-demand API that computes one instrument's valuation percentile against its own historical valuation series.

#### Scenario: Percentile is computed for default metrics
- **WHEN** a caller requests valuation percentiles for an instrument without specifying metrics
- **THEN** the API SHALL compute `pe_ttm`, `pb_mrq`, and `ps_ttm` percentiles using local `valuation_history`
- **AND** it SHALL use the current valuation-history calculation identity
- **AND** it SHALL NOT fetch remote data synchronously

#### Scenario: Window is defined in quarters
- **WHEN** a caller provides `quarters=N`
- **THEN** the percentile sample SHALL use a historical window ending at `as_of_date` and spanning approximately `N` calendar quarters
- **AND** the response SHALL report the effective `window_start`, `window_end`, and valid sample count for each metric

#### Scenario: Negative multiples are handled explicitly
- **WHEN** the sample or current value contains negative valuation multiples
- **THEN** the API SHALL follow the configured `negative_policy`
- **AND** the response SHALL include warnings when negative multiples limit investment interpretation
- **AND** negative PE SHALL NOT be silently presented as a normal cheap valuation signal
- **AND** when `negative_policy=exclude` removes the current non-positive value, the metric SHALL report that the current value was excluded and SHALL NOT return a valid percentile rank

#### Scenario: Data is insufficient
- **WHEN** a metric has fewer valid observations than `min_points`
- **THEN** the metric SHALL be returned with `status=insufficient_data`
- **AND** the response SHALL include the actual sample count and required minimum

#### Scenario: Percentile matrix is not persisted
- **WHEN** valuation percentile is requested for a single instrument
- **THEN** the system SHALL compute it from existing local valuation history at read time
- **AND** it SHALL NOT require a persisted full-market `instrument x date x metric x window` percentile matrix

### Requirement: Shareholder Routing Must Support AkShare as Primary Provider

The research system SHALL allow `shareholders` routing to select `akshare` as the primary shareholder-summary provider.

#### Scenario: Free-chain resolves AkShare first

- **WHEN** the shareholder domain is configured with `akshare:direct` at the head of `free_chain`
- **THEN** shareholder source resolution SHALL include `akshare:direct` before fallback shareholder sources

### Requirement: Shareholder Provider Registry Must Resolve AkShare

The research system SHALL expose an `akshare` shareholder provider through the shareholder provider registry.

#### Scenario: Registry lookup returns AkShare shareholder provider

- **WHEN** the registry is asked for source `akshare`
- **THEN** it SHALL return a shareholder provider that supports the normalized shareholder-summary contract

### Requirement: Shareholder Routing Must Support Executable cninfo Fallback

The research system SHALL allow `shareholders` routing to resolve an executable `cninfo` fallback provider.

#### Scenario: Registry lookup returns cninfo shareholder provider

- **WHEN** the registry is asked for source `cninfo`
- **THEN** it SHALL return a shareholder provider that can emit at least partial normalized shareholder snapshots for fallback use

### Requirement: Shenwan Index-Analysis Historical Backfill
The Research Data Engine SHALL support historical date-range ingestion for Shenwan industry index-analysis daily rows through the project provider layer.

#### Scenario: Historical rows are normalized into the shared index-analysis table
- **WHEN** a historical Shenwan index-analysis provider fetches rows for a configured date range and index type
- **THEN** the system SHALL normalize them into the same `IndustryIndexAnalysisSnapshot` fields used by latest sync
- **AND** the rows SHALL be upserted into `industry_index_analysis_daily` by `(taxonomy_system, taxonomy_version, sw_index_code, trade_date)`

#### Scenario: Historical ingestion does not modify stock memberships
- **WHEN** historical Shenwan index-analysis rows are ingested
- **THEN** the ingestion SHALL NOT write `industry_memberships`, `industry_taxonomy`, or stock-level classification history

### Requirement: Shenwan Index-Analysis Field Units
The Research Data Engine SHALL document and preserve field-unit semantics for Shenwan index-analysis metrics across latest and historical providers.

#### Scenario: Percentage fields are stored as percent values
- **WHEN** Shenwan index-analysis rows are normalized
- **THEN** `markup`, `turnover_rate`, `bargain_sum_rate`, and `dividend_yield` SHALL be stored as percentage values, not decimal ratios

#### Scenario: Volume and market-cap units are explicit
- **WHEN** Shenwan index-analysis rows are exposed through docs, API metadata, or diagnostics
- **THEN** `bargain_volume` SHALL be identified as traded volume in 100 million shares
- **AND** `negotiable_share_sum` and `average_negotiable_share_sum` SHALL be identified as market capitalization in CNY 100 million

### Requirement: Shenwan Index-Analysis Historical Coverage Diagnostics
The Research Data Engine SHALL expose historical index-analysis ingestion coverage so upstream gaps and classification-version changes are visible.

#### Scenario: Backfill reports row and missing-metric counts
- **WHEN** a historical Shenwan index-analysis backfill completes
- **THEN** the result SHALL include rows written, date range, index types, row counts by index type, and missing metric counts by index type

#### Scenario: Localized gaps can be repaired with daily chunks
- **WHEN** a monthly historical backfill hits an upstream pagination error or leaves a localized date gap
- **THEN** the operational backfill entry points SHALL support daily chunk execution
- **AND** the result SHALL report per-day chunk status without clearing already-ingested rows

#### Scenario: Partial historical coverage is not treated as taxonomy failure
- **WHEN** historical index-analysis row counts differ across dates or index types
- **THEN** the system SHALL report the coverage difference as index-analysis data coverage
- **AND** it SHALL NOT mark authoritative stock membership taxonomy as failed solely because of those differences

### Requirement: Research Sync Must Allow Optional-Empty Exchanges

Research sync jobs SHALL allow configured optional-empty exchanges to complete
successfully even if no provider returns rows.

#### Scenario: BSE has no supported upstream rows

- **WHEN** a research module config marks `BSE` as an optional-empty exchange
- **AND** the sync runs for `BSE`
- **AND** no provider returns rows for that exchange
- **THEN** the sync SHALL finish with a successful empty result instead of a
  degraded failure

### Requirement: Research Read APIs Must Return Empty Placeholder Payloads For Optional-Empty Exchanges

Research read APIs SHALL synthesize empty payloads for optional-empty exchanges
when no stored snapshot exists.

#### Scenario: BSE profile read has no stored row

- **WHEN** a client reads a BSE instrument from a module that allows empty
  coverage
- **AND** no stored research snapshot exists
- **THEN** the read path SHALL return an empty placeholder payload rather than
  `None`

### Requirement: Shareholder Readiness Must Exclude Optional-Empty Exchanges From Required Coverage

Shareholder readiness SHALL exclude optional-empty exchanges from required target
coverage.

#### Scenario: BSE is optional-empty for shareholders

- **WHEN** shareholders readiness is calculated
- **AND** `BSE` is configured as optional-empty
- **THEN** BSE instruments SHALL NOT count toward the target snapshot total or
  required-scope coverage thresholds

### Requirement: Strict Shenwan Rollout Readiness Must Be Queryable

The research system SHALL expose a read-only readiness summary for the strict Shenwan standard layer.

#### Scenario: Readiness summary exposes mapping and coverage status

- **WHEN** the caller requests the industry-standard readiness summary
- **THEN** the system SHALL return persisted mapping-cache, official-classification, and authoritative-membership coverage information for the configured taxonomy

### Requirement: Relative Valuation Rollout Readiness Must Report Blockers

The research system SHALL report whether relative valuation is ready for authoritative rollout and, if not, why not.

#### Scenario: Coverage is incomplete

- **WHEN** authoritative membership coverage is below the current target universe
- **THEN** the readiness summary SHALL mark rollout as not ready and include an explicit blocker describing incomplete authoritative coverage

### Requirement: Strict Shenwan Rollout Must Have Repeatable Validation Runner
The research system SHALL provide a repository-level command to validate strict
Shenwan rollout readiness.

#### Scenario: Full validation sequence
- **WHEN** the runner is executed without skip flags
- **THEN** it SHALL run official mapping refresh
- **AND** it SHALL run industry standard sync
- **AND** it SHALL read industry standard readiness
- **AND** it SHALL print a structured JSON result

#### Scenario: Readiness is not met with fail flag
- **WHEN** the runner is executed with `--fail-on-not-ready`
- **AND** the readiness result is not ready
- **THEN** the command SHALL exit with code `2`

#### Scenario: Operator skips expensive phases
- **WHEN** the runner is executed with `--skip-refresh` or `--skip-sync`
- **THEN** the corresponding phase SHALL be skipped
- **AND** the readiness phase SHALL still run

### Requirement: Unmapped Backlog Must Expose Manual-Override Suggestions
The research system SHALL expose derived manual-override suggestions for backlog
rows that are ready for override review.

#### Scenario: Backlog row is override-ready
- **WHEN** an unmapped backlog row is marked `override_candidate_ready = true`
- **THEN** the backlog response SHALL include a suggestion payload matching the
  `manual_overrides` config shape

#### Scenario: Backlog row is not override-ready
- **WHEN** an unmapped backlog row is not ready for override review
- **THEN** the backlog response SHALL omit the manual-override suggestion payload

### Requirement: Backlog Query Must Support Ready-Only Review
The research system SHALL support filtering the backlog to only override-ready
rows.

#### Scenario: Caller requests only ready candidates
- **WHEN** the caller sets the ready-only filter on the backlog API
- **THEN** the response SHALL only include rows with
  `override_candidate_ready = true`
- **AND** the summary counts SHALL reflect the filtered result set

### Requirement: System Must Export Ready Official Override Candidates
The research system SHALL expose a dedicated read API for ready
official-industry-code manual override candidates.

#### Scenario: Ready candidates exist
- **WHEN** the system has backlog rows with
  `override_candidate_ready = true`
- **THEN** the export API SHALL return only those ready rows
- **AND** each row SHALL include its derived
  `manual_override_suggestion`

### Requirement: Export Must Include Config-Shaped Override Bundle
The research system SHALL expose the ready candidates as a config-shaped
`manual_overrides` bundle.

#### Scenario: Exporting override bundle
- **WHEN** the export API returns ready candidates
- **THEN** the response SHALL include a `manual_overrides` object keyed by
  `official_industry_code`
- **AND** each value SHALL match the `manual_overrides` config shape used by
  `industry.standard.official_mapping.manual_overrides`

### Requirement: Export Must Surface Review Summary Counts
The research system SHALL summarize the ready candidate export set.

#### Scenario: Ready candidates are exported
- **WHEN** the export API returns the ready candidate set
- **THEN** the response SHALL include aggregate counts for the exported rows
- **AND** the review-priority summary SHALL reflect the exported result set

### Requirement: Research API Exposes Official Mapping Cache For Audit
The research system SHALL expose the persisted official Shenwan mapping cache through a stable read API for audit and backlog review.

#### Scenario: User lists unmapped official codes
- **WHEN** a caller requests the official mapping cache with `mapping_status = unmapped`
- **THEN** the system SHALL return the filtered cache rows from `industry_official_code_mappings`
- **AND** it SHALL support pagination for large result sets

#### Scenario: User needs mapping payload diagnostics
- **WHEN** a caller requests official mapping cache rows
- **THEN** the system SHALL be able to include the persisted `mapping` payload
- **AND** that payload SHALL preserve override metadata when a manual override was applied

### Requirement: Research API Exposes Detail For One Official Code
The research system SHALL provide a detail read for a single official Shenwan six-digit code.

#### Scenario: Caller requests one official code
- **WHEN** a caller requests a specific official industry code within one taxonomy version
- **THEN** the system SHALL return the persisted cache row if it exists
- **AND** it SHALL return not found if no such cache row exists

### Requirement: System Must Expose Official Override Review Status
The research system SHALL expose a dedicated read API that compares configured
official overrides, ready candidates, and applied cache rows.

#### Scenario: Reviewing current override state
- **WHEN** the caller requests the override review API
- **THEN** the response SHALL include itemized review rows built from the union
  of configured overrides, ready candidates, and applied manual-override cache
  rows
- **AND** each row SHALL include a derived review status

### Requirement: Review API Must Surface Pending Config Fragments
The research system SHALL expose ready candidates that have not yet been
configured as a config-shaped fragment.

#### Scenario: Ready candidates are not configured
- **WHEN** a ready override candidate exists without a matching configured
  `manual_overrides` entry
- **THEN** the review API SHALL include that candidate under
  `pending_manual_overrides`

### Requirement: Review API Must Summarize Status Counts
The research system SHALL summarize the review result set.

#### Scenario: Override review completes
- **WHEN** the review API returns the result set
- **THEN** the response SHALL include aggregate counts for each review status

### Requirement: Official Override Review Must Support Actionable Filtering
The research system SHALL allow callers to narrow the official override review
result set to actionable states.

#### Scenario: Caller requests attention-only rows
- **WHEN** the caller sets `attention_only = true`
- **THEN** the review API SHALL exclude healthy
  `configured_and_applied` rows
- **AND** the summary counts SHALL reflect only the filtered rows

#### Scenario: Caller requests specific review statuses
- **WHEN** the caller provides one or more `review_status` filters
- **THEN** the review API SHALL only return rows whose derived review status is
  included in that filter
- **AND** the summary counts SHALL reflect only the filtered rows

### Requirement: Unmapped Official-Code Backlog Must Expose Override-Review Signals
The research system SHALL expose derived review signals for unmapped official
Shenwan backlog rows so maintainers can prioritize manual override review.

#### Scenario: Backlog row has a strong candidate and current impact
- **WHEN** an unmapped official-code backlog row has current impacted
  classifications and a sufficiently strong top candidate under configured
  thresholds
- **THEN** the backlog response SHALL mark the row as ready for manual override
  review
- **AND** it SHALL expose an explicit reason describing why the row was promoted

#### Scenario: Backlog row remains low-signal
- **WHEN** an unmapped official-code backlog row has no strong candidate or no
  current impact
- **THEN** the backlog response SHALL mark the row as not ready for manual
  override review
- **AND** it SHALL expose a lower review priority

### Requirement: Research Metadata Rollout Readiness Must Be Queryable

The research system SHALL expose a read-only readiness summary for external research metadata domains.

#### Scenario: Readiness summary exposes per-domain coverage and gate status

- **WHEN** the caller requests research metadata readiness
- **THEN** the system SHALL return per-domain target-universe coverage, persisted row count, source distribution, mode distribution, module gate status, and rollout blockers

### Requirement: Metadata Readiness Must Report Coverage Gaps

The research system SHALL report when metadata rows do not cover the current target stock universe.

#### Scenario: Metadata coverage is incomplete

- **WHEN** persisted metadata rows cover fewer distinct instruments than the current target universe
- **THEN** that domain SHALL be marked not ready and include an explicit coverage blocker

### Requirement: Disabled Metadata Modules Must Not Be Rollout Ready

The research system SHALL treat disabled metadata modules as not rollout-ready.

#### Scenario: Metadata module is disabled

- **WHEN** a metadata module is disabled in research config
- **THEN** that domain SHALL be marked not ready and include a module-disabled blocker

### Requirement: CNInfo Announcement Metadata Scan Is Reusable
The Research Data Engine SHALL provide a reusable CNInfo announcement metadata scan capability that can be consumed by shareholder incremental sync and future research workflows.

#### Scenario: Caller scans announcements by market and time window
- **WHEN** a research workflow requests CNInfo announcements for configured markets or columns and a time window
- **THEN** the system SHALL query announcement metadata with configured page size, pagination limits, request pacing, and retry policy
- **AND** it SHALL return normalized announcement records containing announcement ID, title, announcement time, adjunct URL or document reference when available, market or column, instrument identifiers when available, and raw source metadata

#### Scenario: Caller applies domain-specific announcement filters
- **WHEN** a research workflow supplies title, category, or custom predicate filters
- **THEN** the announcement scan capability SHALL apply those filters without embedding domain-specific business rules in the generic scanner
- **AND** it SHALL preserve selection reasons for downstream reporting and audit

#### Scenario: Announcement scan resumes from prior state
- **WHEN** a workflow runs repeatedly with a named scan purpose
- **THEN** the system SHALL maintain a purpose-specific watermark and overlap window
- **AND** it SHALL avoid marking unprocessed pages as successfully scanned

#### Scenario: Future workflow reuses announcement scanner
- **WHEN** another research workflow needs CNInfo announcement metadata for a different business purpose
- **THEN** it SHALL be able to reuse the scan capability with its own purpose key, filters, limits, and watermark state without depending on shareholder sync internals

### Requirement: Shareholder Full Refresh Has Explicit Operator Semantics
The Research Data Engine SHALL expose the existing full-market shareholder refresh as an explicit operator-facing task for manual Telegram execution.

#### Scenario: Operator runs full shareholder refresh
- **WHEN** the operator triggers the full shareholder refresh through Telegram
- **THEN** the system SHALL run the full active `SSE / SZSE / BSE` shareholder refresh path
- **AND** the report SHALL state that the run was a full refresh or full repair
- **AND** the report SHALL include target instruments, refreshed or written instruments, unchanged or reused instruments if tracked, source success/fallback counts, unresolved instruments, readiness status, and elapsed time

#### Scenario: Full refresh remains separate from daily incremental sync
- **WHEN** the daily shareholder incremental job is enabled
- **THEN** it SHALL NOT replace the operator-facing full refresh command
- **AND** the full refresh command SHALL remain available for bootstrap, manual repair, and periodic audit

### Requirement: Shareholder Incremental Sync Uses Announcement-Driven Candidate Discovery
The Research Data Engine SHALL support a daily shareholder incremental sync that discovers candidate instruments from the reusable CNInfo announcement metadata scan capability and local coverage state before fetching shareholder structured data.

#### Scenario: Daily incremental sync scans recent announcements
- **WHEN** the daily shareholder incremental sync starts
- **THEN** the system SHALL invoke the reusable CNInfo announcement scan capability by configured market or column, time window, page size, and overlap policy
- **AND** it SHALL paginate until the configured watermark or limit is reached
- **AND** it SHALL NOT issue one announcement query per active instrument as the normal discovery strategy

#### Scenario: Announcement filtering selects shareholder candidates
- **WHEN** announcement metadata is returned
- **THEN** the system SHALL filter candidates by configured shareholder-relevant title or category patterns
- **AND** it SHALL record the announcement identifiers, announcement times, titles, instruments, and selection reasons used for incremental refresh decisions

#### Scenario: Missing or incomplete snapshots are included
- **WHEN** an active instrument lacks required shareholder coverage or has a stale pending recheck
- **THEN** the daily incremental sync SHALL include it as a candidate even if no new matching announcement is found

### Requirement: Shareholder Incremental Sync Writes Only Material Changes
The Research Data Engine SHALL compare normalized shareholder content hashes before writing latest shareholder snapshots during incremental sync.

#### Scenario: Candidate shareholder content is unchanged
- **WHEN** a candidate instrument's normalized top-holder, holder-count, and ownership-clue hashes match the stored manifest
- **THEN** the system SHALL skip rewriting the latest shareholder snapshot
- **AND** the run report SHALL count the instrument as checked and unchanged

#### Scenario: Candidate shareholder content changed
- **WHEN** a candidate instrument's normalized shareholder hash, latest report date, or required coverage scope differs from the stored manifest
- **THEN** the system SHALL upsert the latest shareholder snapshot
- **AND** it SHALL store the raw payload audit and update the shareholder change manifest
- **AND** the run report SHALL count the instrument as changed or refreshed

#### Scenario: Structured data lags the announcement
- **WHEN** a selected announcement is newer than the local manifest but CNInfo structured shareholder data has not changed
- **THEN** the system SHALL record the instrument as pending recheck until the configured retry horizon expires

#### Scenario: Pending recheck horizon is bounded
- **WHEN** the same announcement set is scanned repeatedly while structured shareholder data remains unchanged
- **THEN** the system SHALL anchor the retry horizon to the first pending time for that announcement set
- **AND** it SHALL NOT extend the pending deadline on every daily scan
- **AND** it SHALL stop selecting the instrument from the pending queue after the deadline expires unless a new announcement set is detected

### Requirement: Shareholder Incremental State Is Auditable
The Research Data Engine SHALL persist incremental shareholder watermarks and per-instrument change-check state so daily runs can be explained and resumed.

#### Scenario: Incremental run completes
- **WHEN** a shareholder incremental run completes successfully or partially
- **THEN** the system SHALL persist the announcement scan watermark, scanned page counts, selected candidate counts, checked instrument counts, changed counts, unchanged counts, pending recheck counts, and failure counts

#### Scenario: Incremental run is interrupted
- **WHEN** a shareholder incremental run is interrupted or times out
- **THEN** the next run SHALL resume from the last durable watermark or configured overlap window
- **AND** it SHALL avoid treating unprocessed pages as successfully scanned

### Requirement: Shareholder Incremental Reports Are Clear To Operators
The Research Data Engine SHALL produce clear scheduler and Telegram reports for shareholder incremental runs.

#### Scenario: Incremental report is generated
- **WHEN** a shareholder incremental run finishes
- **THEN** the report SHALL state whether the run changed data, found no changes, partially completed, or failed
- **AND** it SHALL include announcements scanned, candidate instruments, refreshed instruments, unchanged instruments, pending rechecks, failures, source usage, readiness impact, and elapsed time

#### Scenario: Incremental run finds no changes
- **WHEN** the daily incremental job scans announcements and checks candidates without any material shareholder data changes
- **THEN** the report SHALL explicitly state that no shareholder snapshot changes were written

### Requirement: Shareholder Full Refresh Is Manual And Reconciliation Is Scheduled
The Research Data Engine SHALL not keep full-market shareholder refresh as a standing automatic job when the daily incremental path is active.

#### Scenario: Shareholder update tasks have distinct responsibilities
- **WHEN** shareholder data maintenance is configured
- **THEN** `shareholder_incremental_sync` SHALL perform daily announcement-driven candidate refresh
- **AND** `shareholder_reconciliation_sync` SHALL perform scheduled full-read changed-only reconciliation
- **AND** `shareholder_shadow_sync` SHALL remain a manual full-refresh task

#### Scenario: Operator runs full refresh manually
- **WHEN** an operator sends `/run shareholder_shadow_sync`
- **THEN** the system SHALL execute the full shareholder refresh even though the task is not registered as an automatic cron job

#### Scenario: Operator checks manual-only full refresh status
- **WHEN** an operator sends `/status`
- **THEN** `shareholder_shadow_sync` SHALL remain visible as a manual-only task
- **AND** it SHALL not have an automatic next run time

#### Scenario: Scheduled shareholder reconciliation runs
- **WHEN** the weekly shareholder reconciliation job runs
- **THEN** it SHALL full-read the configured target universe
- **AND** it SHALL write only changed, missing, or required-scope-incomplete shareholder snapshots

### Requirement: Shareholder Incremental Delivery Updates Project Documentation
The Research Data Engine SHALL keep requirements and engineering documentation aligned when shareholder full-refresh and incremental modes are introduced.

#### Scenario: Shareholder incremental feature is implemented
- **WHEN** the implementation is completed
- **THEN** `implementation_plan.md` SHALL describe the new daily incremental mode and the retained full-refresh mode
- **AND** engineering/operator documentation SHALL describe scheduler configuration, Telegram usage, report meanings, data-source assumptions, and known limitations

### Requirement: Shareholder Sync Must Continue Fallback For Unresolved Instruments

The research shareholder sync SHALL continue to later providers for instruments still missing after a partial provider success.

#### Scenario: Primary provider returns only part of the exchange

- **WHEN** the first successful provider returns shareholder snapshots for only a subset of requested instruments
- **THEN** the sync SHALL continue to later providers for the unresolved instruments instead of stopping immediately

### Requirement: Shareholder Sync Must Report Partial Completion

The research shareholder sync SHALL expose whether an exchange finished completely or with unresolved instruments.

#### Scenario: Some instruments remain unresolved

- **WHEN** all configured providers have been attempted and some instruments still have no shareholder snapshot
- **THEN** the sync result SHALL report degraded status together with requested, resolved, and missing instrument counts

### Requirement: Shareholder Rollout Readiness Must Be Queryable

The research system SHALL expose a read-only readiness summary for the shareholder domain.

#### Scenario: Readiness summary exposes coverage and gate status

- **WHEN** the caller requests shareholder readiness
- **THEN** the system SHALL return target-universe coverage, source distribution, mode distribution, delivery gate status, and rollout blockers

### Requirement: Shareholder Readiness Must Report Coverage Gaps

The research system SHALL report when shareholder snapshots do not yet cover the current target stock universe.

#### Scenario: Shareholder coverage is incomplete

- **WHEN** persisted shareholder snapshots are fewer than the current target stock universe
- **THEN** the readiness summary SHALL mark rollout as not ready and include an explicit coverage blocker

### Requirement: Shareholder Sync Must Support Same-Source Recovery For Missing Scope

The research shareholder sync SHALL support a bounded same-source recovery pass
for instruments still missing required scope after a provider's initial batch.

#### Scenario: Same provider can recover missing scope in micro-batch mode

- **WHEN** a provider returns accepted shareholder snapshots but some
  instruments still miss required scope
- **AND** same-source recovery is enabled
- **THEN** the sync SHALL retry that same provider for unresolved instruments in
  bounded micro-batches before moving to later providers

### Requirement: Shareholder Ingestion Metadata Must Report Recovery Activity

The research shareholder sync SHALL report same-source recovery activity in
ingestion metadata.

#### Scenario: Recovery pass runs

- **WHEN** same-source recovery is attempted for one provider candidate
- **THEN** ingestion metadata SHALL include recovery-attempt and recovery-resolve
  counts

### Requirement: Research System Stores Shareholder Summary Snapshots
The research system SHALL support storing the latest shareholder summary snapshot for an instrument in the shadow research database.

#### Scenario: Provider returns shareholder summary data
- **WHEN** a shareholder provider returns holder count, top-holder list, or ownership clues for an instrument
- **THEN** the system SHALL persist a normalized latest shareholder snapshot
- **AND** it SHALL retain a JSON payload for detailed holder rows and raw clue fields

### Requirement: Shareholder Shadow Sync Respects Source Policy
The research system SHALL resolve shareholder sources through the configured research routing policy.

#### Scenario: Free best-effort shareholder sync
- **WHEN** shareholder shadow sync runs under `free_best_effort`
- **THEN** the system SHALL attempt configured free and fallback sources only
- **AND** it SHALL record attempted sources in the ingestion result

### Requirement: Shareholder Snapshot API Is Delivery-Mode Gated
The research system SHALL gate public shareholder snapshot reads by the configured shareholder delivery mode.

#### Scenario: Snapshot API requires paid-high-availability
- **WHEN** the shareholder module is enabled but its `delivery_mode` does not match `snapshot_api_requires_mode`
- **THEN** the public shareholder snapshot read SHALL be rejected with a clear error

#### Scenario: Snapshot API gate is satisfied
- **WHEN** the shareholder module is enabled and its `delivery_mode` matches `snapshot_api_requires_mode`
- **THEN** the system SHALL return the latest persisted shareholder snapshot for the instrument

### Requirement: Mapping Refresh Precedes Membership Sync In The Recommended Mainline
The Research Data Engine execution mainline SHALL treat Shenwan official mapping refresh as a distinct step that precedes authoritative membership sync.

#### Scenario: Maintainer reviews the execution mainline
- **WHEN** a maintainer reads the Research Data Engine execution document
- **THEN** the document SHALL describe the recommended order as mapping refresh first and membership sync second

### Requirement: Research DCF API Must Expose Professional DCF Surfaces

The Research Data Engine SHALL expose professional DCF calculation, readiness, model-profile discovery, assumption, input-gap, and workbook surfaces through local-first research APIs.

#### Scenario: Existing DCF endpoint supports professional options
- **WHEN** a caller requests `GET /api/v1/research/company/{instrument_id}/valuation/dcf`
- **THEN** the endpoint SHALL accept professional DCF options including model profile, model strategy, valuation date, scenario set, projection years, terminal method, forecast rows, sensitivity, lineage, model comparison inclusion, workbook inclusion, force model, and research mode

#### Scenario: DCF endpoint can compare model strategies
- **WHEN** a caller requests `model_strategy=compare` or `include_model_comparison=true`
- **THEN** the endpoint SHALL return industry and company-characteristic candidate metadata and both model results where calculable

#### Scenario: DCF readiness endpoint is available
- **WHEN** a caller requests a company's DCF readiness
- **THEN** the API SHALL return available models, default model, missing fields, data coverage, availability-date coverage, industry classification status, market data status, beta status, assumption status, blockers, and warnings

#### Scenario: Model profiles endpoint is available
- **WHEN** a caller requests professional DCF model profiles
- **THEN** the API SHALL return supported model profiles, required fields, optional fields, default parameter ranges, supported company types, and implementation status

#### Scenario: Workbook artifact can be downloaded
- **WHEN** a DCF run generated a workbook artifact
- **THEN** the API SHALL expose a controlled download or artifact-read path that returns the workbook without requiring recalculation

### Requirement: Research DCF APIs Must Remain Local-First

The Research Data Engine SHALL keep DCF calculation local-first and separate remote data refresh from valuation execution.

#### Scenario: DCF calculation does not fetch remote sources
- **WHEN** a DCF calculation endpoint is executed
- **THEN** it SHALL read local financial facts, market data, valuation inputs, beta outputs, and cached assumptions rather than fetching third-party data inside the calculation path

#### Scenario: Assumption refresh is explicit
- **WHEN** a caller or operator needs updated market assumptions
- **THEN** the refresh SHALL be performed through a dedicated assumption refresh path and recorded with source lineage and diagnostics

#### Scenario: External input refresh is explicit
- **WHEN** a caller or operator needs DCF inputs not available locally
- **THEN** the refresh SHALL be performed through registered external-data providers and SHALL NOT be hidden inside the DCF formula engine

### Requirement: Research DCF Must Preserve Existing Valuation Storage Boundaries

Professional DCF SHALL reuse the valuation domain but SHALL NOT turn DCF into an unbounded daily persisted valuation matrix.

#### Scenario: DCF does not write valuation history by default
- **WHEN** a DCF calculation is requested
- **THEN** the system SHALL NOT write a row to `valuation_history` unless a separate explicitly designed cache or saved-run path is used

#### Scenario: Bounded DCF cache is separate
- **WHEN** DCF run caching is enabled
- **THEN** cached run summaries, forecast rows, sensitivity, assumptions, and workbook artifacts SHALL be stored separately from daily valuation-history rows

### Requirement: Research DCF Must Integrate With Existing Beta Semantics

Professional DCF SHALL continue to use the shared benchmark-aware beta layer when beta is needed for cost of equity.

#### Scenario: Beta lineage is included
- **WHEN** professional DCF uses calculated beta, industry beta, explicit override beta, or fallback beta
- **THEN** the response SHALL record beta source, benchmark identity, window, quality flag, and whether beta was used in the final discount-rate calculation

#### Scenario: Fixed discount rate override is explicit
- **WHEN** a caller or config supplies an explicit discount rate that overrides beta-derived cost of equity
- **THEN** the response SHALL identify the override and SHALL NOT imply that beta drove the discount rate

### Requirement: Research DCF Documentation Must Track The Professional Requirement Report

The Research Data Engine documentation SHALL link the professional DCF requirement report and distinguish it from the existing lightweight DCF baseline.

#### Scenario: DCF requirement report is discoverable
- **WHEN** a maintainer reviews DCF development status
- **THEN** the implementation plan or execution document SHALL link to `docs/development/professional_dcf_requirements.md`

#### Scenario: Lightweight baseline is distinguished
- **WHEN** documentation describes current DCF implementation
- **THEN** it SHALL distinguish the existing lightweight `SimpleGrowthDcfEngine` baseline from the professional DCF model profiles introduced by this change

### Requirement: HKEX Research Modules Use Shared Master Universe
Every HKEX current-universe research module SHALL use the shared HKEX instrument master governance layer before selecting target instruments.

#### Scenario: HKEX research ingestion starts
- **WHEN** an HKEX research ingestion, shadow sync, or current snapshot job starts
- **THEN** it SHALL run or reuse HKEX instrument master governance before resolving the target HKEX universe
- **AND** it SHALL use the post-governance local HKEX instrument state

#### Scenario: HKEX research API reads current universe data
- **WHEN** an HKEX research API or service needs to interpret a current instrument identifier
- **THEN** it SHALL resolve the identifier against the shared local HKEX master data
- **AND** it SHALL NOT require each research module to maintain a separate HKEX symbol dictionary

#### Scenario: HKEX master governance is degraded
- **WHEN** an HKEX research job continues after degraded or failed HKEX master governance
- **THEN** the research job result SHALL include the governance degradation
- **AND** downstream readiness SHALL be allowed to mark HKEX research coverage as degraded

### Requirement: HKEX Research Scope Respects Product Classification
HKEX research modules SHALL use the product classifications produced by HKEX master sync when deciding whether an instrument belongs in their target universe.

#### Scenario: Research module targets ordinary equities only
- **WHEN** a research module is configured for ordinary HKEX equities
- **THEN** it SHALL exclude HKEX instruments classified as debt, warrant, callable bull/bear contract, inline warrant, derivative warrant, or unknown product type

#### Scenario: Research module enables ETFs or REITs
- **WHEN** a research module explicitly enables HKEX ETFs or REITs
- **THEN** it SHALL include only instruments classified as those enabled product types
- **AND** the result SHALL retain product-scope diagnostics for excluded instruments

### Requirement: Research Daily Writes Emit Or Declare Change Semantics
Research-domain scheduled writes SHALL either emit changelog records for inserted and materially changed rows or explicitly declare that the workflow is read-only, diagnostics-only, or unchanged-only.

#### Scenario: Shareholder incremental snapshot changes
- **WHEN** shareholder incremental sync writes a changed normalized snapshot
- **THEN** the research data engine SHALL emit a shareholder-domain change record linked to the ingestion run

#### Scenario: Diagnostic job reads only
- **WHEN** a research diagnostic job only reads local data and writes no persisted result
- **THEN** the job SHALL declare no changelog emission is expected

### Requirement: Derived Research Outputs Preserve Input Lineage
Derived research outputs such as valuation history, technical snapshots, risk snapshots, and DCF inputs SHALL record input hashes, source watermarks, or equivalent lineage so consumers can distinguish source-data changes from recalculation-only changes.

#### Scenario: Valuation history row is recomputed
- **WHEN** valuation history is recomputed for the same instrument and as-of date
- **THEN** the stored output SHALL identify the calculation version and input lineage
- **AND** the changelog SHALL classify a material output change only when the derived semantic hash changes

### Requirement: Research Change Queries Do Not Affect Existing Read APIs
Research changelog surfaces SHALL be read-only additions and SHALL NOT change existing `/api/v1/research/*` default responses.

#### Scenario: Existing financial facts query
- **WHEN** a caller queries existing financial fact or valuation APIs without changelog parameters
- **THEN** the response SHALL follow the pre-existing research API contract

## MODIFIED Requirements

### Requirement: SWS Index Analysis Must Be Separate From Classification

The research system SHALL ingest SWS Research industry index-analysis metrics as
a separate source from stock-to-industry classification.

#### Scenario: Index metrics do not change stock membership

- **WHEN** index-analysis rows are ingested
- **THEN** the system SHALL NOT infer or overwrite stock industry memberships
  from those rows

### Requirement: Index Analysis Rows Must Be Keyed By Index Code And Date

The research system SHALL store SWS index-analysis rows using the Shenwan
industry index code and trade date as the canonical key.

#### Scenario: Daily index row is stored idempotently

- **WHEN** the same `swindexcode` and `bargaindate` row is fetched more than
  once
- **THEN** the system SHALL update the existing index-analysis row rather than
  creating duplicates

### Requirement: Classification Joins Must Use Explicit Index Aliases

The research system SHALL join classification taxonomy to index-analysis rows
only through explicit industry index aliases.

#### Scenario: Classification node has no index alias

- **WHEN** a classification taxonomy node does not have an explicit index-code
  alias
- **THEN** index-analysis metrics SHALL remain unavailable for that node rather
  than being inferred from the six-digit classification code

### Requirement: Index Analysis Metrics Must Be Queryable By API

The research system SHALL expose persisted SWS index-analysis metrics through
the research API layer.

#### Scenario: Latest index-analysis row is requested by index code

- **WHEN** the caller requests an SWS index-analysis benchmark for a Shenwan
  index code
- **THEN** the API SHALL return the latest persisted close index, amount/volume,
  markup, turnover, PE, PB, mean price, market value, average market value,
  dividend yield, trade date, source, and source mode

#### Scenario: Industry benchmark is requested from taxonomy alias

- **WHEN** the caller requests index-analysis metrics for a Shenwan taxonomy
  node that has an explicit index-code alias
- **THEN** the API SHALL resolve the alias and return the corresponding
  index-analysis row
- **AND** it SHALL NOT infer the index code from the official six-digit
  classification code

### Requirement: Technical Latest Snapshots Must Be Persisted

The research system SHALL persist latest technical indicator snapshots derived
from local quote history.

#### Scenario: Latest technical summary is refreshed

- **WHEN** a technical snapshot refresh runs for a supported stock
- **THEN** the system SHALL compute the latest technical summary from local
  quotes and upsert one latest snapshot keyed by instrument, period, adjustment,
  calculation method, calculation version, and parameter hash

### Requirement: Technical Latest Cache Must Not Replace Real-Time Reads

The research system SHALL keep existing real-time technical summary and
indicator-series APIs backward compatible.

#### Scenario: Caller reads real-time technical summary

- **WHEN** the caller requests `/technical/summary`
- **THEN** the system SHALL continue calculating from local quote history without
  requiring a persisted cache row

### Requirement: Technical Cache Readiness Must Be Queryable

The research system SHALL expose read-only readiness for latest technical cache
coverage.

#### Scenario: Cache coverage is incomplete

- **WHEN** persisted latest technical snapshots cover fewer distinct instruments
  than the configured target universe
- **THEN** technical readiness SHALL be marked not ready and include an explicit
  coverage blocker

### Requirement: Industry Standard Sync Must Allow Optional-Empty Exchanges

`industry_standard_sync` SHALL allow configured optional-empty exchanges to
finish successfully even if no authoritative memberships can be produced.

#### Scenario: BSE has no authoritative industry memberships

- **WHEN** `industry.optional_empty_exchanges` contains `BSE`
- **AND** `industry_standard_sync` runs for `BSE`
- **AND** no provider can produce authoritative memberships for that exchange
- **THEN** the exchange result SHALL be `success`
- **AND** the run SHALL not degrade solely because BSE produced zero rows

### Requirement: Industry Standard Sync Must Allow All-Optional-Empty Empty Runs

If every target exchange is optional-empty, an empty run SHALL be treated as a
successful empty sync instead of a degraded result.

#### Scenario: BSE-only strict Shenwan sync has no usable upstream rows

- **WHEN** `industry_standard_sync` runs only for `BSE`
- **AND** `BSE` is configured as optional-empty
- **AND** no provider yields usable taxonomy or memberships
- **THEN** the run SHALL finish successfully with zero rows written

### Requirement: Unmapped Official-Code Backlog Must Be Queryable
The research system SHALL expose a dedicated read path for unmapped official
Shenwan codes so maintainers can review backlog impact and mapping candidates
without direct database access.

#### Scenario: Caller lists unmapped backlog rows
- **WHEN** a caller requests the unmapped official-code backlog for one taxonomy
  version
- **THEN** the system SHALL return unmapped mapping-cache rows ordered by
  current impacted classifications before lower-impact rows
- **AND** each row SHALL include the current impacted classification count for
  that official code
- **AND** each row SHALL include the affected exchanges and sample instruments

#### Scenario: Caller paginates backlog review
- **WHEN** a caller requests the unmapped official-code backlog with pagination
  parameters
- **THEN** the system SHALL return the requested page
- **AND** it SHALL include the filtered backlog total and current impacted
  classification total

### Requirement: Mapping Cache Must Preserve Ranked Candidate Diagnostics
The research system SHALL persist ranked taxonomy candidates for official-code
mapping review, including when the final mapping remains unmapped.

#### Scenario: Mapping rebuild writes candidate rankings
- **WHEN** the system rebuilds the official-code mapping cache
- **THEN** each persisted mapping payload SHALL preserve a ranked list of
  candidate taxonomy codes when overlap data exists
- **AND** unmapped rows SHALL still retain their best candidate diagnostics for
  later manual review

### Requirement: Industry Standard Sync Must Cache Component Sets

`industry_standard_sync` SHALL persist Shenwan component sets when it performs a
live official-code mapping rebuild.

#### Scenario: official mapping cache miss triggers live rebuild

- **WHEN** official-code mapping cache is unavailable
- **AND** the provider fetches Shenwan component sets live
- **THEN** the component sets SHALL be persisted into research storage

### Requirement: Industry Standard Sync Must Reuse Fresh Component Cache

`industry_standard_sync` SHALL reuse fresh component-set cache during official
mapping rebuilds when available.

#### Scenario: component cache is already available

- **WHEN** official-code mapping cache is unavailable
- **AND** fresh component-set cache exists for the taxonomy version
- **THEN** `industry_standard_sync` SHALL rebuild mappings from cached component
  sets instead of calling live `index_component_sw`

### Requirement: Strict Shenwan Rollout Runner Must Close Initialized Resources
The strict Shenwan rollout validation runner SHALL close initialized data
manager resources before process exit.

#### Scenario: Validation succeeds
- **WHEN** the runner initializes the data manager and validation completes
- **THEN** the runner SHALL call the data manager close hook before returning

#### Scenario: Validation fails
- **WHEN** validation raises after data manager initialization
- **THEN** the runner SHALL still call the data manager close hook before
  propagating the error

### Requirement: Research Routing Must Prioritize Stability Before Cost-Free Direct Access

The research system SHALL prioritize stable sources ahead of unstable free direct sources.

#### Scenario: Availability-first routing with paid proxy enabled

- **WHEN** a research domain has stable free candidates, stable paid candidates, and unstable free fallback candidates
- **THEN** source resolution SHALL evaluate them in the order `stable free -> stable paid -> free unstable fallback`

### Requirement: AkShare-Heavy Domains Must Prefer Proxy Before Direct

The research system SHALL prefer `akshare:proxy_patch` before `akshare:direct` for AkShare-heavy research domains that lack a stable free equivalent.

#### Scenario: Financial statements route resolution

- **WHEN** the `financial_statements` domain resolves its source plan under repository defaults
- **THEN** the plan SHALL include `akshare:proxy_patch` before `akshare:direct`

#### Scenario: Shareholders route resolution

- **WHEN** the `shareholders` domain resolves its source plan under repository defaults
- **THEN** the plan SHALL include `akshare:proxy_patch` before any unstable direct AkShare fallback candidate

### Requirement: Industry Source File Freshness
The research system SHALL record source-file metadata for official Shenwan
classification artifacts so freshness and reproducibility can be audited.

#### Scenario: Source artifact metadata is stored
- **WHEN** an official classification file is downloaded or confirmed unchanged
- **THEN** the system SHALL record URL, source, mode, `ETag`,
  `Last-Modified`, content length, sha256, row count, max source update time,
  parser version, and ingestion run id

#### Scenario: Invalid source artifact is rejected
- **WHEN** a downloaded classification artifact is missing required columns,
  has invalid code formats, has too few rows, or fails target-universe coverage
  checks
- **THEN** the system SHALL reject that artifact for authoritative membership
  writes
- **AND** the previous valid classification state SHALL remain available

### Requirement: Shenwan Industry Index Analysis Source
The research system SHALL treat SWS Research index-analysis metrics as a
separate industry-index data source, not as a stock classification source.

#### Scenario: Index analysis rows are stored separately
- **WHEN** `swsresearch_index_analysis_direct` ingests official index-analysis
  rows
- **THEN** it SHALL store rows keyed by Shenwan industry index code and date
- **AND** it SHALL keep those rows separate from stock classification history
  and latest stock memberships

#### Scenario: Index analysis does not infer stock membership
- **WHEN** index-analysis rows contain `swindexcode` and `swindexname`
- **THEN** those rows MAY enrich or validate index aliases
- **BUT** they SHALL NOT create or override stock-to-industry memberships

### Requirement: AkShare Shareholder Provider Must Support Configurable Top-Holder Retry

The AkShare shareholder provider SHALL support configurable pacing and retry for
top-holder batch fetches.

#### Scenario: Top-holder fetch temporarily fails

- **WHEN** the top-holder endpoint raises an exception for one instrument
- **THEN** the provider SHALL retry according to configured retry parameters

#### Scenario: Top-holder fetch returns repeated empty payloads

- **WHEN** the top-holder endpoint repeatedly returns no usable rows for one instrument
- **THEN** the provider SHALL record that failure in raw payload diagnostics

### Requirement: Shareholder Raw Payload Audit Must Distinguish Partial Fetch Failure

The shareholder raw payload audit SHALL distinguish field-level fetch failure from
legitimate empty values.

#### Scenario: Partial shareholder snapshot is still emitted

- **WHEN** holder-count or ownership-clue fields are available but top-holder fetch fails
- **THEN** the snapshot MAY still be emitted
- **AND** raw payload SHALL expose the failed field in fetch diagnostics

### Requirement: Industry Standard Readiness Must Reflect Override Review Health
The research system SHALL include official override review health in the
industry standard readiness response.

#### Scenario: Override review still needs action
- **WHEN** configured, ready, and applied override states are not fully aligned
- **THEN** the readiness response SHALL include an `override_review` summary
- **AND** the readiness blocker list SHALL include
  `official_override_review_requires_attention`

#### Scenario: Override review is healthy
- **WHEN** all reviewed override rows are already `configured_and_applied`
- **THEN** the readiness response SHALL report
  `override_review.requires_attention = false`

### Requirement: Strict Shenwan Readiness Must Expose Unmapped Backlog Summary
The research system SHALL expose unmapped official-code backlog summary data in
the strict Shenwan readiness response.

#### Scenario: Readiness shows top backlog blockers
- **WHEN** the caller requests strict Shenwan readiness
- **THEN** the response SHALL include the total unmapped official-code backlog
- **AND** it SHALL include the current impacted classification total
- **AND** it SHALL include a top-N list of the most impactful unmapped official
  codes

#### Scenario: Current latest classifications are still affected
- **WHEN** one or more current latest classifications still point to unmapped
  official codes
- **THEN** strict Shenwan readiness SHALL include an explicit blocker describing
  unresolved unmapped official-code backlog impact

### Requirement: Current Shenwan Membership Must Use Current Leaf Components
The research system SHALL derive current strict Shenwan stock memberships from
current Shenwan leaf industry component sets.

#### Scenario: Instrument appears in leaf components
- **WHEN** a target stock appears in a Shenwan leaf industry component set
- **THEN** `industry_standard_sync` SHALL write an authoritative membership for
  that stock
- **AND** it SHALL derive L2 and L1 fields from taxonomy parent links
- **AND** it SHALL leave `sw_l3_code` empty when the authoritative leaf node is
  a second-level industry with no third-level children

### Requirement: Official History Must Not Block Current Component Membership
The research system SHALL not require official six-digit historical code mapping
to produce current memberships when component data is available.

#### Scenario: Official historical code is unmapped
- **WHEN** official history returns an unmapped six-digit code
- **AND** the stock appears in current leaf components
- **THEN** current membership SHALL still be written from the component source
- **AND** the official history row MAY be retained as audit/reference data

#### Scenario: Current sync runs without official mapping context
- **WHEN** `industry_standard_sync` runs for current memberships
- **THEN** it SHALL NOT rebuild or require official six-digit-code mapping context
- **AND** official history rows SHALL remain audit-only if fetched

### Requirement: Legacy Fallback May Fill Component Misses
The research system SHALL allow bounded legacy current membership fallback for
target stocks that are missing from component sets.

#### Scenario: Component source misses a target stock
- **WHEN** a target stock is not matched by component sets
- **THEN** the sync MAY attempt bounded legacy fallback for that missing stock

### Requirement: Relative Valuation Must Default To Shenwan L2 Grouping
The research system SHALL support explicit `L1 / L2 / L3` peer grouping for
industry-relative valuation and statistics, and SHALL default to `L2` grouping
when no level is explicitly requested.

#### Scenario: No valuation grouping level is specified
- **WHEN** a relative valuation or industry statistics request omits the target
  Shenwan grouping level
- **THEN** the system SHALL use `sw_l2_code` as the default peer grouping key
- **AND** it SHALL not fabricate `sw_l3_code` for second-level leaf memberships

### Requirement: Beta Calculations Must Be Benchmark-Aware
The research system SHALL compute stock beta against explicit benchmark
identities rather than relying on a single implicit benchmark.

#### Scenario: Beta includes benchmark identity
- **WHEN** beta is calculated for a stock
- **THEN** the result SHALL include benchmark family, benchmark instrument id,
  benchmark name when available, window, calculation method, calculation
  version, parameter hash, observation count, and date range

#### Scenario: Default beta windows are returned
- **WHEN** beta is requested for an instrument without `window_days`
- **THEN** the system SHALL return the configured default windows, initially
  60-day, 120-day, and 252-day results

#### Scenario: Custom beta window is requested
- **WHEN** beta is requested with a valid `window_days` value
- **THEN** the system SHALL calculate beta for exactly that window without
  requiring a precomputed snapshot

### Requirement: Beta Benchmark Selection Must Be Configurable
The research system SHALL resolve default beta benchmarks from configuration or
a tested resolver rather than hard-coded service assumptions.

#### Scenario: Board default benchmark is selected
- **WHEN** a stock has exchange or board attributes that map to a configured
  default benchmark
- **THEN** the beta service SHALL use the configured benchmark and record the
  selection rule in lineage

#### Scenario: Explicit benchmark is requested
- **WHEN** a caller or operator requests a specific benchmark instrument id
- **THEN** the beta service SHALL calculate beta for that benchmark without
  replacing it with the default benchmark

#### Scenario: All benchmark comparison is requested
- **WHEN** beta is requested with `benchmark_family=all`
- **THEN** the beta service SHALL return deduped results for market default,
  board, broad-market, and authoritative Shenwan level-2 industry benchmarks
  where each benchmark can be resolved

### Requirement: Industry Beta Must Use Authoritative Shenwan Membership
The research system SHALL compute industry beta from authoritative Shenwan
level-2 membership when configured.

#### Scenario: Authoritative industry benchmark is available
- **WHEN** a stock has current authoritative Shenwan level-2 membership and the
  corresponding industry index quote series is available locally
- **THEN** the beta service SHALL produce an `industry_sw_l2` beta result using
  that industry benchmark

#### Scenario: Authoritative industry benchmark is unavailable
- **WHEN** a stock lacks authoritative Shenwan level-2 membership or the local
  industry benchmark quote series is missing
- **THEN** the beta result SHALL be unavailable with an explicit missing reason
- **AND** it SHALL NOT fall back to reference-only industry fields

### Requirement: Beta Calculations Must Preserve Statistical Diagnostics
The research system SHALL expose enough statistical diagnostics to judge whether
a beta value is usable.

#### Scenario: Beta is successfully calculated
- **WHEN** stock and benchmark return series have enough aligned observations
- **THEN** the result SHALL include beta, alpha, correlation, r-squared, stock
  volatility, benchmark volatility, residual volatility, tracking error, beta
  standard error, beta t-statistic, beta p-value, quality flag, interpretation
  flags, observation count, and calculation diagnostics

#### Scenario: Beta cannot be calculated
- **WHEN** the benchmark series is missing, variance is zero, or aligned
  observations are below the configured minimum
- **THEN** the result SHALL be marked unavailable with a structured missing
  reason

### Requirement: DCF Must Consume Shared Beta Semantics
DCF valuation SHALL use the shared on-demand beta layer when beta data is
available.

#### Scenario: DCF records beta source
- **WHEN** DCF uses calculated beta, an explicit override, or configured
  fallback beta
- **THEN** the DCF response SHALL record the beta value, beta source, benchmark
  identity when applicable, and parameter lineage

### Requirement: Futures configuration is independently loadable
The Research Data Engine SHALL load futures-domain settings from `config/11_futures.json` while preserving existing access through `ResearchConfig.modules["commodity_market_data"]`.

#### Scenario: Futures config file exists
- **WHEN** the configuration manager loads project configuration and `config/11_futures.json` exists
- **THEN** it SHALL merge `futures_config` into the research commodity market data module
- **AND** existing futures services SHALL be able to read the same runtime keys as before

#### Scenario: Duplicate futures config exists
- **WHEN** both `config/10_research.json` and `config/11_futures.json` define the same futures-domain key
- **THEN** the independent futures config SHALL take precedence
- **AND** the loader SHALL emit a migration warning identifying the duplicated key

### Requirement: Futures scope selection is shared across research services
The Research Data Engine SHALL use a single futures universe selector for futures master data, calendar, price sync, readiness, and API/operator requests.

#### Scenario: Calendar and price sync use same scope
- **WHEN** a configured scope is used for trading-calendar backfill and then price dry-run
- **THEN** both services SHALL resolve the same target exchanges and instruments from the same selector rules

### Requirement: Research Engine Includes Futures Market Data Domain
The Research Data Engine SHALL support futures market data as a local-first research domain with dedicated storage, providers, sync services, scheduler jobs, readiness, and read APIs.

#### Scenario: Futures domain follows shared research rollout template
- **WHEN** the futures market-data domain is implemented
- **THEN** it SHALL include provider abstraction, local storage, sync/update service, scheduler entry, DataManager service methods, read API, readiness API, tests, and documentation

#### Scenario: Futures domain is isolated from stock market data
- **WHEN** the research engine stores or reads futures bars
- **THEN** the implementation SHALL use the futures-domain storage path
- **AND** it SHALL NOT depend on stock quote adjustment, stock instrument lifecycle, or stock daily update tables

### Requirement: Futures Research APIs Are Queryable
The Research Data Engine SHALL expose persisted futures datasets through `/research/futures/*` APIs or a gated response that explains why the domain is unavailable.

#### Scenario: Persisted futures data has read paths
- **WHEN** futures metadata, bars, diagnostics, spreads, or exposure mappings are persisted locally
- **THEN** callers SHALL be able to query those rows through the research API layer

#### Scenario: Futures module is disabled
- **WHEN** the futures module is disabled in research configuration
- **THEN** futures APIs SHALL return a structured unavailable response instead of triggering sync or remote provider calls

### Requirement: Futures Scheduler Jobs Are Separate From Stock Daily Update
The Research Data Engine SHALL schedule futures maintenance through dedicated futures jobs rather than the stock `daily_data_update` job.

#### Scenario: Futures daily sync is configured
- **WHEN** scheduler configuration is loaded
- **THEN** futures daily sync, diagnostics refresh, spread recompute, readiness, and manual backfill jobs SHALL have their own job names and `max_instances=1`

#### Scenario: Historical futures backfill is manual by default
- **WHEN** the scheduler starts in normal production mode
- **THEN** the futures historical backfill job SHALL remain disabled or manual-only unless explicitly enabled by an operator

### Requirement: Research futures APIs shall expose master data and calendars through local read paths

The research data engine SHALL expose futures dictionary, instrument detail, contract, series, calendar, source manifest, price, and continuous mapping APIs using local `futures.db` data only.

#### Scenario: Local dictionary read
- **WHEN** a client calls `/research/futures/dictionary`
- **THEN** the data manager SHALL read local futures master data and dictionary tables
- **AND** it SHALL NOT call official exchange, AkShare, or any other upstream provider.

#### Scenario: Calendar read
- **WHEN** a client calls `/research/futures/calendar` with an exchange and date range
- **THEN** the data manager SHALL return local trading calendar rows with source profile and quality flags.

### Requirement: Research futures readiness shall be trading-calendar aware

The research data engine SHALL use futures trading calendars when evaluating stale series, missing bars, and latest expected trading dates.

#### Scenario: Holiday does not produce readiness gap
- **WHEN** a futures series has no bar on a date marked as a non-trading day for its exchange
- **THEN** readiness SHALL NOT emit a missing-local-bar warning for that date.

#### Scenario: Missing latest expected trading day
- **WHEN** the latest expected trading day from the futures calendar is later than the latest local bar
- **THEN** readiness SHALL emit a stale or missing warning that includes the relevant exchange, expected trade date, latest local trade date, and calendar source profile.

### Requirement: Research futures price APIs shall resolve default series safely

The research data engine SHALL resolve default price queries by root instrument and series type without requiring callers to know the exact `series_id`.

#### Scenario: DCF default main-continuous read
- **WHEN** a caller requests futures prices by `instrument_id` only
- **THEN** the data manager SHALL use the configured default research series type, initially `main_continuous`
- **AND** the response SHALL include the resolved `series_id`.

#### Scenario: Missing default series
- **WHEN** a root instrument has no active default research series
- **THEN** the API SHALL return a structured not-found or input-gap response
- **AND** it SHALL NOT attempt a remote sync.

### Requirement: Research futures APIs shall expose contract lineage for continuous bars

The research data engine SHALL let clients inspect the real contract lineage behind continuous futures bars.

#### Scenario: Continuous mapping endpoint
- **WHEN** a client requests mapping rows for a continuous series and date range
- **THEN** the response SHALL include trade date, series id, underlying contract id, exchange contract code, construction method, construction version, and quality flag.

#### Scenario: Price response lineage
- **WHEN** a client requests futures price bars with lineage enabled
- **THEN** the response SHALL include available source profile, source interface, construction method, and selected contract metadata for continuous bars.

### Requirement: Futures Sync Must Use Trading Day Governance
The Research Data Engine SHALL route commodity futures daily sync, historical backfill, and dry-run through futures trading-day governance before provider fetches.

#### Scenario: Daily sync resolves target trade dates
- **WHEN** futures market-data daily sync starts for one or more exchanges
- **THEN** it SHALL request target trade dates from trading-day governance before calling official or fallback providers
- **AND** it SHALL include calendar source quality, target-date count, skipped-date count, and unresolved blocker summary in ingestion metadata

#### Scenario: Historical backfill receives a natural date range
- **WHEN** a futures backfill receives `start_date` and `end_date`
- **THEN** it SHALL expand that natural date range into exchange-specific target trading dates through trading-day governance
- **AND** it SHALL skip governed non-trading days unless an explicit, audited override includes them

#### Scenario: Provider requests are grouped by governed exchange dates
- **WHEN** the futures sync service prepares provider calls
- **THEN** it SHALL group work by exchange, provider profile, and governed trade date
- **AND** provider implementations SHALL NOT independently infer broad natural-calendar loops for production requests

### Requirement: Futures Dry-run Must Report Calendar Governance Results
The Research Data Engine SHALL include trading-day governance diagnostics in futures dry-run output.

#### Scenario: All-product dry-run is executed
- **WHEN** an all-product futures dry-run is executed
- **THEN** the dry-run report SHALL show target exchanges, governed target trading dates, calendar quality distribution, estimated-calendar usage, unresolved conflicts, and review-required items

#### Scenario: Dry-run target date is legally non-trading
- **WHEN** a requested dry-run period contains dates governed as non-trading days
- **THEN** the dry-run SHALL report those dates as skipped non-trading days rather than provider failures

### Requirement: Futures Readiness Must Include Trading Day Governance
The Research Data Engine SHALL include trading-day governance state in commodity futures readiness and data-quality reporting.

#### Scenario: Calendar is missing for an enabled exchange
- **WHEN** readiness is computed for an enabled futures exchange and no usable calendar exists for the requested horizon
- **THEN** readiness SHALL include a blocker identifying the missing calendar coverage

#### Scenario: Calendar is estimated only
- **WHEN** readiness is computed and the calendar coverage is only `estimated`
- **THEN** readiness SHALL include a warning for dry-run use
- **AND** readiness SHALL mark production-write eligibility according to configured quality gates

#### Scenario: Calendar explains an apparent data gap
- **WHEN** futures price data is missing on a date governed as a non-trading day
- **THEN** readiness SHALL treat the missing price row as a legal non-trading-day skip rather than stale or failed market-data coverage

### Requirement: Futures Ingestion Metadata Must Preserve Calendar Lineage
The Research Data Engine SHALL persist calendar-governance lineage for futures ingestion runs that use governed target dates.

#### Scenario: Ingestion run completes
- **WHEN** a futures sync, backfill, or dry-run resolves target trading dates
- **THEN** the run metadata SHALL include governance run id or expansion id, source quality summary, evidence summary, manual override references, and skipped non-trading-day counts

#### Scenario: Manual override is used
- **WHEN** a manual calendar override changes target-date generation or permits production writes
- **THEN** the ingestion metadata SHALL store the override reference and reason so the run is reproducible

### Requirement: Futures data downloads require official calendar readiness
The Research Data Engine SHALL treat officially verified futures exchange calendars as a prerequisite for production futures daily sync and historical futures data backfill.

#### Scenario: Official calendar rows are missing
- **WHEN** futures production sync or historical backfill is requested for an exchange/date range with missing official calendar rows
- **THEN** the data download SHALL block before provider price requests are made

#### Scenario: Only estimated calendar rows exist
- **WHEN** futures production sync or historical backfill finds only estimated weekday calendar rows for the target exchange/date range
- **THEN** the data download SHALL treat the calendar as not production-ready

#### Scenario: Official calendar is ready
- **WHEN** every target exchange/date has an official or backfilled-verified calendar row
- **THEN** futures data download MAY expand the range to target trading dates and proceed to provider routing

### Requirement: Futures Official Source Failure Classification
The Research Data Engine SHALL classify official futures source probe failures into structured categories before calendar or price backfill operators decide whether to proceed.

#### Scenario: Local network cannot reach an official endpoint
- **WHEN** an official futures probe fails with network unreachable, DNS failure, connection timeout, TLS failure, or HTTP status failure
- **THEN** the diagnostic output SHALL include exchange, trade date, endpoint URL, failure category, raw error summary, and whether the condition may reflect local-IP risk control

#### Scenario: Possible anti-bot or IP risk-control response
- **WHEN** an official endpoint returns an access-denied, challenge, risk-control, CAPTCHA, Riversafe, WAF, or unexpected HTML response instead of the expected data payload
- **THEN** the diagnostic output SHALL classify the result as possible anti-bot or local-IP risk-control evidence rather than treating it as a normal empty trading calendar response

### Requirement: Futures Official Coverage Discovery
The Research Data Engine SHALL provide a bounded official-source coverage discovery workflow that estimates reliable daily-data coverage starts by exchange without writing production calendar rows.

#### Scenario: Probe representative years
- **WHEN** a maintainer runs official coverage discovery for domestic futures exchanges
- **THEN** the workflow SHALL probe bounded representative dates per exchange/year and report the earliest year with successful parseable official rows, failed years, empty years, and unresolved years

#### Scenario: Coverage start is not established
- **WHEN** an exchange/year sample has only empty, unreachable, or parser-failed results
- **THEN** the workflow SHALL NOT mark that year as reliable calendar coverage and SHALL keep it out of production write eligibility

### Requirement: Futures Smoke Failure Reports
The Research Data Engine SHALL persist smoke-validation failure reports even when provider sync raises before normal summary generation.

#### Scenario: Smoke run raises
- **WHEN** a futures official smoke run exits because provider sync raises an exception
- **THEN** the script SHALL write a JSON report containing arguments, database path, source flags, exception type, exception message, and any partial diagnostics that are available

### Requirement: Futures Official Payload Fan-Out
The Research Data Engine SHALL fetch official domestic futures daily market data at `exchange + trade_date` granularity and fan out parsed contract rows to all target series on that exchange before attempting duplicate official requests.

#### Scenario: Multiple series share one official exchange-date payload
- **WHEN** a futures sync targets two or more active series on the same exchange for the same governed trading date
- **THEN** the official provider SHALL request that exchange/date payload no more than once in the sync run
- **AND** the sync SHALL build each series' bars, contract bars, contract master rows, and continuous mapping from the reused parsed rows

#### Scenario: Official payload contains only some target varieties
- **WHEN** an official exchange/date payload is fetched successfully but lacks usable rows for one target series
- **THEN** the sync SHALL record an official empty result for that series/date
- **AND** the sync SHALL attempt the configured fallback source for that series/date when fallback is enabled

### Requirement: Futures Official Fan-Out Diagnostics
The Research Data Engine SHALL expose diagnostics for official futures fan-out runs that distinguish exchange payload requests from per-series artifact construction and fallback attempts.

#### Scenario: Fan-out sync completes
- **WHEN** a futures market-data sync run completes through the official fan-out path
- **THEN** the run metadata SHALL include exchange payload request counts, cache reuse counts, series artifact counts, official empty counts, fallback success/failure counts, and provider-empty-on-trading-day counts

#### Scenario: Dry-run fan-out sync completes
- **WHEN** a futures market-data sync is executed with `dry_run=true`
- **THEN** the result SHALL report fetched rows and would-write rows separately from persisted inserted, changed, and unchanged rows
- **AND** persisted write counters SHALL remain zero unless storage writes actually occurred

### Requirement: Futures DCE Browser Session Reuse
The Research Data Engine SHALL reuse one DCE browser-assisted official session across DCE exchange-date payload requests within a single provider lifecycle.

#### Scenario: DCE sync spans multiple dates
- **WHEN** a futures sync or official calendar backfill probes multiple DCE dates in one provider instance
- **THEN** the provider SHALL reuse the initialized browser session for subsequent DCE requests
- **AND** the provider SHALL close the browser and virtual display resources when the run finishes or fails

### Requirement: Futures Sync Uses Official-First Provider Routing
The Research Data Engine SHALL route futures daily sync through the futures provider abstraction with official exchange providers ahead of aggregator fallback providers.

#### Scenario: Scheduler invokes futures sync
- **WHEN** `futures_market_data_sync` or `futures_market_data_backfill` runs
- **THEN** the DataManager SHALL invoke the unified futures sync service
- **AND** the service SHALL apply configured source priority instead of directly calling a single third-party API

#### Scenario: Official source is disabled
- **WHEN** `exchange_official.enabled=false`
- **THEN** futures sync SHALL preserve the existing AkShare fallback behavior
- **AND** readiness SHALL make clear that official-first acquisition is disabled

### Requirement: Futures Source Configuration Is Operational
The Research Data Engine SHALL expose source configuration sufficient for exchange-scoped official futures fetches, fallback, timeouts, retries, and smoke validation.

#### Scenario: Source config is loaded
- **WHEN** research configuration is loaded
- **THEN** futures official source settings SHALL include enabled exchanges, timeout, retry, request interval, parser version, supported modes, and fallback policy
- **AND** the values SHALL be consumed by the provider/sync implementation rather than only documented

#### Scenario: Developer smoke is run
- **WHEN** the futures official-source smoke script is run against a temp database
- **THEN** it SHALL be able to request a bounded series/date range
- **AND** it SHALL return success, partial, or failed status with source-selection details without writing production `data/futures.db` by default

### Requirement: Research Storage Database Routing Must Be Execution-Isolated
The Research Data Engine SHALL isolate the active physical database route for each concurrent execution thread or equivalent execution context so one API request or scheduler operation cannot change the database selected by another operation.

#### Scenario: Concurrent financial scopes overlap
- **WHEN** two worker threads enter and exit financial database scopes with overlapping lifetimes
- **THEN** each thread SHALL restore its own previous database route
- **AND** the manager SHALL NOT remain routed to `financials.db` for later operations that did not explicitly select that database

#### Scenario: Interest-rate read runs during another domain operation
- **WHEN** an interest-rate series read executes concurrently with a financial or valuation storage operation
- **THEN** the interest-rate read SHALL query the configured interests database
- **AND** it SHALL NOT return a valid-looking empty result solely because another execution context selected `financials.db` or `valuation.db`

#### Scenario: Database scopes are nested in one execution context
- **WHEN** financial, valuation, interests, or default research database scopes are nested within one execution thread
- **THEN** the innermost explicit scope SHALL select its intended database
- **AND** exiting each scope SHALL restore the immediately preceding route in that same execution thread

#### Scenario: Scoped operation raises an exception
- **WHEN** a storage operation raises while an explicit database scope is active
- **THEN** the scope SHALL restore that execution context's previous database route in a `finally` path
- **AND** subsequent operations in that context SHALL resolve their normal configured database

#### Scenario: Routing isolation is deployed
- **WHEN** the routing implementation is upgraded
- **THEN** public storage method signatures, REST request and response schemas, configured database paths, and existing SQLite data formats SHALL remain unchanged
- **AND** the upgrade SHALL NOT require a schema migration or data rewrite
