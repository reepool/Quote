## ADDED Requirements

### Requirement: Backtest-critical datasets have a resource capability record
The system SHALL maintain a versioned, machine-readable capability record for every backtest-critical dataset before production acquisition or publication is enabled.

#### Scenario: Existing resource is assessed
- **WHEN** a dataset is proposed for backtest consumption
- **THEN** its capability record SHALL identify the required markets, history, frequency, point-in-time fields, quality threshold, existing providers, parent job, target-universe owner, transport, checkpoint, store, watermark, and read API
- **AND** it SHALL record separate owners for forward maintenance and historical backfill

#### Scenario: Industry data is already available
- **WHEN** the catalog assesses historical Shenwan membership and industry returns
- **THEN** it SHALL reference the existing industry synchronization and as-of/read contracts
- **AND** it SHALL NOT require a duplicate industry downloader

### Requirement: Reuse evidence gates new acquisition routes
The system SHALL block a new full-market provider route or standalone scheduled download until bounded capability evidence proves that the existing route cannot meet the required contract.

#### Scenario: Existing route meets the contract
- **WHEN** a no-write probe shows that an existing provider and parent workflow supply the required history and semantics
- **THEN** the route decision SHALL be `reuse` or `extend_existing`
- **AND** implementation SHALL attach to that existing owner

#### Scenario: Existing routes do not meet the contract
- **WHEN** bounded probes show that existing free and official routes lack required historical depth or field semantics
- **THEN** the record MAY declare `new_source_required`, `manual_import_only`, or `unavailable`
- **AND** it SHALL retain the probe evidence and unresolved gap

#### Scenario: Probe is incomplete
- **WHEN** date meaning, coverage, units, permissions, rate limits, or empty/error behavior remain unverified
- **THEN** production acquisition and strict readiness SHALL remain disabled for that scope

### Requirement: Resource probes are bounded and non-mutating
Provider capability probes SHALL be bounded, observable, and no-write by default.

#### Scenario: Historical capability is probed
- **WHEN** an operator or test runs a provider capability probe
- **THEN** it SHALL use an explicit small symbol/date scope and report request count, returned coverage, date semantics, fields, units, and failures
- **AND** it SHALL NOT write production facts or advance change watermarks

### Requirement: Runtime readiness is scope-specific
The system SHALL combine the static resource decision with local coverage and run evidence to report readiness for an explicit market and date scope.

#### Scenario: Capability endpoint reports readiness
- **WHEN** a consumer queries backtest-data capabilities
- **THEN** the response SHALL include route decision, target and covered universes, date range, missing fields or dates, unresolved-quality counts, latest successful run, and latest watermark
- **AND** it SHALL distinguish unsupported scope from temporarily stale data

#### Scenario: Only part of history is proven
- **WHEN** a dataset meets its quality threshold only for a narrower market or date range
- **THEN** readiness SHALL be true only for that proven scope
- **AND** the system SHALL NOT expose one unqualified global-ready flag

### Requirement: Current snapshots are not historical evidence
The system SHALL NOT infer historical facts from a provider response that is documented or observed to represent only current state.

#### Scenario: Current-only constituent or ST list is returned
- **WHEN** a provider returns the current index constituents or current ST board without historical effective dates
- **THEN** the data MAY seed a current forward-maintained snapshot
- **AND** it SHALL NOT be backdated or used to claim historical readiness

### Requirement: Existing industry capabilities disclose temporal semantics
The capability catalog SHALL expose existing Shenwan membership and return APIs without overstating their point-in-time guarantees.

#### Scenario: Existing industry datasets are discovered
- **WHEN** the capability response describes Shenwan membership and return datasets
- **THEN** it SHALL link the existing APIs with units, coverage, and their effective-date and knowledge-time semantics
- **AND** it SHALL NOT create a duplicate industry downloader or proxy dataset

#### Scenario: Existing as-of API lacks knowledge-time lineage
- **WHEN** an existing dataset has effective-date history but does not retain availability or revision evidence needed for a `known_at` cutoff
- **THEN** readiness SHALL label it effective-date-only rather than strict point-in-time safe
- **AND** consumers SHALL be able to distinguish that limitation before using it in a backtest
