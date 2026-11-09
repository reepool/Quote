## MODIFIED Requirements

### Requirement: Relative Valuation Must Use Authoritative Peer Groups And Clean Multiples
Relative valuation SHALL use authoritative Shenwan peer groups by default and SHALL exclude invalid or unsupported multiples from all benchmark statistics. Every metric's mean, median, extrema, quartiles, percentile rank and premium to median SHALL use the same filtered peer population. Raw peer values SHALL remain inspectable.

#### Scenario: Peer benchmark is computed
- **WHEN** relative valuation computes industry PE/PB/PS benchmarks
- **THEN** it SHALL use the configured authoritative Shenwan peer level and return peer count, mean, median, quartiles, percentile rank, and premium or discount to median
- **AND** it SHALL disclose that the primary comparison statistics are valid-peer median and quartiles

#### Scenario: Invalid peer multiples exist
- **WHEN** peer valuation rows contain negative denominators, zero denominators, non-positive multiples, missing or non-finite values, or unsupported metric variants
- **THEN** the benchmark calculation SHALL exclude those values and report the valid and excluded peer counts and per-metric reasons
- **AND** subject rank and premium SHALL be null with diagnostics when the subject multiple is invalid

#### Scenario: Explicit high-multiple limits are supplied
- **WHEN** a caller supplies a finite positive max_pe or max_pb
- **THEN** the corresponding metric family SHALL exclude values above that limit with an above_maximum reason and SHALL echo the policy
- **AND** omission of those parameters SHALL NOT silently activate an arbitrary high-value cutoff

#### Scenario: Raw and filtered populations are compared
- **WHEN** finite raw peers include negative or excluded extreme values
- **THEN** each metric SHALL return peer_raw_mean and raw_peer_count over finite raw numeric samples alongside its filtered statistics
- **AND** no contract SHALL require the filtered mean to lie between its quartiles

## ADDED Requirements

### Requirement: Financial Single-Period Reads Default To A Bounded Summary
The single-period financial-statements API SHALL support `view=summary|full`, default to summary, and obtain summary values from the existing authoritative local financial rows without reading or decoding full facts, indicator-detail or statement JSON. Default summary serialization without explicitly requested diagnostics SHALL be less than 8192 UTF-8 bytes and SHALL retain identity, report period, availability, source, scalar core facts, scalar indicators and units.

#### Scenario: Default financial read is consumed inline
- **WHEN** a caller requests one company's financial statements without an explicit full view or diagnostics
- **THEN** the response SHALL contain the normalized summary and SHALL omit full facts payloads, indicator details and raw statements
- **AND** both the response-size limit and absence of full-JSON reads or decoding SHALL be verified

#### Scenario: Full financial payload is explicitly requested
- **WHEN** view=full is requested
- **THEN** the existing complete facts and indicator-detail payload SHALL remain available
- **AND** raw statements SHALL be included unless include_statements=false is explicitly supplied
- **AND** view=summary with include_statements=true SHALL return a structured 422 with a hint to use view=full

### Requirement: Financial Field Projection Selects Values Without Hiding Metadata
Single-period and history financial APIs SHALL support fields containing canonical core keys, scalar `indicators.<key>` and `ratios.<key>`. Projection SHALL include only requested business values plus required identity, report-period, availability, source and unit metadata. Existing requested_canonical_facts SHALL retain its service-layer selection meaning rather than impersonating field projection.

#### Scenario: Two core values are requested
- **WHEN** fields=revenue,net_income is supplied
- **THEN** only revenue and net_income SHALL appear as business values
- **AND** the response SHALL retain required metadata but SHALL NOT append all unrequested indicators or raw facts

#### Scenario: A projection field is unknown
- **WHEN** a field is absent from the supported normalized projection catalog
- **THEN** the API SHALL return structured 422 and SHALL NOT silently return the full payload

#### Scenario: Existing history and source-extension controls are used
- **WHEN** history is called without a view override
- **THEN** its existing report-period selection and default payload behavior SHALL remain unchanged
- **AND** history SHALL accept explicit summary view and the same fields semantics
- **AND** neither view nor fields SHALL enable remote extension without the existing explicit authorization parameter

### Requirement: Research Returns Expose Canonical Ratios And Units
Financial responses SHALL expose a `ratios` object for percentage-semantic financial indicators in decimal-ratio units while retaining legacy values. Technical summary SHALL expose `pct_change_ratio` as an alias of its existing pct_change_1d and SHALL disclose adjusted-close-to-close return basis. Both surfaces SHALL include field-path units without changing per-share values or current/quick-ratio multiples.

#### Scenario: Financial ratios are exposed alongside old fields
- **WHEN** the stored gross_margin is 0.8976189601948095
- **THEN** ratios.gross_margin SHALL retain that decimal value
- **AND** units SHALL identify it as ratio rather than percent
- **AND** current_ratio, quick_ratio and per-share values SHALL NOT be divided by 100

#### Scenario: Technical return is compared with a quote return
- **WHEN** technical summary has pct_change_1d=-0.005513
- **THEN** pct_change_ratio SHALL equal -0.005513 and its unit SHALL be ratio
- **AND** the return-basis declaration SHALL allow callers to distinguish it from source-reference returns on corporate-action days
