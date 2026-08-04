## ADDED Requirements

### Requirement: Security-state history preserves source events and derived intervals
The system SHALL store immutable ST, *ST, warning-removal, suspension, resumption, delisting-decision, and listing-termination events separately from append-only status-interval interpretation revisions.

#### Scenario: Official event has different publication and effective dates
- **WHEN** an official list or announcement supplies a security-state event
- **THEN** the event SHALL retain publication time, effective date, local `available_at`, source profile, artifact lineage, event type, and quality state separately

#### Scenario: Current master state changes after deployment
- **WHEN** shared instrument-master governance observes a current ST or lifecycle state transition
- **THEN** it MAY record forward `observed_transition` evidence and derive a subsequent interval
- **AND** it SHALL NOT infer a historical interval start before the first accepted evidence

#### Scenario: Conflicting events exist
- **WHEN** accepted sources disagree on a transition or effective date
- **THEN** the interval SHALL remain conflict-marked or unresolved according to source authority
- **AND** strict point-in-time state reads SHALL fail closed for the ambiguous period

#### Scenario: Later evidence changes an interval interpretation
- **WHEN** an announcement, review, or source-authority decision changes an earlier status interval
- **THEN** the system SHALL append an interval interpretation revision with its decision `available_at` and input event identities
- **AND** an earlier `known_at` read SHALL retain the interpretation or unavailable result supported at that cutoff

### Requirement: Delisting state retains official authority
The system SHALL reuse official exchange master, list, or announcement evidence for confirmed delisting and SHALL preserve historical records after lifecycle transitions.

#### Scenario: Risk warning is observed
- **WHEN** an announcement reports possible termination, delisting risk, or suspension without confirmed listing termination
- **THEN** the system MAY record the corresponding risk or suspension event
- **AND** it SHALL NOT mark the instrument confirmed delisted

#### Scenario: Listing termination is confirmed
- **WHEN** authoritative lifecycle evidence confirms termination and its effective date
- **THEN** the master state and lifecycle timeline SHALL be updated consistently
- **AND** historical quote, financial, and research data SHALL remain available

### Requirement: Daily price-limit references retain provenance
The system SHALL store immutable revisions of daily upper and lower price-limit references independently from quote rows with decision availability, source mode, rule lineage, and quality.

#### Scenario: Upstream reports complete reference prices
- **WHEN** an existing quote route or bounded enrichment source supplies actual daily limit references
- **THEN** the revision SHALL be marked `source_reported` with source profile and availability lineage

#### Scenario: Rule engine derives limit prices
- **WHEN** no source-reported row exists and all governed rule inputs are known
- **THEN** the system MAY store a `derived_rule` revision with decision `available_at`, rule version, input identities, governed exchange reference price, applicable ex-right/ex-dividend reference-price decision, board, listing age, ST state, trading regime, tick size, rounding method, and quality status
- **AND** the revision SHALL NOT be labeled official or source-reported

#### Scenario: Later source or rule changes the limit
- **WHEN** a source correction or later rule version materially changes a stored daily limit
- **THEN** the system SHALL append a revision
- **AND** point-in-time reads SHALL select only revisions available by `known_at`

#### Scenario: Required state is ambiguous
- **WHEN** the applicable ST interval, listing regime, exchange reference price, corporate-action adjustment, or rounding rule is unknown
- **THEN** the system SHALL NOT emit a strict-ready derived limit row

#### Scenario: Raw prior close differs from the governed reference price
- **WHEN** exchange rules require an adjusted ex-right or ex-dividend reference price for the trading date
- **THEN** the rules engine SHALL use the governed reference-price decision and its availability lineage
- **AND** it SHALL NOT silently substitute the unadjusted prior close

#### Scenario: Limit-hit pool is available
- **WHEN** a dated limit-up or limit-down hit pool is used
- **THEN** it MAY validate instruments that hit their limit
- **AND** it SHALL NOT be treated as complete all-market daily limit-reference coverage

### Requirement: Security-state maintenance reuses master, announcement, and quote workflows
The system SHALL integrate security-state acquisition with shared instrument-master governance, source-neutral official announcements, and daily quote maintenance before adding any new source or job.

#### Scenario: Current-universe master refresh runs
- **WHEN** shared master governance persists current instrument state
- **THEN** it SHALL compare governed state with the prior accepted state and emit eligible transitions without repeating the current-list request in a separate task

#### Scenario: Daily quote universe is resolved
- **WHEN** daily quote maintenance has a governed instrument universe and trading date
- **THEN** price-limit persistence or unresolved-only enrichment SHALL reuse that universe and parent run context
- **AND** it SHALL NOT launch an independent full-market scan over the same date

#### Scenario: Historical recovery is requested
- **WHEN** bounded source probes prove historical state or limit coverage
- **THEN** recovery SHALL use explicit date/instrument scopes, checkpoints, pacing, and dry-run reporting in the governed historical-backfill workflow

### Requirement: Security-state APIs are point-in-time and coverage aware
The system SHALL expose bounded as-of reads for market state and price limits with effective, known-time, provenance, confidence, and coverage fields.

#### Scenario: Backtest requests historical state
- **WHEN** a caller requests an instrument state for an effective date and `known_at`
- **THEN** only events, interval interpretation revisions, and price-limit revisions available by `known_at` SHALL participate in resolution
- **AND** the response SHALL identify unresolved or uncovered intervals rather than silently filling them
