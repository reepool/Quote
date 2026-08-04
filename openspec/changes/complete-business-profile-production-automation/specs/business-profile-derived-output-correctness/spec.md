## ADDED Requirements

### Requirement: Anonymous concentration disclosures SHALL not require entity resolution
The system SHALL create governed concentration facts for anonymous customer or supplier disclosures without attempting to fabricate or resolve a named counterparty edge.

#### Scenario: Customer A concentration
- **WHEN** official evidence discloses an anonymous customer label and a supported concentration share
- **THEN** the system stores a customer concentration fact with exact evidence and creates no relationship edge

### Requirement: Production named relationships SHALL use governed local identities
The production runtime SHALL construct counterparty resolution from local governed legal identities, official identifiers, and approved aliases valid at the knowledge cutoff.

#### Scenario: Unique exact legal name
- **WHEN** a disclosed counterparty name exactly matches one eligible governed legal identity
- **THEN** the relationship records the resolved entity and exact resolution basis without human review

#### Scenario: No unique exact identity
- **WHEN** the disclosed name is missing, ambiguous, or has only a model-proposed id
- **THEN** no named relationship is approved and a machine-rework or bounded quick-review exception is persisted

### Requirement: Processor roles SHALL require explicit transformation evidence
The system SHALL auto-publish a processor role only when governed evidence links an issuer-scoped input and output transformation within the applicable segment and period.

#### Scenario: Standalone processing verb
- **WHEN** an approved activity states only that the issuer processes one object without governed input-output linkage
- **THEN** no processor role candidate is auto-promoted from that activity alone

#### Scenario: Linked transformation
- **WHEN** approved governed facts explicitly link an input transformed into an output for the issuer and segment
- **THEN** the deterministic rule may produce a processor role preserving all supporting fact ids and the rule version

### Requirement: Commodity publications SHALL preserve distinct governed identities
Commodity exposure publications SHALL preserve product identity, commodity identity, and executable price-series identity as separate governed values.

#### Scenario: Single promoted mapping
- **WHEN** one current promoted mapping links an approved product exposure role to a commodity and price series
- **THEN** the publication records that mapping's commodity id and price-series id rather than copying the product id into the commodity field

#### Scenario: Mapping cannot publish
- **WHEN** a mapping is missing, stale, ambiguous, or candidate-only
- **THEN** no approved publication is created and a stable machine-rework exception is persisted for unattended retry

### Requirement: Derived-output gaps SHALL be operationally visible and retryable
Every failed local derivation or publication attempt SHALL produce idempotent persistent diagnostics with instrument, field family, source component, reason, retry tier, and next retry time.

#### Scenario: Weekly run repeats an unchanged gap
- **WHEN** the same unchanged component fails publication in a later run
- **THEN** the existing exception is updated or reused without creating unbounded duplicate rows
