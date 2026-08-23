## ADDED Requirements

### Requirement: Current-catalog derived promotion
The system SHALL preserve immutable evidence processing metadata while binding every newly derived role and exposure fact to the catalog versions active for that derivation run.

#### Scenario: Historical evidence feeds a current derived record
- **WHEN** approved evidence was created under an older product catalog and a current run derives a new exposure fact from it
- **THEN** the evidence retains its historical catalog version and the derived fact passes catalog validation against the current runtime version

### Requirement: Human review blockers exclude automation
The system SHALL block automatic reopening only for actual operator review decisions and SHALL treat `system:` and `automation:` review identities as machine decisions.

#### Scenario: Contract recovery rejected an obsolete record
- **WHEN** an automated contract-recovery audit rejected a record and current immutable filing evidence passes semantic verification
- **THEN** the system reopens and promotes the record without human intervention

#### Scenario: An analyst rejected a record
- **WHEN** a reviewer outside the machine identity namespaces rejected or held a record
- **THEN** automatic reopening remains blocked

### Requirement: Effective semantic activity selection
The system SHALL derive roles and exposure facts from one deterministic current activity when the same filing evidence contains exact semantic duplicates, preferring a canonical product identity over an otherwise identical unmapped record while preserving activities with different subject scopes, object types, geographies, or business regimes.

#### Scenario: A later parser maps an existing polyethylene activity
- **WHEN** approved activities have the same evidence, action, raw object, value, unit, period, and segment but only one has a canonical product ID
- **THEN** derivation and publication use the mapped activity and do not retry publication of the older unmapped activity

#### Scenario: Similar assertions describe different business scopes
- **WHEN** otherwise similar approved activities belong to different subsidiaries, geographies, object types, or business regimes
- **THEN** derivation and publication retain every distinct scoped activity

### Requirement: Fact preservation without fabricated commodity identity
The system SHALL retain approved exposure facts when the filing does not support a unique commodity identity and SHALL NOT classify that condition as a failed publication.

#### Scenario: Composite production process has no product mapping
- **WHEN** an approved production-capacity fact describes a composite process such as `煤制烯烃` without a canonical product ID
- **THEN** the system records a fact-only publication outcome, preserves the approved fact, and creates no machine-rework publication exception

#### Scenario: Known produced product has a mapping
- **WHEN** an approved `produces` activity has a canonical product ID and a unique commodity mapping
- **THEN** the system publishes a positive output commodity exposure

### Requirement: Commodity identity independent of market series
The system SHALL publish a unique canonical commodity identity even when the catalog does not select one executable price series.

#### Scenario: Product has multiple governed market references
- **WHEN** a canonical product maps to one commodity identity but has candidate-only or multiple price references
- **THEN** the commodity exposure is approved with `price_series_id` unset and is not routed to manual review

### Requirement: Automatic exception convergence
The system SHALL keep at most one open exception per current target condition and SHALL resolve stale exceptions when an exact target, replacement fact, or exact semantic relationship assertion succeeds.

#### Scenario: Gate signature changes across reruns
- **WHEN** the same target fails under a new gate signature
- **THEN** the previous open exception is resolved before the current exception is persisted

#### Scenario: Publication succeeds after mapping improvement
- **WHEN** a fact previously had an open publication exception and now publishes or becomes a valid fact-only outcome
- **THEN** all open publication exceptions for that exact fact are resolved

#### Scenario: Local entity resolves its catalog proposal
- **WHEN** a named relationship is promoted after its semantic assertion previously created a catalog proposal
- **THEN** the proposal for that exact assertion is resolved automatically and proposals for other relationships sharing the evidence remain open

### Requirement: Accurate actionable gap reporting
The system SHALL count each unresolved current target once across machine rework, quick review, deep review, and publication input gaps, and SHALL exclude published, unchanged, fact-only, inactive duplicate, and resolved historical outcomes.

#### Scenario: Targeted run has only successful or fact-only outcomes
- **WHEN** all current verified records are promoted and every exposure result is published, unchanged, or fact-only
- **THEN** `publication_gaps` is zero and the task is not degraded for publication gaps

### Requirement: Recovery query remains executable
The system SHALL retrieve the latest selected semantic artifact for a context-reselection retry using valid parameterized SQL.

#### Scenario: Context-incomplete retry loads prior selection
- **WHEN** a context-incomplete machine retry requests the latest completed selected artifact for one instrument, family, and source document
- **THEN** the query executes without syntax error and returns the latest matching artifact when present

### Requirement: Recovered candidates invalidate completed stage output
The system SHALL invalidate stale semantic, verification, and publication stage output when automated contract recovery reopens a record, including when the work item is already `retry_due`, and SHALL publish or report the reopened candidate before declaring the run successful.

#### Scenario: Force rerun follows automated contract recovery
- **WHEN** contract recovery reopens an operating fact and requeues its work item before a targeted `force=true` run
- **THEN** the work item receives a fresh checkpoint, the reopened fact is verified and considered for promotion, and the task does not report success from an empty stale checkpoint

#### Scenario: Recovered candidate succeeds
- **WHEN** the reopened candidate passes its current applicable catalog, evidence, numeric, temporal, and semantic gates
- **THEN** it becomes approved and all open machine-rework exceptions for that exact target are resolved
