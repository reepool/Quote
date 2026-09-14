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

### Requirement: Human-readable independent verification claims
The system SHALL verify semantic business assertions using their original readable filing labels and SHALL NOT use opaque stable identifiers as substitutes for scope or object meaning.

#### Scenario: Anonymous concentration is independently verified
- **WHEN** an anonymous concentration candidate retains `关联方` and `采购额` in its persisted metadata while its stable `fact_scope` is an internal hash
- **THEN** the verifier claim contains the readable scope and object labels, omits the opaque scope hash as semantic content, and can confirm the explicit filing assertion

### Requirement: Program-enforced semantic proof consistency
The system SHALL accept and promote a semantic verification only when its decision, component checks, verifier identity, and proof type form one internally consistent current proof.

#### Scenario: Confirmed response contains a failed check
- **WHEN** the verifier returns `confirmed` while any required semantic check is false
- **THEN** the response is rejected through the existing retry and machine-rework path and the candidate is not promoted

#### Scenario: Deterministic proof is held locally
- **WHEN** deterministic verification skips the LLM but reports `canonical_promotion_allowed=false`
- **THEN** the candidate is not promoted even if its envelope decision is recorded as confirmed

#### Scenario: Repaired concentration set converges
- **WHEN** the four explicit 601088.SH customer, supplier, and related-party concentration assertions pass the current verifier contract
- **THEN** all four facts are approved and no open promotion exception remains for them

#### Scenario: Legacy concentration has no readable scope label
- **WHEN** an anonymous concentration retains only an opaque stable scope identity
- **THEN** the verifier request omits that identity as semantic content and the record fails closed until readable filing context is available

#### Scenario: Local deterministic state changes after a partial verify
- **WHEN** a deterministic candidate's parser, unit, evidence, or manifest state changes before verify resumes
- **THEN** the system recomputes the local proof and does not reuse the prior allow-or-hold decision

#### Scenario: LLM semantic synthesis passes numeric checks
- **WHEN** a segment or operating fact was semantically synthesized by an LLM and its values pass programmatic reconciliation
- **THEN** the values remain program-validated but the business meaning still requires current independent semantic verification

#### Scenario: Deterministic parser proof is locally blocked
- **WHEN** a deterministic parser candidate fails evidence, numeric, or parser-manifest eligibility
- **THEN** its local verification outcome is `held`, it is not sent to the LLM, and it is routed to machine rework rather than promotion

#### Scenario: Verification result is missing
- **WHEN** a business record reaches promotion without either a current semantic verification or a current deterministic proof
- **THEN** the system fails closed, does not infer permission from document-level extraction metadata, and routes the missing stage result to bounded automatic rework

#### Scenario: Deterministic verification recovers after local rules change
- **WHEN** a resumed deterministic candidate now produces a current local proof after an older verify attempt left machine rework for the same target
- **THEN** the current proof replaces the stale verify state and the old machine rework no longer blocks the stage

#### Scenario: Verifier reports insufficient filing context
- **WHEN** the independent verifier returns `insufficient_evidence` rather than an explicit semantic conflict
- **THEN** the candidate enters bounded automatic context rework instead of being sent directly to manual deep review
