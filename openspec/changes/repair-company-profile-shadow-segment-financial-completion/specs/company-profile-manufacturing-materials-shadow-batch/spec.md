## ADDED Requirements

### Requirement: Segment financial partitions reconcile source-bound rows deterministically
For a governed high-cardinality segment-financial scope, the system MUST merge the
separate revenue, cost, and gross-margin partitions by the source-backed segment
dimension and row label. Different approved Evidence sets for different metric cells
MUST be retained and MUST NOT by themselves create a row-identity conflict. Duplicate
metric cells, Evidence outside the prepared scope, conflicting explicit subjects,
conflicting periods or period types, incompatible row classes, and unsupported
`consolidated_group` promotion MUST remain blocking. Ordinary non-adjustment segment
metadata that is deterministically owned by the prepared report and table scope MUST
be resolved locally rather than requiring identical model repetition across partitions.

#### Scenario: Metric cells use different physical Evidence
- **WHEN** revenue, cost, and margin partitions identify the same approved dimension and row label but cite different approved pages or Evidence IDs
- **THEN** the adapter merges the cells into one source-bound segment row and retains the stable union of their Evidence
- **AND** local schema validation and normal Stage 5 candidate validation still run

#### Scenario: Partitions contain a real row conflict
- **WHEN** two partitions for the same dimension and label assert conflicting explicit subjects, report periods, period types, row classes, or duplicate metric cells
- **THEN** the adapter rejects the merged candidate as `candidate_schema_invalid`
- **AND** it does not resolve the conflict by selecting an arbitrary partition

#### Scenario: Consolidated scope lacks source support
- **WHEN** a partition asserts `consolidated_group` without explicit consolidation-adjustment wording or the existing numeric-reconciliation basis
- **THEN** the merged result remains rejected or unresolved under the existing subject policy
- **AND** ordinary segment reconciliation does not upgrade it to a group fact

### Requirement: Cross-page segment tables retain separate scope and row proof
The system MUST allow an approved segment dimension heading to be established by the
complete controlled segment/revenue-cost table scope, including an opening page whose
rows continue on later pages. Every Segment label MUST still occur in its cited row
Evidence, and every Measurement value, unit, header, period, and Evidence reference MUST
remain source-valid under the existing Stage 5 contract. Scope-level dimension proof
MUST NOT permit an unrelated page to supply a row label or numeric value.

#### Scenario: Dimension heading precedes continuation rows
- **WHEN** the opening page contains the approved `分行业`, `分产品`, `分地区`, or equivalent full heading and a continuation page contains the cited segment row and values
- **THEN** the adapter may validate the dimension from the controlled scope and the row from its cited continuation Evidence
- **AND** the resulting Segment and Measurements preserve the physical Evidence chain

#### Scenario: Row label is absent from cited Evidence
- **WHEN** a proposed segment label appears only elsewhere in the scope and not in the row's cited Evidence
- **THEN** the proposal is rejected
- **AND** scope-level dimension support does not substitute for row-level support

### Requirement: Segment owner completion excludes non-owner disclosures
Segment-dimension extraction or legal-empty coverage MUST use an explicit segment,
revenue-cost, or one-segment owner disclosure. Cost-component rows, industry-policy
discussion, audit references, and ordinary company-level income statements MUST NOT be
treated as Segment objects or as evidence that segment disclosure is absent. An
explicit one-reportable-segment statement or explicit governed not-applicable statement
MUST remain eligible for source-bound legal-empty coverage. A cost table MAY contribute
segment facts only when a separate explicit top-level segment dimension and row identity
are present.

#### Scenario: Cost component resembles a segment row
- **WHEN** a table row is `原材料及燃动费`, energy, labor, depreciation, manufacturing overhead, or another cost component without an explicit segment identity
- **THEN** the system rejects that row as a Segment
- **AND** it does not close segment coverage from the cost component

#### Scenario: Incidental disclosure is routed as segment owner
- **WHEN** industry policy, an audit reference, or an ordinary company-level income statement is the only support for segment extraction or legal empty
- **THEN** the system leaves segment coverage unresolved or excludes that scope
- **AND** it does not infer `not_disclosed` from the incidental disclosure

#### Scenario: Source explicitly has one reportable segment
- **WHEN** a governed disclosure explicitly states that the group or issuer has only one reportable segment, or explicitly marks the segment disclosure not applicable
- **THEN** the system may emit the existing source-bound legal-empty coverage
- **AND** no synthetic Segment or financial Measurement is created

### Requirement: Local segment merge failures remain precisely diagnosable
When a segment partition merge, normalization, or final local schema validation fails, the provider adapter MUST retain the existing typed classification and MUST surface a
bounded causal message that distinguishes identity conflict, Evidence failure, owner
failure, and final schema failure. The causal message MUST remain bounded. The system
MUST NOT mutate the historical replay or
invoke a provider to diagnose provider-free fixtures.

#### Scenario: Local merge rejects a partition result
- **WHEN** all provider partitions returned but local reconciliation fails
- **THEN** the trace or surfaced semantic-provider error remains `candidate_schema_invalid` and includes the exact bounded local cause
- **AND** the failure is not reported only as a generic merged-schema violation

### Requirement: Segment completion repair is proven before replay authorization
The change MUST provide provider-free fixtures for the confirmed partition-Evidence,
metadata-drift, cross-page dimension, cost-component, non-owner, one-segment, and legal-
empty cases. Focused tests MUST prove the positive and negative paths with zero provider
calls. Passing this change MUST NOT itself authorize a new twenty-report replay or any
production consumer.

#### Scenario: Provider-free completion suite passes
- **WHEN** all confirmed fixtures and focused regressions pass through the existing provider/planner owners without a provider call
- **THEN** the change may be submitted for review as evidence for a later single-replay proposal
- **AND** `production_authorization=not_authorized`, Stage 6, approved writes, scheduler/backfill, commodity exposure, value-chain publication, and DCF remain closed
