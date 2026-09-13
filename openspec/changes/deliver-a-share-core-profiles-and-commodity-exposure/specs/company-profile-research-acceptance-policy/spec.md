## MODIFIED Requirements

### Requirement: Research acceptance preserves evidence while allowing bounded uncertainty
The research acceptance policy MUST accept a source-backed fact with complete Evidence, value, unit, period, metric meaning, and independent verification under `subject_scope=consolidated_group` with `subject_basis=report_default_group_scope` when the source does not explicitly prove a narrower issuer, subsidiary, or segment scope and contains no unresolved subject conflict. Explicit narrower source wording MUST remain narrow. The policy MUST NOT permit unsupported promotion beyond the report-default group convention, and it MUST retain the original source wording, Evidence, and any uncertainty.

#### Scenario: Company wording supports a qualitative activity
- **WHEN** Evidence says `公司主要从事动力电池研发、生产、销售` and the Activity is independently verified
- **THEN** the Activity may be accepted for research with `subject_scope=consolidated_group` and `subject_basis=report_default_group_scope`
- **AND** the source actor remains `公司`

#### Scenario: Ordinary numeric report fact uses group convention
- **WHEN** an annual-report revenue, margin, operating quantity, or concentration fact has no explicit standalone or segment subject
- **THEN** the fact may be accepted with the default group scope after Evidence and verification pass
- **AND** the projection exposes the default basis for audit

#### Scenario: Explicit standalone fact remains issuer-scoped
- **WHEN** the local Evidence explicitly identifies a parent-only statement, 母公司 scope or 本公司单体 scope rather than ordinary company wording
- **THEN** the fact retains `subject_scope=issuer`
- **AND** the default group convention does not override it

#### Scenario: Unsupported subject rewrite is attempted
- **WHEN** a candidate rewrites explicit issuer, named-subsidiary, or business-segment Evidence as a consolidated-group fact, or claims a narrower subject that the Evidence does not identify
- **THEN** verification blocks the candidate
- **AND** the report-default group convention does not override explicit narrower wording or authorize an unsupported narrow subject

### Requirement: Research usability is separate from production authorization
The policy MUST separate accepted-record delivery, three-dimension core completeness, execution status and consumer authorization. A failed supplementary scope or review workload MUST NOT withhold unrelated accepted facts. Missing principal business, major products/services or revenue-model evidence MUST keep core completeness false even when many numeric facts exist. Historical report states and research_slice_usable MUST retain their original run meaning; current fact-level delivery MUST NOT rewrite historical bundles. Current research fixtures remain accepted_for_review and not_authorized until the explicit new-contract research release is implemented.

#### Scenario: Supplementary regime scope times out
- **WHEN** an event scope fails but accepted core facts exist
- **THEN** the failed scope remains retryable or failed and its gap is displayed
- **AND** unrelated accepted facts remain deliverable without pretending the scope succeeded.

#### Scenario: Core business is missing
- **WHEN** measurements exist but principal-business evidence is absent
- **THEN** partial data is delivered with core completeness false.

#### Scenario: New research release is implemented
- **WHEN** the declared writer and consumer scope passes its integration acceptance
- **THEN** program-owned acceptance can publish permitted new-contract research records
- **AND** no historical fixture or DCF consumer is automatically authorized.

### Requirement: Gold subject refinement is closed and Evidence bounded
Gold subject evaluation MUST use a closed `subject_strictness` policy. In addition to exact subject matching and the existing `allow_unclear_if_not_promoted` behavior, `allow_supported_non_group_refinement` MAY accept a runtime scope of `business_segment`, `named_subsidiary`, or `issuer` when Gold preserves `unclear`, but only after metric, object, source value, period, and physical-anchor checks pass and the runtime scope is directly supported by that Evidence. The mode MUST NOT accept or promote `consolidated_group` and MUST NOT change the Gold expected subject.

#### Scenario: Product row supports a business segment
- **WHEN** Gold preserves `subject_scope=unclear` with `allow_supported_non_group_refinement` and the same anchored product row supports runtime `subject_scope=business_segment`
- **THEN** the evaluator may return `accepted_with_uncertainty`
- **AND** it does not rewrite either Gold or runtime subject scope

#### Scenario: Company wording is promoted to group scope
- **WHEN** runtime uses consolidated_group with report_default_group_scope under the current policy but historical Gold requires unclear
- **THEN** evaluation reports gold_contract_conflict unless a separately versioned Gold policy resolves it
- **AND** runtime is not forced to change to the historical subject; explicit narrower-scope promotion still fails
