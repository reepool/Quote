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

### Requirement: Gold equivalence uses a finite auditable matcher
Gold evaluation MUST distinguish `exact_match`, `semantic_match`, `accepted_with_uncertainty`, `failed`, `not_applicable`, and `gold_contract_conflict`. The evaluator MUST normalize only approved numeric formatting, apply only a closed unit-equivalence table, and align candidates by sample, metric, measured object, and physical anchor. It MUST preserve source-native values and MUST NOT modify Gold expectations or hide a contract conflict as a semantic match.

#### Scenario: Numeric punctuation differs
- **WHEN** Gold expects `316506369` and runtime preserves source-native `316,506,369`
- **THEN** the matcher returns `semantic_match`
- **AND** the source-native runtime value remains unchanged

#### Scenario: A closed unit conversion is allowed
- **WHEN** Gold and runtime use `40 kt/a` and `40,000 吨/年` for the same object and physical anchor
- **THEN** the matcher may return `semantic_match` with an auditable conversion record
- **AND** it MUST NOT merge distinct physical anchors or different `capacity_kind` values

#### Scenario: Subject uncertainty is explicitly allowed
- **WHEN** a Gold annotation has `subject_strictness=allow_unclear_if_not_promoted` and runtime preserves `subject_scope=unclear`
- **THEN** the matcher returns `accepted_with_uncertainty`
- **AND** unsupported promotion means violating current Evidence rules, not a valid consolidated_group / report_default_group_scope convention; a valid default conflicting only with historical Gold is gold_contract_conflict after non-subject fact checks pass

#### Scenario: Frozen contract conflicts with Gold
- **WHEN** Gold expects `not_applicable` but the frozen industry contract requires a source-supported `not_disclosed` result for the same field
- **THEN** the evaluator returns `gold_contract_conflict`
- **AND** it does not mark the runtime result as passed through fuzzy matching

## ADDED Requirements

### Requirement: Gold validation distinguishes factual errors from policy conflicts in order
The evaluator MUST first validate Evidence, object, metric, value/unit, period, physical anchor and actual subject-policy legality. A factual error, contradictory subject Evidence, unsupported narrower scope or override of explicit parent/subsidiary/segment scope MUST remain failed. Only when those checks pass and a valid report_default_group_scope differs from the historical Gold subject policy MUST it return gold_contract_conflict. This result MUST NOT count as a match or trigger rewriting runtime or historical Gold. The remaining subject_strictness rules MUST apply only after this distinction. A valid report default is not an unsupported promotion, including under allow_unclear_if_not_promoted. The narrow explicit consolidation-adjustment rule MUST NOT be borrowed by ordinary company wording to claim direct_source_wording.

#### Scenario: Valid default differs from old unclear Gold
- **WHEN** all fact checks pass and runtime has consolidated_group with a legal report_default_group_scope while historical Gold expects unclear
- **THEN** the result is gold_contract_conflict rather than failed or a matching pass.

#### Scenario: Wrong value also has a different Gold subject
- **WHEN** runtime uses a legal default scope but its value does not match the source fact
- **THEN** the result remains failed and the policy conflict cannot hide the error.

#### Scenario: Default overrides an explicit subsidiary
- **WHEN** runtime widens an explicitly subsidiary-scoped fact to consolidated_group
- **THEN** the result is failed even if its basis is labelled report_default_group_scope.
