# company-profile-research-acceptance-policy Specification

## Purpose
TBD - created by archiving change relax-company-profile-research-acceptance-policy. Update Purpose after archive.
## Requirements
### Requirement: Research acceptance preserves evidence while allowing bounded uncertainty
The research acceptance policy MUST allow a source-backed fact with `subject_scope=unclear` to remain available for research projection when Evidence, value, unit, period, metric meaning, and independent verification are complete. It MUST NOT promote that fact to `consolidated_group` without the existing affirmative source wording or documented numeric reconciliation.

#### Scenario: Company wording supports a qualitative activity
- **WHEN** Evidence says `公司主要从事动力电池研发、生产、销售` and the Activity is independently verified
- **THEN** the Activity may be accepted for research with `subject_scope=unclear`
- **AND** no report-level failure is created solely because the subject is unclear

#### Scenario: Unsupported group promotion is attempted
- **WHEN** a candidate changes source wording `公司` to `consolidated_group` without affirmative evidence
- **THEN** verification blocks the candidate
- **AND** the report retains a subject blocker regardless of other accepted facts

### Requirement: Downstream usage is governed by a closed subject policy
The policy MUST provide a program-owned closed table mapping object or metric family and `subject_scope` to permitted research uses. The LLM MUST NOT provide free-form `allowed_use` or `blocked_use` values. `unclear` revenue, cost, margin, operating quantities, processing volume, material/energy input, and counterparty concentration MAY remain source facts, but MUST NOT enter consolidated aggregation, ranking, or cross-company comparison templates unless the existing subject basis supports that use. `business_segment`, `named_subsidiary`, and explicit `issuer` remain distinct scopes.

#### Scenario: Unclear activity is projected
- **WHEN** an accepted Activity has `subject_scope=unclear`
- **THEN** the research projection may display the Activity and its Evidence
- **AND** the projection does not expose it as a consolidated financial measure

#### Scenario: Unclear margin is requested for a group comparison
- **WHEN** an accepted gross-margin Measurement has `subject_scope=unclear`
- **THEN** the source-native Measurement remains stored in the research bundle
- **AND** the consolidated comparison projection omits it with an explicit usage restriction

#### Scenario: Business segment remains a segment fact
- **WHEN** a product row is accepted with `subject_scope=business_segment`
- **THEN** the projection may display the segment revenue and margin as segment facts
- **AND** it MUST NOT treat the row as a group total without explicit or reconciled group basis

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
- **AND** a runtime promotion to unsupported `consolidated_group` returns `failed`

#### Scenario: Frozen contract conflicts with Gold
- **WHEN** Gold expects `not_applicable` but the frozen industry contract requires a source-supported `not_disclosed` result for the same field
- **THEN** the evaluator returns `gold_contract_conflict`
- **AND** it does not mark the runtime result as passed through fuzzy matching

### Requirement: Negative cases distinguish fixture guards from real-report coverage
The benchmark MUST maintain separate results for local fixture guards and real annual-report triggers. All approved fixture guards MUST pass their blocking behavior. For real reports, an item with `evaluated=true` MUST pass, while `evaluated=false` MUST remain explicitly unevaluated and MUST NOT be counted as either pass or failure.

#### Scenario: Missing-page guard is tested by fixture
- **WHEN** a controlled fixture omits a required page
- **THEN** the preparation guard returns `extraction_failed` before provider invocation
- **AND** the fixture guard is marked passed without altering any real report bundle

#### Scenario: Real reports do not trigger a negative case
- **WHEN** none of the four annual reports contains material for a frozen negative case
- **THEN** the real-report benchmark records `evaluated=false`
- **AND** that item does not block research slice usability

### Requirement: Research usability is separate from production authorization
The policy MUST define report states `usable`, `usable_with_caveats`, `hold`, and `failed`, plus an overall `research_slice_usable` state. A report MUST NOT be `usable` or `usable_with_caveats` when a required core chapter is incomplete, an execution failure remains, or a frozen §14 blocker exists. Research usability MUST retain `accepted_for_review` and `production_authorization=not_authorized`.

#### Scenario: Core regime chapter times out
- **WHEN** a required `extract_business_regime` scope ends with `deadline_exceeded`
- **THEN** the report remains `hold` or `failed` according to the execution contract
- **AND** it cannot be downgraded to a permanent caveat

#### Scenario: Core chapters complete with subject caveats
- **WHEN** all six core chapters are complete and only allowed `unclear` subject restrictions remain
- **THEN** the report may be `usable_with_caveats`
- **AND** the overall slice may be `research_slice_usable` if all other blocking dimensions pass

### Requirement: Confidence is a deterministic four-level projection attribute
Confidence MUST be derived programmatically from existing Evidence completeness, verification/disposition, subject basis, and contradiction state as `high`, `medium`, `low`, or `rejected`. The LLM MUST NOT self-report a probability or confidence value, and the policy MUST NOT introduce weighted scoring.

#### Scenario: Directly evidenced segment fact
- **WHEN** a segment fact has complete Evidence, independent verify pass, and a valid segment subject basis
- **THEN** the projection may derive `high`

#### Scenario: Accepted fact with unclear subject
- **WHEN** Evidence and verify are complete but `subject_scope=unclear`
- **THEN** the projection derives at most `medium`
- **AND** the fact remains subject to the closed usage policy

#### Scenario: Unresolved candidate
- **WHEN** a candidate remains unresolved or contradictory after the bounded workflow
- **THEN** the projection derives `low` or `rejected`
- **AND** it is not exposed as an accepted research fact

### Requirement: Gold subject refinement is closed and Evidence bounded
Gold subject evaluation MUST use a closed `subject_strictness` policy. In addition to exact subject matching and the existing `allow_unclear_if_not_promoted` behavior, `allow_supported_non_group_refinement` MAY accept a runtime scope of `business_segment`, `named_subsidiary`, or `issuer` when Gold preserves `unclear`, but only after metric, object, source value, period, and physical-anchor checks pass and the runtime scope is directly supported by that Evidence. The mode MUST NOT accept or promote `consolidated_group` and MUST NOT change the Gold expected subject.

#### Scenario: Product row supports a business segment
- **WHEN** Gold preserves `subject_scope=unclear` with `allow_supported_non_group_refinement` and the same anchored product row supports runtime `subject_scope=business_segment`
- **THEN** the evaluator may return `accepted_with_uncertainty`
- **AND** it does not rewrite either Gold or runtime subject scope

#### Scenario: Company wording is promoted to group scope
- **WHEN** runtime claims `consolidated_group` from wording that only says `公司`
- **THEN** subject evaluation returns `failed`
- **AND** no subject-strictness mode waives the missing affirmative group basis

### Requirement: Gold event equivalence requires an explicit directional decision
Gold event evaluation MUST require exact event types unless a program-owned directional equivalence is explicitly listed with its Evidence and date conditions. An allowed pair MUST still match the same sample, physical anchor, occurrence or effective date, and governed regime boundary. Unlisted event pairs, different physical anchors, commitments, project launches, and nearby events MUST remain distinct. The evaluator MUST report the applied equivalence reason and MUST NOT modify runtime or Gold event labels.

#### Scenario: Equity transfer is the evidenced restructuring-effective milestone
- **WHEN** Gold expects `major_asset_restructuring_effective`, runtime records `equity_transfer`, both use the same Chengfei transfer-completion anchor, and both effective dates are `2025-01-06`
- **THEN** the evaluator may return `semantic_match` under the explicit directional decision
- **AND** it retains both original event labels in the audit result

#### Scenario: Project launch is not an effective milestone
- **WHEN** a 2023 acquisition-project launch or commitment is compared with the 2025 restructuring-effective Gold event
- **THEN** the evaluator returns `failed`
- **AND** event-family similarity does not override the different anchor or effective time
