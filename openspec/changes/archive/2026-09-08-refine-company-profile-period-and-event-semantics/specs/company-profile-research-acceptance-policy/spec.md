## ADDED Requirements

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
