## ADDED Requirements

### Requirement: Safely blocked supplemental candidates do not erase satisfied coverage
The workflow MUST derive observed coverage from accepted records even when the same field also contains an additional candidate that is safely blocked as `object_not_allowed`. This exception MUST apply only when the field has at least one accepted record and the supplemental candidate's only blocking reason is `object_not_allowed`. The blocked candidate MUST remain in dispositions and review material, MUST remain absent from the research projection, and MUST NOT be treated as accepted. Any other blocked, unresolved, conflicting, missing-verification, Evidence, subject, provider, or contract failure MUST continue to block task completion according to the existing contract.

#### Scenario: Invalid top-five aggregate relationship accompanies valid ranked facts
- **WHEN** a top-five scope has accepted anonymous ranked Relationships and concentration Measurements plus one blocked aggregate Relationship whose only reason is `object_not_allowed`
- **THEN** the accepted records satisfy observed counterparty coverage and the supplemental rejection does not make the task incomplete
- **AND** the aggregate Relationship remains blocked, reviewable, and absent from projection

#### Scenario: Accepted row accompanies a substantive verifier failure
- **WHEN** one field has an accepted record and another record is blocked for `subject_unsupported`, Evidence mismatch, provider failure, or another non-supplemental reason
- **THEN** the failing record remains a task-completion blocker under the existing contract
- **AND** the accepted record does not hide that failure

### Requirement: Explicit consolidation-adjustment rows carry their source-supported subject
The workflow MUST recognize a Segment or Measurement row as an explicit consolidation adjustment only when its source-native row label identifies consolidation and elimination or adjustment semantics and the candidate preserves `row_class=consolidation_adjustment`, `subject_scope=consolidated_group`, and `subject_basis=direct_source_wording`. Independent verification MUST NOT block such a candidate solely with `subject_unsupported`. This rule MUST NOT apply to ordinary product or segment rows, to source wording that only says `公司`, or to a candidate with any additional Evidence, value, unit, field, period, or prohibited-inference failure.

#### Scenario: Consolidation elimination row is explicit
- **WHEN** a source-native row label such as `合并抵消项` passes the adjustment-label validator and its Segment or Measurement carries the required row class and subject fields
- **THEN** verification may pass the source-supported consolidated subject
- **AND** the adjustment fact remains distinct from ordinary product or segment facts

#### Scenario: Ordinary company wording is not promoted
- **WHEN** an ordinary row or narrative uses only `公司` and lacks affirmative consolidated Evidence
- **THEN** verification continues to block or retain `subject_scope=unclear`
- **AND** the consolidation-adjustment rule cannot promote it to `consolidated_group`

#### Scenario: Adjustment row has another verification failure
- **WHEN** an explicit adjustment candidate also has an Evidence, value, unit, field, period, or prohibited-inference reason
- **THEN** that additional reason remains blocking
- **AND** subject normalization cannot convert the check to pass
