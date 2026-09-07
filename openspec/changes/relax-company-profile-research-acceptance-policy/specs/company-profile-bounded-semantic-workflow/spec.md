## ADDED Requirements

### Requirement: Report usability requires complete core chapters and no frozen blocker
The bounded workflow MUST derive report status from the six core chapter tasks: business overview and Activity, segment facts or legal empty, operating quantities or legal empty, material/energy inputs or legal empty, counterparties/concentration or legal empty, and business regime event or legal empty. A required core chapter with `extraction_failed`, `deadline_exceeded`, missing coverage, or unresolved frozen §14 blocker MUST keep the report at `hold` or `failed`; an `unclear` subject alone MUST NOT fail the entire report when the field-level usage policy is satisfied.

#### Scenario: Required chapter is incomplete
- **WHEN** `extract_business_regime` ends with `deadline_exceeded` or another required-result failure
- **THEN** the report remains `hold` or `failed`
- **AND** it cannot be labeled `usable_with_caveats` until the chapter is rerun successfully

#### Scenario: Unclear subject does not block a qualitative result
- **WHEN** an Activity is accepted with complete Evidence and `subject_scope=unclear`
- **THEN** the workflow keeps the Activity available for research projection
- **AND** it does not create a report-level blocker solely for that subject scope

### Requirement: Confidence and usage restrictions are deterministic workflow outputs
The workflow MUST preserve the existing candidate, CoverageResult, disposition, and Evidence models as the authority for acceptance. It MUST derive confidence and usage restrictions programmatically after verification, without LLM-supplied probabilities, free-form usage lists, weighted scores, or automatic subject promotion.

#### Scenario: Accepted uncertain record is bounded
- **WHEN** a record is accepted for research with complete Evidence and `subject_scope=unclear`
- **THEN** the workflow derives a bounded confidence no higher than `medium`
- **AND** its permitted projection uses come from the closed policy table

#### Scenario: Unsupported promotion remains blocked
- **WHEN** a model response promotes source wording `公司` to `consolidated_group` without affirmative evidence
- **THEN** the existing independent verification blocks the candidate
- **AND** the new usability policy does not waive that blocker
