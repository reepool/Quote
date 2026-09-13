## ADDED Requirements

### Requirement: Segment partition identity preserves source period semantics
The Stage 5 provider MUST normalize a compact segment row before reconciliation and MUST use
its source dimension, source label, normalized `reported_period`, and `period_type` as the
partition row identity. Closed annual aliases for the same annual period MAY reconcile, but
different years, interim periods, instant dates, row labels, or dimensions MUST remain
separate. Duplicate metric cells or conflicting metadata inside one normalized identity MUST
remain typed blocking failures.

#### Scenario: Current and comparative rows share one label
- **WHEN** segment partitions contain the same dimension and label for 2025 and 2024
- **THEN** the workflow retains two independent source rows and merges each row only with
  metric partitions for its own period
- **AND** neither year is rejected merely because its label matches the other year.

#### Scenario: Closed annual aliases describe one row
- **WHEN** partitions describe the same duration row as `2025`, `2025年`, `2025年度`, or the
  report-year annual end date already supported by the period policy
- **THEN** those aliases reconcile to one annual row identity
- **AND** an interim period or instant date is not treated as that annual row.

### Requirement: Accepted field semantics close coverage without hiding defects
For a requested checklist field, the Stage 5 application service MUST derive `observed`
coverage from accepted records for that field and MUST accept legal-empty coverage only from
Evidence that owns the exact field or contains its governed explicit not-applicable wording.
A rejected supplemental representation MUST NOT erase already satisfied coverage when it adds
no independent Evidence, subject, value, unit, period, provider, or contract defect. The
rejected item MUST remain visible and excluded from projection. Substantive failures and
missing required fields MUST remain blockers.

#### Scenario: Accepted facts and a redundant invalid representation coexist
- **WHEN** a scope contains one or more accepted records satisfying the requested field and a
  redundant candidate is blocked only because its object representation is not allowed
- **THEN** the accepted records close observed coverage and the redundant candidate remains
  blocked and reviewable
- **AND** task completion is not turned into a false negative by that redundant item.

#### Scenario: Owner-valid legal empty is returned
- **WHEN** a field-owning disclosure explicitly states that a requested item is not disclosed
  or not applicable under the existing field policy
- **THEN** the legal-empty result may close that field with its exact Evidence
- **AND** no fact or reason is synthesized beyond the source disclosure.

#### Scenario: An independent defect remains
- **WHEN** a candidate or coverage result has an Evidence mismatch, unsupported subject,
  incorrect value/unit/period, provider failure, or another substantive contract error
- **THEN** that failure remains visible and task-blocking under the existing policy
- **AND** accepted records in the same scope do not hide it.

## MODIFIED Requirements

### Requirement: Explicit consolidation-adjustment rows carry their source-supported subject
The workflow MUST recognize a Segment or Measurement row as an explicit consolidation
adjustment when its source-native row label identifies consolidation/elimination semantics or
is the narrow source label `分部间抵销`/`分部间抵消`, and the candidate preserves
`row_class=consolidation_adjustment`. For the narrow label, the adapter MAY normalize only the
adjustment subject to `consolidated_group` with `direct_source_wording` so independent
verification does not block it solely with `subject_unsupported` or
`prohibited_inference`. This rule MUST NOT apply to ordinary product or segment rows, to
source wording that only says `公司`, or to a candidate with any additional Evidence, value,
unit, field, period, duplicate, or prohibited-inference failure.

#### Scenario: Inter-segment elimination row is explicit
- **WHEN** a Segment or Measurement preserves source label `分部间抵销` or `分部间抵消` and
  `row_class=consolidation_adjustment`
- **THEN** the adapter treats it as the explicit adjustment row and verification does not
  reject it solely for lacking a separate group word
- **AND** the adjustment remains distinct from ordinary product or segment facts.

#### Scenario: Ordinary company wording is not promoted
- **WHEN** an ordinary row or narrative uses only `公司` and lacks an explicit adjustment
  label or other affirmative consolidated Evidence
- **THEN** the existing subject policy continues to apply
- **AND** the consolidation-adjustment rule cannot promote it to `consolidated_group`.

#### Scenario: Adjustment row has another verification failure
- **WHEN** an explicit adjustment candidate also has an Evidence, value, unit, field, period,
  duplicate-cell, or unrelated prohibited-inference reason
- **THEN** that additional reason remains blocking
- **AND** adjustment subject normalization cannot convert the overall check to pass.
