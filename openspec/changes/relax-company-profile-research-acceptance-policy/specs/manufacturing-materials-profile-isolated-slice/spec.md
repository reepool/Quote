## MODIFIED Requirements

### Requirement: Four-report acceptance uses blocking dimensions and researcher-readable output
The slice MUST generate one researcher-readable company profile per report and a benchmark report covering required task coverage, exact source value/unit/header, metric and logical slot, subject and period, physical Evidence anchor, legal-empty classification, repair boundedness, verify independence, regime boundary, and prohibited inference. A frozen blocking failure in any report MUST make that report `hold` or `failed` regardless of average score. An `unclear` subject alone MUST NOT make the report fail when the field-level usage policy is satisfied. A report MAY be `usable` or `usable_with_caveats` only when the six core chapter tasks are complete, no frozen §14 blocker remains, and any caveats are explicit. The overall result MAY be recorded as `research_slice_usable` only when all four reports meet those conditions and all actually evaluated real-report negative cases pass; fixture guards are evaluated separately and real-report negative cases with no trigger remain `evaluated=false`. Passing MUST NOT authorize production or legacy reset.

#### Scenario: Three reports are usable and one has an execution failure
- **WHEN** three report bundles satisfy the usability dimensions but one required core chapter ends with `deadline_exceeded`
- **THEN** the failed report remains `hold` or `failed`
- **AND** the overall slice cannot be `research_slice_usable`

#### Scenario: Unclear subject is bounded rather than fatal
- **WHEN** a report has complete core chapters and accepted source facts whose subject is `unclear`, with no frozen semantic blocker
- **THEN** the report may be `usable_with_caveats`
- **AND** consolidated-sensitive uses are withheld by the closed projection policy

#### Scenario: Real negative case is not triggered
- **WHEN** a frozen negative case has no trigger in the four annual reports
- **THEN** the real-report benchmark records `evaluated=false`
- **AND** the absence of a trigger does not block `research_slice_usable`

#### Scenario: All four reports satisfy the revised policy
- **WHEN** all four reports have complete core chapters, no frozen blocker, valid bounded usage restrictions, and every evaluated real-report negative case passes
- **THEN** the slice may be recorded as `research_slice_usable`
- **AND** all records remain `accepted_for_review` with `production_authorization=not_authorized`

## ADDED Requirements

### Requirement: Fixture guards cover negative cases absent from the annual-report sample
The slice test suite MUST maintain local fixture guards for approved negative cases that may not occur in the four-report sample, including inventory amount versus quantity confusion, omitted required pages, unreadable required pages, and ambiguous units. Fixture guards MUST execute the same blocking validator or preparation contract as runtime, but MUST NOT be merged into a real annual-report run bundle or presented as observed company facts.

#### Scenario: Fixture guard blocks an inventory amount used as volume
- **WHEN** a local fixture binds a currency inventory amount to an inventory-volume field
- **THEN** the guard fails the candidate with the existing field or unit mismatch reason
- **AND** no real-report benchmark result is changed

#### Scenario: Fixture guards remain separate from research output
- **WHEN** all four local fixture guards pass
- **THEN** the guard suite reports coverage independently of the annual-report benchmark
- **AND** no fixture candidate is written to an isolated company profile bundle
