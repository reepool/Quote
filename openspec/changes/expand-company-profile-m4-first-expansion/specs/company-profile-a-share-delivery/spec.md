## ADDED Requirements

### Requirement: First expansion after 4.1 remains a reviewed change
Meeting the 4.1 numeric gates on the current two-report v4 sample MUST NOT be treated as scale-quality success or production authorization. A first expansion MUST be a separately reviewed change with a new a-priori live plan. The current sample MUST remain documented as 2 independently reviewed reports and 2 occupied strata. `scale_quality_claim_allowed` MUST remain false until a later explicit authorization.

#### Scenario: Current 4.1 result is not a production claim
- **WHEN** source review records recall 7/7, accuracy 7/7, critical numeric 0, 2 reports, and 2 occupied strata with `expansion_gates_met=true`
- **THEN** production authorization remains `not_authorized`
- **AND** scale-quality claims remain forbidden
- **AND** first expansion cannot start from this result alone without the reviewed M4 change
