## ADDED Requirements

### Requirement: First expansion after 4.1 remains a reviewed change
Meeting the 4.1 numeric gates on the current two-report v4 sample MUST NOT be treated as scale-quality success or production authorization. A first expansion MUST be a separately reviewed change that activates a mode on the existing published owner, with an immutable a-priori plan that binds `knowledge_cutoff`, registry identity, and official report versions before enqueue. The current two-company live-run and source-review v1 files MUST remain on disk as the retained baseline of 2 independently reviewed reports and 2 occupied strata. New observations MUST use independent snapshots. Ordinary `run` and `resume` MUST remain available when first-expansion mode is not `active`. `scale_quality_claim_allowed` MUST remain false until a later explicit authorization.

#### Scenario: Current 4.1 result is not a production claim
- **WHEN** source review records recall 7/7, accuracy 7/7, critical numeric 0, 2 reports, and 2 occupied strata with `expansion_gates_met=true`
- **THEN** production authorization remains `not_authorized`
- **AND** scale-quality claims remain forbidden
- **AND** first expansion cannot start from this result alone without the reviewed M4 change
- **AND** the existing two-company v1 report files remain readable after a later expansion snapshot is written
- **AND** an ordinary `run` without an expansion snapshot remains allowed while first-expansion mode is not `active`
