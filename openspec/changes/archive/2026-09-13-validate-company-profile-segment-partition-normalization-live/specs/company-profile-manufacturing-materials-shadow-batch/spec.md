## ADDED Requirements

### Requirement: Segment partition normalization receives bounded live validation

The system MUST validate the archived segment-partition normalization against exactly one prior `reported_period` conflict scope and one prior incomplete report-segment-heading scope. The validation MUST bind the immutable source batch, source report files, prepared Evidence, current normalization implementation, Gemini route, output budgets, deadline, provider-call ceiling, and a new output identity before provider creation.

#### Scenario: Frozen validation inputs match

- **WHEN** both exact source scopes and all bound hashes, route, budgets, and output-absence conditions match the validation contract
- **THEN** the existing Stage 5 application service executes each prepared scope once and retains its extract, optional repair, verify, disposition, and Evidence lineage
- **AND** no historical batch or source report file is modified.

#### Scenario: A validation input drifts

- **WHEN** a source batch/report hash, selected scope, implementation hash, route, budget, or output identity differs from the contract
- **THEN** validation fails before the first semantic provider request
- **AND** no replacement scope or relaxed contract is inferred.

### Requirement: Live validation remains case-local and research-only

The result MUST classify each selected case as resolved, retained substantive failure, or execution failure from the actual source-bound output. The validation MUST NOT recompute the twenty-report readiness result, splice its candidates into a historical batch, authorize another run automatically, or grant production authorization.

#### Scenario: Both normalization cases resolve

- **WHEN** both scopes complete with accepted source-bound segment rows and without their targeted reconciliation failure
- **THEN** the result may recommend evaluating the cost and value of one separate full-cohort replay
- **AND** it MUST NOT claim scale readiness or change `production_authorization=not_authorized`.

#### Scenario: A case remains blocked or fails execution

- **WHEN** either scope retains a semantic conflict or ends in a transport, deadline, parse, schema, or provider failure
- **THEN** the exact diagnostic is preserved and the validation still closes under its sole identity
- **AND** it does not trigger parameter tuning, model substitution, or another provider run.
