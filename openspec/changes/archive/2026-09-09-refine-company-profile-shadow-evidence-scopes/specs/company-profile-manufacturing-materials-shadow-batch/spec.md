## MODIFIED Requirements

### Requirement: Six-core-chapter Evidence plans are generated before semantic execution
For every admitted report, the system MUST use existing immutable PDF page artifacts, governed disclosure templates, and the existing section selector to generate plans for all six core chapter tasks. Each generated scope MUST carry only the existing Stage 5 fields positively supported by that scope's section keys, table signatures, headers, anchors, hints, or source text; it MUST NOT default to the complete chapter field set. The union of scopes in a chapter MUST retain every required chapter field, while conditional fields without a source signal MUST NOT be added merely to force per-scope coverage. Generated scopes MUST be report-local, continuous in physical-page coordinates, bounded to the existing request limits, and preserve available headings, owning table headers, units, footnotes, continuation pages, selector reasons, page hashes, PDF hash, and manifest identity. Plans MUST NOT contain Gold, expected values, subject decisions, semantic answers, or inferred facts. All plans and their provider-free preparation audit MUST be frozen before any semantic provider call.

#### Scenario: A selected scope supports only part of a chapter
- **WHEN** a governed selected scope contains source support for only a subset of its chapter's existing fields
- **THEN** the planner emits only that stable field subset for the scope
- **AND** it does not create `required_coverage_missing` exposure for unrelated chapter fields by copying the full chapter tuple

#### Scenario: A required chapter field has no owning scope
- **WHEN** no governed scope positively supports a required chapter field
- **THEN** planning emits a typed pre-provider chapter/report failure
- **AND** it does not silently omit the requirement or ask a model to choose the request contract

#### Scenario: A table continues across pages
- **WHEN** a selected disclosure contains a continuation marker or depends on an owning header, unit, or footnote from an adjacent selected page
- **THEN** the planner keeps the required context in the same continuous bounded scope
- **AND** it emits `table_context_incomplete` before provider execution when the complete context is unavailable or exceeds the scope bound

## ADDED Requirements

### Requirement: Scope refinement is measured by provider-free replay
The system MUST compare the refined plan with the immutable first shadow plan without modifying either input or invoking an LLM. The replay audit MUST report field/scope pairs, per-scope field counts, field-bound Evidence copies and serialized character volume, required-field ownership, unsupported field assignments, table-context outcomes, and Evidence traceability. The audit MUST retain `production_authorization=not_authorized` and MUST NOT claim semantic precision or report usability from provider-free measurements.

#### Scenario: Refined planning removes mechanical over-assignment
- **WHEN** the frozen twenty-report inputs are rebuilt with the refined planner
- **THEN** every emitted field has a positive source signal, every required field has an owning scope, and Evidence traceability remains 100%
- **AND** the audit records a material reduction in field-bound Evidence copies or serialized character volume relative to the v1 plan

#### Scenario: Replay input differs from the frozen baseline
- **WHEN** the manifest, PDF, page, section, or baseline-plan identity does not match the frozen input hashes
- **THEN** provider-free replay fails without writing a comparison result
- **AND** no provider is called
