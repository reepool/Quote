## MODIFIED Requirements

### Requirement: Automatic Evidence planning reuses governed document owners
The bounded workflow MUST allow the shadow application path to translate existing immutable PDF artifacts and governed selected sections into the existing six chapter tasks and `PreparedRequestScope` contract. This translation MUST preserve physical page and source hashes, MUST derive each scope's field subset from positive source signals inside that scope, MUST retain required owning table context, and MUST validate the existing field contract and report-level required-field ownership before provider invocation. It MUST NOT introduce a new parser, selector, semantic field, verification owner, answer-bearing plan format, or model-selected request contract.

#### Scenario: Generated plan uses an unknown or unsupported field
- **WHEN** automatic planning produces a field outside the existing Stage 5 field contract or assigns a known field without a positive source signal in that scope
- **THEN** preparation or replay validation fails before provider invocation with a typed plan/field-contract diagnostic
- **AND** the planner does not add a new semantic field or rely on model output to accommodate the report

#### Scenario: Generated scopes are semantically executable
- **WHEN** a generated plan passes manifest, hash, page, Evidence, field-signal, table-context, required-field-ownership, and field-contract validation
- **THEN** each scope remains eligible for submission to the existing `CompanyProfileSemanticService` owner
- **AND** verification, dispositions, projection restrictions, and report status continue to use the same rules as the prior research slice

#### Scenario: A chapter is split across multiple scopes
- **WHEN** different governed scopes in one report chapter support different existing fields
- **THEN** the workflow evaluates each scope only against its emitted field subset and aggregates required chapter coverage across the report
- **AND** genuine scope-local schema, verification, Evidence, or prohibited-inference failures remain blockers
