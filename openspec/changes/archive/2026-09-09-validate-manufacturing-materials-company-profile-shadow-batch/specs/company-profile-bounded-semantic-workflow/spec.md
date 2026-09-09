## ADDED Requirements

### Requirement: A hash-bound shadow manifest may admit reports without changing historical closed sets
The bounded workflow MUST support a distinct `manufacturing_materials_shadow_batch` manifest and Evidence-plan mode whose report identities are validated against the active versioned manifest, local PDF hashes, and manifest hash at the application boundary. This mode MUST require exactly twenty reports and MUST NOT add them to `APPROVED_STAGE5_SAMPLES`, `KNOWN_STAGE5_SAMPLES`, or any historical four-report/OOS constant. Existing four-report and OOS manifest kinds, report limits, run-bundle semantics, and success rules MUST remain unchanged.

#### Scenario: Historical Stage 5 mode receives a shadow report
- **WHEN** a four-report or OOS operator receives a report that is present only in a shadow manifest
- **THEN** the existing closed-set validation rejects it
- **AND** the shadow admission mode cannot be selected implicitly

#### Scenario: Shadow report identity matches the active manifest
- **WHEN** a shadow report, Evidence plan, prepared scopes, and report output all match the same active manifest revision, manifest hash, instrument identity, report period, PDF hash, and sample identity
- **THEN** the application service may run the existing bounded semantic workflow for that report
- **AND** the resulting payload remains research-only and is referenced by a separate shadow-batch result

### Requirement: Automatic Evidence planning reuses governed document owners
The bounded workflow MUST allow the shadow application path to translate existing immutable PDF artifacts and governed selected sections into the existing six chapter tasks and `PreparedRequestScope` contract. This translation MUST preserve physical page and source hashes and MUST validate the existing field contract before provider invocation. It MUST NOT introduce a new parser, selector, semantic field, verification owner, or answer-bearing plan format.

#### Scenario: Generated plan uses an unknown field
- **WHEN** automatic planning produces a field outside the existing Stage 5 field contract
- **THEN** preparation fails before provider invocation with a typed plan/field-contract diagnostic
- **AND** the planner does not add a new semantic field to accommodate the report

#### Scenario: Generated scopes are semantically executed
- **WHEN** a generated plan passes manifest, hash, page, Evidence, and field-contract validation
- **THEN** each scope is submitted to the existing `CompanyProfileSemanticService` owner
- **AND** verification, dispositions, projection restrictions, and report status use the same rules as the prior research slice
