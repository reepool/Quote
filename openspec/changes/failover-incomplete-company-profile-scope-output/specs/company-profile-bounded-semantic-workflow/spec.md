## ADDED Requirements

### Requirement: Active extract fields are complete at the provider boundary
Every company-profile extract response MUST represent each active non-optional checklist
field with either a candidate object for that field or a legal-empty coverage result
allowed by the checklist. Empty arrays and partial responses that omit an active
non-optional field MUST fail the model-side response schema as
`schema_validation_error`, so the existing logical pool MAY perform bounded failover.
Optional fields MAY remain absent. A candidate rejected later for Evidence, subject,
metric meaning, or prohibited inference remains a semantic rejection and MUST NOT cause
blind model cycling.

#### Scenario: Required legal-empty response is omitted
- **WHEN** a material-input or business-regime scope returns no candidate and no permitted coverage for its active non-optional field
- **THEN** the response fails schema validation before it is recorded as provider success
- **AND** the existing finite pool failover policy may select another eligible model

#### Scenario: One field in a multi-field scope is missing
- **WHEN** a response represents some active fields but omits another active non-optional field
- **THEN** the response fails the field-completeness constraint
- **AND** no candidate from that failed physical response is spliced into a later attempt

#### Scenario: Optional field is absent
- **WHEN** a response omits an active field whose checklist requirement level is `optional`
- **THEN** the response may remain schema-valid
- **AND** the workflow retains its existing optional coverage semantics

#### Scenario: Represented candidate is semantically rejected
- **WHEN** a response represents the required field but independent verification rejects the candidate for unsupported inference or Evidence mismatch
- **THEN** the semantic disposition is preserved
- **AND** the rejection does not trigger provider failover
