## MODIFIED Requirements

### Requirement: Real Evidence is authoritative and Gold is evaluation-only
Every stage-five candidate, CoverageResult, subject basis, Activity actor, source actor, source verb, period, unit, and physical anchor MUST be reconstructed from the report Evidence supplied to that request. Gold annotations and the stage-four Gold adapter MUST NOT populate or default runtime semantic fields. Gold and negative cases MAY be read only after a run to calculate benchmark results. Missing or ambiguous Evidence MUST produce the existing `unclear` or `extraction_failed` result rather than a Gold-derived or industry-custom default.

#### Scenario: Source says company without affirmative group evidence
- **WHEN** company-owned report Evidence has no explicit narrower scope and no unresolved conflict
- **THEN** new runtime candidates use consolidated_group with report_default_group_scope
- **AND** Gold cannot replace that basis with direct_source_wording or force an old unclear expectation

#### Scenario: Activity actor is not explicit
- **WHEN** the source grammar or economic relationship does not identify the issuer as the action actor
- **THEN** the Activity remains unresolved or uses the source-supported actor
- **AND** the slice does not default `activity_actor`, `source_actor`, or `source_verb` from a fixture adapter
