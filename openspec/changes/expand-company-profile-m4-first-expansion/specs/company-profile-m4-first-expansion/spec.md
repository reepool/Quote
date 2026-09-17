## ADDED Requirements

### Requirement: First expansion records a new plan before new observations
The first M4 expansion MUST record a new `company_profile_live_plan.v1` before any new live observation. The plan MUST occupy at least one disclosure-form stratum that the current independently reviewed sample does not occupy, and that stratum MUST be `service`. `max_companies_this_round` MUST equal the number of reports the new plan will independently review. Sampling MUST use the existing stratified rule and MUST NOT hard-code an instrument id. Missing official assets MUST remain in the universe denominator. `scale_quality_claim_allowed` MUST remain false. Production, DCF, trading, and the legacy writer MUST remain unauthorized.

#### Scenario: Plan is recorded before a new live run
- **WHEN** the first M4 expansion is authorized to execute after scope review
- **THEN** a new live plan exists with `plan_status=recorded_before_live` before the new sample is observed
- **AND** the plan's review sample includes a `service` disclosure-form stratum
- **AND** `scale_quality_claim_allowed` is false
- **AND** production authorization remains `not_authorized`

### Requirement: New-sample source review is recounted independently
After the new plan runs, source review MUST be recorded from independently read official pages. Recall, accuracy, critical numeric errors, reviewed-report count, and occupied strata MUST be derived from that review. The runtime MUST NOT presume the previous 7/7 result. Existing v1–v4 completed work MUST remain on disk. Query MUST continue to return the current published identity unless a later reviewed change publishes a successor.

#### Scenario: Review follows the new plan rather than the v4 score
- **WHEN** the new live sample has been delivered
- **THEN** source review covers exactly the new plan's selected instrument ids
- **AND** expansion gates are recalculated from those findings
- **AND** predecessor v4 JSON files remain readable

### Requirement: Operator closure retires the stale first-expansion gate
`company_profile_operator_closure.v1` MUST stop cataloguing `first_expansion_gates_unmet`. It MUST instead record that the 2-report v4 sample met the 4.1 numeric gates, and that first expansion still requires this reviewed change plus a new a-priori plan. The remaining backlog items MUST stay: manufacturing/materials is not production, non-manufacturing industry packages are absent, unassessed semantics are not a pass, missing assets stay in the denominator, and Gold24/fixture guards are not live quality. Every backlog item MUST keep `execute_this_round=false` and `production_authorized=false`.

#### Scenario: Stale unmet-gate item is replaced
- **WHEN** operator closure is recorded after this change is applied
- **THEN** the backlog no longer contains `first_expansion_gates_unmet`
- **AND** the replacement item states that 4.1 passed on the current sample and first expansion is not production
- **AND** the other five backlog item ids remain

### Requirement: This slice does not implement industry packages
The first M4 expansion MUST NOT implement a manufacturing/materials production enhancement, MUST NOT build banking, service, or TMT industry packages, and MUST NOT project 净息差, 成本收入比, or loan-structure rows. A reusable interpretability gap found on the new sample MUST become a separate change with its own processing identity. This change MUST NOT publish `owned_page_facts=v5` unless that later change is separately reviewed.

#### Scenario: New sample discloses a reusable gap
- **WHEN** independent source review of the new sample finds an important disclosure that v4 cannot interpret
- **THEN** the finding is recorded as not delivered or inaccurate as the facts require
- **AND** this change does not add a new extractor or a new published identity
- **AND** a later change is required before any successor identity is published
