## ADDED Requirements

### Requirement: First-expansion plan is persisted immutably before enqueue
The first M4 expansion MUST persist an immutable `company_profile_first_expansion_plan.v1` snapshot before any enqueue or drain. That snapshot MUST include a stable `plan_id` or content hash, the live-plan rules, `selected_instrument_ids`, and `selected_strata`. The frozen strata MUST include `service`. `max_companies_this_round` MUST equal the number of preselected reports. Sampling MUST use the existing stratified rule and MUST NOT hard-code an instrument id. The snapshot MUST NOT change after the first successful persist. Enqueue and drain MUST refuse to start when the snapshot is missing, mutable, or does not contain `service`. Missing official assets MUST remain in the universe denominator. `scale_quality_claim_allowed` MUST remain false. Production, DCF, trading, and the legacy writer MUST remain unauthorized.

#### Scenario: Plan and sample exist before enqueue
- **WHEN** the first M4 expansion is authorized to execute after scope review
- **THEN** an immutable first-expansion plan snapshot exists before enqueue or drain
- **AND** the snapshot binds instrument IDs and strata
- **AND** the bound strata include `service`
- **AND** `scale_quality_claim_allowed` is false
- **AND** production authorization remains `not_authorized`

#### Scenario: Enqueue without a frozen service sample is refused
- **WHEN** no persisted first-expansion plan snapshot exists, or its strata do not include `service`
- **THEN** enqueue and drain do not start a new observation
- **AND** the two-company baseline reports remain unchanged

### Requirement: New observations keep the two-company baseline
A new live-run report and a new source-review report MUST each persist as an independent snapshot and MUST reference the same first-expansion `plan_id` or content hash. They MUST NOT overwrite `company_profile_live_run.v1.json` or `company_profile_source_review.v1.json`. Those existing files MUST remain the retained 2-report, 2-stratum, 7/7 baseline. After the new review is recorded, that new source-review snapshot is this-round authority; the baseline files MUST stay readable.

#### Scenario: New review writes a separate snapshot
- **WHEN** the frozen first-expansion sample has been delivered and independently reviewed
- **THEN** live-run and source-review snapshots both carry the same plan id or content hash
- **AND** the existing two-company v1 live-run and source-review files still contain the 7/7 baseline
- **AND** the new source-review snapshot is this-round authority

### Requirement: New-sample source review is recounted independently
After the frozen plan runs, source review MUST be recorded from independently read official pages. Recall, accuracy, critical numeric errors, reviewed-report count, and occupied strata MUST be derived from that review and MUST cover exactly the snapshot's selected instrument ids. The runtime MUST NOT presume the previous 7/7 result. Existing v1–v4 completed work MUST remain on disk. Query MUST continue to return the current published identity unless a later reviewed change publishes a successor.

#### Scenario: Review follows the frozen plan rather than the v4 score
- **WHEN** the new live sample has been delivered
- **THEN** source review covers exactly the persisted plan's selected instrument ids
- **AND** expansion gates are recalculated from those findings
- **AND** predecessor v4 JSON files remain readable

### Requirement: Operator closure publishes v2 and keeps v1 readable
The change MUST publish `company_profile_operator_closure.v2` as a new snapshot and MUST NOT change the unique legal backlog of `company_profile_operator_closure.v1`. Existing v1 JSON MUST remain readable against the original catalog that includes `first_expansion_gates_unmet`. The runtime MUST NOT require historical v1 reports to match the v2 catalog. The v2 catalog MUST replace `first_expansion_gates_unmet` and MUST keep the other five backlog item ids: manufacturing/materials is not production, non-manufacturing industry packages are absent, unassessed semantics are not a pass, missing assets stay in the denominator, and Gold24/fixture guards are not live quality. Every backlog item MUST keep `execute_this_round=false` and `production_authorized=false`.

#### Scenario: v1 report remains readable after v2 is published
- **WHEN** a historical `company_profile_operator_closure.v1` report contains `first_expansion_gates_unmet`
- **THEN** that file still loads
- **AND** the current snapshot is `company_profile_operator_closure.v2`
- **AND** the v1 file is not overwritten

### Requirement: v2 replacement backlog uses post-execution wording
`company_profile_operator_closure.v2` MUST be written only after the frozen plan has been executed and the new source-review snapshot exists. The replacement backlog item MUST state that first expansion has already been executed under the reviewed plan, that the new source-review snapshot is this-round authority, and that this change does not authorize the next expansion, a scale-quality claim, or production. The item MUST NOT say that first expansion still requires this change or a new plan.

#### Scenario: Closure text is true at write time
- **WHEN** operator closure v2 is recorded after the new source-review snapshot exists
- **THEN** the backlog no longer contains `first_expansion_gates_unmet`
- **AND** the replacement item states that first expansion has been executed and is not a license for the next round, scale quality, or production
- **AND** the other five backlog item ids remain

### Requirement: This slice does not implement industry packages
The first M4 expansion MUST NOT implement a manufacturing/materials production enhancement, MUST NOT build banking, service, or TMT industry packages, and MUST NOT project 净息差, 成本收入比, or loan-structure rows. A reusable interpretability gap found on the new sample MUST become a separate change with its own processing identity. This change MUST NOT publish `owned_page_facts=v5` unless that later change is separately reviewed.

#### Scenario: New sample discloses a reusable gap
- **WHEN** independent source review of the new sample finds an important disclosure that v4 cannot interpret
- **THEN** the finding is recorded as not delivered or inaccurate as the facts require
- **AND** this change does not add a new extractor or a new published identity
- **AND** a later change is required before any successor identity is published
