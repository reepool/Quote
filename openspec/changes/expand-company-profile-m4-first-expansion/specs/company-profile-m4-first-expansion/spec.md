## ADDED Requirements

### Requirement: First expansion is a mode on the published owner
First expansion MUST run on the existing `company_profile_common_core` owner and the existing published actions. The change MUST NOT add a published action or a second execution chain. The owner MUST persist `first_expansion_mode` as `inactive`, `active`, or `completed`. The default MUST be `inactive`. Ordinary `run` and `resume` MUST keep their current enqueue-or-continue behavior while the mode is `inactive` or `completed`. The owner MUST activate the mode only by persisting the immutable first-expansion plan and setting the mode to `active`. `resume` while `active` MUST continue only the frozen work of that plan. Writing `company_profile_operator_closure.v2` after the new source-review snapshot exists MUST set the mode to `completed`. After `completed`, the owner MUST restore ordinary `run`/`resume`, MUST NOT automatically start another expansion, and MUST NOT rerun the frozen sample. A later reviewed change is required before another first-expansion plan may be recorded.

#### Scenario: Ordinary run does not require an expansion snapshot
- **WHEN** `first_expansion_mode` is `inactive` or `completed` and the operator calls the published `run` or `resume`
- **THEN** the owner does not refuse for a missing first-expansion plan
- **AND** it does not limit work to a previously frozen sample
- **AND** no new published action is used

#### Scenario: Active resume stays on frozen work
- **WHEN** `first_expansion_mode` is `active` and the operator calls `resume`
- **THEN** drain continues only work that matches the frozen plan report references
- **AND** the owner does not enqueue a different instrument or report version

#### Scenario: Completed mode does not rerun the frozen sample
- **WHEN** `first_expansion_mode` is `completed` and the operator calls `run`
- **THEN** the owner does not enqueue the frozen first-expansion sample as a new observation
- **AND** it does not activate another first-expansion plan

### Requirement: First-expansion refusal applies only in active mode
While `first_expansion_mode` is `active`, enqueue and drain MUST refuse when the immutable plan snapshot is missing, mutable, or does not match the frozen `knowledge_cutoff`, registry identity, strata, or report references. The frozen strata MUST include `service`. The owner MUST NOT refuse ordinary `inactive` or `completed` runs for those reasons. A repeated `run` or `resume` while `active` after the frozen work is already delivered MUST return the current state without a new observation. Missing official assets MUST remain in the universe denominator. `scale_quality_claim_allowed` MUST remain false. Production, DCF, trading, and the legacy writer MUST remain unauthorized.

#### Scenario: Active enqueue without a matching snapshot is refused
- **WHEN** `first_expansion_mode` is `active` and no matching first-expansion plan snapshot exists
- **THEN** enqueue and drain do not start a new observation
- **AND** the two-company baseline reports remain unchanged
- **AND** an `inactive` ordinary run is still allowed

#### Scenario: Active rerun after delivery is idempotent
- **WHEN** `first_expansion_mode` is `active` and the frozen sample is already delivered
- **THEN** a later `run` or `resume` does not enqueue or observe the sample again
- **AND** the existing first-expansion snapshots remain the this-round results until closure is written

### Requirement: First-expansion plan freezes report versions before enqueue
The first M4 expansion MUST persist an immutable `company_profile_first_expansion_plan.v1` snapshot before any `active`-mode enqueue or drain. That snapshot MUST include a stable `plan_id` or content hash, the live-plan rules, `knowledge_cutoff`, the candidate-registry identity used to draw the sample (`schema_version`, `as_of`, and `universe_snapshot_id` when the registry has one), `selected_instrument_ids`, `selected_strata`, and one official annual-report reference per selected instrument. Each report reference MUST use the existing fields `asset_id`, `report_id`, `report_period`, and `document_version`. `max_companies_this_round` MUST equal the number of frozen reports. Sampling MUST use the existing stratified rule and MUST NOT hard-code an instrument id. The snapshot MUST NOT change after the first successful persist. Enqueue and resume in `active` mode MUST compare the current effective annual report to those frozen references and MUST refuse on drift; they MUST NOT silently use a corrected filing, a different document version, or a different `knowledge_cutoff`.

#### Scenario: Plan and report versions exist before active enqueue
- **WHEN** first-expansion mode is activated after scope review
- **THEN** an immutable plan snapshot exists before enqueue or drain
- **AND** the snapshot binds `knowledge_cutoff`, registry identity, instrument IDs, strata, and per-report `asset_id`, `report_id`, `report_period`, and `document_version`
- **AND** the bound strata include `service`
- **AND** `scale_quality_claim_allowed` is false
- **AND** production authorization remains `not_authorized`

#### Scenario: Corrected annual report is refused
- **WHEN** `first_expansion_mode` is `active` and the current effective annual report for a frozen instrument has a different `asset_id`, `report_id`, `report_period`, or `document_version`
- **THEN** enqueue and resume refuse that instrument
- **AND** the owner does not process the new version as the planned observation

### Requirement: New observations keep the two-company baseline
A new live-run report and a new source-review report MUST each persist as an independent snapshot. Both MUST carry the same first-expansion `plan_id` or content hash and the same frozen report references. They MUST NOT overwrite `company_profile_live_run.v1.json` or `company_profile_source_review.v1.json`. Those existing files MUST remain the retained 2-report, 2-stratum, 7/7 baseline. After the new review is recorded, that new source-review snapshot is this-round authority; the baseline files MUST stay readable.

#### Scenario: New review writes a separate snapshot
- **WHEN** the frozen first-expansion sample has been delivered and independently reviewed
- **THEN** live-run and source-review snapshots both carry the same plan id or content hash and the same report references
- **AND** the existing two-company v1 live-run and source-review files still contain the 7/7 baseline
- **AND** the new source-review snapshot is this-round authority

### Requirement: New-sample source review is recounted independently
After the frozen plan runs, source review MUST be recorded from independently read official pages. Recall, accuracy, critical numeric errors, reviewed-report count, and occupied strata MUST be derived from that review and MUST cover exactly the snapshot's selected instrument ids and frozen report references. The runtime MUST NOT presume the previous 7/7 result. Existing v1–v4 completed work MUST remain on disk. Query MUST continue to return the current published identity unless a later reviewed change publishes a successor.

#### Scenario: Review follows the frozen plan rather than the v4 score
- **WHEN** the new live sample has been delivered
- **THEN** source review covers exactly the persisted plan's selected instrument ids and report references
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
`company_profile_operator_closure.v2` MUST be written only after the frozen plan has been executed and the new source-review snapshot exists. Writing that report MUST mark `first_expansion_mode=completed`. The replacement backlog item MUST state that first expansion has already been executed under the reviewed plan, that the new source-review snapshot is this-round authority, and that this change does not authorize the next expansion, a scale-quality claim, or production. The item MUST NOT say that first expansion still requires this change or a new plan.

#### Scenario: Closure text is true at write time
- **WHEN** operator closure v2 is recorded after the new source-review snapshot exists
- **THEN** the backlog no longer contains `first_expansion_gates_unmet`
- **AND** the replacement item states that first expansion has been executed and is not a license for the next round, scale quality, or production
- **AND** `first_expansion_mode` is `completed`
- **AND** the other five backlog item ids remain

### Requirement: This slice does not implement industry packages
The first M4 expansion MUST NOT implement a manufacturing/materials production enhancement, MUST NOT build banking, service, or TMT industry packages, and MUST NOT project 净息差, 成本收入比, or loan-structure rows. A reusable interpretability gap found on the new sample MUST become a separate change with its own processing identity. This change MUST NOT publish `owned_page_facts=v5` unless that later change is separately reviewed.

#### Scenario: New sample discloses a reusable gap
- **WHEN** independent source review of the new sample finds an important disclosure that v4 cannot interpret
- **THEN** the finding is recorded as not delivered or inaccurate as the facts require
- **AND** this change does not add a new extractor or a new published identity
- **AND** a later change is required before any successor identity is published
