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
The first M4 expansion MUST persist an immutable `company_profile_first_expansion_plan.v1` snapshot before any `active`-mode enqueue or drain. That snapshot MUST include a stable `plan_id` or content hash, the live-plan rules, `knowledge_cutoff`, the candidate-registry identity used to draw the sample (`schema_version`, `as_of`, and `universe_snapshot_id` when the registry has one), `selected_instrument_ids`, `selected_strata`, and one official annual-report reference per selected instrument. Each report reference MUST use the existing fields `asset_id`, `report_id`, `report_period`, and `document_version`. The frozen sample MUST contain at most two companies, the existing live-plan company budget. First-expansion sampling MUST reserve one seat for `service` and MUST fill the other seat from the remaining review-eligible candidates using the existing global stratum priority. The reserved `service` candidate MUST be chosen only from names with `asset_status=available` and a latest effective annual report, in exchange order SSE, SZSE, BSE, and by ascending `instrument_id` within that exchange. The final sample MUST contain at least one `service` stratum and at least two different strata. Sampling MUST NOT hard-code an instrument id, read model output, change the universe denominator, change ordinary live-run `_STRATUM_PRIORITY`, or increase the sample size above two. If the registry has no legal `service` candidate, the owner MUST refuse to record or activate the plan. `max_companies_this_round` MUST equal the number of frozen reports and MUST NOT exceed two. Schema validation MUST reject a plan payload that sets the budget, selected instruments, strata, and report references together to three or more companies. The snapshot MUST NOT change after the first successful persist. Enqueue and resume in `active` mode MUST compare the current effective annual report to those frozen references and MUST refuse on drift; they MUST NOT silently use a corrected filing, a different document version, or a different `knowledge_cutoff`.

#### Scenario: First expansion reserves one service seat inside two companies
- **WHEN** review-eligible `service` candidates exist and the ordinary two-company priority would fill both seats with earlier manufacturing strata
- **THEN** the first-expansion sample still contains one deterministic `service` candidate and one other candidate from the remaining global priority
- **AND** the sample contains at most two companies and at least two different strata
- **AND** ordinary live-run stratum priority is unchanged

#### Scenario: No legal service candidate refuses the plan
- **WHEN** the registry has no review-eligible `service` candidate
- **THEN** the owner does not record or activate a first-expansion plan
- **AND** the sample size is not increased above two companies

#### Scenario: Three-company plan payload is refused
- **WHEN** an external plan JSON sets the company budget, selected instruments, strata, and report references to three companies together
- **THEN** schema validation rejects that plan
- **AND** the owner does not activate it

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

### Requirement: Operator closure publishes v2 and keeps the v1 schema compatible
The change MUST publish `company_profile_operator_closure.v2` as a new snapshot and MUST NOT change the unique legal backlog of the `company_profile_operator_closure.v1` schema. The v1 schema and catalog MUST remain supported, including `first_expansion_gates_unmet`. A legal v1 JSON fixture MUST still load, and the runtime MUST NOT require that fixture to match the v2 catalog. If a historical v1 JSON file exists, it MUST remain readable and v2 MUST NOT overwrite it. This official checkpoint has no discovered v1 snapshot, so the change MUST NOT reconstruct or forge one. Absence of that file MUST NOT prevent v2 from being this round's official closure. The v2 catalog MUST replace `first_expansion_gates_unmet` and MUST keep the other five backlog item ids: manufacturing/materials is not production, non-manufacturing industry packages are absent, unassessed semantics are not a pass, missing assets stay in the denominator, and Gold24/fixture guards are not live quality. Every backlog item MUST keep `execute_this_round=false` and `production_authorized=false`.

#### Scenario: v1 report remains readable after v2 is published
- **WHEN** a historical `company_profile_operator_closure.v1` report contains `first_expansion_gates_unmet`
- **THEN** that file still loads
- **AND** the current snapshot is `company_profile_operator_closure.v2`
- **AND** the v1 file is not overwritten

#### Scenario: Official v1 snapshot is absent
- **WHEN** the official checkpoint has no `company_profile_operator_closure.v1` file
- **THEN** `load_operator_closure_report()` returns none
- **AND** the written v2 snapshot remains this round's official closure
- **AND** no v1 file is created to impersonate that missing history
- **AND** the v1 loader and schema compatibility tests remain in place

### Requirement: v2 replacement backlog uses post-execution wording
`company_profile_operator_closure.v2` MUST be written only after the frozen plan has been executed and the new source-review snapshot exists. Writing that report MUST mark `first_expansion_mode=completed`. The replacement backlog item MUST state that first expansion has already been executed under the reviewed plan, that the new source-review snapshot is this-round authority, and that this change does not authorize the next expansion, a scale-quality claim, or production. The item MUST NOT say that first expansion still requires this change or a new plan.

#### Scenario: Closure text is true at write time
- **WHEN** operator closure v2 is recorded after the new source-review snapshot exists
- **THEN** the backlog no longer contains `first_expansion_gates_unmet`
- **AND** the replacement item states that first expansion has been executed and is not a license for the next round, scale quality, or production
- **AND** `first_expansion_mode` is `completed`
- **AND** the other five backlog item ids remain

### Requirement: A complete failed review can end the round
`completed` MUST mean only that this round's observation has ended. It MUST NOT mean `expansion_gates_met=true`, and it MUST NOT authorize the next expansion, a scale-quality claim, or production. Operator closure v2 MUST be allowed for an assessed-accuracy review and for a zero-delivery review whose accuracy stays `unassessed`. The zero-delivery path MUST require every frozen live-run outcome `delivered=true`, findings that cover exactly the frozen sample, all three aspects `core_skeleton`, `important_disclosure`, and `commodity_role` for every selected company, assessed recall, assessed critical numeric errors, every finding `present_in_delivery=false`, and every `fact_accurate` null. The authoritative failure MUST remain in the this-round source-review. Closure MUST NOT add a `failed` mode or extend the closure v2 schema. Closure MUST refuse a partial sample, a company missing any of the three aspects, a frozen work that is not delivered, a delivered finding whose accuracy is still unassessed, unassessed recall or critical numeric errors, and any drift in the plan, report references, or source-review binding.

#### Scenario: Zero delivery closes without becoming a quality pass
- **WHEN** both frozen works are delivered and the source review covers both companies and all three aspects with recall 0/9, unassessed accuracy, zero critical numeric errors, no delivered facts, and `expansion_gates_met=false`
- **THEN** operator closure v2 may be written and the mode may become `completed`
- **AND** the source review remains recall 0/9, accuracy unassessed, and `expansion_gates_met=false`
- **AND** completion does not authorize the next expansion, a scale-quality claim, or production

#### Scenario: An unfinished or mixed review cannot close
- **WHEN** the review covers fewer or more companies than the frozen sample, omits an aspect, includes an undelivered frozen work, leaves accuracy null on a delivered finding, or leaves recall or critical numeric errors unassessed
- **THEN** operator closure v2 is refused
- **AND** the mode stays `active`

### Requirement: This slice does not implement industry packages
The first M4 expansion MUST NOT implement a manufacturing/materials production enhancement, MUST NOT build banking, service, or TMT industry packages, and MUST NOT project 净息差, 成本收入比, or loan-structure rows. A reusable interpretability gap found on the new sample MUST become a separate change with its own processing identity. This change MUST NOT publish `owned_page_facts=v5` unless that later change is separately reviewed.

#### Scenario: New sample discloses a reusable gap
- **WHEN** independent source review of the new sample finds an important disclosure that v4 cannot interpret
- **THEN** the finding is recorded as not delivered or inaccurate as the facts require
- **AND** this change does not add a new extractor or a new published identity
- **AND** a later change is required before any successor identity is published
