# deliver-company-profile-m4-next-small-batch-research Specification

## Purpose

This capability delivers two new official annual reports through the existing common-core owner under `owned_page_facts=v8` and `material_input_facts=v1`. The frozen round is service company `600007.SH` and manufacturing company `600010.SH`, cutoff `2026-09-17`, with one shared 50000-token budget. The archived independent reading is recall 5/9, accuracy 5/5, and critical numeric errors 0. That accuracy scores only the five delivered hits. The retained misses are the service company's revenue sentence, its steam, hot-water, and electricity fee, and the manufacturing company's explicit product-sales and raw-material roles. `expansion_gates_met` stays false. `production_authorization` stays `not_authorized` and `scale_quality_claim_allowed` stays false. This archive does not authorize production, scale quality, DCF, or trading, and it does not repair those misses.

## Requirements

### Requirement: The next batch stays closed until review
Before the scope review passes, the change MUST only contain its scope documents. It MUST NOT change Python. It MUST NOT enqueue, replay, or select the two reports. It MUST NOT modify `first_expansion_mode`, the first-expansion plan pointer, closure v2, or the historical v8 and 9/9 observations. It MUST NOT authorize production, scale quality, DCF, or trading.

#### Scenario: Scope review does not start a run
- **WHEN** the scope review has not yet accepted this contract
- **THEN** no new plan snapshot and no new live-run exist
- **AND** the historical 8/9 observation remains unchanged

### Requirement: The batch uses the current identity and the existing owner
The batch MUST run on `company_profile_common_core` through `CompanyProfileTaskService` and the existing preview, run, query, and export actions. The processing identity MUST be `owned_page_facts=v8` and `material_input_facts=v1`. The change MUST NOT add a published action or a second execution chain. `600004.SH` and `600006.SH` MUST remain the completed historical sample. Their v8 observation MUST remain recall 8/9, accuracy 8/8, critical numeric errors 0, and `expansion_gates_met=false`. Their later 9/9 successor MUST NOT be written back onto that v8 observation.

#### Scenario: Historical eight-of-nine stays historical
- **WHEN** this batch selects its two reports
- **THEN** it does not treat the v8 8/9 result as an open repair
- **AND** it does not enqueue `600004.SH` or `600006.SH` again

### Requirement: Two new reports are frozen before enqueue
After review, the owner MUST select two official annual reports that have no delivered runtime record under the current identity: the first available service disclosure and the first available manufacturing disclosure, ordered by SSE, SZSE, then BSE, and by ascending `instrument_id` within an exchange. The selection MUST NOT hard-code an instrument id. If either disclosure form has no legal candidate, the owner MUST refuse to record the plan. Before enqueue, the owner MUST persist an immutable plan with the knowledge cutoff, both report references (`asset_id`, `report_id`, `report_period`, `document_version`), `max_companies_this_round=2`, and one shared `token_budget=50000`. A later effective report that differs in any frozen reference field MUST be refused for that instrument.

#### Scenario: The plan exists before the first company runs
- **WHEN** the batch is allowed to enqueue
- **THEN** the plan snapshot already names both report identities and the shared budget
- **AND** a drifted document version is not substituted

### Requirement: The owner consumes the frozen plan across two runs
`CompanyProfileTaskService` MUST compare each ordinary run's effective annual report with the frozen reference and MUST refuse a drifted `asset_id`, `report_id`, `report_period`, or `document_version`. The live-run snapshot MUST keep both companies after the two separate runs; the second run MUST NOT replace the first company's outcome with only the securities of that call. Source review MUST read that combined observation. The 50000-token budget MUST be cumulative. Tokens consumed by a failed call MUST reduce the remainder. Reuse of a completed scope MUST add no consumption. The second run MUST receive the remaining budget rather than a fresh 50000.

#### Scenario: Drift is refused and both companies stay in one observation
- **WHEN** the service run has been recorded and the manufacturing run uses a drifted report version
- **THEN** that manufacturing instrument is refused
- **AND** the round observation still contains the service company's recorded outcome

#### Scenario: The second run receives the remaining budget
- **WHEN** the first run consumed tokens, including a failed call, and a completed scope is reused
- **THEN** only the failed and newly executed calls reduce the 50000 budget
- **AND** the second run is given the remainder

### Requirement: Snapshots for this round stay separate
This round's plan, live-run, and source-review MUST be stored under `reports/m4_next_small_batch/<plan_id>/`. They MUST NOT overwrite the first-expansion plan pointer, mode file, closure v2, or the historical live-run and source-review files. `first_expansion_mode` MUST remain `completed`.

#### Scenario: Old closure remains completed
- **WHEN** this round writes its own plan snapshot
- **THEN** `company_profile_operator_closure.v2` is unchanged
- **AND** `first_expansion_mode` is still `completed`

### Requirement: One disclosure form is delivered before the other
The owner MUST run the service company through evidence selection, extraction and acceptance, persistence, query, and export before running the manufacturing company through that same path. The manufacturing run MUST receive the remaining shared token budget. A repeated submit MUST reuse a completed scope for the current identity, MUST NOT call the provider again for that scope, and MUST NOT add token consumption for that reuse. Failure of one company MUST remain in the combined observation and MUST NOT prevent the other company from being delivered.

#### Scenario: The second company still runs after the first fails
- **WHEN** the service company fails or is refused
- **THEN** its failure is retained
- **AND** the manufacturing company can still be enqueued and delivered

### Requirement: Core answers and explicit commodity roles are judged before the run
For each delivered company, principal business, products or services, and revenue source MUST each be a substantive answer bound to source evidence or an explicit gap with `missing_reason`. An empty dimension without a gap MUST NOT count as delivered. An explicit commodity role in the source MUST appear in query and export with its period and source reference. A completed check with no explicit association MUST be shown as no explicit association found, not as zero exposure. An unchecked company MUST remain not assessed. Recall, accuracy, and critical numeric errors MUST be counted by an independent reading of the two frozen reports after delivery. The review MUST NOT prefill 8/9 or 9/9. A missed criterion MUST be retained and MUST NOT be rewritten as a pass. `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false.

#### Scenario: The score is not copied from the historical repair
- **WHEN** the source review for this batch is written
- **THEN** its denominator comes from the two new reports
- **AND** the historical 8/9 and 9/9 values are not reused as this batch's score
