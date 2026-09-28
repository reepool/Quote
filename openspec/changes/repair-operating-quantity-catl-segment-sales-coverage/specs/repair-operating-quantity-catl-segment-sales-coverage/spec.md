## ADDED Requirements

### Requirement: The repair reuses the current operating-quantity chapter and sample
The repair MUST use the existing `extract_operating_quantities` chapter and the four-report defining sample already accepted for that chapter. It MUST NOT add a report, replace a report, or review regime. The code change MUST be limited to the two missed segment sales volumes exposed by the `300750.SZ` 2025 annual report. The other three reports MUST remain regression samples. Instrument identifiers, page numbers, and product names MUST remain acceptance fixtures and MUST NOT be hard-coded extraction rules. The repair MUST NOT enable the six-chapter manufacturing package, counterparties, DCF, trading, or price sensitivity.

#### Scenario: The sample and chapter stay the same
- **WHEN** the repair scope is reviewed
- **THEN** the chapter is `extract_operating_quantities`
- **AND** the defining reports remain `300750.SZ`, `603659.SH`, `920015.BJ`, and `302132.SZ`
- **AND** no regime finding is added or reopened

### Requirement: Two CATL segment sales stay independent facts
The repair MUST deliver the company power-battery sales volume and the company storage-battery sales volume as two `sales_volume` facts. Each fact MUST keep its own measured object, source value, unit, one-based physical page, bounded quote, evidence identifier, report identity, report period, and page text hash. The repair MUST NOT add the two values together and MUST NOT replace the battery-system sales row in the operating table with that sum or with either segment.

#### Scenario: Power-battery sales stay on their own page
- **WHEN** page 21 of the `300750.SZ` 2025 report states that power-battery sales are 541 GWh
- **THEN** one sales-volume fact has object 动力电池, value 541, unit GWh, and page 21
- **AND** its bounded quote is a contiguous slice of that page
- **AND** the fact is not merged into the 661 GWh battery-system row

#### Scenario: Storage-battery sales stay on their own page
- **WHEN** page 22 of the `300750.SZ` 2025 report states that storage-battery sales are 121 GWh
- **THEN** one sales-volume fact has object 储能电池, value 121, unit GWh, and page 22
- **AND** its evidence identifier differs from the power-battery fact
- **AND** the two values are not added into 662

#### Scenario: The operating-table total remains
- **WHEN** the operating table states battery-system sales of 661 GWh
- **THEN** that fact remains value 661 and unit GWh
- **AND** it is not rewritten as 662 or removed in favor of the two segment facts

### Requirement: Previously reviewed facts and coverage do not regress
The successor run MUST still deliver the 36 facts judged accurate by the 20260928.2 source review and MUST keep the 13 coverage statuses. `920015.BJ` omitted volumes MUST remain `not_disclosed`. `302132.SZ` classified volumes MUST remain `not_applicable`. A `legal_empty` bundle outcome MUST continue to wrap the concrete coverage status and MUST NOT be replaced by an invented quantity. The Putailai page-15 qualifiers `超过` and `已达` MUST remain on their own evidence. The base-film 20 亿平方米 addition MUST remain one fact. The page 14 processing volume and the page 19 official sales volume MUST remain separate.

#### Scenario: The prior successor facts stay accurate
- **WHEN** the new successor run reviews the four reports
- **THEN** the previously accurate 36 facts are still present and accurate
- **AND** the 13 coverage statuses are unchanged
- **AND** no legal-empty field is replaced by a quantity

### Requirement: The successor replay leaves both earlier replays immutable
Implementation after scope approval MUST write enqueue, run, and result only in a new research isolation directory under this change. The plan version MUST be `manufacturing_materials_stage4_operating_quantities.2026-09-28.3`. The repair MUST NOT write into `replay/20260928.2` or the historical `replay/20260928`, and MUST NOT modify their enqueue, run, result, or source-review files. Output MUST remain `accepted_for_review` with zero provider calls. The processing identity MUST remain `{"rules":"company_profile_common_core.v1","owned_page_facts":"v8","material_input_facts":"v1"}`. The repair MUST NOT write common-core, publication, closure, completed mode, or production authorization. `scale_quality_claim_allowed` MUST remain false and `production_authorization` MUST remain `not_authorized`.

#### Scenario: Earlier replay hashes stay byte-stable
- **WHEN** the successor replay is created
- **THEN** it uses plan `manufacturing_materials_stage4_operating_quantities.2026-09-28.3`
- **AND** its directory is neither `replay/20260928.2` nor `replay/20260928`
- **AND** the 20260928.2 and 20260928 artifact hashes are unchanged

### Requirement: Metrics are not preset by this repair
This change MUST NOT record source recall, source accuracy, critical numeric errors, or `expansion_gates_met` before a new independent source review rereads the successor output. The historical 36/38, 36/36, and zero critical errors describe only the 20260928.2 run. The operating-quantity 26/36 and 26/27, the material-input 23/23, the stage-4 19/23, and the fixed two-company 9/9 MUST NOT be reused as this repair's result. This change MUST NOT be archived, and its close-out MUST wait until that new source review passes.

#### Scenario: Scope approval does not fill the gate
- **WHEN** this repair is accepted at scope review or implemented before the successor source review
- **THEN** recall, accuracy, critical numeric errors, and the gate remain unfilled for the successor
- **AND** the 20260928.2 gate remains false
- **AND** no archive is started
