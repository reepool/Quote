# repair-operating-quantity-603659-coverage-and-evidence-binding Specification

## Purpose

This capability repairs missed and mis-bound operating quantities for the Putailai 2025 report inside the existing `extract_operating_quantities` chapter. The 23 target facts in `replay/20260928.2` were delivered correctly. The four-report regression remains source recall 36/38, source accuracy 36/36, critical numeric errors 0, coverage 13/13, and `expansion_gates_met=false`, because that bundle does not contain the two later CATL segment sales. The archived CATL `.3` result of 38/38 is a separate repair and is not a recount of this bundle. The original `replay/20260928` result of 26/36, 26/27, and one critical error stays a third observation.

## Requirements

### Requirement: The repair reuses the current operating-quantity chapter and sample
The repair MUST use the existing `extract_operating_quantities` chapter and the four-report defining sample already accepted for that chapter. It MUST NOT add a report, replace a report, or review regime. The code change MUST be limited to the missed and mis-bound operating quantities exposed by the `603659.SH` 2025 annual report. Instrument identifiers, page numbers, and product names MUST remain acceptance fixtures and MUST NOT be hard-coded extraction rules. The repair MUST NOT enable the six-chapter manufacturing package, counterparties, DCF, trading, or price sensitivity.

#### Scenario: The sample and chapter stay the same
- **WHEN** the repair scope is reviewed
- **THEN** the chapter is `extract_operating_quantities`
- **AND** the defining reports remain `300750.SZ`, `603659.SH`, `920015.BJ`, and `302132.SZ`
- **AND** no regime finding is added or reopened

### Requirement: Ten missed Putailai quantities are delivered as separate facts
The repair MUST deliver each company-owned quantity that the 2026-09-28 source review counted as missing from `603659.SH`. Each delivered fact MUST keep its measured object, metric type, source value, comparison qualifier when the source states one, unit, one-based physical page, and a bounded quote from that page. A rounded restatement of a formal table row, an equipment single-line nameplate, an industry statistic, a future-year target, and a conditional later project phase MUST NOT be added as one of these ten facts.

#### Scenario: The ten missing disclosures are the repair fixture
- **WHEN** the `603659.SH` 2025 report is read for this repair
- **THEN** the delivered facts include base-film formed capacity 21 亿㎡ on page 14
- **AND** base-film sales volume 14.95 亿㎡ on page 14
- **AND** commissioned electrode-line capacity 8 GWh on page 14
- **AND** anode formed capacity 25 万吨 on page 15
- **AND** PVDF effective capacity of more than 3 万吨 on page 15
- **AND** boehmite and alumina effective capacity stated as having reached 3 万吨 on page 15
- **AND** base-film under-construction capacity 20 亿平方米 counted once across pages 15 and 27
- **AND** coating under-construction capacity 30 亿平方米 on page 27
- **AND** the Sichuan Zichen integrated project capacity 28 万吨 on page 27
- **AND** the Sichuan Zichen phase-one capacity 10 万吨 on page 27

#### Scenario: Excluded quantities stay outside the ten
- **WHEN** the same report states a 2026 target, an equipment single-line nameplate, an industry shipment, or a rounded narrative restatement of a page 19 table row
- **THEN** that statement is not required as an additional repair fact
- **AND** it is not used to satisfy one of the ten missing disclosures

### Requirement: Page 15 keeps two capacity facts and their own qualifiers
A capacity sentence MUST bind its measured object and comparison qualifier to that sentence's own bounded quote. `超过` MUST be retained as a lower-bound qualifier and MUST NOT be stored as an exact value. `已达` MUST be retained as its own attained qualifier and MUST NOT be rewritten as `超过`. The PVDF sentence and the boehmite-and-alumina sentence on page 15 MUST be two facts with two evidence records. The historical record that assigns the PVDF quote to 勃姆石和氧化铝 and stores an exact 3 MUST NOT be treated as an accurate fact. The repair MUST NOT correct that record by renaming the object or silently replacing its evidence inside the 20260928 result.

#### Scenario: PVDF keeps the lower bound
- **WHEN** page 15 states that PVDF effective capacity is more than 3 万吨
- **THEN** the fact object is PVDF
- **AND** the value retains the `超过` qualifier
- **AND** the bounded quote is the PVDF sentence
- **AND** the value is not an unqualified exact 3

#### Scenario: Boehmite and alumina keep their own sentence
- **WHEN** page 15 states that boehmite and alumina effective capacity has reached 3 万吨
- **THEN** that fact uses the boehmite and alumina sentence as its bounded quote
- **AND** the qualifier is `已达`
- **AND** the evidence identifier differs from the PVDF fact

#### Scenario: The historical misbinding is not reused
- **WHEN** a delivered fact uses the PVDF lower-bound sentence as evidence
- **THEN** its measured object is not 勃姆石和氧化铝
- **AND** the 20260928 result file is not edited to hide the original record

### Requirement: Repeated capacity is counted once and distinct anchors stay apart
The same under-construction capacity restated on two pages MUST be delivered once. A processing volume and a sales volume MUST remain separate facts when the source gives them different labels or units, even if their magnitudes are close after unit conversion. The page 14 coating processing volume and the page 19 coated-separator sales volume MUST stay two anchors.

#### Scenario: The 20 billion square-meter base-film addition is one fact
- **WHEN** page 15 and page 27 both state the same 20 亿平方米 base-film addition
- **THEN** the repair delivers one under-construction fact for that quantity
- **AND** it does not deliver a second fact for the repeated sentence

#### Scenario: Processing volume is not merged into table sales
- **WHEN** page 14 states 涂覆加工量（销量） of 109.42 亿㎡ and page 19 states coated-separator sales volume of 1,094,249.25 万㎡
- **THEN** both facts remain
- **AND** neither fact is converted into the other unit to replace the other fact

### Requirement: Previously correct facts and legal-empty coverage remain
The 26 facts judged accurate by the 2026-09-28 source review MUST remain accurate in the successor run. The 13 coverage rows MUST keep their coverage statuses. `302132.SZ` classified volumes MUST remain `not_applicable`. `920015.BJ` omitted production, sales, inventory, and processing volumes MUST remain `not_disclosed`. A `legal_empty` bundle outcome MUST continue to wrap the concrete coverage status and MUST NOT be replaced by an invented quantity. The repair MUST NOT calculate a volume from capacity or utilization, and MUST NOT treat a currency amount as a physical volume.

#### Scenario: Legal-empty coverage is not filled
- **WHEN** the successor run reviews the four reports
- **THEN** the previously accurate 26 facts are still present and accurate
- **AND** the 13 coverage statuses are unchanged
- **AND** no legal-empty field is replaced by a quantity

### Requirement: The successor replay leaves the failed replay immutable
Implementation after scope approval MUST write enqueue, run, and result only in a new research isolation directory. The plan version MUST be `manufacturing_materials_stage4_operating_quantities.2026-09-28.2`. The repair MUST NOT force a replay in `replay/20260928` and MUST NOT modify that directory's enqueue, run, result, or source_review. Output MUST remain `accepted_for_review` with zero provider calls. The processing identity MUST remain `{"rules":"company_profile_common_core.v1","owned_page_facts":"v8","material_input_facts":"v1"}`. The repair MUST NOT write common-core, publication, closure, completed mode, or production authorization. `scale_quality_claim_allowed` MUST remain false and `production_authorization` MUST remain `not_authorized`.

#### Scenario: The 20260928 artifacts stay byte-stable
- **WHEN** the successor replay is created
- **THEN** it uses plan `manufacturing_materials_stage4_operating_quantities.2026-09-28.2`
- **AND** its directory is not `replay/20260928`
- **AND** the 20260928 enqueue, run, result, and source_review hashes are unchanged

### Requirement: The closed four-report regression stays failed
The independent source review of `replay/20260928.2` records that the 23 Putailai target facts were delivered correctly, while the four-report result remains source recall 36/38, source accuracy 36/36, critical numeric errors 0, coverage 13/13, and `expansion_gates_met=false`. The false gate MUST remain the absence of the two CATL segment sales from that bundle. The later CATL `.3` result of 38/38 MUST NOT be written back into this observation. The original `replay/20260928` result of 26/36, 26/27, and one critical error MUST stay a third record. Archiving this change MUST keep those bytes unchanged and MUST NOT treat the failed gate as a pass.

#### Scenario: Archive retains the failed regression
- **WHEN** this change is archived
- **THEN** the recorded gate stays false
- **AND** the 23 target facts remain a correct delivery inside that failed regression
- **AND** the CATL 38/38 result is not described as a recount of this bundle
