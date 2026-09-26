## ADDED Requirements

### Requirement: Stage-four scope uses the verified cross-sample portfolio
This change MUST use at least three locally valid official annual reports from at least two companies, and MUST include at least two reports that challenge any single focus report. The minimum slice sample MUST be `300750.SZ` 2025, `603659.SH` 2025, and `920015.BJ` 2025, covering SZSE, SSE, and BSE. These three reports are the floor for this slice, not the whole stage-three sample. Stage-three regime coverage is already complete: verified `302132.SZ` 2025 remains the historical regime baseline and MUST NOT be part of this card's defining sample. This change MUST NOT select `extract_business_regime` and MUST NOT claim that results from the three reports re-validate regime capability. `600004.SH` and `600006.SH` MUST NOT be the sole or defining sample. Each dossier MUST record exchange, report identity, business mode, disclosure form, and coverage gaps from the report itself. A narrow disclosure-form or business-mode overlap MUST remain a recorded gap. This change MUST NOT treat the historical four-report six-chapter slice as the current admission contract.

#### Scenario: Three reports are the slice floor, not the whole stage-three sample
- **WHEN** the scope review lists `300750.SZ`, `603659.SH`, and `920015.BJ` across SZSE, SSE, and BSE
- **THEN** at least two reports are challengers to any one focus report
- **AND** verified `302132.SZ` stays the stage-three regime baseline outside this defining sample
- **AND** the fixed `600004.SH` / `600006.SH` pair is not used to define the slice

#### Scenario: The three-report slice does not re-validate regime
- **WHEN** dossiers or later results are produced only for the three defining reports
- **THEN** the change does not select `extract_business_regime`
- **AND** those results are not reported as a new regime validation

#### Scenario: Disclosure diversity is not invented
- **WHEN** the three reports do not yet show distinct disclosure forms or business modes
- **THEN** the dossiers record that coverage gap
- **AND** the gap is not filled by claiming diversity or by adding `600004.SH` or `600006.SH`

### Requirement: Independent dossiers precede field obligations and slice selection
Each selected report MUST have its own dossier before any common field obligation or vertical slice is chosen. The dossier MUST record the business-overview source, chapter-task map, candidate fields, subject scope, period, source-native unit, physical page anchor, Evidence, legal-empty cases, extraction failures, and unresolved questions. A field observed in only one report MUST remain subtype-specific, conditional, optional, or unresolved. `required` MUST mean required inspection, not guaranteed disclosure.

#### Scenario: One report discloses a field the others do not
- **WHEN** only one dossier contains a candidate manufacturing or materials field
- **THEN** that field is not marked package-wide required
- **AND** the other dossiers are completed before any common obligation is written

### Requirement: The minimum slice is extract_material_inputs
The dossiers MUST be complete before the slice is named. The named slice MUST be the existing chapter task `extract_material_inputs`, because the `300750.SZ`, `603659.SH`, and `920015.BJ` dossiers each contain a company-owned named-material input sentence with a different disclosure shape. `extract_business_regime`, operating quantities, counterparties, and every other existing chapter MUST stay inactive. The change MUST NOT enable the six-chapter package, and MUST NOT hard-code one company, one page, or one material name. If those dossiers had not supported one material-input meaning, the slice MUST NOT have been forced by industry knowledge.

#### Scenario: Three dossiers support named material inputs
- **WHEN** `300750.SZ` physical page 40 names cathode material, anode material, separator, and electrolyte as production inputs, `603659.SH` physical page 33 names business-line production materials, and `920015.BJ` physical page 26 names the product raw materials
- **THEN** `extract_material_inputs` is the single selected chapter
- **AND** at least two companies, in this case all three, support that selection

#### Scenario: Other chapters stay closed
- **WHEN** the material-input slice is selected
- **THEN** `extract_business_regime`, operating quantities, and customer or supplier chapters remain inactive
- **AND** the six-chapter package is not enabled

### Requirement: Material-input acceptance follows the dossier boundaries
A material input MUST be a named material the same report sentence binds to the company's own production or operating input. Sales evidence alone MUST NOT create an input role. A generic direct-material cost or a raw-material inventory amount MUST be refused. Outsourced processing, including Jinhua's 丁酮肟 processing, MUST NOT be recorded as a material input. Absence of a quantity MUST NOT block delivery of an otherwise explicit named input. The same source-native name MAY also have a separate product or sales fact, and that overlap MUST NOT erase the input fact or be netted. Legal non-disclosure means the report does not state the named input or expressly omits it. `unclear` means the cited evidence does not uniquely bind the material to the company's own input. Extraction failure means the page, header, unit, or evidence context cannot be bound. These three outcomes MUST NOT be rewritten as a zero, a guessed commodity id, or a successful fact.

#### Scenario: A named production input has no quantity
- **WHEN** a dossier sentence names the company's production raw materials and gives no purchase quantity
- **THEN** the named input remains deliverable for research review
- **AND** no quantity is invented

#### Scenario: Sales evidence or product overlap does not create the input by itself
- **WHEN** a report lists a material as a product sold by the company and does not also bind it as the company's own input
- **THEN** that sales or product evidence does not create an input role
- **AND** a separate accepted input sentence for the same source-native name may still be kept without netting

#### Scenario: Generic cost and inventory amounts are refused
- **WHEN** the text only states direct material cost or a raw-material inventory balance
- **THEN** no named material input is created from that amount

#### Scenario: Outsourced processing is not a material input
- **WHEN** `920015.BJ` states that an outside party processes 丁酮肟 from material supplied by the company
- **THEN** that processing arrangement is not recorded as the material-input fact
- **AND** the separately named raw materials on physical page 26 are not replaced by the processing sentence

#### Scenario: Legal non-disclosure, unclear binding, and extraction failure stay distinct
- **WHEN** a report has no named-input sentence, the cited sentence does not uniquely bind the material to the company, or the page, header, unit, or evidence cannot be bound
- **THEN** the outcome is legal non-disclosure, `unclear`, or extraction failure respectively
- **AND** none of those outcomes is delivered as an accurate material input

### Requirement: Legal non-disclosure stays distinct from extraction failure
The slice MUST distinguish subject, period, unit, physical page anchor, and Evidence from one another. A source that lawfully omits a named input or other slice field MUST be recorded as legal-empty or not disclosed. Missing context, an unreadable page, or an unbound unit MUST be recorded as extraction failure. Absence MUST NOT be rewritten as a zero, a guessed commodity id, a profit direction, or a successful fact.

#### Scenario: A report does not name the slice object
- **WHEN** an in-scope report has no explicit disclosure for the selected slice field
- **THEN** the dossier records legal-empty or not disclosed
- **AND** no fact is created to raise recall

#### Scenario: Evidence context is incomplete
- **WHEN** the selected task's page, header, unit, or subject cannot be bound
- **THEN** the result is extraction failure or unresolved
- **AND** it is not counted as an accurate disclosure

### Requirement: Research output stays isolated from common-core production
Implementation after scope approval MUST reuse existing Evidence preparation, Stage 5 extract, repair, and verify, and the existing research isolation bundle. Candidates MUST remain `accepted_for_review`. Gold MUST NOT populate runtime fields. The current processing identity `{"rules":"company_profile_common_core.v1","owned_page_facts":"v8","material_input_facts":"v1"}` MUST stay unchanged. The change MUST NOT write common-core production, modify publication, closure, or completed mode, start another expansion, or enable DCF, trading, the legacy writer, or a full manufacturing package.

#### Scenario: A focused research run finishes
- **WHEN** the one selected slice produces reviewable candidates for the approved reports
- **THEN** those candidates stay in the isolated research bundle with disposition `accepted_for_review`
- **AND** common-core publication, identity, closure, and completed mode remain unchanged

### Requirement: Scope review blocks implementation and does not presume metrics
No Python change, enqueue, replay, or checkpoint write is authorized before independent scope review accepts sample diversity, the one-slice rule, field boundaries, research isolation, and the later verification method. Later focused replay MUST recount recall, accuracy, critical numeric errors, and expansion gates from that run. This scope MUST NOT presume that recall, accuracy, or expansion gates pass.

#### Scenario: Scope review has not passed
- **WHEN** task 1.1 is still unchecked
- **THEN** implementation, enqueue, and official replay are refused
- **AND** no recall or gate result is written in advance

#### Scenario: Implementation later reaches focused replay
- **WHEN** an approved slice is replayed on the approved reports
- **THEN** recall, accuracy, critical numeric errors, and expansion gates are derived from that replay
- **AND** the fixed two-company 9/9 observation is not reused as this slice's result
