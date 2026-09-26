## ADDED Requirements

### Requirement: Stage-four scope uses the verified cross-sample portfolio
This change MUST use at least three locally valid official annual reports from at least two companies, and MUST include at least two reports that challenge any single focus report. The minimum portfolio MUST be the stage-three reports `300750.SZ` 2025, `603659.SH` 2025, and `920015.BJ` 2025, covering SZSE, SSE, and BSE. `600004.SH` and `600006.SH` MUST NOT be the sole or defining sample. Each dossier MUST record exchange, report identity, business mode, disclosure form, and coverage gaps from the report itself. A narrow disclosure-form or business-mode overlap MUST remain a recorded gap. This change MUST NOT add an unverified issuer to close the stage-three regime gap, and MUST NOT treat the historical four-report six-chapter slice as the current admission contract.

#### Scenario: The stage-three portfolio is the scope floor
- **WHEN** the scope review lists the three stage-three annual reports across SZSE, SSE, and BSE
- **THEN** at least two reports are challengers to any one focus report
- **AND** the fixed `600004.SH` / `600006.SH` pair is not used to define the slice

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

### Requirement: Exactly one existing chapter task is the minimum slice
After the dossiers are complete, the change MUST select exactly one existing chapter task as the manufacturing/materials vertical slice. The selected task MUST be supported by dossiers from at least two companies. The other chapter tasks MUST stay inactive. The change MUST NOT enable the full six-chapter industry package in the same implementation, and MUST NOT hard-code one company, one page, or one material name.

#### Scenario: Dossiers support one shared task
- **WHEN** at least two company dossiers support the same existing chapter task and its legal-empty behavior
- **THEN** that task may be named as the single minimum slice
- **AND** the remaining chapter tasks stay out of scope

#### Scenario: No cross-company task is supported
- **WHEN** no existing chapter task is supported by two companies after the dossiers
- **THEN** the slice is not invented from industry knowledge
- **AND** implementation stays blocked instead of enabling all six chapters

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
