## ADDED Requirements

### Requirement: The segment-financials slice uses only the approved research list
The defining sample MUST contain at least three official annual reports and MUST cover SZSE, SSE, and BSE. Every report MUST already belong to the approved manufacturing and materials manifest `manufacturing_materials.2026-09-03.4`: `300750.SZ`, `603659.SH`, `920015.BJ`, and `302132.SZ`. The change MUST NOT add an issuer outside that list. `302132.SZ` MAY enter the defining sample only after a new segment-financials dossier confirms that its segment revenue, cost, and gross-margin section fits `extract_segment_financials`. If that dossier does not confirm fitness, the change MUST stop and MUST NOT substitute another issuer. Admitting `302132.SZ` MUST NOT be treated as a regime review, and the old regime dossier MUST NOT replace the new dossier.

#### Scenario: The sample covers three exchanges
- **WHEN** the scope review names the defining reports
- **THEN** the set contains at least three reports and covers SZSE, SSE, and BSE
- **AND** no issuer outside the four approved reports is added

#### Scenario: The restructuring report waits for its own dossier
- **WHEN** `302132.SZ` does not yet have a segment-financials dossier that confirms the chapter fits
- **THEN** it is not yet a defining report
- **AND** the change does not select a replacement issuer

### Requirement: The slice is the existing extract_segment_financials chapter
The selected chapter MUST be the existing `extract_segment_financials` task. The change MUST NOT create a new financial chapter and MUST NOT enable counterparties, regime, the six-chapter manufacturing package, DCF, trading, price sensitivity, or the legacy writer. Customer and supplier extraction MUST remain a later conditional enhancement. Archived operating-quantity results of 26/36, 36/38, and 38/38, and archived material-input results of 19/23, 23/23, and 9/9, MUST stay independent observations. This change MUST NOT treat any of those gates as a segment-financials result.

#### Scenario: Only one existing chapter is selected
- **WHEN** the scope review selects the next stage-4 slice
- **THEN** the selected chapter is `extract_segment_financials`
- **AND** counterparties, regime, DCF, trading, price sensitivity, the legacy writer, and the six-chapter package stay inactive

### Requirement: One formal table keeps dimension, revenue, cost, reported margin, and elimination distinct
A formal segment table MUST keep its dimension, operating revenue, operating cost, source-reported gross margin, and consolidation elimination as separate facts. The dimension MUST preserve whether the source labels the row as a segment, product, industry, region, or sales mode. Operating revenue and operating cost MUST remain monetary measurements. A gross margin MUST be recorded only when the source states the percentage directly. The change MUST NOT calculate gross margin from revenue and cost. A consolidation-elimination row MUST use `consolidation_adjustment` and MUST NOT be stored as an ordinary product segment or as an aggregate named 其他. A summary sentence MUST NOT replace a formal table cell. An amount table MUST NOT be converted into an Activity.

#### Scenario: Reported margin is not derived
- **WHEN** a formal table states revenue and cost for a row and does not state a gross-margin percentage
- **THEN** no gross-margin fact is calculated from those amounts
- **AND** the missing margin keeps a concrete coverage status rather than a synthesized percentage

#### Scenario: Elimination stays marked
- **WHEN** the formal table contains a consolidation-elimination row
- **THEN** that row is recorded with `consolidation_adjustment`
- **AND** its revenue, cost, and any reported margin remain separate values on that marked row

#### Scenario: Narrative does not replace the table
- **WHEN** a management discussion states a revenue mix or margin change without a formal table cell anchor
- **THEN** that sentence is not a segment-financials fact

### Requirement: Coverage statuses stay distinct inside any legal-empty result
A readable applicable section that does not disclose the fact MUST be recorded as `not_disclosed`. An express or structural exclusion MUST be recorded as `not_applicable`. Evidence that cannot uniquely determine the dimension, subject, unit, period, or header MUST be recorded as `unclear`. An unbound page, header, unit, or continuation MUST be recorded as `extraction_failed`. None of these statuses MUST be rewritten as zero or as a successful fact. If the bundle uses `legal_empty`, that result MUST wrap one of those coverage statuses and MUST NOT replace it. The word 公司 MUST NOT by itself promote the subject to the consolidated group.

#### Scenario: An unclear subject is not promoted
- **WHEN** the formal table names the subject only as 公司 and does not state a consolidated or parent-only basis
- **THEN** the subject remains unclear
- **AND** it is not promoted to the consolidated group by default

#### Scenario: A legal-empty result keeps the coverage status
- **WHEN** a research bundle records `legal_empty` for a missing segment fact
- **THEN** the record still carries `not_disclosed`, `not_applicable`, `unclear`, or `extraction_failed`
- **AND** `legal_empty` does not replace that coverage status

### Requirement: Dossiers precede field obligations
Field labels required, conditional, optional, `not_disclosed`, `not_applicable`, `unclear`, and `extraction_failed` MUST be written only after each candidate report has its own segment-financials dossier. The dossier MUST identify the formal table, its dimension, and whether revenue, cost, reported margin, and elimination are present. Page numbers and segment names are dossier fixtures, not hard-coded extraction rules. The defining sample MUST show at least two real disclosure forms before implementation is authorized.

#### Scenario: No dossier means no field obligation
- **WHEN** a candidate report does not yet have a segment-financials dossier
- **THEN** its fields are not marked required, conditional, or optional for this change
- **AND** prior gold annotations are not written into runtime fields

### Requirement: Research output stays outside common-core production
Implementation after scope and dossier approval MUST reuse the existing chapter and the research isolation bundle. Output MUST remain `accepted_for_review` in a new isolation directory. The processing identity MUST remain `{"rules":"company_profile_common_core.v1","owned_page_facts":"v8","material_input_facts":"v1"}`. The change MUST NOT write common-core production, publication, closure, completed mode, or a checkpoint, and MUST NOT modify existing operating-quantity or material-input replay artifacts. `scale_quality_claim_allowed` MUST remain false and `production_authorization` MUST remain `not_authorized`. Recall, accuracy, critical numeric errors, and the gate MUST NOT be preset.

#### Scenario: Scope approval does not authorize production
- **WHEN** the 1.1 review accepts this scope
- **THEN** production authorization remains `not_authorized`
- **AND** no segment-financials metric or gate is filled
- **AND** existing replay artifacts stay unchanged
