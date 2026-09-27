## ADDED Requirements

### Requirement: The operating-quantity slice uses only the approved research list
The defining sample MUST contain at least three official annual reports and MUST cover SZSE, SSE, and BSE. Every report MUST already belong to the approved manufacturing and materials research list: `300750.SZ`, `603659.SH`, `920015.BJ`, and `302132.SZ`. The change MUST NOT add an issuer that has not passed dossier review. At least one defining report MUST be a holdout that was not used in the material-input replay of `300750.SZ`, `603659.SH`, and `920015.BJ`. `302132.SZ` MAY be that holdout only after its own operating-quantity dossier confirms that `extract_operating_quantities` fits the report. If that dossier does not confirm fitness, the change MUST stop and MUST NOT substitute an issuer from outside the approved list. Admitting `302132.SZ` MUST NOT be treated as a new regime validation.

#### Scenario: The sample covers three exchanges and one holdout
- **WHEN** the scope review names the defining reports
- **THEN** the set contains at least three reports, covers SZSE, SSE, and BSE, and includes at least one report absent from the material-input replay
- **AND** no issuer outside `300750.SZ`, `603659.SH`, `920015.BJ`, and `302132.SZ` is added

#### Scenario: The holdout is not admitted before its dossier
- **WHEN** `302132.SZ` is only a candidate and its operating-quantity dossier has not confirmed that the chapter fits
- **THEN** it is not yet a defining report
- **AND** the change does not select a replacement issuer outside the approved list

### Requirement: The slice is the existing extract_operating_quantities chapter
The selected chapter MUST be the existing `extract_operating_quantities` task. The change MUST NOT create a new quantity chapter and MUST NOT enable the six-chapter manufacturing package, counterparties, regime, DCF, trading, or price sensitivity. The archived material-input observation MUST stay limited to its three reports, plan `.2026-09-26.2`, and `extract_material_inputs`. This change MUST NOT treat that 23/23 result, the stage-4 19/23 observation, or the fixed two-company 9/9 observation as an operating-quantity result.

#### Scenario: Only one existing chapter is selected
- **WHEN** the scope review selects the second stage-4 slice
- **THEN** the selected chapter is `extract_operating_quantities`
- **AND** counterparties, regime, DCF, trading, price sensitivity, and the six-chapter package stay inactive

### Requirement: Capacity, volume, amount, and utilization stay distinct
The chapter MUST distinguish production capacity, capacity under construction, capacity utilization, production volume, sales volume, and inventory volume by the source label. A reported capacity or utilization MUST NOT be used to calculate a production volume. A sales amount, revenue amount, or order amount MUST NOT be recorded as sales volume. An inventory balance-sheet amount MUST NOT be recorded as inventory volume. Industry knowledge MUST NOT supply a missing quantity. Each observed quantity MUST keep its source unit, period, and one-based physical page anchor. A table that continues across pages MUST anchor the row on the page where that row is printed.

#### Scenario: Capacity without output stays empty
- **WHEN** a readable operating section reports capacity or utilization and does not report production volume
- **THEN** production volume is legal empty
- **AND** no production volume is calculated from capacity or utilization

#### Scenario: Money amounts are not physical volumes
- **WHEN** the source states a sales amount or an inventory amount and does not state a physical quantity and unit
- **THEN** no sales volume or inventory volume is created from that amount

### Requirement: Empty, unclear, and extraction failure remain separate outcomes
A readable report that omits a quantity, or that expressly marks the quantity disclosure not applicable, MUST be recorded as legal empty or not applicable. Evidence that names a quantity but does not uniquely determine the metric, unit, period, subject, or table header MUST be recorded as unclear. A page, header, unit, or cross-page context that cannot be bound MUST be recorded as extraction failure. None of these outcomes MUST be rewritten as zero, a guessed unit, or a successful fact. An unclear subject MUST NOT be promoted to the consolidated group by default.

#### Scenario: An express not-applicable statement is legal empty
- **WHEN** the report states that classified physical quantities are not applicable or cannot be reported
- **THEN** the outcome is legal empty or not applicable
- **AND** no quantity is invented to fill the disclosure

#### Scenario: Unbound context is extraction failure
- **WHEN** the selected table's page, header, unit, or continuation cannot be bound
- **THEN** the outcome is extraction failure
- **AND** it is not counted as an accurate quantity

### Requirement: One entrance covers at least two disclosure forms
The same `extract_operating_quantities` entrance MUST cover at least two real disclosure forms. One form MUST separate production volume, sales volume, and inventory volume by source labels. Another form MUST be a capacity, utilization, and under-construction disclosure that can remain legal empty for production volume. The change MUST NOT accept the slice by checking only one of those forms. Page numbers and product names are dossier fixtures, not hard-coded extraction rules. Field labels required, conditional, optional, legal empty, unclear, and extraction failure MUST be written only after each report has its own operating-quantity dossier.

#### Scenario: Two forms use the same chapter
- **WHEN** one report discloses a classified physical-volume table and another discloses capacity and utilization without production volume
- **THEN** both forms are handled by `extract_operating_quantities`
- **AND** the extraction rule does not depend on one instrument, one page, or one product name

#### Scenario: Dossiers precede field obligations
- **WHEN** a candidate report does not yet have an operating-quantity dossier
- **THEN** its fields are not marked required, conditional, or optional for this change
- **AND** prior gold annotations are not written into runtime fields

### Requirement: Research output stays outside common-core production
Implementation after scope approval MUST reuse existing Evidence preparation, Stage 5 extract, repair, and verify, and the research isolation bundle. Candidates MUST remain `accepted_for_review`. The processing identity MUST remain `{"rules":"company_profile_common_core.v1","owned_page_facts":"v8","material_input_facts":"v1"}`. The change MUST NOT write common-core production, publication, closure, completed mode, or a checkpoint. `scale_quality_claim_allowed` MUST remain false and `production_authorization` MUST remain `not_authorized`.

#### Scenario: A later research run stays isolated
- **WHEN** an approved operating-quantity slice produces reviewable candidates
- **THEN** those candidates stay in the isolated research bundle with disposition `accepted_for_review`
- **AND** identity, publication, closure, completed mode, and production authorization remain unchanged

### Requirement: Scope review blocks implementation and does not presume metrics
No Python change, Evidence-plan change, dossier write, enqueue, or replay is authorized before independent scope review accepts the sample contract, the single-chapter rule, the quantity boundaries, the separate empty outcomes, the two-form entrance, and research isolation. The change MUST NOT presume recall, accuracy, critical numeric errors, or the expansion gate. It MUST NOT start another expansion or authorize scale quality or production before that review.

#### Scenario: Scope review has not passed
- **WHEN** task 1.1 is still unchecked
- **THEN** dossiers, implementation, enqueue, and replay are refused
- **AND** no recall, accuracy, critical-error, or gate value is written in advance
