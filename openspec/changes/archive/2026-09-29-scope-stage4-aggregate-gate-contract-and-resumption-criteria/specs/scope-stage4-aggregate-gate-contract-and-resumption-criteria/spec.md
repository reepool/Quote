## ADDED Requirements

### Requirement: The contract names chapters, plans, and one report set
The aggregate contract MUST include only `extract_material_inputs` at plan `manufacturing_materials_stage4_material_inputs.2026-09-26.2`, `extract_operating_quantities` at plan `manufacturing_materials_stage4_operating_quantities.2026-09-28.3`, `extract_segment_financials` at plan `manufacturing_materials_stage4_segment_financials.2026-09-29.3`, and `extract_counterparties_and_concentration` at plan `manufacturing_materials_stage4_counterparties.2026-09-29.1`. It MUST NOT include business overview, business regime, or the six-chapter package. The only report set that can satisfy the aggregate MUST be `300750.SZ` on SZSE, `603659.SH` on SSE, `920015.BJ` on BSE, and `302132.SZ` on SZSE, together in that same set. A three-report material-input result MUST NOT be rewritten to include `302132.SZ`.

#### Scenario: A smaller report set does not satisfy the aggregate
- **WHEN** a chapter gate is true only for three of the four approved reports
- **THEN** that gate does not satisfy the aggregate report set
- **AND** the missing report is not added after the fact
- **AND** another chapter's four-report result does not replace it

### Requirement: This contract is hold-only
The 1.1 review MUST classify this contract as hold-only. The included material-input plan `manufacturing_materials_stage4_material_inputs.2026-09-26.2` has only three reports, so it cannot satisfy the four-report set that includes `302132.SZ`. Under this contract the current aggregate MUST be impossible to pass. The review MUST NOT create a four-report material-input successor or edit the `.2` observation. Task 2.2 has not yet recorded the admission hold, and this requirement MUST NOT prefill aggregate `expansion_gates_met=true` or a cross-chapter metric.

#### Scenario: The included material-input plan blocks the aggregate
- **WHEN** the aggregate requires `302132.SZ` in the same report set as the other three approved reports
- **THEN** plan `.2026-09-26.2` fails that requirement
- **AND** the contract remains hold-only until a later contract names a different material-input plan

### Requirement: Each included plan keeps its own review binding
Each included plan MUST keep a source-review bound to that plan's own enqueue, run, and result. The identity binding MUST copy only that row's instrument, report identifier, document version, report period, and processing identity. A missing processing identity MUST stay blank. Historical failed observations MUST remain visible with their original recall, accuracy, critical numeric errors, and false gates, including operating-quantity critical numeric errors 1 and segment-financial critical numeric errors 4. A later local true MUST NOT backfill, replace, or be added across chapters.

#### Scenario: A later pass leaves the earlier failure intact
- **WHEN** a chapter has both a failed historical observation and a later local true
- **THEN** both remain separate rows
- **AND** the later true does not change the earlier critical numeric errors or gate

### Requirement: An unmet condition remains a hold
Before the 1.1 review passes, the change MUST NOT change Python, enqueue, replay, archived artifacts, the Stage 4 ledger, publication, closure, mode, identity, or checkpoint, and MUST NOT prefill an aggregate gate or a cross-chapter metric. When the contract is later applied, any unmet condition MUST produce hold. Hold MUST NOT create aggregate `expansion_gates_met=true`, MUST NOT authorize a restricted-promotion design or implementation, and MUST NOT start the six-chapter package, scale quality, or production. A future aggregate true MUST at most allow a separate restricted-promotion design card. `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false.

#### Scenario: Scope approval does not resume implementation
- **WHEN** the 1.1 review has not yet accepted this contract
- **THEN** no aggregate judgment is recorded as complete
- **AND** no promotion implementation or successor replay starts
