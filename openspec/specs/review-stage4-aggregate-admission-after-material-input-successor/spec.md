# review-stage4-aggregate-admission-after-material-input-successor Specification

## Purpose

This read-only review asks whether four current local passes can form a Stage 4 aggregate admission. The closed result is hold. Material-input `.2026-09-30.3`, operating-quantity `.2026-09-28.3`, segment-financial `.2026-09-29.3`, and counterparties `.2026-09-29.1` each stay on their own plan and the four-report set. MI-2 remains the three-report `.2026-09-26.2` observation. Operating-quantity failures 26/36 with critical numeric errors 1 and 36/38, and segment-financial failures 122/132 with critical numeric errors 4 and recall 102/132 with accuracy 114/142, remain visible. Hold does not create aggregate `expansion_gates_met=true`, a cross-chapter score, restricted promotion, the six-chapter package, scale quality, or production. `stage4_aggregate_expansion_gates_met` stays false. `production_authorization` stays `not_authorized` and `scale_quality_claim_allowed` stays false.

## Requirements

### Requirement: The admission review stays read-only until scope approval
Before the 1.1 review passes, the change MUST only contain its scope documents. It MUST NOT modify a historical ledger, an archived aggregate contract, MI-1, MI-2, a dossier, an assessment, or a replay. It MUST NOT create an aggregate row, a cross-chapter recall, accuracy, or critical-error score, or aggregate `expansion_gates_met=true`. It MUST NOT create a replay, change Python, or start restricted promotion, the six-chapter package, scale quality, or production.

#### Scenario: Scope review does not record admission
- **WHEN** the 1.1 review has not yet accepted this change
- **THEN** no aggregate admission result is written
- **AND** the archived hold-only aggregate contract remains unchanged

### Requirement: Four local passes stay on their own plans
The review MUST read only these current local passes: `extract_material_inputs` at `manufacturing_materials_stage4_material_inputs.2026-09-30.3`, `extract_operating_quantities` at `manufacturing_materials_stage4_operating_quantities.2026-09-28.3`, `extract_segment_financials` at `manufacturing_materials_stage4_segment_financials.2026-09-29.3`, and `extract_counterparties_and_concentration` at `manufacturing_materials_stage4_counterparties.2026-09-29.1`. Each pass MUST stay bound to its own plan, its own source-review, and the report set `300750.SZ`, `603659.SH`, `920015.BJ`, and `302132.SZ`. The material-input `.3` observation MUST remain independent of MI-2 and MUST NOT add `302132.SZ` to the three-report result.

#### Scenario: The four-report material-input pass does not rewrite MI-2
- **WHEN** the review lists material-input `.2026-09-30.3`
- **THEN** that row stays a separate observation
- **AND** MI-2 remains the three-report plan `.2026-09-26.2`

### Requirement: Failed observations remain visible
The review MUST retain operating-quantity recall 26/36 with accuracy 26/27 and critical numeric errors 1, operating-quantity recall 36/38 with accuracy 36/36 and critical numeric errors 0, segment-financial recall 122/132 with accuracy 129/133 and critical numeric errors 4, and segment-financial recall 102/132 with accuracy 114/142 and critical numeric errors 0. A later local true MUST NOT erase or replace those rows. `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false. Publication, closure, mode, identity, and checkpoint MUST remain unchanged.

#### Scenario: A later local pass leaves the earlier failure intact
- **WHEN** a chapter has both a failed historical observation and a later four-report local true
- **THEN** both rows remain visible with their own plans and gates
- **AND** the later true is not added to the failed row

### Requirement: A new admission judgment cannot edit the old archive
The admission judgment recorded in this change is hold. It MUST NOT edit the archived hold-only aggregate contract. Because the retained failed observations are still present, the judgment MUST NOT create aggregate `expansion_gates_met=true`. `stage4_aggregate_expansion_gates_met` MUST remain false. A future resumption MUST be a new aggregate-gate contract and MUST NOT be inferred from this archive.

#### Scenario: Retained failures block an aggregate true
- **WHEN** the admission judgment finds the operating-quantity or segment-financial failures still retained
- **THEN** the admission result is hold
- **AND** no aggregate true and no production admission are created
