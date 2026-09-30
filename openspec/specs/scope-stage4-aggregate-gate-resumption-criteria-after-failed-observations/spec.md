# scope-stage4-aggregate-gate-resumption-criteria-after-failed-observations Specification

## Purpose

This contract records how Stage 4 aggregate admission can be judged again while failed observations remain. The closed admission is hold. Material-input `.2026-09-30.3`, operating-quantity `.2026-09-28.3`, segment-financial `.2026-09-29.3`, and counterparties `.2026-09-29.1` each stay on their own plan and the four-report set. MI-2 remains the three-report `.2026-09-26.2` observation. Operating-quantity failures 26/36 with accuracy 26/27 and critical numeric errors 1, and 36/38 with accuracy 36/36 and critical numeric errors 0, remain visible. Segment-financial failures 122/132 with accuracy 129/133 and critical numeric errors 4, and recall 102/132 with accuracy 114/142 and critical numeric errors 0, remain visible. Later local true results do not erase those rows. Hold does not create aggregate `expansion_gates_met=true`, a cross-chapter score, a repair successor, or a replay. `stage4_aggregate_expansion_gates_met` stays false. `production_authorization` stays `not_authorized` and `scale_quality_claim_allowed` stays false. A future resumption requires a new independently reviewed aggregate contract or a same-set chapter successor, and cannot be inferred from this archive.

## Requirements

### Requirement: The resumption contract stays closed until review
Before the 1.1 review passes, the change MUST only contain its scope documents. It MUST NOT modify a historical archive, ledger, replay, or source-review. It MUST NOT create a replay, change Python, create an aggregate row, or create a cross-chapter recall, accuracy, or critical-error score. It MUST NOT prefill aggregate `expansion_gates_met=true`. It MUST NOT start an operating-quantity or segment-financial repair successor, restricted promotion, the six-chapter package, scale quality, or production. `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false.

#### Scenario: Scope review does not open a repair
- **WHEN** the 1.1 review has not yet accepted this contract
- **THEN** no repair successor and no replay are created
- **AND** the archived hold remains unchanged

### Requirement: Current local passes and the report set stay named
The contract MUST name only these current local passes: `extract_material_inputs` at `manufacturing_materials_stage4_material_inputs.2026-09-30.3`, `extract_operating_quantities` at `manufacturing_materials_stage4_operating_quantities.2026-09-28.3`, `extract_segment_financials` at `manufacturing_materials_stage4_segment_financials.2026-09-29.3`, and `extract_counterparties_and_concentration` at `manufacturing_materials_stage4_counterparties.2026-09-29.1`. Each pass MUST stay bound to its own source-review. The only report set MUST be `300750.SZ`, `603659.SH`, `920015.BJ`, and `302132.SZ`. MI-2 at `.2026-09-26.2` MUST remain a separate three-report observation.

#### Scenario: A four-report material-input pass does not rewrite MI-2
- **WHEN** the contract cites material-input `.2026-09-30.3`
- **THEN** that observation stays independent
- **AND** `302132.SZ` is not added to MI-2

### Requirement: Failed observations and critical errors stay historical
The contract MUST retain operating-quantity recall 26/36 with accuracy 26/27 and critical numeric errors 1, operating-quantity recall 36/38 with accuracy 36/36 and critical numeric errors 0, segment-financial recall 122/132 with accuracy 129/133 and critical numeric errors 4, and segment-financial recall 102/132 with accuracy 114/142 and critical numeric errors 0. A later local true MUST NOT erase, replace, or backfill those rows. Critical numeric errors 1 and 4 MUST NOT be rewritten as 0.

#### Scenario: A later local pass leaves the critical errors visible
- **WHEN** a chapter later has a local true
- **THEN** the earlier failed observation remains a separate historical row
- **AND** its critical numeric errors stay at the original value

### Requirement: Resumption needs a new same-set successor
While operating-quantity critical numeric errors 1 or segment-financial critical numeric errors 4 remain among the observations included in the judgment, the aggregate gate MUST stay hold and `stage4_aggregate_expansion_gates_met` MUST remain false. A chapter MAY re-enter a future judgment only through a new successor for that chapter, using the same four-report set, a new plan, an independent source-review, and hashes bound to that successor's enqueue, run, result, and source-review. The old failed row MUST remain. The future judgment MUST read the new artifact hashes and MUST NOT edit historical artifacts. This contract MUST NOT create that successor. Publication, closure, mode, identity, and checkpoint MUST remain unchanged.

#### Scenario: Retained critical errors block resumption
- **WHEN** no new operating-quantity or segment-financial successor has been independently reviewed
- **THEN** the aggregate gate remains hold
- **AND** no aggregate true, repair replay, or production admission starts
