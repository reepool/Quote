# scope-manufacturing-materials-stage4-material-input-four-report-successor Specification

## Purpose

This successor observes `extract_material_inputs` on the four approved 2025 reports under plan `manufacturing_materials_stage4_material_inputs.2026-09-30.3`. The closed source review is recall 23/23, accuracy 23/23, critical numeric errors 0, and `expansion_gates_met=true` only for that plan, those four reports, and that chapter. `302132.SZ` remains coverage-only `legal_empty` and adds no `material_input` fact. `stage4_aggregate_expansion_gates_met` stays false. The observation does not rewrite MI-2 or the operating-quantity and segment-financial failures. `production_authorization` stays `not_authorized` and `scale_quality_claim_allowed` stays false.

## Requirements

### Requirement: The successor scope stays closed until review
Before the 1.1 review passes, the change MUST only contain its scope documents. It MUST NOT change Python, enqueue, replay, write a source-review, or prefill recall, accuracy, critical numeric errors, or a gate. It MUST NOT modify MI-1, MI-2, the archived `302132.SZ` dossier, the archived assessment, the aggregate ledger, or any historical replay.

#### Scenario: Scope review does not start the replay
- **WHEN** the 1.1 review has not yet accepted this change
- **THEN** no four-report material-input enqueue or replay exists
- **AND** the three-report MI-2 artifacts remain unchanged

### Requirement: The successor uses one chapter and one four-report set
The successor MUST use only the existing chapter `extract_material_inputs` and the existing `replay_material_input_research` path. It MUST NOT create a second extractor. The only report set MUST be `300750.SZ` on SZSE, `603659.SH` on SSE, `920015.BJ` on BSE, and `302132.SZ` on SZSE, each with its approved report identifier, document version, report period `2025-12-31`, and PDF content hash from manifest `manufacturing_materials.2026-09-03.4`. The plan version MUST be `manufacturing_materials_stage4_material_inputs.2026-09-30.3`. Plans `.2026-09-26.1` and `.2026-09-26.2` MUST remain the owners of MI-1 and MI-2.

#### Scenario: A new plan does not replace the three-report result
- **WHEN** the successor plan is named
- **THEN** `302132.SZ` is not added to the MI-2 result
- **AND** the historical `.2026-09-26.2` replay stays unchanged

### Requirement: Named inputs stay distinct from lawful coverage
A named material input MUST be a source-native name bound by the same report sentence to the company's own production or operating input. Energy, a generic direct-material or material-cost label, a raw-material inventory amount, a supplier total, a related-party row whose content is only "采购商品", and an accounting-policy phrase MUST NOT by themselves become a `material_input` fact. When `302132.SZ` is fully reviewed and still has no named material, the successor MUST record evidenced `not_disclosed` or `not_applicable` coverage and MUST NOT invent a fact. `not_disclosed` MUST NOT be recorded as `extraction_failed`.

#### Scenario: A complete read without a named material stays coverage
- **WHEN** the four-report successor reviews `302132.SZ` and finds no source-native named material
- **THEN** the report records `not_disclosed` or `not_applicable`
- **AND** no material-input fact is created for that report

### Requirement: Replay and review stay bound to the new plan
The controlled replay wrote only to `replay/20260930/` under this change, with run id `stage4-material-inputs-20260930`, disposition `accepted_for_review`, and no provider call. The bundle does not contain recall, accuracy, critical numeric errors, a gate, or a source-review. The source-review is a separate file bound to this plan's enqueue, run, and result hashes. Its closed result is recall 23/23, accuracy 23/23, critical numeric errors 0, and `expansion_gates_met=true` only for this plan, these four reports, and `extract_material_inputs`. `302132.SZ` remains coverage-only. The successor MUST NOT rewrite operating-quantity failures 26/36 or 36/38, segment-financial failures 122/132 or 102/132, or their critical numeric errors, and MUST NOT create aggregate `expansion_gates_met=true`. `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false. Restricted promotion, the six-chapter package, scale quality, and production MUST stay inactive.

#### Scenario: A material-input successor does not pass Stage 4
- **WHEN** the four-report material-input source-review is read
- **THEN** that review stays limited to this plan, these four reports, and `extract_material_inputs`
- **AND** the Stage 4 aggregate remains hold
