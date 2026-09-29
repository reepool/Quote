# reconcile-segment-financial-repair-observations Specification

## Purpose

This ledger closes the two failed segment-financial observations without rewriting them. `replay/20260928` stays recall 122/132, accuracy 129/133, critical numeric errors 4, and `expansion_gates_met=false`. `replay/20260929.2` stays recall 102/132, accuracy 114/142, critical numeric errors 0, and `expansion_gates_met=false`. The archived footnote repair's 132/132 and 144/144 remain a later slice-limited pass and are not written back. The close-out archives both failed changes. It does not create a successor replay, and it does not authorize scale quality or production.

## Requirements

### Requirement: The original segment-financial observation stays a failed replay
The reconciliation MUST preserve `scope-manufacturing-materials-stage4-segment-financials/replay/20260928` as an immutable observation. Its recorded result MUST remain source recall 122/132, source accuracy 129/133, critical numeric errors 4, and `expansion_gates_met=false`. The failure reason MUST remain the page-25 column-role misbinding and the ten missing current-period cells. Task 3.3 MUST remain unchecked. The reconciliation MUST NOT rewrite that enqueue, run, result, or source_review.

#### Scenario: The original bundle is not restated as a pass
- **WHEN** the reconciliation records the original segment-financial replay
- **THEN** the four-report gate remains false
- **AND** task 3.3 remains unchecked
- **AND** the four artifact hashes stay `935578cc64a58a84d84224443354c8a6e5f0d9a6167fd1753cbf156e7079ffe3`, `974dbbfda8608cd1de535215390a9b1accf67b8e7810a9770966620e2e45634b`, `7c2d5ea6e6433586b46545ad2c597a3c7cc60016ca1023559e7a137c8117d859`, and `ff2f01fd4365cc621dcfeac2b2778d97b446bcf3d73c1d7f3bb06cb7b9035c2d`

### Requirement: The column-binding observation stays a failed source-dimension regression
The reconciliation MUST preserve `repair-segment-financial-column-binding-and-cell-coverage/replay/20260929.2` as an immutable observation. Its recorded result MUST remain source recall 102/132, source accuracy 114/142, critical numeric errors 0, and `expansion_gates_met=false`. The failure reason MUST remain that page 139 stored `地区分部` and `业务分部` as generic `报告分部`, and page 178 stored `报告分部的财务信息` as generic `报告分部`. Task 3.2 MUST remain unchecked. The reconciliation MUST NOT rewrite that enqueue, run, result, or source_review.

#### Scenario: The column-binding bundle is not restated as a pass
- **WHEN** the reconciliation records the column-binding repair
- **THEN** the four-report gate remains false
- **AND** critical numeric errors remain 0 while recall and accuracy remain below 1
- **AND** the four artifact hashes stay `2a4c778790678197eda9472a907b8fc1c132be7f08fdbd5d95a3a0cc86c70f19`, `b5aad9e75ea4200a385ad107f6c044d7d14787f231156b077888018a68708b5e`, `6ca3e2960e11f90a13aaaa75e71dc43452344864b78d10db51566f9bff6912b1`, and `6851d7d6b80274a03a14b04f7fe200278a7fff53b5ea4ed734c3470bb015a0ff`

### Requirement: The footnote repair does not recalculate either failed bundle
The archived footnote-dimension repair at plan `manufacturing_materials_stage4_segment_financials.2026-09-29.3` MUST remain a later, independent repair. Its recall 132/132, accuracy 144/144, critical numeric errors 0, and `expansion_gates_met=true` MUST cover only the four frozen 2025 reports, that plan, and `extract_segment_financials`. Those values MUST NOT be written back into `replay/20260928` or `replay/20260929.2`, and MUST NOT be described as a recount of either failed observation.

#### Scenario: Three segment-financial results remain distinct
- **WHEN** a reader compares the three segment-financial reviews
- **THEN** 122/132 with four critical errors, 102/132 with gate false, and 132/132 with a slice-limited gate are three records
- **AND** no record is replaced by another

### Requirement: Closure archives both failed changes without a new replay
Because archived project changes already retain `expansion_gates_met=false` as historical observations, this reconciliation MUST close the original segment-financial change and the column-binding repair by archiving each with its failed gate still visible. It MUST NOT create another successor replay in order to obtain a true gate, and MUST NOT edit the old artifacts so that either gate becomes true. After scope approval, each archive MUST use a dated directory and a rename that preserves the artifact bytes. `production_authorization` MUST remain `not_authorized`, and `scale_quality_claim_allowed` MUST remain false. The reconciliation MUST NOT enable counterparties, the six-chapter package, regime review, DCF, trading, or price sensitivity. It MUST NOT reconcile the operating-quantity 26/36 observation.

#### Scenario: Scope approval selects archive over a new replay
- **WHEN** the 1.1 review accepts this reconciliation
- **THEN** the authorized close-out is an archive of both failed changes with their gates still false
- **AND** no new replay directory is created
- **AND** Python, publication, closure, mode, and checkpoint stay unchanged

#### Scenario: A rejected review does not invent a pass
- **WHEN** the 1.1 review rejects archiving a failed gate
- **THEN** both changes stay active
- **AND** a later successor replay, if any, is a new change rather than an edit of `.1` or `.2`

### Requirement: The completed archive keeps both gates false
The reconciliation close-out records that both failed changes are archived and their gates remain false. The original change is at `openspec/changes/archive/2026-09-29-scope-manufacturing-materials-stage4-segment-financials/`, with task 3.3 unchecked. The column-binding change is at `openspec/changes/archive/2026-09-29-repair-segment-financial-column-binding-and-cell-coverage/`, with task 3.2 unchecked. The footnote `.3` observation remains the independent slice result of recall 132/132, accuracy 144/144, critical numeric errors 0, and `expansion_gates_met=true` for only the four frozen 2025 reports, plan `manufacturing_materials_stage4_segment_financials.2026-09-29.3`, and `extract_segment_financials`. No further successor replay MUST be created to archive these failed observations.

#### Scenario: Close-out leaves three records in place
- **WHEN** this reconciliation is archived
- **THEN** the original gate stays false at 122/132 and 129/133 with four critical errors
- **AND** the column-binding gate stays false at 102/132 and 114/142 with zero critical errors
- **AND** the `.3` slice gate stays limited to its own plan and chapter
