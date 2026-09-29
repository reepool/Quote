## ADDED Requirements

### Requirement: The original operating-quantity observation stays a failed holdout
The reconciliation MUST preserve `scope-manufacturing-materials-stage4-operating-quantities-holdout/replay/20260928` as an immutable observation. Its recorded result MUST remain source recall 26/36, source accuracy 26/27, critical numeric errors 1, and `expansion_gates_met=false`. The failure reason MUST remain the page-15 PVDF and 勃姆石和氧化铝 evidence/object binding. Tasks 3.2 and 3.3 MUST remain unchecked. The reconciliation MUST NOT rewrite that enqueue, run, result, or source_review.

#### Scenario: The original bundle is not restated as a pass
- **WHEN** the reconciliation records the original operating-quantity holdout
- **THEN** the four-report gate remains false
- **AND** tasks 3.2 and 3.3 remain unchecked
- **AND** the four artifact hashes stay `5fffde878890fb43c136c4e0082f4eba32fc9e65402e166a96cca164396fab7a`, `3fa107b4bf914cf7603ee1e2e73937392a04ad95bf52df4e68d5bff304a5e9cf`, `67e37d98ed0d0dc57f9672f6ef224482db52fc3e9ce96ece8f366866a1e87302`, and `5568d4ee73cbc332c63cb935fba42a13ea99e4dfe0344f6b700ad4fcc212ab0b`

### Requirement: The Putailai repair does not recalculate the original holdout
The archived Putailai repair at plan `manufacturing_materials_stage4_operating_quantities.2026-09-28.2` MUST remain a later, independent repair. Its recall 36/38, accuracy 36/36, critical numeric errors 0, and `expansion_gates_met=false` MUST stay visible. The 23 Putailai target facts MUST remain a correct delivery inside that failed regression. Those values MUST NOT be written back into `replay/20260928`. Its artifact hashes MUST stay `b38b3fb11ea3f3a59b21f3072ac719ed7c7fbe4bc1063ea39294b9ca8afa79bc`, `387340ccfaa95237010b4fe7fa2cffd4a8467a11cc3b4b5679260ac97b246319`, `17c990a55ce099459fdf8c24683fbc34499c5819fd987e6483d93c8846ece571`, and `81ed17c742fc6f64e1e2aa88b5e5ac8068c73c036e03374b1757b3d7c45575ea`.

#### Scenario: The Putailai gate stays false
- **WHEN** a reader compares the original holdout with the Putailai `.2` review
- **THEN** `.1` stays 26/36 with one critical error
- **AND** `.2` stays 36/38 with gate false and 23 correctly delivered target facts
- **AND** neither file is edited to match the other

### Requirement: The CATL repair does not recalculate either earlier bundle
The archived CATL repair at plan `manufacturing_materials_stage4_operating_quantities.2026-09-28.3` MUST remain a later, independent repair. Its recall 38/38, accuracy 38/38, critical numeric errors 0, and `expansion_gates_met=true` MUST cover only the four frozen 2025 reports, that plan, and `extract_operating_quantities`. Those values MUST NOT be written back into `replay/20260928` or `replay/20260928.2`, and MUST NOT be described as a recount of either earlier observation. Its enqueue, run, and result hashes MUST stay `30315acceaed18e870e3ded36ca5f5beb58c54e919d667b100bef8c81b9b2eb2`, `f14af3b32355551805e5dca0c9d6e732dc06c8bed0a76c2d83fa0f28232588c5`, and `4a1f9823922881a9e210b9889067afeb0063b6106c7f5669791e6199c830d420`.

#### Scenario: Three operating-quantity results remain distinct
- **WHEN** a reader compares the three operating-quantity reviews
- **THEN** 26/36 with one critical error, 36/38 with gate false, and 38/38 with a slice-limited gate are three records
- **AND** no record is replaced by another

### Requirement: Closure archives the failed holdout without a new replay
Because archived project changes already retain `expansion_gates_met=false` as historical observations, this reconciliation MUST close the original operating-quantity holdout by archiving it with the failed gate still visible. It MUST NOT create another successor replay in order to obtain a true gate, and MUST NOT edit the old artifacts so that the gate becomes true. After scope approval, the archive MUST use a dated directory and a rename that preserves the four `.1` artifact bytes. `production_authorization` MUST remain `not_authorized`, and `scale_quality_claim_allowed` MUST remain false. The reconciliation MUST NOT enable the six-chapter package, counterparties, regime, DCF, trading, or price sensitivity. It MUST NOT rewrite the segment-financial observations.

#### Scenario: Scope approval selects archive over a new replay
- **WHEN** the 1.1 review accepts this reconciliation
- **THEN** the authorized close-out is an archive of the original holdout with 26/36 and gate false preserved
- **AND** no new replay directory is created
- **AND** Python, publication, closure, mode, and checkpoint stay unchanged

#### Scenario: A rejected review does not invent a pass
- **WHEN** the 1.1 review rejects archiving a failed gate
- **THEN** the original holdout stays active
- **AND** a later successor replay, if any, is a new change rather than an edit of `.1`
