## ADDED Requirements

### Requirement: The Putailai .2 observation stays a failed four-report regression
The reconciliation MUST preserve `repair-operating-quantity-603659-coverage-and-evidence-binding/replay/20260928.2` as an immutable observation. Its recorded result MUST remain source recall 36/38, source accuracy 36/36, critical numeric errors 0, coverage 13/13, and `expansion_gates_met=false`. The failure reason MUST remain the absence of `300750.SZ` page 21 power-battery sales 541 GWh and page 22 storage-battery sales 121 GWh from that bundle. The 23 Putailai target facts in that bundle MUST remain recorded as correctly delivered. The reconciliation MUST NOT rewrite that enqueue, run, result, or source_review.

#### Scenario: The historical bundle is not restated as a pass
- **WHEN** the reconciliation records the Putailai repair
- **THEN** the four-report gate remains false
- **AND** the 23 target facts remain a correct delivery inside that failed regression
- **AND** the four artifact hashes stay `b38b3fb11ea3f3a59b21f3072ac719ed7c7fbe4bc1063ea39294b9ca8afa79bc`, `387340ccfaa95237010b4fe7fa2cffd4a8467a11cc3b4b5679260ac97b246319`, `17c990a55ce099459fdf8c24683fbc34499c5819fd987e6483d93c8846ece571`, and `81ed17c742fc6f64e1e2aa88b5e5ac8068c73c036e03374b1757b3d7c45575ea`

### Requirement: The CATL .3 result does not recalculate the Putailai bundle
The archived CATL repair at plan `manufacturing_materials_stage4_operating_quantities.2026-09-28.3` MUST remain a later, independent repair. Its recall 38/38, accuracy 38/38, critical numeric errors 0, and `expansion_gates_met=true` MUST cover only that four-report slice, that plan, and `extract_operating_quantities`. Those values MUST NOT be written back into the Putailai `.2` source review and MUST NOT be described as a recount of `.2`.

#### Scenario: Two repairs keep separate gates
- **WHEN** a reader compares the Putailai `.2` review with the CATL `.3` review
- **THEN** `.2` stays 36/38 with gate false
- **AND** `.3` stays 38/38 with a gate limited to its own slice
- **AND** neither file is edited to match the other

### Requirement: The stage-4 original observation stays a third result
The original operating-quantity replay at `replay/20260928` MUST remain recall 26/36, accuracy 26/27, critical numeric errors 1, and `expansion_gates_met=false`. The material-input 23/23, the stage-4 material-input 19/23, and the fixed two-company 9/9 MUST also stay independent. This reconciliation MUST NOT merge any of these observations into the Putailai 36/38 or the CATL 38/38.

#### Scenario: Three operating-quantity results remain distinct
- **WHEN** the reconciliation lists the operating-quantity history
- **THEN** 26/36 with one critical error, 36/38 with gate false, and 38/38 with a slice-limited gate are three records
- **AND** no record is replaced by another

### Requirement: Closure archives the failed regression without a new replay
Because archived project changes already retain `expansion_gates_met=false` as historical observations, this reconciliation MUST close the Putailai repair by archiving it with the failed four-report gate still visible. It MUST NOT create another successor replay in order to obtain a true gate, and MUST NOT edit the old artifacts so that the gate becomes true. After scope approval, the archive MUST use a dated directory and a rename that preserves the four `.2` artifact bytes. `production_authorization` MUST remain `not_authorized`, and `scale_quality_claim_allowed` MUST remain false. The reconciliation MUST NOT enable the six-chapter package, counterparties, regime, DCF, trading, or price sensitivity.

#### Scenario: Scope approval selects archive over a new replay
- **WHEN** the 1.1 review accepts this reconciliation
- **THEN** the authorized close-out is an archive of the Putailai change with 36/38 and gate false preserved
- **AND** no new replay directory is created
- **AND** Python, publication, closure, mode, and checkpoint stay unchanged

#### Scenario: A rejected review does not invent a pass
- **WHEN** the 1.1 review rejects archiving a failed gate
- **THEN** the Putailai change stays active
- **AND** a later successor replay, if any, is a new change rather than an edit of `.2`
