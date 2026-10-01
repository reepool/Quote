# scope-stage4-operating-quantity-successor-after-failed-observations Specification

## Purpose

This successor covers only `extract_operating_quantities` at plan `manufacturing_materials_stage4_operating_quantities.2026-10-01.4`. The closed inclusion judgment is that `.4` is the current operating-quantity inclusion observation. The independent reread recorded recall 38/38, accuracy 38/38, and critical numeric errors 0. That local gate applies only to this plan, the four frozen 2025 reports, and `extract_operating_quantities`. OQ-1 remains recall 26/36, accuracy 26/27, and critical numeric errors 1. OQ-2 remains recall 36/38 and accuracy 36/36. OQ-3 remains recall 38/38 and accuracy 38/38 on `.2026-09-28.3` only. `stage4_aggregate_expansion_gates_met` stays false. `production_authorization` stays `not_authorized` and `scale_quality_claim_allowed` stays false. This archive does not start a new aggregate admission.

## Requirements

### Requirement: The operating-quantity successor scope stays closed until review
Before the 1.1 review passes, the change MUST only contain its scope documents. It MUST NOT change Python. It MUST NOT create an enqueue, a replay, or a source-review. It MUST NOT calculate a new recall, accuracy, or critical-error count, and it MUST NOT prefill 38/38 or any artifact SHA-256. It MUST NOT rewrite OQ-1 critical numeric errors 1 as 0, and it MUST NOT backfill OQ-3 into OQ-1 or OQ-2. It MUST NOT create an aggregate row, a cross-chapter score, or aggregate `expansion_gates_met=true`. It MUST NOT start restricted promotion, the six-chapter package, scale quality, or production. `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false. `stage4_aggregate_expansion_gates_met` MUST remain false. Publication, closure, mode, identity, and checkpoint MUST remain unchanged.

#### Scenario: Scope review does not start a replay
- **WHEN** the 1.1 review has not yet accepted this successor scope
- **THEN** `replay/20261001/` does not exist
- **AND** OQ-1, OQ-2, OQ-3, and the ledger remain unchanged

### Requirement: The successor uses a new plan and the existing replay entry
The successor MUST cover only `extract_operating_quantities`. Its plan MUST be `manufacturing_materials_stage4_operating_quantities.2026-10-01.4`, and its run id MUST be `stage4-operating-quantities-20261001`. It MUST NOT reuse `.2026-09-27.1`, `.2026-09-28.2`, or `.2026-09-28.3`. A later implementation MUST call `replay_operating_quantity_research` with that explicit `plan_version` and run id, and MUST NOT change the function default away from `.2026-09-28.3`. The existing `run.json` schema MUST bind that plan through the enqueue hash and MUST NOT gain a new plan field. It MUST NOT add an extractor or change an extraction rule. Output MUST be written only under this change at `replay/20261001/`.

#### Scenario: The default plan stays on the current observation
- **WHEN** a later implementation calls the replay entry for `.4`
- **THEN** the explicit plan is `.2026-10-01.4`
- **AND** the default plan remains `.2026-09-28.3`

### Requirement: Report identity and dossier bytes stay frozen
The successor MUST use the same four report identities and PDF hashes as OQ-1, OQ-2, and OQ-3: `300750.SZ` with `asset_3b09f6c831975c7177b6bb3287cab781` and `ver_09c0e677ec8192dc4fc12cb620069f29`, `603659.SH` with `asset_50c70429093f66b34fc57ad8f896fcee` and `ver_c867a6a692048e88fd9cb80473fbf908`, `920015.BJ` with `asset_b87f1d1a48e662dae376c540cd021f69` and `ver_cfdbd2d058af825b1fc39f494d7a9bd3`, and `302132.SZ` with `asset_0a488da55636b09107be6d719c9ebf39` and `ver_2d20ba3aebc5fac6c562cd619695995a`, each for report period 2025-12-31. The PDF content hashes MUST remain `c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9`, `4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6`, `4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a`, and `605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020` respectively. The source-page set MUST be the OQ-3 set: 26–27 and 21–22 for `300750.SZ`, 14, 15, 19, and 27 for `603659.SH`, 49–50 for `920015.BJ`, and 15 for `302132.SZ`. OQ-2 added `603659.SH` page 27, and OQ-3 added `300750.SZ` pages 21–22. Historical enqueues MUST remain unchanged. The sample id `manufacturing-materials-302132-2025-regime` MUST remain a historical sample identity. The later path correction MUST point dossiers at `openspec/changes/archive/2026-09-29-scope-manufacturing-materials-stage4-operating-quantities-holdout/dossiers/` and MUST keep dossier hashes `180cbb1d5e3bcae46e4dcc78047b13b25bf0602204c9250fa5ff02c552a1b0a7`, `4458a489585741d36fe3bd44156b06067e6194bccd9adab2cbda7b51a18ebaee`, `b49b61490d9740b0218af424ee10674f8bbf5f519136edb37db53ef22905a6b8`, and `f0b8123f3f6abe9a769a0b918398808dd8060a2c1326749d9343ac4cb9aa2e36`. It MUST NOT edit dossier bytes.

#### Scenario: The path correction does not rewrite a dossier
- **WHEN** the later implementation makes the archived dossiers readable
- **THEN** the four dossier hashes remain the hashes named in this requirement
- **AND** the PDF hashes and source pages stay unchanged

### Requirement: Historical observations stay byte-stable
OQ-1 at `.2026-09-27.1` MUST remain recall 26/36, accuracy 26/27, and critical numeric errors 1, with enqueue `5fffde878890fb43c136c4e0082f4eba32fc9e65402e166a96cca164396fab7a`, run `3fa107b4bf914cf7603ee1e2e73937392a04ad95bf52df4e68d5bff304a5e9cf`, result `67e37d98ed0d0dc57f9672f6ef224482db52fc3e9ce96ece8f366866a1e87302`, and source-review `5568d4ee73cbc332c63cb935fba42a13ea99e4dfe0344f6b700ad4fcc212ab0b`. OQ-2 at `.2026-09-28.2` MUST remain recall 36/38 and accuracy 36/36, with critical numeric errors 0, enqueue `b38b3fb11ea3f3a59b21f3072ac719ed7c7fbe4bc1063ea39294b9ca8afa79bc`, run `387340ccfaa95237010b4fe7fa2cffd4a8467a11cc3b4b5679260ac97b246319`, result `17c990a55ce099459fdf8c24683fbc34499c5819fd987e6483d93c8846ece571`, and source-review `81ed17c742fc6f64e1e2aa88b5e5ac8068c73c036e03374b1757b3d7c45575ea`. OQ-3 at `.2026-09-28.3` MUST remain recall 38/38 and accuracy 38/38, with critical numeric errors 0, enqueue `30315acceaed18e870e3ded36ca5f5beb58c54e919d667b100bef8c81b9b2eb2`, run `f14af3b32355551805e5dca0c9d6e732dc06c8bed0a76c2d83fa0f28232588c5`, result `4a1f9823922881a9e210b9889067afeb0063b6106c7f5669791e6199c830d420`, and source-review `f1e041ecfc6a7d35ad041fcaef753adce13e51e21f871acf79a4052663a06345`. A later `.4` artifact MUST use different hashes. The Stage 4 ledger MUST NOT be edited.

#### Scenario: The critical error stays on the historical row
- **WHEN** the successor scope cites OQ-3
- **THEN** OQ-1 remains critical numeric errors 1
- **AND** OQ-3 remains a separate local observation

### Requirement: The successor inherits the verified quantity boundaries
The `.4` reread MUST keep power-battery sales of 541 GWh, storage-battery sales of 121 GWh, and battery-system sales of 661 GWh as separate facts. It MUST NOT add 541 and 121 into 662, and it MUST NOT replace 661 with either segment value. Processing volume and sales volume MUST remain separate. Capacity category, project stage, the qualifiers `超过` and `已达`, and inventory footnotes MUST keep their existing bindings. A disclosed omission MUST remain `not_disclosed`, and an express or structural exclusion MUST remain `not_applicable`. A `legal_empty` outcome MUST wrap that coverage status and MUST NOT become an invented quantity.

#### Scenario: The three gigawatt-hour facts stay separate
- **WHEN** the `.4` source-review reads the `300750.SZ` sales evidence
- **THEN** 541 GWh, 121 GWh, and 661 GWh remain three facts
- **AND** none of them is rewritten as 662

### Requirement: A local pass comes from the reread
A `.4` local gate MUST be true only when all four reports have been independently reread, source recall is 100%, source accuracy is 100%, critical numeric errors are 0, and the inherited quantity boundaries hold. The denominator MUST come from that reread. Complete artifact hashes MUST NOT by themselves count as this pass. The review MUST NOT prefill recall 38/38 or accuracy 38/38. Those ratios MUST remain the OQ-3 observation. Current-chapter inclusion MUST wait until that business pass and the artifact binding both hold. This successor alone MUST NOT restore the Stage 4 aggregate.

#### Scenario: The old local ratio is not copied forward
- **WHEN** the `.4` source-review is written
- **THEN** its denominator is counted from the annual reports
- **AND** the OQ-3 ratio 38/38 is not reused as that denominator
