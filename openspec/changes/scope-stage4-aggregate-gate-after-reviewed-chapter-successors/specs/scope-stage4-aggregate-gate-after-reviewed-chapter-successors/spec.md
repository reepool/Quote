## ADDED Requirements

### Requirement: The aggregate scope stays closed until review
Before the 1.1 review passes, the change MUST only contain its scope documents. It MUST NOT modify a historical archive, ledger, replay, or source-review. It MUST NOT create a replay or change Python. It MUST NOT record aggregate `expansion_gates_met=true`, and it MUST NOT add chapter recall, accuracy, or critical-error scores together. It MUST NOT start restricted promotion, the six-chapter package, scale quality, or production. `production_authorization` MUST remain `not_authorized`, `scale_quality_claim_allowed` MUST remain false, and `stage4_aggregate_expansion_gates_met` MUST remain false. Publication, closure, mode, identity, and checkpoint MUST remain unchanged.

#### Scenario: Scope review does not apply the gate
- **WHEN** the 1.1 review has not yet accepted this contract
- **THEN** no aggregate pass and no aggregate hold application are recorded
- **AND** the historical failure rows remain unchanged

### Requirement: The judgment set contains four current observations
The judgment set MUST contain only `extract_material_inputs` at `manufacturing_materials_stage4_material_inputs.2026-09-30.3` with recall 23/23 and accuracy 23/23, `extract_operating_quantities` at `manufacturing_materials_stage4_operating_quantities.2026-10-01.4` with recall 38/38 and accuracy 38/38, `extract_segment_financials` at `manufacturing_materials_stage4_segment_financials.2026-10-01.4` with recall 137/137 and accuracy 144/144, and `extract_counterparties_and_concentration` at `manufacturing_materials_stage4_counterparties.2026-09-29.1` with recall 45/45 and accuracy 45/45. Each row MUST keep critical numeric errors 0 inside its own plan. The four scores MUST NOT be added across chapters.

#### Scenario: A local ratio stays inside its chapter
- **WHEN** the contract lists the four current observations
- **THEN** each ratio remains bound to its own plan and chapter
- **AND** no cross-chapter score is created

### Requirement: One report identity is shared and artifact hashes stay per chapter
Every included row MUST use `300750.SZ` with `asset_3b09f6c831975c7177b6bb3287cab781`, `ver_09c0e677ec8192dc4fc12cb620069f29`, and PDF `c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9`; `603659.SH` with `asset_50c70429093f66b34fc57ad8f896fcee`, `ver_c867a6a692048e88fd9cb80473fbf908`, and PDF `4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6`; `920015.BJ` with `asset_b87f1d1a48e662dae376c540cd021f69`, `ver_cfdbd2d058af825b1fc39f494d7a9bd3`, and PDF `4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a`; and `302132.SZ` with `asset_0a488da55636b09107be6d719c9ebf39`, `ver_2d20ba3aebc5fac6c562cd619695995a`, and PDF `605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020`. Each report period MUST be 2025-12-31. Material-input `.3` MUST keep enqueue `09edffece890b126e33679a70b19800c8ad6b4fe094efe2aebf1793b168d15f8`, run `00aa0f4d5bacbf0ff914b9cc7fc3a9740e6b60fa08a5c7c693699f5b77d1714e`, result `2e4b795206b308caa5df6be9ff03c5dd8b8b864b734414faffe6e1df35ad0ed4`, and source-review `b1346b1547b91f34c48aab1d4060062904596f534786b276cef59686bb9c6585`. Operating-quantity `.4` MUST keep enqueue `6f2363a463627a5e233a5f76e094577294b7276de90bd51db61598967e706e52`, run `977939ba5ceabe8da0238f594a284cdc6352edca5bafd9cfae8e4021d11a8788`, result `6ccd40a07803ac7411631729a1abac69debd3d10119f80260a5731e73f747add`, and source-review `1a5c4753d0a3b407c84a188d68b98258e764e9ef463293ed5bdac0d0fcff8cdf`. Segment-financial `.4` MUST keep enqueue `8c2c40e06848563723e208ca4d60d0310804ff986fa7ae86b50aecc7200d3bfa`, run `8f007a464e89adfd9bfcf80d810c27d47ce321e885d207e5faedfe6ecf897b90`, result `6c5188ef7dc43c45bb69c23478d2dbc3bd19e9e657ca3afb674ad2494c9acc59`, and source-review `0a38ac4c78d65dacd756cf310ea1033e9cd1e40e793a6e0a5a5ac256030dcfbd`. Counterparties `.1` MUST keep enqueue `7e9c1a72f11723f2d8508d751c27f8ea3f96cae048eb0ab6edc6224eb301bf7a`, run `8d3c7bd7a6c1f81a2e2b76064eb3d7fe9685d7570442a1e4d113b786a4cf0d7f`, result `b0344d3d577f423a1716b6dacc7c46b100ef3fe9ddddb5d37ef0160c2458a57a`, and source-review `b2c38f9fa1cf947ab69e542606332689ac25ea1e6df63facc50ac6a1fa56baf4`.

#### Scenario: One chapter does not lend its hashes to another
- **WHEN** the contract cites segment-financial `.2026-10-01.4`
- **THEN** that row keeps its own enqueue, run, result, and source-review hashes
- **AND** the four report identities stay the same on every included row

### Requirement: Historical observations stay outside the judgment set
MI-1 MUST remain recall 19/23 and accuracy 19/19 with critical numeric errors 0. MI-2 at `.2026-09-26.2` MUST remain a separate three-report 23/23 observation. OQ-1 MUST remain recall 26/36, accuracy 26/27, and critical numeric errors 1. OQ-2 MUST remain recall 36/38, accuracy 36/36, and critical numeric errors 0. OQ-3 at `.2026-09-28.3` MUST remain recall 38/38 and accuracy 38/38 on that old plan. SF-1 MUST remain recall 122/132, accuracy 129/133, and critical numeric errors 4. SF-2 MUST remain recall 102/132 and accuracy 114/142 with critical numeric errors 0. SF-3 at `.2026-09-29.3` MUST remain recall 132/132 and accuracy 144/144 on that old plan. Critical numeric errors 1 and 4 MUST NOT be rewritten as 0. The old hold conclusions MUST remain unchanged.

#### Scenario: A current successor does not erase a failed row
- **WHEN** the judgment set includes operating-quantity `.2026-10-01.4` or segment-financial `.2026-10-01.4`
- **THEN** OQ-1 remains critical numeric errors 1 and SF-1 remains critical numeric errors 4
- **AND** those historical rows are not members of the judgment set

### Requirement: Pass and hold are defined before they are applied
A later application of this contract MUST record a research-scope aggregate pass only when all four conditions hold: the four report identities match this contract, each included source-review remains a business pass with critical numeric errors 0, each chapter's four artifact hashes match this contract, and each included source-review's recorded semantic and coverage boundaries still hold. If any condition fails, the application MUST record hold. The application MUST NOT add the chapter scores together. This scope MUST NOT record that pass or that hold application. Until the application is complete, `stage4_aggregate_expansion_gates_met` MUST remain false. A recorded pass MUST NOT authorize the six-chapter package, scale quality, or production, and MUST at most allow a separate restricted-promotion design card. `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false.

#### Scenario: The scope review does not decide the gate
- **WHEN** the 1.1 review accepts only this frozen set and these conditions
- **THEN** no aggregate pass is written
- **AND** the current aggregate remains hold
