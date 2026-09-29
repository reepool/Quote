# Aggregate contract binding

This file copies the already reviewed ledger at `openspec/changes/archive/2026-09-29-review-stage4-aggregate-admission-and-restricted-promotion/ledger.md`. It does not recompute the nine observations, does not edit that ledger, and does not record a 2.2 admission result. There is no aggregate row and no cross-chapter score.

## Included chapters and plans

| Chapter | Fixed plan | Ledger row | Report set on that row |
|---|---|---|---|
| `extract_material_inputs` | `manufacturing_materials_stage4_material_inputs.2026-09-26.2` | MI-2 | three reports |
| `extract_operating_quantities` | `manufacturing_materials_stage4_operating_quantities.2026-09-28.3` | OQ-3 | four reports |
| `extract_segment_financials` | `manufacturing_materials_stage4_segment_financials.2026-09-29.3` | SF-3 | four reports |
| `extract_counterparties_and_concentration` | `manufacturing_materials_stage4_counterparties.2026-09-29.1` | CP-1 | four reports |

The required aggregate report set is one set of four reports:

| Instrument | Exchange | report_id | document_version | report_period |
|---|---|---|---|---|
| 300750.SZ | SZSE | `asset_3b09f6c831975c7177b6bb3287cab781` | `ver_09c0e677ec8192dc4fc12cb620069f29` | 2025-12-31 |
| 603659.SH | SSE | `asset_50c70429093f66b34fc57ad8f896fcee` | `ver_c867a6a692048e88fd9cb80473fbf908` | 2025-12-31 |
| 920015.BJ | BSE | `asset_b87f1d1a48e662dae376c540cd021f69` | `ver_cfdbd2d058af825b1fc39f494d7a9bd3` | 2025-12-31 |
| 302132.SZ | SZSE | `asset_0a488da55636b09107be6d719c9ebf39` | `ver_2d20ba3aebc5fac6c562cd619695995a` | 2025-12-31 |

## Included-plan bindings

Copied from the ledger. MI-2 and OQ-3 have no `processing_identity` in their archived enqueue, run, or result. SF-3 and CP-1 copy only their own result identity: `rules=company_profile_common_core.v1`, `owned_page_facts=v8`, `material_input_facts=v1`. That identity is not copied onto MI-2 or OQ-3.

| Row | Archive directory | Replay | enqueue SHA-256 | run SHA-256 | result SHA-256 | source-review SHA-256 |
|---|---|---|---|---|---|---|
| MI-2 | `2026-09-27-repair-material-input-procurement-and-materials-table-coverage` | `replay/20260926.2` | `4152290506bdb18f0aa6d48187c54243bca4ca7c9249fe927723636608995ea8` | `dbaa0aef4edf1a5f516f55c7349b8cbb82be900f0d8df1a8a051d04bc3eadf87` | `d4c81a5e554c01c619e89885f1fac304e09688c582a603919256cf0a7947ff51` | `b6aec33bcf7402e4d880a19f232bc1f7001e2c90793b900136e6fcf3cab6f6f9` |
| OQ-3 | `2026-09-28-repair-operating-quantity-catl-segment-sales-coverage` | `replay/20260928.3` | `30315acceaed18e870e3ded36ca5f5beb58c54e919d667b100bef8c81b9b2eb2` | `f14af3b32355551805e5dca0c9d6e732dc06c8bed0a76c2d83fa0f28232588c5` | `4a1f9823922881a9e210b9889067afeb0063b6106c7f5669791e6199c830d420` | `f1e041ecfc6a7d35ad041fcaef753adce13e51e21f871acf79a4052663a06345` |
| SF-3 | `2026-09-29-repair-segment-financial-footnote-dimension-binding` | `replay/20260929.3` | `0f2ebfa087f8aa4327d15d4e0366af568bcd06a41ab923ba595899ec4e3cdd9b` | `2ab053d1b3e80b52351ea7963e45f323b97a2706971514c99486eaefd3234d1f` | `92ec1e2a8be5d4a6ee8bc5b492d7dc09a26dbc84bfbd0ad592ca5aab6cdc1eeb` | `4446fb7619f39b49e865bc563cf7673b98d2288d481c0b229de67394ee9c3c74` |
| CP-1 | `2026-09-29-scope-manufacturing-materials-stage4-counterparties-and-concentration` | `replay/20260929` | `7e9c1a72f11723f2d8508d751c27f8ea3f96cae048eb0ab6edc6224eb301bf7a` | `8d3c7bd7a6c1f81a2e2b76064eb3d7fe9685d7570442a1e4d113b786a4cf0d7f` | `b0344d3d577f423a1716b6dacc7c46b100ef3fe9ddddb5d37ef0160c2458a57a` | `b2c38f9fa1cf947ab69e542606332689ac25ea1e6df63facc50ac6a1fa56baf4` |

MI-2's own frozen reports are only `300750.SZ`, `603659.SH`, and `920015.BJ`. `302132.SZ` is not part of that row.

## Incompatibility

The aggregate report set requires `302132.SZ` together with the other three reports. MI-2 remains a three-report observation. This binding does not add `302132.SZ` to MI-2 and does not replace MI-2 with OQ-3, SF-3, or CP-1. This file does not turn that fact into a 2.2 admission result.

## Retained failed observations

These rows stay at their original values. Later local true rows do not replace them, and their metrics are not added together.

| Row | Plan | Recall | Accuracy | Critical numeric errors | Gate |
|---|---|---:|---:|---:|---|
| MI-1 | `manufacturing_materials_stage4_material_inputs.2026-09-26.1` | 19/23 | 19/19 | 0 | false |
| OQ-1 | `manufacturing_materials_stage4_operating_quantities.2026-09-27.1` | 26/36 | 26/27 | 1 | false |
| OQ-2 | `manufacturing_materials_stage4_operating_quantities.2026-09-28.2` | 36/38 | 36/36 | 0 | false |
| SF-1 | `manufacturing_materials_stage4_segment_financials.2026-09-28.1` | 122/132 | 129/133 | 4 | false |
| SF-2 | `manufacturing_materials_stage4_segment_financials.2026-09-29.2` | 102/132 | 114/142 | 0 | false |

| Row | Archive directory | enqueue SHA-256 | run SHA-256 | result SHA-256 | source-review SHA-256 |
|---|---|---|---|---|---|
| MI-1 | `2026-09-26-scope-manufacturing-materials-stage4-minimum-slice` | `0a7fc577840ce3bed45c02ec268fecf289b776104f05ae0ff3806c2daa0f1a84` | `f80c96e5523704d05cc880c3a56cd4201549081f2fca9a10e8a0433629819852` | `9a7444d285b8c8897f5206cce0abe00420edc12ba133ae29e3ccd795e82279b9` | `9df647e0cc2674c483bd3629b85a90bb0592b7f27e120e5ffccae29de08275b1` |
| OQ-1 | `2026-09-29-scope-manufacturing-materials-stage4-operating-quantities-holdout` | `5fffde878890fb43c136c4e0082f4eba32fc9e65402e166a96cca164396fab7a` | `3fa107b4bf914cf7603ee1e2e73937392a04ad95bf52df4e68d5bff304a5e9cf` | `67e37d98ed0d0dc57f9672f6ef224482db52fc3e9ce96ece8f366866a1e87302` | `5568d4ee73cbc332c63cb935fba42a13ea99e4dfe0344f6b700ad4fcc212ab0b` |
| OQ-2 | `2026-09-28-repair-operating-quantity-603659-coverage-and-evidence-binding` | `b38b3fb11ea3f3a59b21f3072ac719ed7c7fbe4bc1063ea39294b9ca8afa79bc` | `387340ccfaa95237010b4fe7fa2cffd4a8467a11cc3b4b5679260ac97b246319` | `17c990a55ce099459fdf8c24683fbc34499c5819fd987e6483d93c8846ece571` | `81ed17c742fc6f64e1e2aa88b5e5ac8068c73c036e03374b1757b3d7c45575ea` |
| SF-1 | `2026-09-29-scope-manufacturing-materials-stage4-segment-financials` | `935578cc64a58a84d84224443354c8a6e5f0d9a6167fd1753cbf156e7079ffe3` | `974dbbfda8608cd1de535215390a9b1accf67b8e7810a9770966620e2e45634b` | `7c2d5ea6e6433586b46545ad2c597a3c7cc60016ca1023559e7a137c8117d859` | `ff2f01fd4365cc621dcfeac2b2778d97b446bcf3d73c1d7f3bb06cb7b9035c2d` |
| SF-2 | `2026-09-29-repair-segment-financial-column-binding-and-cell-coverage` | `2a4c778790678197eda9472a907b8fc1c132be7f08fdbd5d95a3a0cc86c70f19` | `b5aad9e75ea4200a385ad107f6c044d7d14787f231156b077888018a68708b5e` | `6ca3e2960e11f90a13aaaa75e71dc43452344864b78d10db51566f9bff6912b1` | `6851d7d6b80274a03a14b04f7fe200278a7fff53b5ea4ed734c3470bb015a0ff` |
