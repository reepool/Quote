## ADDED Requirements

### Requirement: The segment-financial re-entry scope stays read-only until review
Before the 1.1 review passes, the change MUST only contain its scope documents. It MUST NOT modify a historical segment-financial observation, the Stage 4 ledger, or a historical replay. It MUST NOT create a successor, an enqueue, a replay, or a source-review. It MUST NOT change Python. It MUST NOT backfill `.2026-09-29.3` into `.2026-09-28.1` or `.2026-09-29.2`. It MUST NOT create an aggregate row, a cross-chapter recall, accuracy, or critical-error score, or aggregate `expansion_gates_met=true`. It MUST NOT start restricted promotion, the six-chapter package, scale quality, or production. `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false. Publication, closure, mode, identity, and checkpoint MUST remain unchanged.

#### Scenario: Scope review does not open a successor
- **WHEN** the 1.1 review has not yet accepted this contract
- **THEN** no segment-financial successor and no replay are created
- **AND** the historical failure rows remain unchanged

### Requirement: Three segment-financial observations stay separate
The contract MUST cover only `extract_segment_financials`. It MUST retain SF-1 at `manufacturing_materials_stage4_segment_financials.2026-09-28.1` with recall 122/132, accuracy 129/133, critical numeric errors 4, and local gate false. It MUST retain SF-2 at `manufacturing_materials_stage4_segment_financials.2026-09-29.2` with recall 102/132, accuracy 114/142, critical numeric errors 0, and local gate false. It MUST retain SF-3 at `manufacturing_materials_stage4_segment_financials.2026-09-29.3` with recall 132/132, accuracy 144/144, and critical numeric errors 0. SF-3's existing local gate MUST stay limited to that plan and chapter. The contract MUST NOT prefill a new successor recall, accuracy, or critical-error count. Critical numeric errors 4 MUST NOT be rewritten as 0.

#### Scenario: The local pass does not erase the critical errors
- **WHEN** the contract cites the `.2026-09-29.3` local pass
- **THEN** SF-1 remains recall 122/132, accuracy 129/133, and critical numeric errors 4
- **AND** SF-2 remains recall 102/132 and accuracy 114/142

### Requirement: The report set and artifact hashes stay bound by observation
SF-1, SF-2, and SF-3 MUST use the same four reports: `300750.SZ` with `asset_3b09f6c831975c7177b6bb3287cab781` and `ver_09c0e677ec8192dc4fc12cb620069f29`, `603659.SH` with `asset_50c70429093f66b34fc57ad8f896fcee` and `ver_c867a6a692048e88fd9cb80473fbf908`, `920015.BJ` with `asset_b87f1d1a48e662dae376c540cd021f69` and `ver_cfdbd2d058af825b1fc39f494d7a9bd3`, and `302132.SZ` with `asset_0a488da55636b09107be6d719c9ebf39` and `ver_2d20ba3aebc5fac6c562cd619695995a`, each for report period 2025-12-31. Each observation MUST keep its own enqueue, run, result, and source-review hashes. SF-1 MUST remain enqueue `935578cc64a58a84d84224443354c8a6e5f0d9a6167fd1753cbf156e7079ffe3`, run `974dbbfda8608cd1de535215390a9b1accf67b8e7810a9770966620e2e45634b`, result `7c2d5ea6e6433586b46545ad2c597a3c7cc60016ca1023559e7a137c8117d859`, and source-review `ff2f01fd4365cc621dcfeac2b2778d97b446bcf3d73c1d7f3bb06cb7b9035c2d`. SF-2 MUST remain enqueue `2a4c778790678197eda9472a907b8fc1c132be7f08fdbd5d95a3a0cc86c70f19`, run `b5aad9e75ea4200a385ad107f6c044d7d14787f231156b077888018a68708b5e`, result `6ca3e2960e11f90a13aaaa75e71dc43452344864b78d10db51566f9bff6912b1`, and source-review `6851d7d6b80274a03a14b04f7fe200278a7fff53b5ea4ed734c3470bb015a0ff`. SF-3 MUST remain enqueue `0f2ebfa087f8aa4327d15d4e0366af568bcd06a41ab923ba595899ec4e3cdd9b`, run `2ab053d1b3e80b52351ea7963e45f323b97a2706971514c99486eaefd3234d1f`, result `92ec1e2a8be5d4a6ee8bc5b492d7dc09a26dbc84bfbd0ad592ca5aab6cdc1eeb`, and source-review `4446fb7619f39b49e865bc563cf7673b98d2288d481c0b229de67394ee9c3c74`.

#### Scenario: One observation does not replace another observation's hashes
- **WHEN** the contract lists SF-3
- **THEN** SF-1 and SF-2 keep their own enqueue, run, result, and source-review hashes
- **AND** the four report identities stay the same across the three observations

### Requirement: Re-entry requires a new observation and leaves the failures historical
A segment-financial successor MUST NOT become the current aggregate inclusion row unless it is a new plan other than `.2026-09-28.1`, `.2026-09-29.2`, and `.2026-09-29.3`, uses the same four report identities, and has its own source-review and its own enqueue, run, result, and source-review hashes. The inclusion statement MUST live only in that new observation. It MUST state that SF-1 and SF-2 remain historical rows and are not the current inclusion rows. It MUST NOT edit the historical replay, source-review, or ledger, MUST NOT backfill SF-3 into SF-1 or SF-2, and MUST NOT add the three observations together. This contract MUST NOT create that successor and MUST NOT fill its recall, accuracy, or critical numeric errors. Until that successor is independently reviewed, SF-1 and SF-2 MUST remain in the aggregate judgment set, SF-3 MUST remain only a local pass, and `stage4_aggregate_expansion_gates_met` MUST remain false. Segment-financial re-entry alone MUST NOT reopen the aggregate gate.

#### Scenario: No successor leaves the historical failures in the judgment set
- **WHEN** no new segment-financial successor has been independently reviewed
- **THEN** the aggregate admission remains hold
- **AND** no aggregate true, repair replay, or production admission starts
