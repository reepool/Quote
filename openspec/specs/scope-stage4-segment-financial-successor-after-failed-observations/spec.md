# scope-stage4-segment-financial-successor-after-failed-observations Specification

## Purpose

This successor covers only `extract_segment_financials` at plan `manufacturing_materials_stage4_segment_financials.2026-10-01.4`. The closed inclusion judgment is that `.4` is the current segment-financial inclusion observation. The independent reread recorded recall 137/137, accuracy 144/144, and critical numeric errors 0. That local gate applies only to this plan, the four frozen 2025 reports, and `extract_segment_financials`. SF-1 remains recall 122/132, accuracy 129/133, and critical numeric errors 4. SF-2 remains recall 102/132 and accuracy 114/142. SF-3 remains recall 132/132 and accuracy 144/144 on `.2026-09-29.3` only. `stage4_aggregate_expansion_gates_met` stays false. `production_authorization` stays `not_authorized` and `scale_quality_claim_allowed` stays false. This archive does not start an operating-quantity successor or a new aggregate admission.

## Requirements

### Requirement: The successor scope stays closed until review
Before the 1.1 review passes, the change MUST only contain its scope documents. It MUST NOT change Python. It MUST NOT create an enqueue, a replay, or a source-review. It MUST NOT calculate a new recall, accuracy, or critical-error count, and it MUST NOT prefill those values or any artifact SHA-256. It MUST NOT backfill SF-1 or SF-2, and it MUST NOT rewrite the SF-3 result of recall 132/132 and accuracy 144/144 as an older observation. It MUST NOT create an aggregate row, a cross-chapter score, or aggregate `expansion_gates_met=true`. It MUST NOT start an operating-quantity successor, restricted promotion, the six-chapter package, scale quality, or production. `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false. Publication, closure, mode, identity, and checkpoint MUST remain unchanged.

#### Scenario: Scope review does not start a replay
- **WHEN** the 1.1 review has not yet accepted this successor scope
- **THEN** `replay/20261001/` does not exist
- **AND** SF-1, SF-2, SF-3, the ledger, and their replays remain unchanged

### Requirement: The successor uses a new plan and the frozen four reports
The successor MUST cover only `extract_segment_financials`. Its plan MUST be `manufacturing_materials_stage4_segment_financials.2026-10-01.4`. It MUST NOT reuse `manufacturing_materials_stage4_segment_financials.2026-09-28.1`, `manufacturing_materials_stage4_segment_financials.2026-09-29.2`, or `manufacturing_materials_stage4_segment_financials.2026-09-29.3`. It MUST use the same four reports as SF-1, SF-2, and SF-3: `300750.SZ` with `asset_3b09f6c831975c7177b6bb3287cab781` and `ver_09c0e677ec8192dc4fc12cb620069f29`, `603659.SH` with `asset_50c70429093f66b34fc57ad8f896fcee` and `ver_c867a6a692048e88fd9cb80473fbf908`, `920015.BJ` with `asset_b87f1d1a48e662dae376c540cd021f69` and `ver_cfdbd2d058af825b1fc39f494d7a9bd3`, and `302132.SZ` with `asset_0a488da55636b09107be6d719c9ebf39` and `ver_2d20ba3aebc5fac6c562cd619695995a`, each for report period 2025-12-31. The sample id `manufacturing-materials-302132-2025-regime` MUST remain a historical sample identity and MUST NOT be treated as a new material-input or segment-financial conclusion. A later implementation MUST call `replay_segment_financial_research` with explicit `plan_version` `.2026-10-01.4` and MUST NOT change that function's default away from `.2026-09-29.3`. It MUST NOT add a parser repair. The four PDF content hashes MUST remain `c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9` for `300750.SZ`, `4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6` for `603659.SH`, `4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a` for `920015.BJ`, and `605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020` for `302132.SZ`. The source scopes MUST remain physical pages 24–25, 195, and 223 for `300750.SZ`; 18–19 for `603659.SH`; 16, 17, and 139 for `920015.BJ`; and 14–15 and 178 for `302132.SZ`. Archived dossier bytes that match the SF-3 dossier hashes MUST remain unchanged.

#### Scenario: The new plan stays distinct from the three historical plans
- **WHEN** the successor scope names its plan
- **THEN** the plan is `.2026-10-01.4`
- **AND** SF-1, SF-2, and SF-3 keep their own plan versions

### Requirement: Historical failure rows stay byte-stable
SF-1 MUST remain recall 122/132, accuracy 129/133, and critical numeric errors 4, with enqueue `935578cc64a58a84d84224443354c8a6e5f0d9a6167fd1753cbf156e7079ffe3`, run `974dbbfda8608cd1de535215390a9b1accf67b8e7810a9770966620e2e45634b`, result `7c2d5ea6e6433586b46545ad2c597a3c7cc60016ca1023559e7a137c8117d859`, and source-review `ff2f01fd4365cc621dcfeac2b2778d97b446bcf3d73c1d7f3bb06cb7b9035c2d`. SF-2 MUST remain recall 102/132 and accuracy 114/142, with enqueue `2a4c778790678197eda9472a907b8fc1c132be7f08fdbd5d95a3a0cc86c70f19`, run `b5aad9e75ea4200a385ad107f6c044d7d14787f231156b077888018a68708b5e`, result `6ca3e2960e11f90a13aaaa75e71dc43452344864b78d10db51566f9bff6912b1`, and source-review `6851d7d6b80274a03a14b04f7fe200278a7fff53b5ea4ed734c3470bb015a0ff`. SF-3 MUST remain recall 132/132 and accuracy 144/144 on `.2026-09-29.3` only, with enqueue `0f2ebfa087f8aa4327d15d4e0366af568bcd06a41ab923ba595899ec4e3cdd9b`, run `2ab053d1b3e80b52351ea7963e45f323b97a2706971514c99486eaefd3234d1f`, result `92ec1e2a8be5d4a6ee8bc5b492d7dc09a26dbc84bfbd0ad592ca5aab6cdc1eeb`, and source-review `4446fb7619f39b49e865bc563cf7673b98d2288d481c0b229de67394ee9c3c74`. Critical numeric errors 4 MUST NOT be rewritten as 0. The Stage 4 ledger and the three historical replays MUST NOT be edited.

#### Scenario: The successor does not replace a historical hash
- **WHEN** a later `.2026-10-01.4` artifact is created
- **THEN** its enqueue, run, result, and source-review hashes differ from the SF-1, SF-2, and SF-3 hashes
- **AND** the historical files keep the hashes named in this requirement

### Requirement: Later artifacts stay isolated and do not restore the aggregate
After 1.1, replay output MUST be written only under this change at `replay/20261001/`, with run id `stage4-segment-financials-20261001`. The successor MUST have its own enqueue, run, result, and source-review, each with its own SHA-256. The source-review MUST state that SF-1 and SF-2 remain historical rows, that this observation does not modify the old ledger or old replays, and that the SF-3 local result is not rewritten as the current result of SF-1 or SF-2. That inclusion statement MUST NOT be written before the source-review. A local gate, if one is later earned, MUST apply only to plan `.2026-10-01.4`, these four reports, and `extract_segment_financials`. `stage4_aggregate_expansion_gates_met` MUST remain false. This successor alone MUST NOT restore the Stage 4 aggregate. The judgment of whether `.4` is the current segment-financial inclusion row MUST wait until the business pass and the artifact binding both hold. Artifact binding alone MUST NOT be treated as that pass. This scope MUST NOT prefill that judgment or the ratios 132/132 and 144/144.

#### Scenario: A later local pass stays inside this plan
- **WHEN** the `.2026-10-01.4` source-review later records a local pass
- **THEN** that pass does not create aggregate `expansion_gates_met=true`
- **AND** SF-1 and SF-2 remain historical rows with their original critical numeric errors

### Requirement: The successor inherits the verified business boundaries
`.2026-10-01.4` MUST NOT open a new parser repair. SF-1 critical numeric errors 4 MUST remain a historical observation, and the SF-3 repair MUST remain the verified fix. The `.4` reread MUST keep current-period columns bound to the complete printed header: page-25 prior-year values `251,677,045`, `69.52%`, `110,335,509`, and `30.48%` stay absent, while domestic cost `223,497,885` with margin `24.00%` and overseas cost `88,885,412` with margin `31.44%` remain. The ten previously repaired current-period cells MUST each remain one Measurement. On `920015.BJ` physical page 139, `地区分部` and `业务分部` MUST stay separate source dimensions. On `302132.SZ` physical page 178, current revenue and cost measurements MUST use the printed title `报告分部的财务信息`, and `分部间抵销` MUST keep `consolidation_adjustment`. Elimination amounts MUST stay `consolidation_adjustment`, an empty elimination amount MUST NOT be stored as zero, and the elimination margin MUST stay `not_disclosed`. A total-row dash, an empty elimination amount, the company-level `29.98%`, and the values `118.30` and `104.19` MUST NOT become segment amounts or zero. Empty margin cells, empty elimination amounts, and `not_applicable` single-segment coverage MUST NOT be recall gaps.

#### Scenario: The reread keeps the repaired boundaries
- **WHEN** the `.2026-10-01.4` source-review rereads the four reports
- **THEN** the page-139 sections, the page-178 formal title, the ten repaired cells, and the column-role boundary still hold
- **AND** no new parser repair is introduced to reach that result

### Requirement: A local pass is a business result
A `.2026-10-01.4` local gate MUST be true only when all four reports have been independently reread, source recall is 100%, source accuracy is 100%, critical numeric errors are 0, and the inherited evidence and coverage boundaries hold. The denominator MUST come from that reread. Complete enqueue, run, result, and source-review hashes MUST NOT by themselves count as this pass. The review MUST NOT prefill recall 132/132 or accuracy 144/144. Those ratios MUST remain the SF-3 observation.

#### Scenario: Artifact hashes do not replace the reread
- **WHEN** the `.4` enqueue, run, result, and source-review hashes exist but the reread has not shown 100% recall, 100% accuracy, and zero critical numeric errors
- **THEN** the observation is not a local pass
- **AND** current-chapter inclusion is not granted

### Requirement: Inclusion needs the business pass and the artifact binding
The 4.1 judgment MUST confirm `.2026-10-01.4` as the current segment-financial inclusion row only when the business pass and the artifact binding both hold. If either fails, the failed observation MUST remain and `.4` MUST NOT receive inclusion eligibility. Stage 4 aggregate admission MUST stay a separate judgment, and `stage4_aggregate_expansion_gates_met` MUST remain false.

#### Scenario: A failed reread does not receive inclusion
- **WHEN** the `.4` reread misses 100% recall or accuracy, or finds a critical numeric error
- **THEN** `.4` does not become the current segment-financial inclusion row
- **AND** the aggregate gate remains a separate judgment
