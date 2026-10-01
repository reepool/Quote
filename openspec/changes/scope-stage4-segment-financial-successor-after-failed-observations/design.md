## Context

已归档的 re-entry 合同结论是：分部财务若要重新进入 aggregate 判断，必须另立 successor。SF-1 `.2026-09-28.1` 仍是 recall 122/132、accuracy 129/133、critical numeric errors 4。SF-2 `.2026-09-29.2` 仍是 recall 102/132、accuracy 114/142。SF-3 `.2026-09-29.3` 的 recall 132/132、accuracy 144/144 只属于该计划。本 change 是那张独立 successor 卡。1.1 通过前不实现、不 replay。

现有代码的默认计划仍是 `.2026-09-29.3`。本卡不改 Python，因此不改动这条默认路径。

## Goals / Non-Goals

**Goals:**

- 冻结新计划、同一四报告身份，以及本 change 内的隔离输出目录。
- 规定将来的制品必须有自己的 enqueue、run、result、source-review 和 SHA-256。
- 规定将来的纳入声明如何保留 SF-1 与 SF-2，而不修改旧文件。

**Non-Goals:**

- 1.1 通过前不改 Python，不创建 enqueue、replay 或 source-review。
- 不计算新的 recall、accuracy 或 critical numeric errors。
- 不回填 SF-1 或 SF-2，不把 SF-3 的结果改写成旧观察。
- 不创建 aggregate 行、跨章节分数或 aggregate `expansion_gates_met=true`。
- 不启动产销量 successor、restricted-promotion、六章包、规模质量或生产。
- 不改 publication、closure、mode、identity、checkpoint。

## Decisions

1. 范围只有 `extract_segment_financials`。新计划是 `manufacturing_materials_stage4_segment_financials.2026-10-01.4`。它不是 `.2026-09-28.1`、`.2026-09-29.2` 或 `.2026-09-29.3`。后续实现只能新增这条计划的选择路径，不能把现有 `.3` 默认 replay 改成 `.4`。
2. 四份报告与 SF-1、SF-2、SF-3 的 enqueue 相同，报告期都是 2025-12-31：

| Instrument | Exchange | report_id | document_version | sample_id |
|---|---|---|---|---|
| 300750.SZ | SZSE | `asset_3b09f6c831975c7177b6bb3287cab781` | `ver_09c0e677ec8192dc4fc12cb620069f29` | `manufacturing-materials-300750-2025` |
| 603659.SH | SSE | `asset_50c70429093f66b34fc57ad8f896fcee` | `ver_c867a6a692048e88fd9cb80473fbf908` | `manufacturing-materials-603659-2025` |
| 920015.BJ | BSE | `asset_b87f1d1a48e662dae376c540cd021f69` | `ver_cfdbd2d058af825b1fc39f494d7a9bd3` | `manufacturing-materials-920015-2025` |
| 302132.SZ | SZSE | `asset_0a488da55636b09107be6d719c9ebf39` | `ver_2d20ba3aebc5fac6c562cd619695995a` | `manufacturing-materials-302132-2025-regime` |

`manufacturing-materials-302132-2025-regime` 仍只是历史 sample_id，不是新的材料或分部财务结论。

3. 隔离目录是本 change 下的 `replay/20261001/`。将来的 run id 是 `stage4-segment-financials-20261001`。enqueue、run、result、source-review 都只放在这里。1.1 通过前该目录不存在，SHA-256 不预填。完成后的四个哈希必须不同于下列历史哈希：

| 行 | enqueue SHA-256 | run SHA-256 | result SHA-256 | source-review SHA-256 |
|---|---|---|---|---|
| SF-1 | `935578cc64a58a84d84224443354c8a6e5f0d9a6167fd1753cbf156e7079ffe3` | `974dbbfda8608cd1de535215390a9b1accf67b8e7810a9770966620e2e45634b` | `7c2d5ea6e6433586b46545ad2c597a3c7cc60016ca1023559e7a137c8117d859` | `ff2f01fd4365cc621dcfeac2b2778d97b446bcf3d73c1d7f3bb06cb7b9035c2d` |
| SF-2 | `2a4c778790678197eda9472a907b8fc1c132be7f08fdbd5d95a3a0cc86c70f19` | `b5aad9e75ea4200a385ad107f6c044d7d14787f231156b077888018a68708b5e` | `6ca3e2960e11f90a13aaaa75e71dc43452344864b78d10db51566f9bff6912b1` | `6851d7d6b80274a03a14b04f7fe200278a7fff53b5ea4ed734c3470bb015a0ff` |
| SF-3 | `0f2ebfa087f8aa4327d15d4e0366af568bcd06a41ab923ba595899ec4e3cdd9b` | `2ab053d1b3e80b52351ea7963e45f323b97a2706971514c99486eaefd3234d1f` | `92ec1e2a8be5d4a6ee8bc5b492d7dc09a26dbc84bfbd0ad592ca5aab6cdc1eeb` | `4446fb7619f39b49e865bc563cf7673b98d2288d481c0b229de67394ee9c3c74` |

4. 历史行保持原值。SF-1 的 critical numeric errors 4 不能改成 0。SF-3 的 132/132、144/144 不能写回 SF-1 或 SF-2。旧 ledger `openspec/changes/archive/2026-09-29-review-stage4-aggregate-admission-and-restricted-promotion/ledger.md` 和三份旧 replay 只读。
5. 将来的 source-review 才写纳入声明。声明必须说明 SF-1 与 SF-2 仍是历史行，本观察不修改它们，也不把 SF-3 当作它们的当前结果。1.1 不写这份声明，也不判断 `.4` 是否已经成为当前纳入行。该判断留在 source-review 通过之后的单独任务。
6. 若 `.4` 以后得到局部 gate，该 gate 只属于这个计划、这四份报告和 `extract_segment_financials`。`stage4_aggregate_expansion_gates_met` 保持 false。产销量失败观察仍要自己的 successor。
7. `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。

## Risks / Trade-offs

- [把 `.4` 写进 `.3` 的 replay 目录] → 制品只进入 `replay/20261001/`。
- [用 SF-3 的 132/132 预填 successor 分数] → 1.1 前不计算新指标。分数只来自以后的独立 source-review。
- [局部通过后直接恢复 aggregate] → 本卡不产生 aggregate `expansion_gates_met=true`。
- [修改 SF-1 以消除 critical numeric errors 4] → 旧行只读。新证据只能来自 `.4` 自己的制品。

## Migration Plan

无部署。1.1 通过前只有范围文档。通过后的顺序是：最小实现与定向测试，再受控 replay，再独立 source-review，最后才另行判断纳入资格。不能从已归档的 re-entry 合同直接启动 replay。

## Open Questions

`.4` 的 recall、accuracy、critical numeric errors 和局部 gate 尚未发生。它们不在本卡预填。`.4` 能否成为当前分部财务纳入行，留到 source-review 通过之后。
