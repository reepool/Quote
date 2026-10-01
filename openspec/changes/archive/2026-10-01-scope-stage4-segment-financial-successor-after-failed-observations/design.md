## Context

已归档的 re-entry 合同结论是：分部财务若要重新进入 aggregate 判断，必须另立 successor。SF-1 `.2026-09-28.1` 仍是 recall 122/132、accuracy 129/133、critical numeric errors 4。SF-2 `.2026-09-29.2` 仍是 recall 102/132、accuracy 114/142。SF-3 `.2026-09-29.3` 的 recall 132/132、accuracy 144/144 只属于该计划。本 change 是那张独立 successor 卡。1.1 通过前不实现、不 replay。

现有代码的默认计划仍是 `.2026-09-29.3`。本卡不改 Python，因此不改动这条默认路径。

## Goals / Non-Goals

**Goals:**

- 冻结新计划、同一四报告身份，以及本 change 内的隔离输出目录。
- 规定将来的制品必须有自己的 enqueue、run、result、source-review 和 SHA-256。
- 规定将来的纳入声明如何保留 SF-1 与 SF-2，而不修改旧文件。
- 写明 `.4` 的业务通过标准。制品哈希绑定完整不等于业务通过。

**Non-Goals:**

- 1.1 通过前不改 Python，不创建 enqueue、replay 或 source-review。
- 不计算新的 recall、accuracy 或 critical numeric errors。
- 不回填 SF-1 或 SF-2，不把 SF-3 的结果改写成旧观察。
- 不创建 aggregate 行、跨章节分数或 aggregate `expansion_gates_met=true`。
- 不启动产销量 successor、restricted-promotion、六章包、规模质量或生产。
- 不改 publication、closure、mode、identity、checkpoint。

## Decisions

1. 范围只有 `extract_segment_financials`。新计划是 `manufacturing_materials_stage4_segment_financials.2026-10-01.4`。它不是 `.2026-09-28.1`、`.2026-09-29.2` 或 `.2026-09-29.3`。正式入口是 `replay_segment_financial_research`，它已经接受显式 `plan_version`，默认值仍是 `.2026-09-29.3`。2.1 必须复用该入口并显式传入 `.4`，不得把默认值改成 `.4`，也不得再开一轮解析器修复。SF-1 的 4 个 critical numeric errors 留在历史观察里。SF-3 已经验证修复通过。`.4` 要做的是新计划下的独立重读和纳入资格证明。
2. 四份报告与 SF-1、SF-2、SF-3 的 enqueue 相同，报告期都是 2025-12-31：

| Instrument | Exchange | report_id | document_version | sample_id |
|---|---|---|---|---|
| 300750.SZ | SZSE | `asset_3b09f6c831975c7177b6bb3287cab781` | `ver_09c0e677ec8192dc4fc12cb620069f29` | `manufacturing-materials-300750-2025` |
| 603659.SH | SSE | `asset_50c70429093f66b34fc57ad8f896fcee` | `ver_c867a6a692048e88fd9cb80473fbf908` | `manufacturing-materials-603659-2025` |
| 920015.BJ | BSE | `asset_b87f1d1a48e662dae376c540cd021f69` | `ver_cfdbd2d058af825b1fc39f494d7a9bd3` | `manufacturing-materials-920015-2025` |
| 302132.SZ | SZSE | `asset_0a488da55636b09107be6d719c9ebf39` | `ver_2d20ba3aebc5fac6c562cd619695995a` | `manufacturing-materials-302132-2025-regime` |

`manufacturing-materials-302132-2025-regime` 仍只是历史 sample_id，不是新的材料或分部财务结论。

四份 PDF 的 `content_hash` 已与磁盘文件核对，并且与 `segment_financial_research_bindings()` 和 SF-3 enqueue 一致。来源范围沿用现有 binding 的物理页：

| Instrument | PDF SHA-256 | 物理页范围 |
|---|---|---|
| 300750.SZ | `c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9` | 24–25、195、223 |
| 603659.SH | `4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6` | 18–19 |
| 920015.BJ | `4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a` | 16、17、139 |
| 302132.SZ | `605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020` | 14–15、178 |

当前 binding 里的 dossier 路径仍指向已归档前的活动目录，该目录现在不存在。同一 dossier 字节在 `openspec/changes/archive/2026-09-29-scope-manufacturing-materials-stage4-segment-financials/dossiers/`，哈希与 SF-3 enqueue 一致。2.1 不得改这些 dossier 字节或 PDF 哈希。

3. 隔离目录是本 change 下的 `replay/20261001/`。将来的 run id 是 `stage4-segment-financials-20261001`。enqueue、run、result、source-review 都只放在这里。1.1 通过前该目录不存在，SHA-256 不预填。完成后的四个哈希必须不同于下列历史哈希：

| 行 | enqueue SHA-256 | run SHA-256 | result SHA-256 | source-review SHA-256 |
|---|---|---|---|---|
| SF-1 | `935578cc64a58a84d84224443354c8a6e5f0d9a6167fd1753cbf156e7079ffe3` | `974dbbfda8608cd1de535215390a9b1accf67b8e7810a9770966620e2e45634b` | `7c2d5ea6e6433586b46545ad2c597a3c7cc60016ca1023559e7a137c8117d859` | `ff2f01fd4365cc621dcfeac2b2778d97b446bcf3d73c1d7f3bb06cb7b9035c2d` |
| SF-2 | `2a4c778790678197eda9472a907b8fc1c132be7f08fdbd5d95a3a0cc86c70f19` | `b5aad9e75ea4200a385ad107f6c044d7d14787f231156b077888018a68708b5e` | `6ca3e2960e11f90a13aaaa75e71dc43452344864b78d10db51566f9bff6912b1` | `6851d7d6b80274a03a14b04f7fe200278a7fff53b5ea4ed734c3470bb015a0ff` |
| SF-3 | `0f2ebfa087f8aa4327d15d4e0366af568bcd06a41ab923ba595899ec4e3cdd9b` | `2ab053d1b3e80b52351ea7963e45f323b97a2706971514c99486eaefd3234d1f` | `92ec1e2a8be5d4a6ee8bc5b492d7dc09a26dbc84bfbd0ad592ca5aab6cdc1eeb` | `4446fb7619f39b49e865bc563cf7673b98d2288d481c0b229de67394ee9c3c74` |

4. 历史行保持原值。SF-1 的 critical numeric errors 4 不能改成 0。SF-3 的 132/132、144/144 不能写回 SF-1 或 SF-2。旧 ledger `openspec/changes/archive/2026-09-29-review-stage4-aggregate-admission-and-restricted-promotion/ledger.md` 和三份旧 replay 只读。
5. `.4` 继承 SF-3 已验证的业务边界，不把这些边界当成新的修复任务：当期列绑定完整表头；十个已补交的当期单元格仍各是一条 Measurement；`920015.BJ` 物理页 139 的 `地区分部` 与 `业务分部` 分开；`302132.SZ` 物理页 178 使用正式标题 `报告分部的财务信息`；抵消行保持 `consolidation_adjustment`，空抵消金额不是 0，抵消毛利率保持 `not_disclosed`；合计行破折号、空抵消金额、公司整体 `29.98%`，以及 `118.30` 和 `104.19`，都不是分部金额或 0。页 25 的上年值 `251,677,045`、`69.52%`、`110,335,509`、`30.48%` 继续缺席；境内成本 `223,497,885` 与毛利率 `24.00%`、境外成本 `88,885,412` 与毛利率 `31.44%` 继续保留。空毛利率、空抵消金额和 `not_applicable` 的单一分部覆盖不是 recall 缺口。
6. 局部 gate 沿用 SF-3 的判据，但这是验收条件，不预填分数。四份报告必须独立重读；source recall 与 source accuracy 都必须是 100%；critical numeric errors 必须是 0；上面的证据和 coverage 边界必须核对通过。分母来自这次重读。不得把 SF-3 的 132/132 或 144/144 抄成 `.4` 的结果。enqueue、run、result、source-review 哈希齐全只说明制品绑定完整，本身不是业务通过。
7. 4.1 只有在业务通过和制品绑定同时成立时，才能确认 `.4` 是当前分部财务纳入行。任一失败都保留该失败观察，不给予纳入资格。Stage 4 aggregate 仍是独立判断。`stage4_aggregate_expansion_gates_met` 保持 false。产销量失败观察仍要自己的 successor。
8. 将来的 source-review 才写纳入声明。声明必须说明 SF-1 与 SF-2 仍是历史行，本观察不修改它们，也不把 SF-3 当作它们的当前结果。1.1 不写这份声明，也不提前确认纳入资格。
9. `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。

## Risks / Trade-offs

- [把 `.4` 写进 `.3` 的 replay 目录] → 制品只进入 `replay/20261001/`。
- [用 SF-3 的 132/132 预填 successor 分数] → 验收条件只要求重读后的 100%。不抄 132/132 或 144/144。
- [把制品哈希齐全当成业务通过] → 哈希绑定和业务判据必须同时成立。
- [局部通过后直接恢复 aggregate] → 本卡不产生 aggregate `expansion_gates_met=true`。
- [修改 SF-1 以消除 critical numeric errors 4] → 旧行只读。新证据只能来自 `.4` 自己的制品。

## Migration Plan

无部署。1.1 通过前只有范围文档。通过后的顺序是：最小实现与定向测试，再受控 replay，再独立 source-review，最后才另行判断纳入资格。不能从已归档的 re-entry 合同直接启动 replay。

## Open Questions

`.4` 的纳入判断写在下一节。本结论不改旧 ledger，也不重新打开 aggregate。

## Inclusion judgment

4.1 对照 source review 和四份制品。业务通过与制品绑定同时成立，没有重跑 replay，也没有修改 enqueue、run、result 或 source review。

`.2026-10-01.4` 是当前 `extract_segment_financials` 纳入观察。指标来自这次独立重读：recall 137/137，accuracy 144/144，critical numeric errors 0。分报告披露单元格是 300750.SZ 34、603659.SH 29、920015.BJ 42、302132.SZ 32。局部 gate 只属于这个计划、这四份报告和该章节。

四报告身份与 SF-1、SF-2、SF-3 相同，报告期 2025-12-31：

| Instrument | report_id | document_version |
|---|---|---|
| 300750.SZ | `asset_3b09f6c831975c7177b6bb3287cab781` | `ver_09c0e677ec8192dc4fc12cb620069f29` |
| 603659.SH | `asset_50c70429093f66b34fc57ad8f896fcee` | `ver_c867a6a692048e88fd9cb80473fbf908` |
| 920015.BJ | `asset_b87f1d1a48e662dae376c540cd021f69` | `ver_cfdbd2d058af825b1fc39f494d7a9bd3` |
| 302132.SZ | `asset_0a488da55636b09107be6d719c9ebf39` | `ver_2d20ba3aebc5fac6c562cd619695995a` |

本观察的制品哈希：

| 制品 | SHA-256 |
|---|---|
| enqueue | `8c2c40e06848563723e208ca4d60d0310804ff986fa7ae86b50aecc7200d3bfa` |
| run | `8f007a464e89adfd9bfcf80d810c27d47ce321e885d207e5faedfe6ecf897b90` |
| result | `6c5188ef7dc43c45bb69c23478d2dbc3bd19e9e657ca3afb674ad2494c9acc59` |
| source-review | `0a38ac4c78d65dacd756cf310ea1033e9cd1e40e793a6e0a5a5ac256030dcfbd` |

SF-1 仍是 recall 122/132、accuracy 129/133、critical numeric errors 4。SF-2 仍是 recall 102/132、accuracy 114/142。SF-3 的 recall 132/132、accuracy 144/144 仍只属于 `.2026-09-29.3`。这三行保持原值，旧 ledger 不改。

本结论只授予当前分部财务纳入资格。不计算跨章节分数，不启动产销量 successor、restricted-promotion 或生产。`stage4_aggregate_expansion_gates_met` 保持 false。`production_authorization` 保持 `not_authorized`。`scale_quality_claim_allowed` 保持 false。

## Acceptance

1.1 核对了计划、四报告身份、PDF `content_hash`、现有来源页，以及 `replay_segment_financial_research` 的显式 `plan_version`。默认计划仍是 `.3`。业务通过标准、制品绑定和 4.1 的纳入条件写在上面。本卡不产生代码或 replay 制品，也不另开解析器修复。
