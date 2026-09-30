## Context

MI-2 计划 `manufacturing_materials_stage4_material_inputs.2026-09-26.2` 只覆盖 `300750.SZ`、`603659.SH`、`920015.BJ`。aggregate 合同要求的四报告集另含 `302132.SZ`，所以材料投入报告集仍然不齐。已归档 reconciliation 把 `302132.SZ` 的完整读取判为 coverage-only suitable：没有具名材料时可以记录 `not_disclosed` 或 `not_applicable`，但不能把旧 assessment 的 unsuitable 改写成通过，也不能把这份报告追写进 MI-2。

现有入口是 `research/company_profile/material_input_research.py` 的 `replay_material_input_research`。它当前服务 `.2026-09-26.2` 和三份报告。本 change 不改这个函数，只冻结以后的 successor 必须复用它，并使用新计划和新隔离目录。

## Goals / Non-Goals

**Goals:**

- 冻结四份已批准 2025 年报的身份和唯一章节。
- 冻结新计划版本、隔离 replay 目录，以及 source-review 只能绑定这次制品。
- 规定 `302132.SZ` 的合法 coverage 与具名 fact 的边界。

**Non-Goals:**

- 1.1 通过前不改 Python，不入队，不 replay，不写 source-review。
- 不改 MI-1、MI-2、旧 assessment、dossier、aggregate ledger 或历史 replay。
- 不把 `302132.SZ` 追写进三报告结果，不预填 recall、accuracy、critical numeric errors 或 gate。
- 不把本 successor 当成 Stage 4 aggregate 通过。
- 不消除产销量或分部财务的失败观察和 critical numeric errors。
- 不启动 restricted-promotion、六章包、规模质量或生产。

## Decisions

1. 章节只有现有 `extract_material_inputs`。后续实现复用 `replay_material_input_research`，不另建抽取器。
2. 新计划版本是 `manufacturing_materials_stage4_material_inputs.2026-09-30.3`。`.2026-09-26.1` 仍只属于 MI-1，`.2026-09-26.2` 仍只属于 MI-2。
3. 报告集是 manifest `manufacturing_materials.2026-09-03.4` 的四份 2025 年报，身份不另选：

   | Instrument | Exchange | report_id | document_version | report_period | PDF content hash |
   |---|---|---|---|---|---|
   | 300750.SZ | SZSE | `asset_3b09f6c831975c7177b6bb3287cab781` | `ver_09c0e677ec8192dc4fc12cb620069f29` | 2025-12-31 | `c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9` |
   | 603659.SH | SSE | `asset_50c70429093f66b34fc57ad8f896fcee` | `ver_c867a6a692048e88fd9cb80473fbf908` | 2025-12-31 | `4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6` |
   | 920015.BJ | BSE | `asset_b87f1d1a48e662dae376c540cd021f69` | `ver_cfdbd2d058af825b1fc39f494d7a9bd3` | 2025-12-31 | `4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a` |
   | 302132.SZ | SZSE | `asset_0a488da55636b09107be6d719c9ebf39` | `ver_2d20ba3aebc5fac6c562cd619695995a` | 2025-12-31 | `605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020` |

4. 隔离目录是本 change 下的 `replay/20260930/`。run id 为 `stage4-material-inputs-20260930`。输出只到该目录，disposition 为 `accepted_for_review`。bundle 不得包含 recall、accuracy、critical numeric errors、gate 或 source-review。
5. 后续 source-review 是新文件，只绑定 `.2026-09-30.3` 的 enqueue、run、result 哈希、run id 和 bundle 路径。它不改 MI-2 的 source-review，也不从 bundle 行数推断 gate。
6. `302132.SZ` 若完整审核后仍无具名材料，记录有证据的 `not_disclosed` 或 `not_applicable`，不生成虚构 fact。具名投入仍必须是同一句绑定到公司自身生产或经营投入的 source-native 名称。能源、泛称“直接材料”或“材料费”、存货“原材料”金额、供应商合计、关联交易里的“采购商品”和会计政策不能单独成为 material_input。`not_disclosed` 不是 `extraction_failed`。
7. 本 successor 的局部结果，无论后来 gate 真假，都不回填 MI-2，不改写产销量 26/36 与 36/38、分部财务 122/132 与 102/132，也不产生 aggregate `expansion_gates_met=true`。

## Risks / Trade-offs

- [把 coverage-only suitable 当成四报告 replay 已经完成] → 本卡停在范围。replay 和 source-review 是 1.1 之后的独立任务。
- [用新计划覆盖 MI-2] → `.2` 的三报告制品保持原字节。四报告结果只写入 `replay/20260930/`。
- [为了凑齐具名材料而补写 302132] → 没有 source-native 名称时只能记 coverage，不能生成 fact。
- [把材料投入四报告通过当成 Stage 4 通过] → 产销量和分部财务的失败观察继续保留，aggregate 继续 hold。

## Migration Plan

无部署。1.1 通过前不改代码和历史制品。通过后先做最小实现和定向测试，再做受控 replay，再做独立 source-review，最后才决定是否归档这次观察。

## Open Questions

四报告 replay 的 recall、accuracy、critical numeric errors 和 gate 尚未发生。本设计不预填。
