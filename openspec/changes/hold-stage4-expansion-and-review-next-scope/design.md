## Context

阶段 4 已归档的观察按章节、计划和报告集分开。下面的数字来自各自的 source-review，本 change 不重算、不回填。

材料投入，`extract_material_inputs`：

- 原始 `openspec/changes/archive/2026-09-26-scope-manufacturing-materials-stage4-minimum-slice/replay/20260926`：recall 19/23，accuracy 19/19，critical numeric errors 0，`expansion_gates_met=false`。
- 后续局部修复 `openspec/changes/archive/2026-09-27-repair-material-input-procurement-and-materials-table-coverage/replay/20260926.2`，计划 `manufacturing_materials_stage4_material_inputs.2026-09-26.2`：recall 23/23，accuracy 23/23，critical numeric errors 0，`expansion_gates_met=true`。这个 true 只覆盖那三份冻结报告、该计划和该章节。

产销量，`extract_operating_quantities`：

- 原始 `openspec/changes/archive/2026-09-29-scope-manufacturing-materials-stage4-operating-quantities-holdout/replay/20260928`，计划 `.2026-09-27.1`：recall 26/36，accuracy 26/27，critical numeric errors 1，`expansion_gates_met=false`。失败点是第 15 页 PVDF 与「勃姆石和氧化铝」的 Evidence/对象绑定。enqueue `5fffde878890fb43c136c4e0082f4eba32fc9e65402e166a96cca164396fab7a`，run `3fa107b4bf914cf7603ee1e2e73937392a04ad95bf52df4e68d5bff304a5e9cf`，result `67e37d98ed0d0dc57f9672f6ef224482db52fc3e9ce96ece8f366866a1e87302`，source-review `5568d4ee73cbc332c63cb935fba42a13ea99e4dfe0344f6b700ad4fcc212ab0b`。
- Putailai `.2`：`openspec/changes/archive/2026-09-28-repair-operating-quantity-603659-coverage-and-evidence-binding/replay/20260928.2`，计划 `.2026-09-28.2`：recall 36/38，accuracy 36/36，critical numeric errors 0，`expansion_gates_met=false`。
- CATL `.3`：`openspec/changes/archive/2026-09-28-repair-operating-quantity-catl-segment-sales-coverage/replay/20260928.3`，计划 `.2026-09-28.3`：recall 38/38，accuracy 38/38，critical numeric errors 0，`expansion_gates_met=true`。这个 true 只覆盖四份冻结报告、该计划和该章节。

分部财务，`extract_segment_financials`：

- 原始 `openspec/changes/archive/2026-09-29-scope-manufacturing-materials-stage4-segment-financials/replay/20260928`，计划 `.2026-09-28.1`：recall 122/132，accuracy 129/133，critical numeric errors 4，`expansion_gates_met=false`。
- column-binding `.2`：`openspec/changes/archive/2026-09-29-repair-segment-financial-column-binding-and-cell-coverage/replay/20260929.2`，计划 `.2026-09-29.2`：recall 102/132，accuracy 114/142，critical numeric errors 0，`expansion_gates_met=false`。
- footnote `.3`：`openspec/changes/archive/2026-09-29-repair-segment-financial-footnote-dimension-binding/replay/20260929.3`，计划 `.2026-09-29.3`：recall 132/132，accuracy 144/144，critical numeric errors 0，`expansion_gates_met=true`。这个 true 只覆盖四份冻结报告、该计划和该章节。

固定两家公司 9/9 和 common-core `owned_page_facts=v8` 的 8/9 不属于这三条竖切，也不并入本索引的通过结论。

## Goals / Non-Goals

**Goals:**

- 把上述观察写成只读索引，并声明没有 aggregate gate。
- 保持 `scale_quality_claim_allowed=false` 和 `production_authorization=not_authorized`。
- 规定下一项只评估 `extract_counterparties_and_concentration` 是否具备合法研究范围。

**Non-Goals:**

- 1.1 通过前不改 Python，不入队，不 replay。
- 不把多个局部 true 拼成 Stage 4 整体通过。
- 不启用完整六章包，不打开 regime 作为本轮实现。
- 不从已批准清单外补报告。
- 不改 identity、publication、closure、mode、checkpoint。
- 不重算、不回填任何已归档指标。

## Decisions

1. 观察索引只引用已归档 source-review 和路径。本 change 不生成新的 recall 或 gate。
2. 任一章节的局部 true 只在其计划、报告集和章节内有效。跨章节相加不是质量门。
3. 下一项候选固定为现有 `extract_counterparties_and_concentration`。1.1 通过后只做 dossier 评估：已批准四份报告之内，至少三份，覆盖 SZSE、SSE、BSE，并至少有两种披露形态。达不到就停止。
4. dossier 评估通过之前，不得写实现、不得入队、不得 replay。即使 dossier 通过，实现也必须是另一个 change。
5. 1.1 通过前，本文件中的索引只是待审合同，不授权下一章开工。

## Risks / Trade-offs

- [把 23/23、38/38、132/132 读成整体通过] → 索引写明每个 true 的计划、报告集和章节，并声明没有 aggregate gate。
- [客户供应商评估滑成六章包] → 候选只有一个现有章节，其余章节保持关闭。
- [样本不够时从清单外补报告] → 合同要求停止。
- [1.1 未过就开始 dossier 或代码] → tasks 把 dossier 放在 1.1 之后，并把实现排除在本 change 之外。
