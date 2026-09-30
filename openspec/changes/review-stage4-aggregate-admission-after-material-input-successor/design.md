## Context

旧 aggregate 合同 `openspec/changes/archive/2026-09-29-scope-stage4-aggregate-gate-contract-and-resumption-criteria/` 纳入的材料投入计划是三报告的 `.2026-09-26.2`，结论是 hold-only。现在另有一份四报告材料投入观察 `.2026-09-30.3`：recall 23/23，accuracy 23/23，critical numeric errors 0，局部 `expansion_gates_met=true`，`stage4_aggregate_expansion_gates_met=false`。`302132.SZ` 在该观察中是 coverage-only `legal_empty`。这份观察没有替换 MI-2。

另外三个当前局部通过是产销量 `.2026-09-28.3`、分部财务 `.2026-09-29.3`、客户供应商 `.2026-09-29.1`。它们各自绑定自己的计划和 source-review。产销量失败观察 26/36（critical numeric errors 1）和 36/38，以及分部财务失败观察 122/132（critical numeric errors 4）和 102/132、accuracy 114/142，仍在已审核账本中。

## Goals / Non-Goals

**Goals:**

- 冻结四个当前局部通过的计划、报告集和“不得回填”边界。
- 规定失败观察继续保留，旧 aggregate 归档不得被改写。
- 把准入判定留给 1.1 之后的独立任务。

**Non-Goals:**

- 不在 1.1 中写下 hold 或 aggregate true。
- 不修改历史账本、旧归档、MI-1、MI-2、dossier、assessment 或 replay。
- 不创建跨章节 recall、accuracy 或 critical-error 分数。
- 不把四个局部 true 相加。
- 不新建 replay，不改 Python。
- 不启动 restricted-promotion、六章包、规模质量或生产。
- 不改 publication、closure、mode、identity、checkpoint。

## Decisions

1. 本复核只读这四个当前局部通过，每个都要核对自己的计划、四报告集和 source-review：
   - `extract_material_inputs`，`manufacturing_materials_stage4_material_inputs.2026-09-30.3`
   - `extract_operating_quantities`，`manufacturing_materials_stage4_operating_quantities.2026-09-28.3`
   - `extract_segment_financials`，`manufacturing_materials_stage4_segment_financials.2026-09-29.3`
   - `extract_counterparties_and_concentration`，`manufacturing_materials_stage4_counterparties.2026-09-29.1`
2. 统一报告集仍是 `300750.SZ`、`603659.SH`、`920015.BJ`、`302132.SZ`。材料投入 `.3` 覆盖这四份报告，但不能把 `302132.SZ` 补入 MI-2，也不能把 MI-2 的 23/23 改写成四报告结果。
3. 必须保留的失败观察至少包括：产销量 26/36、accuracy 26/27、critical numeric errors 1，以及 36/38、accuracy 36/36、critical numeric errors 0；分部财务 122/132、accuracy 129/133、critical numeric errors 4，以及 recall 102/132、accuracy 114/142、critical numeric errors 0。后来的局部 true 不擦除这些行。
4. 已归档 hold-only 合同保持原字节。重新评估只能发生在本 change 中，不能回去改那份归档。
5. 1.1 通过之后，下一步才根据这些冻结事实记录准入判定。若上述失败观察仍在，该判定只能是 hold，并且不得写成 aggregate `expansion_gates_met=true`。本设计不预填这个判定。
6. `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。

## Risks / Trade-offs

- [用材料投入四报告通过替换旧 hold] → 旧合同归档不改。新判定若发生，只写在本 change。
- [把 23/23 回填成 MI-2 的四报告结果] → `.3` 与 `.2` 保持两行独立观察。
- [把四个局部 true 加总] → 本卡禁止跨章节分数和 aggregate true。
- [忽略 26/36 或 122/132] → 失败观察是后续判定的输入，不是可删历史。

## Migration Plan

无部署。1.1 通过前只有本 change 的范围文档。通过后的判定任务仍不创建 replay 或生产准入。

## Open Questions

2.1 已在下一节记录准入结论。旧 hold-only aggregate 合同未改。

## Admission

2.1 只读取本 change 已冻结的观察。没有重算指标，没有把四个局部 true 相加，也没有修改历史账本、旧归档、replay 或 source-review。

四个局部结果各自成立，但只属于各自计划、报告集和章节：

| 章节 | 计划 | 局部结果 |
|---|---|---|
| `extract_material_inputs` | `.2026-09-30.3` | recall 23/23，accuracy 23/23，critical numeric errors 0 |
| `extract_operating_quantities` | `.2026-09-28.3` | 38/38 |
| `extract_segment_financials` | `.2026-09-29.3` | recall 132/132，accuracy 144/144 |
| `extract_counterparties_and_concentration` | `.2026-09-29.1` | 45/45 |

它们都使用 `300750.SZ`、`603659.SH`、`920015.BJ`、`302132.SZ`。MI-2 `.2026-09-26.2` 仍是三报告 23/23，与材料投入 `.3` 保持独立，没有被回填。

失败观察没有被后续局部 true 擦除或替换：

- 产销量 26/36，accuracy 26/27，critical numeric errors 1；另有 36/38，accuracy 36/36，critical numeric errors 0。
- 分部财务 122/132，accuracy 129/133，critical numeric errors 4；另有 recall 102/132，accuracy 114/142，critical numeric errors 0。

准入结论是 **hold**。`stage4_aggregate_expansion_gates_met` 保持 false。

- 不产生 aggregate `expansion_gates_met=true`。
- 不创建跨章节 recall、accuracy 或 critical-error 分数。
- 不授权 restricted-promotion、六章包、规模质量或生产。
- `production_authorization` 保持 `not_authorized`。`scale_quality_claim_allowed` 保持 false。
- 不改 publication、closure、mode、identity、checkpoint。
