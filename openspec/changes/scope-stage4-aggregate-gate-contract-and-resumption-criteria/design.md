## Context

已审核账本 `openspec/changes/archive/2026-09-29-review-stage4-aggregate-admission-and-restricted-promotion/ledger.md` 保留九行独立观察。admission 已归档为 hold。本 change 只定义以后判断 aggregate gate 时必须使用的合同，不重算那九行，也不把 hold 提前写成 2.2 的完成结论。

## Goals / Non-Goals

**Goals:**

- 冻结纳入章节、计划版本、统一报告集和交易所覆盖。
- 冻结 source-review、身份和制品绑定，以及失败观察的保留规则。
- 规定不满足时只能 hold，通过后最多进入 restricted-promotion 设计。

**Non-Goals:**

- 不改 Python，不入队，不 replay。
- 不修改任何归档 replay、source-review、`ledger.md` 或失败账本。
- 不新增报告，不重选样本。
- 不预填 aggregate `expansion_gates_met` 或跨章节 recall、accuracy、critical-error 分数。
- 不启动六章包、规模质量、生产或 restricted-promotion 实现。
- 不改 publication、closure、mode、identity、checkpoint。

## Decisions

1. aggregate 只纳入四个已完成的 Stage 4 章节，不纳入业务概览或经营体制：
   - `extract_material_inputs`，计划 `manufacturing_materials_stage4_material_inputs.2026-09-26.2`
   - `extract_operating_quantities`，计划 `manufacturing_materials_stage4_operating_quantities.2026-09-28.3`
   - `extract_segment_financials`，计划 `manufacturing_materials_stage4_segment_financials.2026-09-29.3`
   - `extract_counterparties_and_concentration`，计划 `manufacturing_materials_stage4_counterparties.2026-09-29.1`
2. 统一报告集固定为 manifest `manufacturing_materials.2026-09-03.4` 的四份 2025 年报：`300750.SZ`（SZSE）、`603659.SH`（SSE）、`920015.BJ`（BSE）、`302132.SZ`（SZSE）。四份必须同时出现在同一报告集中。材料投入 `.2` 的三报告通过不能补入 `302132.SZ`，也不能代替这个报告集。
3. 每个纳入计划必须有绑定到该计划 enqueue、run、result 的 source-review，并带有该行自己的冻结身份。身份只复制该行制品中的 instrument、report_id、document_version、report_period 和 processing identity。制品里没有 processing identity 时保持空白。
4. 下列失败观察保持原值和原 gate，不被上列计划覆盖：
   - 材料投入 `.2026-09-26.1`：recall 19/23，accuracy 19/19，critical numeric errors 0，gate false
   - 产销量 `.2026-09-27.1`：recall 26/36，accuracy 26/27，critical numeric errors 1，gate false
   - 产销量 `.2026-09-28.2`：recall 36/38，accuracy 36/36，critical numeric errors 0，gate false
   - 分部财务 `.2026-09-28.1`：recall 122/132，accuracy 129/133，critical numeric errors 4，gate false
   - 分部财务 `.2026-09-29.2`：recall 102/132，accuracy 114/142，critical numeric errors 0，gate false
5. 局部 true 只在自己的计划、报告集和章节内有效。不得回填到更早计划，不得跨章节相加。
6. 1.1 把本合同裁决为 hold-only 合同。纳入的材料投入计划 `.2026-09-26.2` 只有三份报告，而 aggregate 要求同一报告集包含 `302132.SZ`。该计划因此不可能满足这份 aggregate gate。不得把 `302132.SZ` 补进三报告结果，也不得用其他章节的四报告结果替代。2.2 仍是以后对照账本的正式记录；在那之前不把 hold 写成已经完成的复述，也不产生 aggregate `expansion_gates_met=true`。
7. aggregate gate 通过前不得打开 restricted-promotion 设计。将来若有独立审核通过的 aggregate true，下一张卡最多是设计卡，不授权生产。`production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。

## Risks / Trade-offs

- [把四个候选计划写成已经通过的整体门] → 合同只冻结纳入范围和失败保留，1.1 不预填 aggregate gate。
- [用三报告材料投入充当四报告集] → 统一报告集写明必须包含 `302132.SZ`，且不得事后补入。
- [失败观察被局部 true 擦除] → 五条失败行的原指标和 critical numeric errors 保持可见。
- [合同审核滑进实现] → 2.1 之后才允许冻结记录，全程不改 Python、不 replay、不改历史制品。

## 1.1 adjudication

1.1 选择 hold-only 合同，不选择可恢复准入合同。

当前纳入的 `extract_material_inputs` 计划 `manufacturing_materials_stage4_material_inputs.2026-09-26.2` 只冻结 `300750.SZ`、`603659.SH`、`920015.BJ`。统一报告集还要求 `302132.SZ`。这两个冻结条件不相容，所以按本合同，当前 aggregate 不可能通过。

不接受的解决办法：事后把 `302132.SZ` 补进 `.2` 的三报告结果；用产销量、分部财务或客户供应商的四报告结果代替材料投入的报告集；在本卡创建材料投入四报告 successor，或修改 `.2` 的历史观察。

因此 2.1 只能冻结这份不相容的合同要素，2.2 以后只能记录 hold。本裁决不预填跨章节分数，也不勾选 2.1 或 2.2。

## Contract binding

2.1 的落账写在 `contract-binding.md`。它从已审核账本复制四个纳入计划、统一四报告集、各自行的 source-review 与制品哈希，以及五条失败观察的原指标。材料投入 `.2` 仍只有三份报告，没有补入 `302132.SZ`。该文件没有 aggregate 行，没有跨章节分数，也没有把 hold 写成 2.2 的完成结论。已归档的 `ledger.md` 未改。`production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。restricted-promotion、六章包、规模质量和生产保持关闭。
