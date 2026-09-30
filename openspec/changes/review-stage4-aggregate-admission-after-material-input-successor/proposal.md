## Why

材料投入 `.2026-09-30.3` 已经是四报告局部通过，客户供应商也有四报告局部通过。产销量仍保留 26/36、critical numeric errors 1，以及 36/38 的失败观察。分部财务仍保留 122/132、critical numeric errors 4，以及 recall 102/132、accuracy 114/142 的失败观察。这些失败不能被后来的局部 true 擦除，也不能直接相加成 aggregate true。旧的 hold-only aggregate 合同纳入的是三报告 MI-2，不能改写那份归档来换成新的材料投入计划。

## What Changes

- 新建一份只读准入复核。1.1 通过前只写 proposal、design、spec、tasks，不写准入结论，不创建 aggregate 行或跨章节分数。
- 冻结四个当前局部通过：材料投入 `manufacturing_materials_stage4_material_inputs.2026-09-30.3`、产销量 `.2026-09-28.3`、分部财务 `.2026-09-29.3`、客户供应商 `manufacturing_materials_stage4_counterparties.2026-09-29.1`。统一报告集仍是 `300750.SZ`、`603659.SH`、`920015.BJ`、`302132.SZ`。
- 材料投入 `.3` 只作为独立观察。它不回填 MI-2，也不改写 MI-1 的 19/23。
- 产销量和分部财务的失败观察及 critical numeric errors 必须原样保留。
- 已归档 hold-only aggregate 合同不得直接修改。若要重新评估，只能由本 change 这种新的独立准入复核来做。
- 1.1 通过后的下一步才记录当前 aggregate 是否仍因失败观察而 hold。本卡不预填该判定，不新建 replay，不改 Python，不启动 restricted-promotion、六章包、规模质量或生产。

## Capabilities

### New Capabilities

- `review-stage4-aggregate-admission-after-material-input-successor`: 复核四个当前局部通过能否形成 Stage 4 aggregate 准入。1.1 通过前不写 hold 或 aggregate true。

### Modified Capabilities

- 无。不改已归档 aggregate 合同、材料投入 successor、产销量、分部财务或客户供应商观察。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 不改 publication、closure、mode、identity、checkpoint。`production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。
