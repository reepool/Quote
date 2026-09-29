## Why

材料投入切片已经在三份冻结年报上完成局部闭环，但仍不能说明制造/材料能力具备跨样本普适性。下一步应验证另一个已经存在、且尚未进入 common-core production 的章节 `extract_operating_quantities`，而不是继续扩修已归档的材料入口或启动生产。

## What Changes

- 新建一份阶段 4 第二个最小竖切的范围审核。1.1 独立范围审核通过前，不改 Python，不改 Evidence plan，不改 identity、publication、closure 或 completed mode，不入队，不 replay。
- 只选择现有章节 `extract_operating_quantities`。检查范围是产能、在建产能、产能利用率，以及生产量、销量、库存量的区分。单位、期间、跨页表格和物理页锚点属于这条章节的证据边界。空结果必须分开落盘：目标章节可读且该数量适用但未披露为 `not_disclosed`；原文明示或结构明确不适用为 `not_applicable`；证据存在但主体、单位、期间或表头不能唯一确定为 `unclear`；页、表头、单位或续页无法绑定为 `extraction_failed`。`legal_empty` 如果出现在 bundle 层，只能包住其中一个具体 `coverage_status`，不能代替它。`920015.BJ` 的产能章节可读但未披露产销量，fixture 是 `not_disclosed`；`302132.SZ` 写明产品众多、无法分类统计，fixture 是 `not_applicable`。
- 样本只能来自已经批准的制造/材料研究清单：`300750.SZ`、`603659.SH`、`920015.BJ`，以及此前材料投入 replay 未使用的 holdout 候选 `302132.SZ`。不新增未经 dossier 审核的发行人。定义样本至少 3 份，并覆盖 SZSE、SSE、BSE；其中至少 1 份必须是材料投入 replay 的 holdout。`302132.SZ` 只有在独立 dossier 确认该章节适合后再进入定义样本；不适合时停止，不另找清单外的报告。
- 至少两份报告必须呈现不同的数量披露形态，不能只用同一张产销量表完成验收。已有研究基线里可见的两种形态是：分类实物量把生产量、销量和库存量分开；产能、利用率和在建产能章节可读，但没有产量。后一种的产量是 `not_disclosed`，不是 `not_applicable`。这两种形态都要由新的 dossier 重新确认，不能把旧 gold 写进运行时。
- 复用现有 Evidence、Stage 5 extract/repair/verify 和研究隔离 bundle。输出只到 `accepted_for_review`。不得从产能或利用率倒算产量，不得把销售金额当销量，不得把存货金额当库存量，不得用行业常识补数量。
- 不启用完整六章制造业包、客户供应商、regime、DCF、交易或价格敏感性。不预设 recall、accuracy、critical numeric errors 或 gate。不授权下一轮扩大、规模质量声明或生产。

## Capabilities

### New Capabilities

- `scope-manufacturing-materials-stage4-operating-quantities-holdout`: 用已批准清单上的跨交易所样本，审核现有 `extract_operating_quantities` 是否能以同一个最小入口覆盖至少两种数量披露形态，并保持研究隔离。1.1 通过前不授权实现。

### Modified Capabilities

- 无。已归档的材料投入 23/23 只覆盖三份报告、`.2026-09-26.2` 和 `extract_material_inputs`。阶段 4 的 19/23 与固定两家公司的 9/9 保持独立。本 change 不改这些观察，也不改 common-core 规范。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的实现只复用现有产销量章节和研究隔离 bundle，不写 common-core checkpoint、work、publication、closure、mode 或生产 identity。
- `scale_quality_claim_allowed` 保持 false，`production_authorization` 保持 `not_authorized`。
- 材料投入 replay 使用过的三份报告可以再次出现，但定义样本必须另含至少一份 holdout。`302132.SZ` 仍是阶段 3 regime 基线；本 change 不因此重新验证 regime。
