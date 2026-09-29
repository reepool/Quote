## Why

阶段 4 的 aggregate 合同已经归档为 hold。阻塞不是实现缺陷：`302132.SZ` 从未作为 `extract_material_inputs` 的定义样本，现有 MI-2 只有三份报告，因此四个纳入计划不是同一四报告集。历史上排除它，是因为它当时只作为 regime 基线，并不等于已经证明材料投入章节适合或不适合。现在必须先做专章 dossier，不能直接创建四报告 replay，也不能重开 aggregate 合同。

## What Changes

- 新建一份只读范围与 dossier 评估。1.1 通过前只写 proposal、design、spec、tasks，不写 dossier，不判断适合或不适合。
- 只评估 `302132.SZ` 的 2025 年报，且只评估现有 `extract_material_inputs`。报告身份、PDF 和报告期 `2025-12-31` 使用已经批准的四报告绑定，不另选文件。
- 2.1 必须新建专门的材料投入 dossier。旧 regime dossier 和 regime 样本身份不能当作本章审核，也不重新审核重大重组或 package regime。
- dossier 只核对采购模式及具名采购对象、正式主要原材料表、成本构成中的具名投入、关联采购中的具名材料行、风险章节中的明确原材料披露，以及可能成立的 `not_disclosed`、`not_applicable`、`unclear` 或 `extraction_failed`。
- 每个候选必须绑定物理页、正式栏目和表头、bounded quote、页面文本 hash、主体、报告期、source-native 名称及必要单位。不得用航空制造常识、能源、销售对象、存货金额或泛称“直接材料”补出具名投入，不得混合重组前后事实，也不得把合法空值写成已观察事实。
- 2.2 只允许两种结果。适合时，只允许以后另立一个独立的四报告 material-input successor 范围 change，本卡仍不实现、不 replay。不适合时，记录原因并停止，Stage 4 aggregate 继续 hold，不从清单外补发行人。
- 不改 MI-1、MI-2 或任何历史制品，不把 `302132.SZ` 事后补入 MI-2，不创建四报告 successor replay，不重开 aggregate gate。`production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。

## Capabilities

### New Capabilities

- `assess-302132-material-input-four-report-successor-scope`: 只判断已批准的 `302132.SZ` 2025 年报是否适合现有 `extract_material_inputs`。1.1 通过前不写 dossier，不预填适合或不适合。

### Modified Capabilities

- 无。不改材料投入历史观察、aggregate 合同或其他章节规格。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的 2.1 只写新的材料投入 dossier，2.2 才给出适合或不适合。本卡不预填该结论。
- 不改 Python，不入队，不 replay，不新增发行人，不启动 restricted-promotion、六章包、规模质量或生产。
