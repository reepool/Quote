## Why

`replay/20260929.2` 的独立 source-review 没有通过：source recall 102/132，source accuracy 114/142，critical numeric errors 0，`expansion_gates_met=false`。金额、单位、Evidence 页面和关键数字没有错；失败收敛为附注表的来源维度没有绑到印刷栏目。当前 change 与原分部财务 change 都保持未归档，这两次失败观察保持不变。

## What Changes

- 新建一份最小修复范围。1.1 通过前只写本 change 的范围文档，不改 Python，不 replay，不预填 recall、accuracy、critical numeric errors 或 gate。
- 只复用现有 `extract_segment_financials`。样本仍是已批准的四份 2025 年报：`300750.SZ`、`603659.SH`、`920015.BJ`、`302132.SZ`。不新增发行人，不重选样，不复核 regime。
- 修复 `920015.BJ` 物理页 139。地区表使用印刷表头「地区分部」，业务表使用印刷表头「业务分部」。两个栏目即使金额相同也分别保留。合计行、普通行和抵消列不得因金额相同合并。
- 修复 `302132.SZ` 物理页 178。来源维度使用该页正式标题「报告分部的财务信息」。整页金额不再归入泛化的「报告分部」。分部间抵销继续使用 `consolidation_adjustment`。
- 抽取规则读取页面上的印刷栏目名，不得写死证券代码、页码、公司名或产品名。这些名称只作为 source-review fixture。
- 已正确的结果必须保留：`300750.SZ` 页 25 的列角色修复、十个已补交单元格、`603659.SH` 的抵消行及空毛利率、`920015.BJ` 的合计行「-」、空抵消金额和公司整体 `29.98%` 边界，以及全部报告身份、单位、页码、bounded quote 和页面文本 hash。
- successor 使用新计划 `manufacturing_materials_stage4_segment_financials.2026-09-29.3`，输出写入本 change 下的新隔离目录。`replay/20260929.2` 的 enqueue、run、result 与 `replay/20260928` 的 enqueue、run、result、source-review 保持字节不变。原分部财务 change 的 3.2 继续保持未通过，不由本 change 改写。
- 修复完成后仍需新的受控 replay 和独立 source-review。本范围不假定 gate 通过。

## Capabilities

### New Capabilities

- `repair-segment-financial-footnote-dimension-binding`: 在原四份报告上把附注表来源维度绑回印刷栏目，并保持列角色修复和已补交单元格。1.1 通过前不授权实现。

### Modified Capabilities

- 无。不改写 `replay/20260929.2` 或 `replay/20260928` 的失败观察，也不改产销量或材料投入的历史结果。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的实现只复用现有 `extract_segment_financials`、Stage 5 单章节准备和研究隔离 bundle。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。common-core identity、publication、closure、mode、checkpoint 和既有 replay 不改。
