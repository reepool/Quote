## Why

`replay/20260928` 的独立 source-review 没有通过：source recall 122/132，source accuracy 129/133，critical numeric errors 4，`expansion_gates_met=false`。失败集中在同一页多张表的列角色串线，以及十个当期正式表单元格没有交付。当前 change 保持未归档，这次失败观察保持不变。

## What Changes

- 新建一份最小修复范围。1.1 通过前只写本 change 的范围文档，不改 Python，不 replay，不预填 132/132、accuracy、critical numeric errors 或 gate。
- 只复用现有 `extract_segment_financials`。样本仍是已批准的四份 2025 年报：`300750.SZ`、`603659.SH`、`920015.BJ`、`302132.SZ`。不新增发行人，不重选样。
- 修复 `300750.SZ` 物理页 25 的表边界和列角色。收入构成续表的上年收入、上年占比和增减列不得写成当期营业成本或毛利率。source-review 中的四条错误记录必须从 successor 结果中消失，不能靠改旧 Evidence、改名或替换旧 result 掩盖。同一页 10% 以上表的境内、境外当期成本和报告毛利率继续保留。
- 补齐 source-review 列出的十个当期单元格，并各自形成 Measurement：`300750.SZ` 六个，`920015.BJ` 两个，`302132.SZ` 两个。
- 表识别必须绑定完整表头、列角色和表边界。抽取规则不得写死证券代码、页码、公司名或产品名；这些名称只作为 source-review fixture。
- 已正确的记录和 coverage 边界不得回退。`118.30` 与 `104.19` 仍不是毛利率；合计行“-”和空抵消金额仍不是 0；公司整体 `29.98%` 仍不是分部毛利率；`consolidation_adjustment`、`not_applicable`、`not_disclosed`、`unclear` 保持原语义。
- successor 计划版本固定为 `manufacturing_materials_stage4_segment_financials.2026-09-29.2`，输出写入新的隔离目录。原 `replay/20260928` 的 enqueue、run、result、source-review 保持字节不变。
- 修复完成后仍需新的受控 replay 和独立 source-review。补齐这十四项不能假定 gate 通过。

## Capabilities

### New Capabilities

- `repair-segment-financial-column-binding-and-cell-coverage`: 在原四份报告上修复分部财务的列角色串线，并补齐 source-review 列出的十个当期单元格。1.1 通过前不授权实现。

### Modified Capabilities

- 无。不改写 `replay/20260928` 的失败观察，也不改产销量或材料投入的历史结果。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的实现只复用现有 `extract_segment_financials`、Stage 5 单章节准备和研究隔离 bundle。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。common-core identity、publication、closure、mode、checkpoint 和既有 replay 不改。
