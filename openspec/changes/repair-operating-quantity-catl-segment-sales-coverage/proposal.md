## Why

`repair-operating-quantity-603659-coverage-and-evidence-binding` 的 successor source-review 已经成立，但业务门槛不通过：source recall 36/38、source accuracy 36/36、critical numeric errors 0、coverage 13/13、`expansion_gates_met=false`。失败原因是 `300750.SZ` 2025 年报另有两条独立销量，不是璞泰来修复回退，也不是 Evidence 错绑。

## What Changes

- 新建一份最小 repair 范围审核。1.1 独立范围审核通过前，不改 Python，不入队，不 replay，不写 source-review，不预填 recall、accuracy、critical numeric errors 或 gate。
- 只补 `300750.SZ` 2025 年报第 21 页和第 22 页，只复用现有 `extract_operating_quantities`。不新增报告，不重选样，不复核 regime。另外三份报告只作为 successor 回归样本。
- 动力电池销量 541 GWh 和储能电池销量 121 GWh 各自成为独立 `sales_volume` 事实。每条事实绑定自己的物理页、对象、bounded quote、Evidence、报告身份和页面文本 hash。不得把 541 与 121 相加成 662，也不得用它们覆盖产销表里的电池系统 661 GWh。
- 不改当前璞泰来 repair 的 36 条结果、13 条 coverage，也不改写它的 `replay/20260928.2` enqueue、run、result 或 source_review。旧 `replay/20260928` 制品同样保持不变。
- 研究输出只到本 change 的新隔离目录，disposition 为 `accepted_for_review`。计划版本冻结为 `manufacturing_materials_stage4_operating_quantities.2026-09-28.3`。不写 common-core、publication、closure、mode 或生产授权。
- 不授权下一轮扩大、规模质量声明、生产、六章制造业包、客户供应商、regime、DCF、交易或价格敏感性。

## Capabilities

### New Capabilities

- `repair-operating-quantity-catl-segment-sales-coverage`: 在现有 `extract_operating_quantities` 上补齐宁德时代动力电池和储能电池两条分段销量。1.1 通过前不授权实现。

### Modified Capabilities

- 无。璞泰来 repair 的 20260928.2 观察保持为 recall 36/38、accuracy 36/36、critical numeric errors 0、coverage 13/13、`expansion_gates_met=false`。本 change 不改写该观察，也不改 20260928 的失败观察、材料投入 23/23、阶段 4 的 19/23 或固定两家公司的 9/9。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的实现只复用现有产销量章节和研究隔离 bundle。证券代码、页码和产品名只作为验收 fixture，不得写进抽取规则。
- `scale_quality_claim_allowed` 保持 false，`production_authorization` 保持 `not_authorized`。
- 新的 recall、accuracy、critical numeric errors 和 gate 只由后续独立 source-review 重读后填写。通过前不进入本 change 的收口，也不归档。
