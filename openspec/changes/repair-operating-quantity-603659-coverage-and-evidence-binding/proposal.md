## Why

2026-09-28 的产销量 source-review 已经绑定四份冻结年报，但业务验收不通过：source recall 26/36、source accuracy 26/27、critical numeric errors 1、coverage 13/13、`expansion_gates_met=false`。缺口和错绑都在 `603659.SH` 2025 年报，不是合法空值，也不是 replay 控制面。

## What Changes

- 新建一份最小 repair 范围审核。1.1 独立范围审核通过前，不改 Python，不改 Evidence plan，不入队，不 replay，不写新的 source-review，不预填 recall、accuracy、critical numeric errors 或 gate。
- 只修 `603659.SH` 2025 年报，只复用现有 `extract_operating_quantities`。不新增报告，不重选样，不复核 regime。定义样本仍是已经审核过的四份报告。
- 修复 2026-09-28 source-review 列出的 10 个未交付数量事实。第 15 页必须分成两条事实：PVDF 有效产能保留「超过」限定词；勃姆石和氧化铝有效产能保留「已达」限定词。两条事实各自使用自己的 bounded quote、对象和 Evidence。现有把 PVDF quote 绑到勃姆石和氧化铝、并把「超过 3 万吨」落成精确值 3 的记录不得再视为正确事实。
- 基膜 20 亿平方米在第 15 页和第 27 页的重复披露只计一次。第 14 页涂覆加工量和第 19 页涂覆隔膜正式销量继续保持两个独立锚点，不换算、不合并。
- 保留这次复核里已经正确的 26 条事实和 13 条 coverage。`302132.SZ` 的 `not_applicable` 和 `920015.BJ` 的 `not_disclosed` 保持不变。合法空值不得改写成数量。
- 研究输出仍只到新的隔离目录，disposition 为 `accepted_for_review`。不写 common-core、publication、closure、mode 或生产授权。不在 `replay/20260928` 内 force replay，也不改写该目录的 enqueue、run、result 或 source_review。后续使用新的 successor 计划和新的 replay 目录。
- 不授权下一轮扩大、规模质量声明、生产、六章制造业包、客户供应商、DCF、交易或价格敏感性。

## Capabilities

### New Capabilities

- `repair-operating-quantity-603659-coverage-and-evidence-binding`: 在现有 `extract_operating_quantities` 上补齐璞泰来漏交付的数量事实，并拆开第 15 页两条产能 Evidence。1.1 通过前不授权实现。

### Modified Capabilities

- 无。`scope-manufacturing-materials-stage4-operating-quantities-holdout` 的 20260928 replay 保持为未通过的历史观察：recall 26/36、accuracy 26/27、critical numeric errors 1、coverage 13/13、`expansion_gates_met=false`。本 change 不改写这些制品，也不改材料投入 23/23、阶段 4 的 19/23 或固定两家公司的 9/9。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的实现只复用现有产销量章节和研究隔离 bundle。页码、产品名和比较限定词只作为验收 fixture，不得写进抽取规则。
- `scale_quality_claim_allowed` 保持 false，`production_authorization` 保持 `not_authorized`。
- 新的 recall、accuracy、critical numeric errors 和 gate 只由后续独立 source-review 重读后填写。通过前不进入原 change 的 3.3，也不归档。
