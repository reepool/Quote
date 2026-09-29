## Why

产销量原始 holdout 仍是活动中的失败观察：`replay/20260928` 为 recall 26/36、accuracy 26/27、critical numeric errors 1、gate false。失败点是第 15 页 PVDF 与「勃姆石和氧化铝」的 Evidence/对象绑定。Putailai `.2` 已归档并保持 36/38、gate false。CATL `.3` 已归档并保持 38/38、gate true，但不能回填原始观察。需要一份只做账本收口的 reconciliation，把这三层分开，并把原始失败观察归档且继续显示 false。

## What Changes

- 新建一份 reconciliation。1.1 通过前只写本 change 的范围文档，不改 Python，不 replay，不改任何历史制品，不启动六章包、客户供应商、规模质量或生产。
- 原始 `.1` 的 enqueue、run、result、source-review 按原字节和哈希冻结。task 3.2 与 task 3.3 保持未勾选。
- Putailai `.2` 保持 36/38、36/36、critical numeric errors 0、gate false。其中 23 条目标事实正确交付，但不回填原始 `.1`。
- CATL `.3` 保持 38/38、38/38、critical numeric errors 0、gate true。这个 true 只属于计划 `manufacturing_materials_stage4_operating_quantities.2026-09-28.3` 和 `extract_operating_quantities`，不重算 `.1` 或 `.2`。
- 1.1 通过后，才更新原始 operating-quantity change 的失败账本，保留 26/36 和 gate false，用日期目录归档并核对四份制品字节。不为这次失败再建 successor replay。
- 原始失败观察归档并核对通过后，才归档本 reconciliation 自身。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。

## Capabilities

### New Capabilities

- `reconcile-operating-quantity-repair-observations`: 把产销量原始 26/36 失败观察作为保留失败观察归档，并与已归档的 Putailai `.2` 和 CATL `.3` 分开。1.1 通过前不授权归档。

### Modified Capabilities

- 无。不改写三层 replay 的指标，也不改分部财务已经收口的三层观察。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的改动限于原始 operating-quantity change 的账本文字和归档路径，以及随后本 reconciliation 自身的归档。
- 不改 common-core identity、publication、closure、mode、checkpoint 或生产授权。
