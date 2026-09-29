## Why

分部财务现在有三层独立观察，其中两层仍是活动中的失败账本。原始 `replay/20260928` 是 recall 122/132、accuracy 129/133、critical numeric errors 4、gate false。列角色修复 `.2` 是 recall 102/132、accuracy 114/142、critical numeric errors 0、gate false。已归档的 footnote-dimension `.3` 是 recall 132/132、accuracy 144/144、critical numeric errors 0、gate true，但不能回填前两层。需要一份只做账本收口的 reconciliation，把失败观察归档并继续显示 false。

## What Changes

- 新建一份 reconciliation。1.1 通过前只写本 change 的范围文档，不改 Python，不 replay，不改旧制品，不启动客户供应商、六章包、规模质量或生产。
- 原始 `.1` 与 column-binding `.2` 的 enqueue、run、result、source-review 按原字节和哈希冻结。`.3` 的 132/132、144/144 是后续独立 repair，不回填 `.1` 或 `.2`，也不把历史失败改成通过。
- 账本保持三层独立：原始分部财务失败；列角色修复后的来源维度失败；footnote-dimension 修复后的局部通过。
- 参照已归档的失败观察先例，1.1 通过后可以把这两个 gate false 的历史 change 归档为“保留失败观察”。归档账本必须继续显示 false。原始分部财务 task 3.3 与 column-binding task 3.2 继续保持未勾选。
- 通过后只更新这两个 change 的 design/tasks 账本，以日期目录归档，并逐字节核对旧制品。活动目录删除后，归档路径必须可追溯。
- 不处理 operating-quantity 的 26/36。那是另一章的历史观察，应另立独立 reconciliation。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。

## Capabilities

### New Capabilities

- `reconcile-segment-financial-repair-observations`: 把分部财务的两本失败账本作为保留失败观察归档，并与已通过的 `.3` 局部观察分开。1.1 通过前不授权归档。

### Modified Capabilities

- 无。不改写三层 replay 的指标，也不改 operating-quantity 的 26/36。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的改动限于两本失败 change 的账本文字和归档路径。
- 不改 common-core identity、publication、closure、mode、checkpoint 或生产授权。
