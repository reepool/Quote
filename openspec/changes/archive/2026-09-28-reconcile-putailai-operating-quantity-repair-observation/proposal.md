## Why

璞泰来 operating-quantity repair 仍是活动 change，但它的四报告回归已经有独立 source-review：recall 36/38、accuracy 36/36、critical numeric errors 0、`expansion_gates_met=false`。失败原因是该 bundle 没有交付宁德时代第 21、22 页两条分段销量。CATL `.3` 后来补上了这两条并得到 38/38，那是下一次独立修复，不能回填璞泰来 `.2`。

## What Changes

- 新建一份只做账本收口的 reconciliation。1.1 通过前不改 Python，不重写任何 replay，不入队，不写新的 source-review，不预填指标。
- 冻结璞泰来 `replay/20260928.2` 的 enqueue、run、result、source_review 字节。权威结果保持 recall 36/38、accuracy 36/36、critical numeric errors 0、coverage 13/13、`expansion_gates_met=false`。
- 分开三层事实：璞泰来目标修复的 23 条事实已正确交付；该次四报告回归仍是 36/38；两个缺口已由已归档的 CATL `.3` change 修复，但不覆盖璞泰来历史 bundle。CATL `.3` 的 38/38 不是璞泰来 `.2` 的重算。
- 收口方式按仓库已有先例确定：`expansion_gates_met=false` 的历史观察可以归档，且必须保留失败，不得改写成通过。因此本 reconciliation 选择把璞泰来 repair 作为「目标修复已完成、四报告回归失败观察予以保留」的历史 change 归档。不新建 successor replay，也不修改旧制品。
- 阶段 4 原始 operating-quantity 观察仍是 `replay/20260928` 的 26/36、26/27、critical numeric errors 1、gate false。它、璞泰来 36/38、CATL 38/38 继续作为三个独立结果。材料投入 23/23、阶段 4 材料投入 19/23 和固定两家公司 9/9 也不并入其中。
- 不授权扩大、规模质量、生产、六章制造业包、客户供应商、regime、DCF、交易或价格敏感性。

## Capabilities

### New Capabilities

- `reconcile-putailai-operating-quantity-repair-observation`: 记录璞泰来 `.2` 的失败回归如何与 CATL `.3` 的后续修复并存，并规定该活动 change 以保留失败观察的方式归档。1.1 通过前不执行归档。

### Modified Capabilities

- 无。不改写璞泰来 `.2`、CATL `.3` 或 `replay/20260928` 的规范观察。

## Impact

- 1.1 通过前只增加本 change 的范围文档。
- 通过后的收口只更新璞泰来 change 的账本文字并按日期归档；四份 `.2` 制品必须保持 100% 重命名、字节不变。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。common-core identity、publication、closure、mode 和 checkpoint 不改。
