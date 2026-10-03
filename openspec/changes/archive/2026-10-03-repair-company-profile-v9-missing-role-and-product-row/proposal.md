## Why

v9 轮 20/22 的两项漏交均有原文依据，不能靠删减分母解决；复核本身还有两处口径问题：同一张产品表内电力及热力行被独立计召回而港口、运输产品行被视为重复，身份规则不一致，且 30/30 只对应已写判定，六维答案中的收入答案与石化两条普通 Activity（石油与天然气勘探开采、管道运输）没有明确的审核对应。扩批继续暂停。

## What Changes

- 补交化工原料油内部销售：在既有句子绑定中保留源名"化工原料油"、主体"炼油事业部"、量词"部分"、口径"内部销售"及接收方"化工事业部"。原文未披露数量就保持为空；目录未收录则 pending。验收到接受、查询和导出。
- 恢复折行产品行并打通接受链：修表内标签拼接，恢复 `product/电力及热力` 及原始金额、单位。纯内存诊断显示，仅补标签会让行业和产品两条收入 Measurement 同时触发 `occurrence_semantic_conflict`；须在现有身份判定（reconciliation slot）中区分两种栏目，确保新增产品记录时，已有行业收入仍被接受和交付。
- 统一口径后做隔离复核。观察前固定规则：不同栏目/dimension 的来源行分别判断；同一身份跨页重述只算一次；同一来源生成 Segment 和 Measurement 时，重复数值不增加召回，每条交付仍需核对。明确列出六维答案、角色及全部 accepted facts 的审核对应。使用新身份（`revenue_sentence_repair=v10`）、独立目录和同一冻结报告重跑，重新计分；耗时按整轮计算，沿用 300 秒、共享 50000 token 及原质量门槛。

## Capabilities

### New Capabilities

- `repair-company-profile-v9-missing-role-and-product-row`: 补交两项漏交并以统一口径完成隔离复核。

### Modified Capabilities

- 无。v9 的 20/22、30/30、false 保留原样，不回写。

## Impact

- 改动在既有句子绑定、表内标签拼接和 reconciliation slot 判定。不新增执行链。
- 生产授权保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。
- successor 通过后才另立未见样本卡；本卡不选择新公司。
