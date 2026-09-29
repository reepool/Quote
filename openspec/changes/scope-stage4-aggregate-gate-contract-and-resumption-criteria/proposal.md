## Why

阶段 4 的四个章节切片和九行历史观察都已归档，aggregate admission 的结论是 hold。缺的是一份可以独立审核的 Stage 4 aggregate gate 合同。没有这份合同，局部通过不能推导整体质量，也不能恢复扩大。

## What Changes

- 新建一份只读合同。1.1 通过前只写 proposal、design、spec、tasks，不改 Python，不入队，不 replay，不预填 aggregate gate 或跨章节指标。
- 冻结 aggregate 纳入的四个章节和对应计划版本。业务概览与经营体制不纳入。六章包保持关闭。
- 冻结统一报告集为已批准的四份 2025 年报，并要求同一报告集同时覆盖 SZSE、SSE、BSE。三报告的材料投入通过不能改写成四报告样本。
- 规定每章 source-review、冻结身份和制品哈希的绑定要求。历史失败观察与 critical numeric errors 必须保留。局部 true 不得回填、覆盖或跨章节相加。
- 任一条件不满足时，后续判断只能记录 hold。aggregate gate 通过前不得进入 restricted-promotion 设计。即使将来通过，也最多允许另开设计卡，不直接授权生产。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。publication、closure、mode、identity、checkpoint 保持不变。

## Capabilities

### New Capabilities

- `scope-stage4-aggregate-gate-contract-and-resumption-criteria`: 定义 Stage 4 aggregate gate 的纳入范围、报告集、失败保留和恢复条件。1.1 通过前不授权判断或实现。

### Modified Capabilities

- 无。不改写已归档观察、账本或 admission hold。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的 2.1 只冻结合同要素，2.2 才对照已审核账本做判断。本卡不预填该判断。
- 不新增报告，不重选样本，不新建 successor replay，不实现 promotion。
