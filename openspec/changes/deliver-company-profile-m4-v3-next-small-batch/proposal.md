## Why

`revenue_sentence_repair=v3` 已在 `600007.SH` 和 `600010.SH` 上跑通，召回 18/18、准确 21/21、关键数字错误 0。扩大门槛仍是 false，因为运行耗时未评估，不能回填估计值。下一阶段要在两份尚未交付的年报上验证同一身份的业务交付。

## What Changes

- 新批次显式使用 v3 身份、调用方指定的快照目录、两家公司和共享 50000 token。真实耗时只记录实测值。
- 冻结计划的保存和读回使用同一个目录，避免新批次写入旧的唯一计划目录。
- 按既定交易所和代码顺序各选一家 v3 下尚未交付的服务公司和制造公司，跑接受、持久化、查询和导出。
- 独立重读按原文清单计召回，角色按公司、方向和源名去重，准确率覆盖全部交付关联，并记录实际 token 和耗时后再计算门槛。

## Capabilities

### New Capabilities

- `deliver-company-profile-m4-v3-next-small-batch`: 用已归档的 v3 修复身份交付下一批两份年报，并按实测成本计算门槛。

### Modified Capabilities

- 无。已归档的 5/9、18/18、19/21 和 v3 的 18/18 不改写。

## Impact

- 冻结写入在 `CompanyProfileTaskService.freeze_m4_next_batch_plan`。owner 仍是这一处，不另建执行链。
- 商品补选仍是现有名称路径。新报告用来观察它的真实覆盖，不在本变更里预填分数。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。
