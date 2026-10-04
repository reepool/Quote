## Why

v16 轮收费机制正文已完整,但 owner 审核复现"公司的经营模式主要为:为客户提供……服务,计划收取货物堆存费……"仍得到收入 answered=true(v15 同句为 false):现行计划守卫只检查当前从句,服务从句借用了后一从句的计划性收取动词。此外 v16 复核把收费机制判定计入准确率(并非仅召回),且同一日照 Overview 又有 carrier 判定,准确率 29 与"28 个对象各评价一次"不一致;守卫条款也未同步进主规格。

## What Changes

- 修复计划收费提前通过:计划性收取(尚未/拟/计划/预期/将+收取/形成)升级为句级否决——服务描述从句借用后续计划性收取动词时不得成立;以真实 p9 形态反例固化自动测试,经接受与答案生成链验证;免费附带服务从句、两个收费正例及其余四个负例全部保持。
- v17 隔离重跑与计分收口:新 `revenue_sentence_repair=v17` 累积身份、独立目录、同一冻结计划,首次真实执行计整轮耗时,共享 50000 token、300 秒门槛。收费机制判定对应唯一日照 Overview 并核完整正文,去掉该记录重复的 carrier 判定,准确率按实际对象(六维答案+22 条事实=28)各评价一次;保留旧观察并附更正说明,同步主规格中的最终行为与有限验收结论。

## Capabilities

### New Capabilities

- `repair-company-profile-v16-planned-fee-and-review-counts`: 计划性收取句级否决与计分去重后的 v17 正式验收。

### Modified Capabilities

- 无。v16 的 17/17、29/29、true 保留原样,不回写。

## Impact

- 改动在收入判定的守卫作用域与复核的判定映射。不新增执行链。
- `production_authorization` 保持 `not_authorized`,`scale_quality_claim_allowed` 保持 false。
- 双 100%、关键数字错误 0、整轮≤300 秒通过后,才下发排除十六家已观察公司的未见样本范围卡。
