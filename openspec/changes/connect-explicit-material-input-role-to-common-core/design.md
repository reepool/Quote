## Context

v8 repair 已归档。正式复核是 recall 8/9、accuracy 8/8、critical numeric errors 0、`expansion_gates_met=false`。产品合同要求原文明示投入品形成 `raw_material_input`。`ChapterTask.EXTRACT_MATERIAL_INPUTS`、`material_input` Relationship、Stage 5 provider/acceptance/writer，以及 `derive_commodity_role` 对 `RelationshipType.MATERIAL_INPUT` 派生 `raw_material_input` 都已经存在。当前 `COMMON_CORE_CHAPTERS` 只有 overview 和 segment，`EXTRACT_MATERIAL_INPUTS` 被固定为 `not_activated`。缺口在章节激活和 Evidence 路由，不是缺少领域模型。

## Goals / Non-Goals

**Goals:**

- 原文明示具名物料是公司生产或经营投入时，沿现有 owner 交付 `material_input` Relationship 和 `CommodityExposure(role=raw_material_input)`。
- 保留来源名称、主体、报告身份、期间和 Evidence。没有数量或市场序列仍交付关系。
- 用正交 identity `material_input_facts=v1` 发布 successor，同时保留 `owned_page_facts=v8`。
- 用通用规则覆盖正式正例、无明示角色的负例，以及至少一个既有非东风制造业 fixture。

**Non-Goals:**

- 不新建 extractor、模型、ChapterTask、Role、action、writer 或执行链。
- 不启用产销量、材料成本比例、采购模式、战略储备、供应商或客户章节。
- 不抽机场吞吐量、补偿或汽车产销量。
- 不建设完整制造/材料 package，不扩其他行业。
- 不改 publication scope、closure、completed mode、生产授权或下一轮扩大状态。
- 不启用 DCF、交易、价格预测或市场序列绑定。
- 不在完全相同的 v8 identity 下 force replay。不预设 recall 9/9 或门槛通过。

## Decisions

1. **只连接现有材料路径。**
   激活条件从“本切片固定不启用”改为“原文已经明示具名投入品”。投影、接受和商品角色继续走 Stage 5 与 `derive_commodity_role`。能源证据仍走既有 `energy_consumption`。

2. **明示投入才激活。**
   句子必须把具名物料说成公司生产或经营投入。只说价格上涨、只给“直接材料成本”或“原材料存货”等泛称、只给资产负债表金额，都不激活。销售证据本身不能推出投入角色。若另一份独立且已接受的证据说明同一 source-native 商品也是生产投入，则同时保留 `product_sales` 和 `raw_material_input`，不覆盖、不净额化。不得用行业常识补名称。

3. **映射失败不丢掉已成立角色。**
   复用现有 `mapped` / `pending` / `ambiguous`。无匹配为 `pending`，多个候选无法唯一选择为 `ambiguous`，二者 `commodity_id` 都为空，并保留 `source_native_name`。只有 `mapped` 才允许非空唯一 `commodity_id`。不新建 catalog，不改映射表，不绑市场序列。

4. **identity 正交，不覆盖 v8。**
   新 work 使用 `{"rules":"company_profile_common_core.v1","owned_page_facts":"v8","material_input_facts":"v1"}`。v1–v8 work 和全部旧 source-review 保留。查询按当前 identity 选择 successor。

5. **样本只在实现审核之后 replay。**
   正式年报里已确认的具名投入句是正例形状；无明示商品角色的服务公司年报是负例形状。规则不得按证券、页码或具体物料名硬编码。另用一个既有非东风制造业 fixture 证明不是单公司补丁。受控 replay 和独立重算晚于 1.1 范围审核和实现审核。

## Risks / Trade-offs

- [价格风险句被放得过宽] → 没有把具名物料说成公司投入时拒绝。
- [销售证据被当成投入] → 只有销售证据时不生成 `raw_material_input`；独立投入证据存在时两个角色都保留，不净额化。
- [目录无法唯一映射] → 仍交付投入角色，`pending` 或 `ambiguous`，`commodity_id` 为空。
- [能源被标成原料] → 既有能源判断优先，角色保持 `energy_consumption`。
- [replay 仍达不到扩大门槛] → 记录现场结果，不改门槛，不授权生产或下一轮扩大。

## Migration Plan

1. 1.1 范围审核通过前不改业务代码，不 replay。
2. 实现章节激活、路由和反例测试。
3. 审核通过后只对已冻结报告做受控 successor replay。
4. 独立重算 recall、accuracy、critical numeric errors 和 workload。回滚时保留 v8 文件和失败观察，不删除 successor work，除非另有审核。

## Open Questions

- 无。正式样本的指标留到 replay 之后再记，不在本设计里预设 9/9 或门槛通过。
