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
   具名物料和公司自身投入必须在同一完整句里写明。采购、购入、消耗、投入的主语必须是公司或本公司，客户、供应商、下游企业不能充当主语。主要原材料清单不能只因为后面出现“制造”或“生产”就激活，句子必须写到公司自身的制造、生产或经营成本。只说价格上涨、只给“直接材料成本”或“原材料存货”等泛称、只给资产负债表金额，都不激活。销售证据本身不能推出投入角色。若另一份独立且已接受的证据说明同一 source-native 商品也是生产投入，则同时保留 `product_sales` 和 `raw_material_input`，不覆盖、不净额化。不得用行业常识补名称。

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

## Field result

受控 successor 只覆盖已冻结的 600004.SH 与 600006.SH。3.1 新增两条 successor work。3.2 和 3.3 没有再新增 work。没有重新选样，也没有启动下一轮扩大。两家各仍只有 6 条 completed work，其中 3.1 新增的是：

- 600004.SH：`bp-work-946a0a5296c2144c1ebc161a`
- 600006.SH：`bp-work-7b81740827ce0ccca634e2e9`

processing identity 为 `{"rules":"company_profile_common_core.v1","owned_page_facts":"v8","material_input_facts":"v1"}`。

身份绑定链是：

1. `data/checkpoints/company_profile_common_core/reports/material_input_facts_v1_successor_enqueue.20260926.json`
2. 上面两个 work ID
3. 各 work 内的 processing identity
4. query 优先返回这两个 work 的交付
5. `data/checkpoints/company_profile_common_core/reports/material_input_facts_v1_successor_source_review.20260926.json`，SHA-256 `9f6fb61e4e7fdbee9e680d091c0bc4bc13ca89f319b92cdd0457fdc3e2cdfbec`

`material_input_facts_v1_successor_live_run.20260926.json` 的 SHA-256 是 `456345ea55253717dc2316227810f12e414ba1df9a26b92d19fd780216fd2871`，与 v5–v8 live-run 字节相同。它只证明冻结样本和两家都已交付，不单独证明 successor 身份。live-run schema 不改。

Source review 由正式模型派生，不是手工填写门槛：

- source recall 9/9
- source accuracy 9/9
- critical numeric errors 0
- independently reviewed reports 2，occupied strata 2
- tokens 0
- elapsed 5.149175 秒
- human review unassessed
- `expansion_gates_met=true`

三层语义分开：

- `expansion_gates_met=true` 只表示固定两家公司、本次 successor identity 的验收门槛通过。
- `scale_quality_claim_allowed=false`，不能声称全市场或规模化质量已经成立。
- `production_authorization=not_authorized`，不授权生产。

本轮没有修改 first-expansion completed mode，没有覆盖 closure v2，没有启用完整制造业包，没有抽取产销量、材料成本率、采购、储备、供应商或客户，也没有开启 DCF、交易、价格敏感性或生产。

## Open Questions

- 无。现场结果已记在 Field result。门槛通过不授权下一轮扩大。
