## Why

v8 的 common-core repair 已归档，正式复核是 recall 8/9、accuracy 8/8、critical numeric errors 0、`expansion_gates_met=false`。剩下的已确认缺口是原文明示的具名投入品没有进入 `raw_material_input`。产品合同已经要求这条角色，现有 `extract_material_inputs`、`material_input` Relationship 和商品角色派生也已经存在，但 common-core 只跑 overview 与 segment，并把材料章节固定为 `not_activated`。现在要连接这条已有能力，而不是再建一套制造业框架。

## What Changes

- 只在原文明示具名物料是公司生产或经营投入时，通过现有 `company_profile_common_core` owner 激活 `ChapterTask.EXTRACT_MATERIAL_INPUTS`。
- 路径保持：正式原文 → 现有 `extract_material_inputs` → `material_input` Relationship → `CommodityExposure(role=raw_material_input)` → 现有 `company_facts` / `commodity_associations` 读取范围。
- 保留来源名称、主体、报告身份、期间和 Evidence。没有数量或市场序列不阻止关系交付。
- 不根据行业常识补齐。价格风险、泛称成本和存货金额继续拒绝。销售证据本身不能推出投入角色；若另有独立且已接受的投入证据，同一 source-native 商品同时保留 `product_sales` 和 `raw_material_input`，不覆盖、不净额化。能源输入继续走既有 `energy_consumption`。
- catalog 无匹配时仍交付投入角色，`mapping_status=pending` 且 `commodity_id=null`。多个候选无法唯一选择时用 `ambiguous`，不猜测 commodity id。只有 `mapped` 才允许非空唯一 `commodity_id`。不新建 catalog 或市场序列。
- successor 保留 `owned_page_facts=v8`，增加正交的 `material_input_facts=v1`。不得在完全相同的 v8 identity 下 force replay。v1–v8 work 和全部旧 source-review 保留。
- 实现审核通过后才对冻结报告做受控 replay，并重新独立计算指标。不预设 recall 9/9 或门槛通过。

## Capabilities

### New Capabilities

- `explicit-material-input-role`: 把原文明示的具名投入品接到现有材料章节、Relationship 和 `raw_material_input` 读取，并规定正例、负例与拒绝边界。

### Modified Capabilities

- `common-core-owned-page-gap-repair`: 当前 published identity 在保留 `owned_page_facts=v8` 的同时增加 `material_input_facts=v1`。v8 观察和 v1–v8 work 继续可读，不授权下一轮扩大或生产。
- `company-profile-common-core-owned-page-facts`: 查询优先当前 successor identity，而不是把材料事实写回完全相同的 v8 work。

## Impact

- 缺口在章节激活和 Evidence 路由，不在缺少领域模型。实现应落在现有 `company_profile_common_core` owner，复用 Stage 5 provider、acceptance 和 writer。
- 不新增 action、writer、ChapterTask、Role 枚举或执行链。不改 publication scope、closure、completed mode、生产授权或下一轮扩大状态。
- 不启用产销量、材料成本比例、采购模式、战略储备、供应商或客户章节，也不建设完整制造/材料 package。
- 本卡只定义范围。1.1 独立范围审核通过前不得改业务代码，不得 replay。
