# 制造/材料上市公司画像行业需求

> 文档类型：industry requirements
> schema/version：`company_profile_manufacturing_materials_requirements.v1`
> 状态：`in_review`
> 日期：2026-09-02
> production authorization：`not_authorized`
> 上位需求：`company_profile_product_and_industry_semantic_requirements.md`
> 研究方法：`company_profile_industry_research_method.md`
> sample manifest：`company_profile_manufacturing_materials_sample_manifest.v1.json`
> field ledger：`company_profile_manufacturing_materials_field_decision_ledger.md`

## 1. 行业边界与第一版研究问题

### 1.1 包含范围

第一版制造/材料包覆盖以实物产品、材料加工或复杂装备制造为主要经营活动的 A 股公司，允许以下 subtype 共存：

- `material_product_manufacturing`：化工、新能源材料、功能材料等；
- `processing_service`：涂覆、代工、委外加工、循环加工等；
- `equipment_manufacturing`：自动化装备及其他可辨认设备；
- `complex_assembly_manufacturing`：电池系统、航空等复杂产品制造。

第一版适用 SSE、SZSE、BSE 正式年度报告。港股、半年报、季度报告、金融、资源/矿业、能源公用事业、消费、医药、TMT 的行业专用字段不在本包内。产品名称含“矿产”“能源”不能单独启用尚未审核的扩展包。

### 1.2 第一版必须能回答

1. 公司在该报告期主要做什么，主要产品、材料、装备或服务是什么；
2. 报告披露的经营分部，尤其产品、行业、地区的收入、成本和毛利率如何；
3. 适用时披露了哪些产能、在建产能、产量、销量、库存量或加工量；
4. 正式披露了哪些原材料、采购模式、客户、供应商和集中度；
5. 报告期主业是稳定、扩展还是发生重大重组/转型，当前事实属于哪个 regime；
6. 每个检查项是 observed、未披露、不适用、抽取失败还是语义不清。

以上是研究问题清单，具体完成义务以字段 checklist 的 `requirement_level` 为准；原材料、产能、具名客户等不因出现在问题清单中自动成为每家公司必须披露的事实。

### 1.3 第一版明确不做

- 不自动判定完整产业链地位、商品暴露方向或价格敏感性；
- 不把行业行情、商品价格、预测、竞争优势判断写成公司 approved fact；
- 不将 BusinessOverview 或研究文字作为新事实来源；
- 不根据当前公司名称、行业标签或重组后主业覆盖历史报告期；
- 不实现生产 schema、prompt、selector、writer、resolver、数据库和 backfill；
- 不将经营量、收入、成本或毛利率塞入 Activity。

## 2. 样本和共性依据

行业合同由四份正式、`local_valid` 年报支持：

| sample | 主要挑战 |
|---|---|
| 宁德时代 2025 | 电池系统完整产品收支利、产能、产量、销量、库存、匿名合同与客户供应商 |
| 璞泰来 2025 | 材料、加工、装备、多子公司、多量纲、合并抵消项 |
| 锦华新材 2025 | BSE 模板、精细化工、`kt/a`、具名关联方与匿名客户供应商、无标准产销存表 |
| 中航成飞 2025 | 同一控制重大资产重组、名称和主营结构变化、敏感制造导致分类实物量无法披露 |

宁德时代是 focus sample，但不能单独定义字段。通用 required inspection 来自跨公司支持；单样本内容只作为 conditional、subtype-specific、optional 或 unresolved。

## 3. BusinessRegime 与包启用

### 3.1 Regime 状态

每份报告必须检查并返回：

- `stable`：主要业务和 package 未发生实质变化；
- `business_extension`：新增产品/应用，但未改变主包；
- `transition`：报告期处于旧业务与新业务并行切换；
- `restructuring`：重大资产重组、同一控制合并或借壳导致经营范围实质变化；
- `unclear`：证据不足以确定。

### 3.2 生效与历史保护

- package assignment 必须绑定报告期和可核验 event/effective date；
- 重组样本优先使用股权过户、资产交割或法律生效日，不用新闻发布日期替代；
- 当前 package 不追溯覆盖旧期间；历史 Measurement 保持原报告主体、期间和当时 package；
- 同一控制合并的比较数要保存 `comparison_basis`，不得当作 predecessor 当时知识时点事实；
- 首个生产竖切仍需人工批准 package manifest；阶段 3 不实现自动 resolver。

中航成飞样本初标：2025-01-06 成飞 100% 股权完成过户后，post-transition primary package 为制造/材料；pre-transition 精细 package 保持 `unclear`，不得因当前航空制造主业反写旧画像。

## 4. 章节任务地图

章节编号只作样本证据，选择器必须按语义家族、标题别名、表头和上下文工作。

| chapter family | task | heading aliases / anchors | table signatures | required context | allowed outputs | deterministic opportunity | LLM fallback | legal empty / failure |
|---|---|---|---|---|---|---|---|---|
| `business_overview` | `extract_business_overview` | 主要业务、业务情况、业务概要、主营业务、主要产品、经营模式 | 通常为叙述 | 连续段落、脚注、主体说明 | BusinessOverview、Activity candidate、regime clue | 标题定位、连续段落保存 | 区分产品/服务/模式和主体 | 章节可读但未写业务为 `unclear`；不可读为 `extraction_failed` |
| `segment_performance` | `extract_segment_financials` | 收入与成本、主营业务分析、收入构成、分产品/行业/地区 | 行标签 + revenue/cost/margin，单位常在表头 | 多层表头、单位、脚注、合并抵消、续表 | Segment + Measurements | 表格结构稳定时逐 cell 提取 | 复杂表头、跨页、抵消项语义 | 制造公司存在表却静默为空为 blocker |
| `operating_volume_capacity` | `extract_operating_quantities` | 产销情况、产能情况、供应链及产能、实物销售 | capacity/production/sales/inventory/utilization | 对象、单位、期间、比较符、库存脚注 | Measurements | 标准表直接映射 | 叙述型、多对象、多单位和脚注 | 完整检查后可 `not_disclosed/not_applicable`；丢页为 failure |
| `materials_and_procurement` | `extract_material_inputs` | 采购模式、主要原材料及能源、成本构成、原材料风险 | 原料/能源/采购模式/成本构成 | 业务 segment、是否能源、价格方向是否公司披露 | Relationship candidate、material facts、Measurements | 行业专表和明确清单 | 叙述分类、segment 归属 | 未具名可 `not_disclosed`；不允许从行业常识补原料 |
| `customers_and_suppliers` | `extract_counterparties_and_concentration` | 主要客户、主要供应商、重大合同、前五名 | identity/amount/share/related-party | 匿名标签、排名、保密说明、合计行 | Relationship candidate + concentration Measurements | 标准排名表 | 跨页、同一控制合并、合同与排名同一性 | 名称未披露可合法空；空数组不能代替 coverage |
| `business_change_and_regime` | `extract_business_regime` | 业务重大变化、合并范围、重大资产重组、名称变更、收购资产 | 选择项、事件叙述、时间和资产 | 法律生效证据、旧/新业务、同一控制基础 | BusinessEvent、Regime candidate | 明确日期和选择项 | 多事件排序、旧新业务边界 | 证据矛盾为 `unclear`，不能以 current profile 覆盖历史 |

## 5. 对象与职责

### 5.1 BusinessOverview

保存主要业务章节的原始证据、报告/page/section、主体、期间、知识可得日和 evidence hash。研究视图可以模板化摘要，但只能引用已批准原文和结构化事实，不得新增数字、角色、因果或判断。

### 5.2 Segment

表达产品、行业、地区、销售模式或业务板块等报告原生维度。相同金额出现在不同 dimension 不代表同一事实；“其他”和“合并抵消项”保留 source-native 标签，并显式标记 aggregate/adjustment。

### 5.3 Activity

v1 只使用少量、可判定动作：

| action | positive rule | negative rule |
|---|---|---|
| `develops` | 原文明示研发某产品/技术 | 研发费用不能自动生成具体 develops 对象 |
| `produces` | 原文明示生产/制造具体产品 | 产量数值本身是 Measurement，不是 Activity |
| `processes` | 原文明示加工、提纯、涂覆、回收转化 | 产品销售不能自动变成 processing |
| `sells` | 原文明示销售具体产品 | 营业收入表不能用 numeric sells 表示 |
| `purchases` | 原文明示采购具体原料/设备/服务 | 成本金额不能自动推出具体原料 |
| `provides_service` | 原文明示代工、维修、服务保障 | 产品交付不能无证据改成服务 |
| `operates` | 原文明示运营工厂、资源或设施 | 投资或在建项目不等于已运营 |

### 5.4 Measurement

每个数字只表达一个 metric、一个 subject、一个 object/segment、一个期间和一个 source-native unit。表格一行多指标必须按 logical slot 分拆，不能共用单一 occurrence。

第一版 `metric_type → logical_slot` 对照：

| metric_type | logical_slot |
|---|---|
| `operating_revenue` | `revenue` |
| `operating_cost` | `cost` |
| `gross_margin_reported` | `gross_margin` |
| `production_capacity` | `capacity` |
| `capacity_under_construction` | `capacity_under_construction` |
| `capacity_utilization` | `capacity_utilization` |
| `production_volume` | `production_volume` |
| `sales_volume` | `sales_volume` |
| `inventory_volume` | `inventory_volume` |
| `processing_volume` | `processing_volume` |
| `customer_sales_amount` | `customer_sales_amount` |
| `supplier_purchase_amount` | `supplier_purchase_amount` |
| `disclosed_share` | `disclosed_share` |

目录使用 `metric_type`；物理身份使用 `logical_slot + physical_anchor`。第一版不创建第三套同义词。

### 5.5 Relationship

只保存年报明示的客户、供应商、合同和采购/销售关系。匿名身份是 report-local disclosed identity，不要求实体目录解析，不跨报告合并。集中度本身不创建 Relationship。

### 5.6 BusinessEvent

至少包含 event type、证据、事件日期、生效日期、涉及主体/资产、旧 regime、新 regime、状态和不确定性。新产品发布可为 business extension；重大资产置入和主业改变才是 package regime 候选。

## 6. 字段级检查清单

| field_id | object | task | activation | requirement level | subject | period | source-unit rule | allowed coverage | blocker |
|---|---|---|---|---|---|---|---|---|---|
| `business_overview_source` | BusinessOverview | overview | manufacturing package active | required | all governed scopes | duration | n/a, preserve text | observed/failed/unclear | required passage silent omission |
| `business_regime` | BusinessEvent/Regime | regime | every report | required | listed company/group | event | n/a | observed/unclear/failed | current business applied retroactively |
| `segment_dimension` | Segment | segment | segment table or narrative exists | required inspection | table subject | duration | n/a | observed/not_disclosed/not_applicable/failed/unclear | table present but task silently empty |
| `operating_revenue` | Measurement | segment | active row has revenue | conditional | row/table subject | duration | currency source scale | observed/unclear/failed | amount or unit overwritten |
| `operating_cost` | Measurement | segment | active row has cost | conditional | same | duration | currency source scale | observed/unclear/failed | cost confused with other field |
| `gross_margin_reported` | Measurement | segment | active row has reported margin | conditional | same | duration | percent source token | observed/unclear/failed | 100x conversion or invented derived margin |
| `production_capacity` | Measurement | volume | capacity disclosure exists | conditional | object/plant/segment | instant/duration per text | preserve rate token | observed/not_disclosed/not_applicable/failed/unclear | capacity kind lost |
| `capacity_under_construction` | Measurement | volume | project capacity exists | conditional | project/segment | expected/event | preserve rate token | same | merged into current capacity |
| `capacity_utilization` | Measurement | volume | reported | conditional | capacity subject | duration | percent | same | used to fabricate reported output |
| `production_volume` | Measurement | volume | applicable table/text exists | conditional | product/segment | duration | source dimension | same | confused with capacity |
| `sales_volume` | Measurement | volume | applicable table/text exists | conditional | product/segment | duration | source dimension | same | confused with revenue/order amount |
| `inventory_volume` | Measurement | volume | applicable table/text exists | conditional | product/segment | instant | source dimension + footnote | same | confused with balance-sheet inventory value |
| `processing_volume` | Measurement | volume | processing service explicitly disclosed | subtype | service/segment | duration | area/mass/energy per report | same | forced into sales_volume |
| `material_input` | Relationship/fact | materials | explicit material/energy disclosure | conditional | business/segment | duration/current | preserve name/unit if any | same | common-knowledge completion |
| `counterparty_relationship` | Relationship | counterparties | named or anonymous row/contract exists | conditional | group/issuer | duration/event | identity is source-native | same | anonymous catalog failure or cross-report merge |
| `customer_concentration` | Measurement | counterparties | concentration disclosed | conditional | report subject | duration | currency/share | same | concentration creates fake relationship |
| `supplier_concentration` | Measurement | counterparties | concentration disclosed | conditional | report subject | duration | currency/share | same | same |

## 7. 主体决策树

1. 读取明确的“合并”“母公司”“本公司”“子公司/事业部”表头和脚注；
2. 有 named subsidiary 或 business segment 时绑定该主体，不提升为 group；
3. 管理层讨论使用集团、合并经营数据且与合并财务一致时，可提议 `consolidated_group`；
4. 只有 issuer 单体证据时使用 `issuer`；
5. “公司”且无足够上下文时使用 `unclear`，不得按开发者习惯默认；
6. 同一 physical row 的 Measurements 必须继承相同主体，除非单元格脚注明确改变；
7. 重组前后主体和 package 分开，不能因法律主体延续而认为业务语义连续。

## 8. 期间和单位

### 8.1 期间

- revenue、cost、margin、production、sales、processing：duration；
- inventory volume：通常为报告期末 instant，按表头/脚注确认；
- capacity：记录 `capacity_kind` 和适用时点；在建/预计产能有 event/expected 语义；
- business event：event/effective date；
- 知识可得日采用正式报告或更正稿发布日。

### 8.2 Source-native 单位

- currency：元、千元、万元、亿元；
- percent：保留 `%` 和 reported precision；
- energy：Wh/kg、GWh 等，性能指标与经营量不能混用；
- mass：吨、万吨；
- area：㎡、万㎡、亿㎡；
- capacity-rate：吨/年、万吨/年、`kt/a`、GWh production-line capacity；
- equipment count：台、套，仅报告披露时启用；
- comparison qualifier：`>3 万吨` 的 `>` 必须保存。

LLM 只抄 source-native；canonical conversion owner 是程序。不得按数量级猜百分比或单位，不跨维度换算。

## 9. 来源优先级与冲突

1. 同一报告期的正式更正稿优先于原年度报告对应事实；
2. 正式年度报告是画像经营事实主来源；
3. 官方专项公告用于补足 event/contract 生效证据，不覆盖不同主体或期间的年报表；
4. 公司官网只作补充核验；
5. 东方财富、同花顺等聚合源只能形成 cross-check candidate。正式报告 `extraction_failed` 时，聚合数字不能升为 approved，也不能把失败改成成功；
6. 同主体/期间/维度的数值表优先于摘要性叙述；叙述负责 overview、活动、模式和原因；
7. 冲突先比较主体、期间、segment dimension、单位、脚注和更正关系，仍无法解释则 `unclear`。

## 10. 确定性提取边界

- 业务概览：确定性定位标题和连续段落，语义对象由 bounded LLM 或人工标注；
- 收支利表：表头、单位、row label、column header 和 footnote 完整时直接逐 cell 提取；
- 产销存：标准列名可直接映射；多对象叙述、复合单位和比较符必须保留并可转 LLM；
- 客户供应商：排名、匿名标签、金额、比例和关联方列可直接提取；identity parity 不能仅凭金额推断；
- regime：明确选择项、资产过户日、公司名称变更和置入资产可以确定性提取，旧新业务映射需语义核验；
- 任何跨页表必须读取续表页、表头和脚注；页预算截断为 `extraction_failed`。

## 11. Coverage 与失败

只使用：

- requirement level：`required`、`conditional`、`optional`、`not_applicable_by_design`；
- coverage：`observed`、`not_disclosed`、`not_applicable`、`extraction_failed`、`unclear`；
- assertion：`reported_fact`、`deterministic_derivation`、`research_assumption`。

完成门按 active package × chapter task checklist 逐项出状态。空数组不是成功。required chapter/table 静默遗漏、事实/推导混淆、无证据主体/单位修正和清单外产业链推断均为 blocker。

## 12. 多包组合

- v1 只加载通用基础包和人工批准的制造/材料 primary package；
- subtype checklist 由报告内明确业务证据触发，不创建独立行业 package；
- “电池矿产资源”作为产品收入行处理，不自动启用资源/矿业包；
- 转型期间按有效日期选择主包，冲突为 `package_assignment_unclear`；
- 未研究或未审核扩展包不得因 LLM 行业标签临时叠加。

## 13. LLM 合同关系

权威行业 LLM 合同见 `company_profile_manufacturing_materials_llm_contract.md`。每个 chapter task 分别定义 `extract/repair/verify`；不存在统一大 prompt，也不存在 `human_review_package` LLM 调用。LLM 不决定 approval、canonical unit、package assignment、产业链角色、商品方向或 DCF 输入。

## 14. Benchmark 与验收

Gold 和 Benchmark 必须覆盖四份报告、不同交易所、不同量纲、匿名披露、合法未披露、复杂抵消项和中航成飞 regime transition。以下任一非零即 hold：

- required 章节或表静默遗漏；
- revenue/cost/margin、capacity/production、sales volume/revenue、inventory volume/inventory value 混淆；
- source value/unit/header/footnote 被改写；
- 主体或期间被无证据强制；
- 匿名身份被解析失败或跨报告合并；
- 当前包追溯覆盖旧 regime；
- 研究文字或 LLM 引入新事实；
- 清单外完整产业链、商品价格或 DCF 推断。

## 15. DCF、商品暴露和研究视图边界

阶段 3 不向 DCF 或商品暴露写入。未来只有 approved、时点有效、明确允许的 reported Measurement 或 deterministic derivation 可供下游读取。商品价格路径、预测、传导率、完整产业链地位和竞争优势判断不得由画像自动产生。

研究视图使用模板组合 approved BusinessOverview 和结构化事实；LLM 若仅做措辞调整，不得新增数字、对象、角色、因果或判断。

## 16. 未解决问题

| issue | current disposition | blocking for research review |
|---|---|---|
| `processing_volume` 是否独立于 `sales_volume` | 初标为独立 metric | yes, needs reviewer decision |
| 合并抵消项 object/measurement 表达 | 保留 adjustment row，不生成经营活动 | yes, needs reviewer confirmation |
| 管理层讨论隐含合并口径的最小主体证据 | 使用决策树，证据不足为 unclear | yes |
| 同一控制合并比较数与历史知识时点并列方式 | 保存 comparison basis，不覆盖 predecessor | yes |

## 17. 审核结论

- artifact completeness：`ready_for_independent_review`；
- sample sufficiency：`pass`；
- research status：`in_review`；
- production authorization：`not_authorized`；
- 下一步：完成 LLM 合同、Gold、Benchmark 后由外部 AI 审核，再由用户裁决未决项。未验收前不得进入阶段 4 实现。
