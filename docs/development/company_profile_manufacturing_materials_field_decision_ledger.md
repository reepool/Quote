# 制造/材料公司画像字段决策账本

> artifact type：`company_profile_industry_field_decision_ledger`
> 状态：`revised_after_user_semantic_ruling`
> 日期：2026-09-03
> 样本版本：`manufacturing_materials.2026-09-03.2`
> production authorization：`not_authorized`

## 1. 决策原则

本账本在宁德时代、璞泰来、锦华新材三个稳定主业 dossier 完成后才开始归纳，并使用中航成飞重大资产重组样本校验 business regime。字段成为包级 `required` 检查项，必须至少得到两家不同公司和不同披露形态支持；`required` 表示程序必须检查并给出 coverage，不表示公司一定披露事实。

义务分类：

- `common_required_inspection`：制造/材料报告必须执行章节检查并输出 coverage；
- `conditional`：触发条件成立后必须抽取，未触发不构成失败；
- `subtype_specific`：仅特定材料、加工、设备或监管模板适用；
- `optional`：披露时可记录，不进入 v1 完成门；
- `unresolved`：证据或术语尚不足，不进入合同。

## 2. 跨样本核心决策

| field / task | 300750 | 603659 | 920015 | 302132 regime | decision | 理由 |
|---|---|---|---|---|---|---|
| `business_overview` | observed | observed | observed | observed | `common_required_inspection` | 四种业务和模板均存在主要业务原文 |
| `explicit_activity` | develops/produces/sells/processes/purchases/operates | develops/produces/sells/processes/provides_service/purchases | develops/produces/sells/purchases/processes | develops/produces/sells/provides_service | `conditional` | 只在原文明示且命中 v1 闭集时输出；其他动词留 source candidate |
| `business_regime` | stable | stable | stable | restructuring | `common_required_inspection` | 必须判断稳定/转型，不能用当前主业覆盖历史 |
| `segment_performance` task | product/industry/region | product/industry/region/mode | product/region | industry/product/region/mode | `common_required_inspection` | 四份均存在经营分解，但维度不完全一致 |
| `operating_revenue` | observed | observed | observed | observed | `conditional` per active segment row | 任务 required；具体维度/行由报告披露触发 |
| `operating_cost` | observed | observed | observed | observed | `conditional` per active segment row | 与 revenue 同物理行但独立 logical slot |
| `gross_margin` | observed | observed | observed | observed | `conditional` per active segment row | 优先 reported，不强制生成 derived |
| `production_capacity` | GWh | 亿㎡/万吨/GWh | 吨/年 | not disclosed | `conditional` | 量纲和定义高度依业务变化，未披露可合法为空 |
| `capacity_under_construction` | GWh | narrative projects | 吨/年 | not disclosed | `conditional` | 必须与在产 capacity 分开 |
| `capacity_utilization` | observed | not standardized | observed | not disclosed | `conditional` | 不得用其倒算 reported production |
| `production_volume` | GWh | 吨/万㎡ | not disclosed | explicitly unclassifiable | `conditional` | 至少两家公司支持；按披露任务触发 |
| `sales_volume` | GWh | 吨/万㎡/加工量 | not disclosed | explicitly unclassifiable | `conditional` | 绝不等于销售额；加工量需 subtype 语义 |
| `inventory_volume` | GWh | 吨/万㎡ | not disclosed | unclassifiable | `conditional` | 不等于资产负债表存货金额 |
| `processing_volume` | recycling/processing narrative | coating/CAAS | outsourced processing | repair/service | `subtype_specific` | 独立于 sales_volume；来源括注可保留，但同一锚点不得双写 metric |
| `materials_and_procurement` task | observed | observed | observed | partial | `common_required_inspection` | 可以合法未披露具体原料，但不能静默空 |
| explicit material input | named categories | segment-specific categories | named chemicals/energy | sensitive/partial | `conditional` | 只有原文明示才建 material input candidate |
| customer/supplier task | anonymous rows | concentration only | named + anonymous | concentration only | `common_required_inspection` | 四份均有章节，但名称披露义务不同 |
| named/anonymous relationship | anonymous contract/ranks | no annual top-name rows | named + masked | no names | `conditional` | 只有有交易对手行才建 Relationship |
| customer concentration | observed | observed | observed | observed | `conditional` after section present | Measurement，与 Relationship 分离 |
| supplier concentration | observed | observed | observed | observed | `conditional` after section present | 同上 |
| `business_event` | no major change | no major change | new product, no regime change | major restructuring | `common_required_inspection` | 稳定结果也要显式记录 |

## 3. Coverage 状态操作定义

| coverage | 使用条件 | 禁止用法 |
|---|---|---|
| `observed` | 目标章节已定位，证据直接支持字段或明确否定结论 | 不得用模型常识补值 |
| `not_disclosed` | active checklist 已执行，目标章节和连续页可读，报告未披露该事实或名称 | 不能用于未读页、截断页或解析失败 |
| `not_applicable` | 行业/报告模板/业务形态明确不适用，或报告明确勾选不适用 | 不能因为抽取器没找到就判不适用 |
| `extraction_failed` | 目标存在或应检查，但页面不可读、表格断裂、超预算丢页、解析器/LLM 未完成 | 不能改写为空数组成功 |
| `unclear` | 证据存在但字段、主体、期间、单位、表头或同一性无法唯一判断 | 不能为了 completion 强制选一个候选 |

## 4. 产能、产量、销量、库存决策

1. `operating_volume_capacity` 是制造/材料包的 required inspection task。
2. `production_capacity`、`production_volume`、`sales_volume`、`inventory_volume` 是 conditional fields：报告披露对应表或叙述时必须抽取；完整检查后未披露可为 `not_disclosed`。
3. 产能不是同一语义：可为在产设计产能、有效产能、生产线产能、在建产能、加工服务能力，必须由 `capacity_kind` 区分。
4. 产量、销量和库存仅在对象、单位、期间可对齐时比较；不得把销售额、订单额、出货排名、市场份额或存货金额替代经营量。
5. 产能利用率是 reported Measurement；除非后续合同明确允许 deterministic derivation，否则不得据此倒算产量并标为 reported。

## 5. 材料加工与设备制造的包边界

第一版不拆成两个独立行业包，而采用一个制造/材料 primary package 加 subtype checklist：

- `material_product_manufacturing`：质量、面积、能量等产品经营量；
- `processing_service`：涂覆加工、极片代工、委外加工等处理量和服务关系；
- `equipment_manufacturing`：装备产品、订单、交付和台套数量；没有台套披露时不拿订单金额冒充销量；
- `complex_assembly_manufacturing`：航空等多产品、敏感制造；允许分类实物量 `not_applicable/not_disclosed`。

共享的是 BusinessOverview、Segment、Activity/Measurement 分离、主体、期间、source-native 单位、coverage 和证据规则；subtype 专有字段只有触发后才检查。

## 6. 主体与 period 决策

- 表格主体优先读取明确表头、导语、脚注中的合并/集团/母公司说明和 named subsidiary；
- “公司”不能无条件等于 issuer，也不能无条件等于 consolidated group；
- 没有明文合并口径时，只有表格合计与同报告合并利润表完成核对，才可提议 `consolidated_group`；必须记录 `subject_basis=numeric_reconciliation_to_consolidated_statement`、核对页和 uncertainty；
- 多事业部/子公司报告允许 `business_segment` 或 `named_subsidiary`，无法唯一化为 `unclear`；
- 收入、成本、产量、销量为 duration；库存量通常为 report-end instant，必须读取表头/脚注；
- capacity 必须记录 `capacity_kind` 与适用时点/期间；在建产能不是当期可用产能；
- regime 变化分开 `reported_period`、`knowledge_time`、`regime_effective_at`、`comparison_basis`；中航成飞成飞股权过户生效日为 2025-01-06；重组后 `same_control_restated` 比较数与 predecessor `original_as_published` 并列，当前包和后来重述均不得追溯覆盖此前知识状态。

## 7. 单位与 physical anchor 决策

- 研究层保存 source value、原单位 token、表头、脚注、页码、行标签和列标题；
- `evidence.page` 固定为一基 PDF 文件物理页；印刷页码只另存 `printed_page_label`，不得参与机器锚定；
- 表格 Measurement identity 使用 `logical_slot + physical_anchor`，不能只用 row label；
- currency、percent、energy、mass、area、capacity-rate、equipment-count 不跨维度折叠；
- `GWh`、`亿㎡`、`万㎡`、`吨`、`万吨/年`、`kt/a` 均先保持 source-native；
- `>3 万吨` 等比较符是数值语义的一部分；
- canonical conversion owner 为程序，LLM 不转换、不按数量级猜单位。

## 8. 客户、供应商和匿名身份决策

1. customer/supplier task 为 required inspection；具体名称可以合法 `not_disclosed`。
2. “第一名”“客户 A”“J 公司”等是报告内完整匿名身份，scope 为该报告、该关系类型和该排名/标签。
3. 匿名身份不要求 catalog resolution，不跨报告合并，也不因金额相同自动等同。
4. 前五名合计金额/比例是 concentration Measurement；只有具体交易对手行才形成 Relationship candidate。
5. 叙述中的合作客户名单不等同年度前五名，不继承前五名金额。

## 9. 用户已确认的口径裁决

- `processing_volume` 是独立 metric；“涂覆加工量（销量）”保留来源标签但只生成一条 processing Measurement。第 19 页销售量是独立锚点，不自动合并。
- “合并抵消项”使用 `row_class=consolidation_adjustment`，行上的 revenue/cost/margin 独立保存并继承标记；“其他”仍是 aggregate。
- 管理层讨论认定合并主体需要明文口径或与合并利润表金额核对；只有“公司”时为 `unclear`。
- 同一控制合并按四个时钟和 comparison basis 并列，不覆盖 predecessor 原披露。

上述裁决由用户于 2026-09-03 接受。行业包仍保持 `in_review`，因为尚需未参与初标的外部 AI 独立盲审。
