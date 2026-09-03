# 制造/材料公司画像阶段 3 独立盲审交接单

> artifact type：`company_profile_industry_blind_review_brief`
> 协议版本：`manufacturing_materials_blind_review.v1`
> 日期：2026-09-03
> production authorization：`not_authorized`

## 1. 审核目标与隔离要求

本轮只验证四份正式年报能否支持下列制造/材料画像检查清单，并独立发现样本偏差、字段遗漏、语义混淆、主体/期间/单位错标和证据锚定问题。

盲标提交前，审核方只能接收本交接单、四份 PDF 和中性输出模板 `company_profile_manufacturing_materials_blind_review_output.template.json`。不得读取或接收：

- `company_profile_manufacturing_materials_gold_annotations.v1.json`；
- 四份 `company_profile_manufacturing_materials_dossier_*.md`；
- `company_profile_manufacturing_materials_field_decision_ledger.md`；
- `company_profile_manufacturing_materials_benchmark_acceptance.md`；
- 既有审核意见、预期答案或逐报告结论。

若审核方在提交前已经看到上述材料，必须将 `gold_seen_before_submission` 标为 `true`，本次只能算非盲独立复核，不能单独关闭 OpenSpec 8.1。

## 2. 只读输入

页码统一使用 PDF 文件中从 1 开始的物理页序号；印刷页码如不同，只能另记为 `printed_page_label`。

| sample_id | 报告 | PDF |
|---|---|---|
| `manufacturing-materials-300750-2025` | 宁德时代 2025 年报 | `data/filings/announcements/blobs/c1/c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9.pdf` |
| `manufacturing-materials-603659-2025` | 璞泰来 2025 年报 | `data/filings/announcements/blobs/4e/4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6.pdf` |
| `manufacturing-materials-920015-2025` | 锦华新材 2025 年报 | `data/filings/announcements/blobs/4d/4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a.pdf` |
| `manufacturing-materials-302132-2025-regime` | 中航成飞 2025 年报 | `data/filings/announcements/blobs/60/605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020.pdf` |

## 3. 中性字段检查清单

`required` 表示每份报告都必须执行检查，不表示必然存在事实；`conditional` 表示先判断触发条件，再记录事实或合法覆盖状态。

| field_id | object | chapter_task | level | 中性业务定义与关键边界 |
|---|---|---|---|---|
| `business_overview_source` | BusinessOverview | `extract_business_overview` | required | 主要业务原文及证据；不得把研究总结当原文 |
| `explicit_activity` | Activity | `extract_business_overview` | conditional | 明示动作；v1 仅允许 `develops/produces/processes/sells/purchases/provides_service/operates`，不得塞入数值 |
| `business_regime` | BusinessEvent | `extract_business_regime` | required | 主业、重大重组或并表生效边界；不得用当前主业追溯改写历史 |
| `segment_dimension` | Segment | `extract_segment_financials` | required | 产品、行业、地区、销售模式或有证据的调整行 |
| `operating_revenue` | Measurement | `extract_segment_financials` | conditional | 表中营业收入，`logical_slot=revenue`；保留币种和缩放单位 |
| `operating_cost` | Measurement | `extract_segment_financials` | conditional | 表中营业成本，`logical_slot=cost` |
| `gross_margin_reported` | Measurement | `extract_segment_financials` | conditional | 报告直接披露的毛利率，`logical_slot=gross_margin`；不得自行补造 derived 值 |
| `production_capacity` | Measurement | `extract_operating_quantities` | conditional | 已形成或当前产能；不得与在建产能、产量混用 |
| `capacity_under_construction` | Measurement | `extract_operating_quantities` | conditional | 在建/计划产能及预计时间；跨页证据必须分别锚定 |
| `capacity_utilization` | Measurement | `extract_operating_quantities` | conditional | 报告披露的产能利用率；不得据此虚构产量 |
| `production_volume` | Measurement | `extract_operating_quantities` | conditional | 产品/分部产量；不得把产能当产量 |
| `sales_volume` | Measurement | `extract_operating_quantities` | conditional | 实物销售量；不得把营业收入、订单额或合同额当销量 |
| `inventory_volume` | Measurement | `extract_operating_quantities` | conditional | 实物库存量及脚注；不得把资产负债表存货金额当库存量 |
| `processing_volume` | Measurement | `extract_operating_quantities` | conditional | 加工服务量；来源括注或别名保留在 source-native，同一物理事实不得双写指标 |
| `material_input` | Relationship | `extract_material_inputs` | conditional | 年报明示的原材料或能源输入；不得用行业常识补齐 |
| `counterparty_relationship` | Relationship | `extract_counterparties_and_concentration` | conditional | 具名或报告内匿名客户/供应商；匿名身份不得跨报告合并 |
| `customer_concentration` | Measurement | `extract_counterparties_and_concentration` | conditional | 客户集中度或相应金额；只有比例不得虚构客户关系 |
| `supplier_concentration` | Measurement | `extract_counterparties_and_concentration` | conditional | 供应商集中度或相应金额；只有比例不得虚构供应商关系 |

## 4. 覆盖状态与证据要求

- `observed`：原报告明确支持，并提供页、章节、表格行列或有界引文；
- `not_disclosed`：相应章节/表已完整阅读且适用，但报告确未披露；
- `not_applicable`：报告明示不适用，或业务/披露结构可证明该检查项不适用；
- `extraction_failed`：目标页、表头、续页、脚注或文本不可读/不完整，不能把失败写成未披露；
- `unclear`：证据存在但字段、主体、期间、单位或语义无法可靠判定。

每条 `observed` 记录至少保留：`field_id`、`chapter_task`、物理 PDF 页、章节标题、行/列/单元格或有界引文、source-native 名称/值/单位/表头、主体、期间和不确定性。单位不得按数量级或行业习惯猜测；仅写“公司”不足以自动判定 `consolidated_group`。

## 5. 提交流程

1. 四份报告逐份独立阅读和标注，不参考其他报告的预期答案；
2. 对 required 项逐项给出覆盖状态，对 conditional 项先给出是否触发及依据；
3. 单列发现的清单缺口、样本偏差、合同矛盾和 blocker；
4. 使用中性模板提交，并确认 `gold_seen_before_submission=false`；
5. 提交后才揭示 Gold、dossier、ledger 和 Benchmark，进入逐项 `accepted/rejected/deferred` 对账。

本交接单不包含 Gold 答案，也不构成行业包批准或生产授权。
