# 制造/材料公司画像 Benchmark 验收报告

> artifact type：`company_profile_industry_benchmark_acceptance`
> version：`manufacturing_materials_benchmark.v1`
> 状态：`approved_research_contract_implementation_evaluation_pending`
> 日期：2026-09-03
> production authorization：`not_authorized`

## 1. 当前结论

研究样本、四份 dossier、字段账本、行业 requirements、LLM 合同和 Gold 初标已经齐备。独立盲审已于 2026-09-03 完成：覆盖 72 个报告字段检查位、提交 74 条事实标注，四份报告均无 blocking finding；盲审发现的 7 个合同边界已逐项对账并获得用户最终接受。阶段 3 研究合同据此为 `approved`。阶段 3 没有实现或运行生产抽取器，因此本报告只批准验收方法和 Gold 研究基线，不声称模型指标已经通过；后续阶段 4/5 实现仍须在本 Gold 上通过阻塞项和分项阈值。

## 2. 样本覆盖

| 维度 | 目标 | 实际 | 未覆盖边界 | 阻塞 |
|---|---:|---:|---|---|
| 报告数 | >=3 | 4 | 无 | no |
| 公司数 | >=2 | 4 | 无 | no |
| focus 外挑战报告 | >=2 | 3 | 无 | no |
| 交易所/模板 | SSE/SZSE/BSE | 3 个交易所 | 港股不在 v1 | no |
| 业务形态 | 材料/加工/装备/复杂制造 | 已覆盖 | 航空专用包不在 v1 | no |
| business regime | stable + restructuring | 3 stable + 1 restructuring | predecessor 精细字段待历史报告 | no，已标 unclear |

## 3. Gold 构成

- 真实报告 annotations：24 条；
- 覆盖对象：BusinessOverview、Segment、Activity、Measurement、Relationship、BusinessEvent；
- 覆盖业务：产品收支利、产能、销量、库存、加工量、明确原料、匿名关系、集中度、合并抵消、合法空值、重大重组及同一控制重述比较数；
- contract negative cases：19 条；
- review status：独立盲审、Gold 对账和用户最终 acceptance 完成；24 条 Gold 标注批准为研究基线，不等于生产事实 approved。

## 4. 实现版本分项阈值

所有比率均按 Gold annotation 独立计算，不使用单一平均总分。

| dimension | pass threshold | blocker override |
|---|---:|---|
| required task coverage | 100% | 任一 required 静默遗漏即 hold |
| source value exact | 100% on Gold numeric cells | 任一 source value 改写即 hold |
| source unit/header/qualifier | 100% | 单位、`%`、`>`、脚注丢失即 hold |
| metric/logical slot | 100% on blocker pairs | 销量/销售额、库存量/存货金额、产能/产量混淆即 hold |
| subject/period | >=98% overall | 无证据强制主体或 regime 越界即 hold |
| physical evidence anchor | 100% required/conditional observed | 无页/行/列或 quote 即 hold |
| legal empty classification | 100% on Gold legal-empty cases | failure 改写为空成功即 hold |
| prohibited inference | 0 occurrences | 任一清单外事实即 hold |
| repair boundedness | 100% | repair 扩章节/扩字段即 hold |
| verify independence | 100% | verify 新增或改写事实即 hold |

## 5. Chapter task 验收

| task | 必测样本 | 核心断言 |
|---|---|---|
| `extract_business_overview` | 4 份 | 保存原始业务证据，Activity 与数字分离 |
| `extract_segment_financials` | 4 份 | 一 cell 一 Measurement，抵消项非产品，reported margin 不重算 |
| `extract_operating_quantities` | 4 份 | 多量纲正确；合法 not_disclosed/not_applicable；不混销售额/存货金额 |
| `extract_material_inputs` | 3 稳定样本 + 中航成飞 partial | material/energy/segment 分离，不补常识 |
| `extract_counterparties_and_concentration` | 4 份 | 匿名 identity 合法，relationship 与 concentration 分离 |
| `extract_business_regime` | 4 份 | stable/extension/restructuring 分离，2025-01-06 生效边界不追溯 |

## 6. Blocking failures

以下任一非零即 `hold`，不受平均分影响：

| blocker | current research count | implementation exit condition |
|---|---:|---|
| required chapter/table silently omitted | 0 | Gold 全覆盖且 coverage manifest 完整 |
| Activity/Measurement confused | 0 | 所有数字使用 Measurement |
| sales volume / sales amount confused | 0 | negative case 通过 |
| inventory volume / inventory value confused | 0 | negative case 通过 |
| capacity / production confused | 0 | capacity kind 与 logical slot 通过 |
| source unit/value/header overwritten | 0 | exact match 100% |
| anonymous identity treated as catalog failure | 0 | anonymous Gold 全通过 |
| subject/period forced | 0 | unclear 路径可达且无无证据强制 |
| current package applied to old regime | 0 | 中航成飞时间边界通过 |
| later same-control restatement overwrites predecessor fact | 0 | comparison basis 与 knowledge time 并列 |
| printed page label replaces PDF physical page | 0 | page-coordinate negative case 通过 |
| one processing fact is emitted as two metrics | 0 | processing-volume negative case 通过 |
| buyer-side/internal/recycling volume is mislabeled as processing service output | 0 | processing-direction negative case 通过 |
| observed capacity omits capacity kind | 0 | capacity-kind negative case 通过 |
| restated comparative omits comparison basis | 0 | same-control required-basis negative case 通过 |
| related-party evidence fabricates top-five name coverage | 0 | counterparty coverage negative case 通过 |
| confidentiality/exemption is inferred without source text | 0 | disclosure-reason negative case 通过 |
| third-party sales action is assigned to issuer | 0 | activity-actor negative case 通过 |
| LLM/research prose introduced new fact | 0 | summary negative case 通过 |

## 7. Legal empty 与失败诚实性

- 璞泰来只披露前五名集中度而未列名称：`not_disclosed`；
- 锦华新材目标章节完整但无产量/销量/库存表：`not_disclosed`；
- 中航成飞明确产品众多无法分类统计：`not_applicable` 或带原因的合法空；
- 若 continuation page、表头、单位、脚注或 required page 未进入 evidence bundle：`extraction_failed`；
- 主体或语义存在多个合法解释：`unclear`；
- 空数组、部分 candidates 或聚合源数字不得改变上述状态。

## 8. 已裁决口径与最终验收

1. 涂覆加工量只生成 `processing_volume`，来源双重叫法原样保留；第 19 页表格销售量是独立锚点；
2. 合并抵消项采用带 `consolidation_adjustment` 标记的行及三个独立 Measurements，不新增对象；
3. 合并主体必须有明文口径或与合并利润表完成金额核对，仅有“公司”时为 `unclear`；
4. 同一控制重述与 predecessor 原披露按四时钟和 comparison basis 并列，不相互覆盖。

上述四项口径已经用户确认并进入 Gold。独立盲审已按两步法完成，详细裁决见 `company_profile_manufacturing_materials_blind_review_adjudication_20260903.md`。新增接受项包括：加工服务方向、产能 kind、重述 basis、仅合计披露的关系/coverage 分离、未披露原因护栏、库存脚注可选性和 Activity actor；用户于 2026-09-03 最终接受全部新增裁决。

## 9. 阶段 3 验收状态

- sample sufficiency：`pass`；
- artifact completeness：`pass_independent_review_complete`；
- independent annotation review：`complete_2026-09-03`；
- user semantic acceptance：`accepted_2026-09-03`；
- user blind-review adjudication acceptance：`accepted_2026-09-03`；
- model implementation benchmark：`not_run_by_design`；
- final research decision：`approved`；
- next step：可另开阶段 4 change，实现通用最小语义模型和严格 extract/repair/verify 合同；生产授权继续为 `not_authorized`。
