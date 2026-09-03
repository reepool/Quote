# 制造/材料公司画像 Benchmark 验收报告

> artifact type：`company_profile_industry_benchmark_acceptance`
> version：`manufacturing_materials_benchmark.v1`
> 状态：`hold_pending_independent_review_and_implementation_evaluation`
> 日期：2026-09-03
> production authorization：`not_authorized`

## 1. 当前结论

研究样本、四份 dossier、字段账本、行业 requirements、LLM 合同和 Gold 初标已经齐备，用户已于 2026-09-03 接受四项口径裁决，足以进入独立盲审。阶段 3 没有实现或运行生产抽取器，因此本报告只冻结验收方法和 Gold 基线，不声称模型指标已经通过。当前结论为 `hold`，解除条件是：外部独立盲审完成且无未解决 blocker，后续实现版本在本 Gold 上通过阻塞项和分项阈值。

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
- contract negative cases：13 条；
- review status：全部 `pending`，等待独立审核。

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
| LLM/research prose introduced new fact | 0 | summary negative case 通过 |

## 7. Legal empty 与失败诚实性

- 璞泰来只披露前五名集中度而未列名称：`not_disclosed`；
- 锦华新材目标章节完整但无产量/销量/库存表：`not_disclosed`；
- 中航成飞明确产品众多无法分类统计：`not_applicable` 或带原因的合法空；
- 若 continuation page、表头、单位、脚注或 required page 未进入 evidence bundle：`extraction_failed`；
- 主体或语义存在多个合法解释：`unclear`；
- 空数组、部分 candidates 或聚合源数字不得改变上述状态。

## 8. 已裁决口径与剩余审核

1. 涂覆加工量只生成 `processing_volume`，来源双重叫法原样保留；第 19 页表格销售量是独立锚点；
2. 合并抵消项采用带 `consolidation_adjustment` 标记的行及三个独立 Measurements，不新增对象；
3. 合并主体必须有明文口径或与合并利润表完成金额核对，仅有“公司”时为 `unclear`；
4. 同一控制重述与 predecessor 原披露按四时钟和 comparison basis 并列，不相互覆盖。

上述口径已经用户确认并进入 Gold。盲审采用两步法：第一步只提供四份 PDF、冻结 checklist、字段定义和输出格式，不提供 Gold 预期标签或 dossier 结论；第二步才揭示 Gold，逐条对账并登记 `accepted/rejected/deferred`。该流程未完成时行业状态维持 `in_review/hold`。

## 9. 阶段 3 验收状态

- sample sufficiency：`pass`；
- artifact completeness：`pass_for_independent_review`；
- independent annotation review：`pending`；
- user semantic acceptance：`accepted_2026-09-03`；
- model implementation benchmark：`not_run_by_design`；
- final research decision：`hold`；
- next step：按“盲标—揭示 Gold 对账”完成外部独立审核并逐项记录 accepted/rejected/deferred；无 blocker 后才能将研究状态改为 `approved` 并另开阶段 4 change。
