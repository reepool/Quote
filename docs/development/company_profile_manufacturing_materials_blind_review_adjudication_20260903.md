# 制造/材料公司画像独立盲审对账与裁决

> 文档类型：blind-review adjudication
> 日期：2026-09-03
> 行业包：`manufacturing_materials`
> 当前状态：`independent_review_complete_pending_user_acceptance`
> production authorization：`not_authorized`

## 1. 盲审有效性结论

独立盲审提交为 `company_profile_manufacturing_materials_blind_review_submission.zcode.20260903.json`，提交版本为 `ccbb4e5`。审核方声明 `gold_seen_before_submission=false`，只使用盲审交接单、中性模板和四份原 PDF；其在开始前只探测过另一份未跟踪提交文件的顶层结构、reviewer 和条数，未读取任何标注、引文或结论。该探测已在提交文件中披露，不足以使本次盲标失去独立性。

盲审覆盖四份报告的全部 18 个 checklist 字段，共 72 个“报告 × 字段”检查位；因璞泰来 `production_capacity` 和锦华新材 `capacity_under_construction` 各有两个独立物理事实，实际提交 74 条标注，其中 68 条带 bounded quote。四份报告的 `blocking_findings` 均为空。因此，OpenSpec 8.1 的独立盲标输入隔离、提交完整性和原文核验要求成立。

盲审结果不能原样替换 Gold。盲标用于发现合同边界和初标偏差；最终合同仍受已冻结规则约束，尤其是主体肯定证据、一个物理事实只对应一个主 metric、合法空值和业务动作 actor 规则。

## 2. 七项合同发现裁决

| # | 盲审发现 | 裁决 | 合同处理 |
|---:|---|---|---|
| 1 | `processing_volume` 未区分加工服务提供方、委外采购方、自营回收和内部工序 | `accepted` | v1 仅表示公司或业务分部**对外提供加工服务**形成的实物处理量。委外采购只形成采购/关系或费用事实；内部工序不形成该指标；自营回收量保留为未来 `recycling_volume` 候选，不借用 `processing_volume`。璞泰来第 14 页正例保持不变。 |
| 2 | 产能并存报告期产能、有效产能和设计产能 | `accepted` | observed `production_capacity` 强制携带 `capacity_kind`，至少区分 `report_period_capacity`、`effective_capacity`、`design_capacity`、`source_native_other`、`unclear`；不同 kind 不直接比较。在建产能继续使用独立 metric。 |
| 3 | 仅披露前五名合计时，`counterparty_relationship` 的合法状态不清 | `accepted_with_clarification` | 前五名名称 coverage 仍为 `not_disclosed`。同报告关联交易表中的具名关系或“集团所属单位”等聚合身份可形成**独立** Relationship candidate，使用 `identity_class=report_local_aggregate`；不得反向把前五名名单改为 observed。 |
| 4 | 同一控制重述时 `comparison_basis` 未被清单强制 | `accepted` | 只要比较列被明确重述，`comparison_basis` 为 required-when-restated；缺失即 blocker。它与 `reported_period`、`knowledge_time`、`regime_effective_at` 分开保存。 |
| 5 | `not_disclosed` 未区分保密、披露豁免和未说明原因 | `accepted_with_guardrail` | coverage 仍使用 `not_disclosed`，另以 reason code 区分 `explicit_confidentiality`、`explicit_disclosure_exemption`、`source_reason_unspecified`。只有原文明确说明时才可使用前两者；不得仅凭军工惯例推断保密。 |
| 6 | 库存量无脚注时如何标注不明确 | `accepted` | 脚注是“来源存在时必须保留”，不是 observed 的前置条件。无脚注但表头、对象、值、单位、时点明确时仍可 observed，`footnote_refs=[]`，且不得自行补充库存范围。 |
| 7 | 军贸链条中的销售动作可能被错误归给上市公司 | `accepted` | Activity actor 必须由原文直接语法或经济关系支持。军贸公司向最终用户销售不能改写为上市公司直接向最终用户 `sells`；公司自身有明确销售证据时仍可记录其销售活动。无需扩展 v1 action enum。 |

## 3. Gold 与盲标的关键分歧

以下分歧维持现有 Gold，不接受盲标覆盖：

1. 盲标 74 条中 67 条使用 `consolidated_group`，但大量依据只是“公司”或年报习惯，未满足主体决策树。宁德时代 overview、Activity、销量和客户关系，璞泰来库存/前五名/集中度，锦华新材毛利率/原料/客户关系等继续保持 `unclear`，除非存在明文合并口径或完成合并利润表金额核对。
2. 中航成飞 2025-01-06 股权过户是上市公司层面的 regime event，Gold 的 `subject_scope=issuer` 保持不变；不能因其影响合并范围而改成 `consolidated_group`。
3. 中航成飞 2024 重述收入的 `comparison_basis=same_control_restated` 有充分证据，但主体未完成正式合并利润表核对，继续为 `unclear`。
4. 中航成飞“产品众多，无法进行分类统计”是报告明确声明该分类量不可适用，Gold 的 `sales_volume=not_applicable` 保持不变，不降为一般 `not_disclosed`。
5. 璞泰来第 14 页“涂覆加工量（销量）109.42 亿㎡”继续标为 `processing_volume`；第 19 页产销表的独立单元格继续标为 `sales_volume`。两条物理锚点可对账，但不能把第 14 页重新标为销售量或由同一锚点双写。
6. 璞泰来关联交易表可以产生独立关系候选，但不能把“前五名客户/供应商名称未披露”的 coverage 改为 observed。
7. 宁德时代自营回收、锦华新材委外加工采购和中航成飞内部工序均不满足 v1 `processing_volume` 的对外服务方向；对应检查应保持未触发/`not_applicable`，不接受盲标中的 observed 或 `not_disclosed` 覆盖。

## 4. 清单缺口和跨报告建议

| 候选 | 裁决 | 理由 |
|---|---|---|
| `recycling_volume` | `deferred` | 目前只有单一样本方向，作为 subtype 候选保留，不扩大 v1。 |
| `order_backlog` | `deferred` | 不属于当前冻结的制造/材料第一版核心问题。 |
| 定性自给率 | `deferred` | 可保留来源叙述，不作为数值 Measurement。 |
| 独立 `related_party_counterparty` 字段 | `rejected` | 关联属性可进入现有 Relationship，无需新增顶级字段。 |
| 境外收入新字段 | `rejected` | 已由 `segment_dimension=region` 和收入 Measurement 表达。 |
| 副产品客户新字段 | `rejected` | 可由现有 Relationship 加 segment/context 表达。 |
| 委外加工费替代加工量 | `rejected` | 金额不能替代实物 `processing_volume`。 |
| 仅披露产量、无产能的 holdout 形态 | `deferred_non_blocking` | 作为后续 holdout 泛化样本，不阻塞阶段 3。 |

## 5. 阶段结论

- 独立盲审：`complete`；
- 盲审原文核验：`pass`；
- 报告级 blocking finding：`0`；
- 合同发现：7 项已逐项提出裁决并同步到研究合同；
- 非阻塞扩展候选：保持 deferred/rejected，不扩大 v1；
- Gold/Benchmark：继续是研究验收基线，不代表生产模型已通过；
- 生产授权：`not_authorized`；
- 阶段 4：未启动；
- 最终行业登记：待用户接受本文件新增裁决后，才执行 OpenSpec 8.3 并将 `in_review` 改为 `approved`。
