## Context

阶段 5/5.5 的真实运行已经能够从四份制造/材料年报抽取大量有 Evidence 的事实，但当前完成门把三种不同问题混在一起：原文只写“公司”导致的主体不确定、数字/表头等可规范化的形式差异、以及四份年报没有触发的防守型负例。这样会让有研究价值的事实被整报 `hold`，也会诱使实现者为了通过门槛而猜主体、改写 Gold 或制造异常输入。

本 change 只调整研究验收政策。现有 `subject_scope`、`subject_basis`、Evidence、Coverage、disposition 和隔离 run bundle 继续作为唯一事实来源；生产链、阶段 3 行业合同、旧 run、数据库和发布入口不变。

## Goals / Non-Goals

**Goals:**

- 让 `unclear` 成为字段/用途限制，而不是整份报告的自动失败。
- 用封闭的用途政策限制主体不确定事实进入合并口径计算。
- 让 Gold 识别有限的数值、单位、指标、对象和物理锚点等价，同时保留合同冲突。
- 将 fixture guard 与真实年报 Benchmark 分开，避免要求每份小样本触发全部异常。
- 增加可解释的报告级 `usable` / `usable_with_caveats` / `hold` / `failed` 和整体 `research_slice_usable`。
- 从既有结构程序派生离散置信度，支持大规模研究收集而不引入伪精确概率。

**Non-Goals:**

- 不新增 `explicit`、`reconciled`、`uncertain` 等主体枚举或每条记录的自由用途列表。
- 不修改阶段 3 的 Gold 事实、行业字段、Activity 闭集或语义裁决。
- 不建设置信度评分平台、用途本体、研究数仓、PDF/OCR 解析器或生产 writer。
- 不把历史 run-x/run-y 拼接、回写或改标；政策生效后只允许一次新的完整四报告验证运行。
- 不打开 approved 表、scheduler、旧 backfill、CommodityExposure、ValueChainRole、DCF 或阶段 6。

## Decisions

### 1. Reuse existing subject fields and move the gate to usage

保留现有 `subject_scope` / `subject_basis`。`unclear` 记录仍可进入研究投影，但合并敏感的 Measurement 和 Relationship 由封闭用途表拦截。只有无证据把“公司”升级为 `consolidated_group` 才作为主体阻塞。这样避免新建平行身份模型，也避免把研究可用误当合并可用。

### 2. Apply a closed usage policy in projection

程序根据 `object_type`、`metric_type`、`measured_object`、`subject_scope` 和 `subject_basis` 决定是否允许合并使用。LLM 不填写用途；投影不接受自由文本覆盖。`business_segment`、`named_subsidiary` 和明确 `issuer` 保留各自口径，不自动提升为集团合计。

### 3. Make Gold equivalence finite and auditable

Gold evaluator 在运行之后工作，按固定顺序执行数值规范化、封闭单位换算、指标/对象/物理锚点对齐和 `subject_strictness`。原始 `source_native`、单位和期望值不改写。只要是合同冲突（例如成飞销量的 not-applicable 与冻结保密规则不一致），结果必须是 `gold_contract_conflict`，不能被语义 matcher 抹平。

### 4. Separate fixture guards from real-report coverage

四类没有现实触发材料的负例使用本地受控 fixture 验证防守行为；真实年报 Benchmark 只对 `evaluated=true` 的条目判定。未触发条目保留 `evaluated=false`，不算通过也不算失败，不要求在四份报告中人为制造触发。

### 5. Derive status and confidence deterministically

报告状态由六个核心章节、冻结 §14 blocker、执行失败和主体用途违规汇总得到。required 核心章节未完成时至少为 `hold`；只有核心章节完成且没有 hold 级错误时才可为 `usable` 或 `usable_with_caveats`。置信度仅由 Evidence 完整性、verify/disposition、主体依据和冲突状态派生为 `high` / `medium` / `low` / `rejected`，不使用模型自报概率或加权分数。

### 6. Preserve research-only semantics

新状态只描述隔离 research projection 的可用程度。记录仍是 `accepted_for_review`，整体通过名为 `research_slice_usable`，且始终保留 `production_authorization=not_authorized`。历史 `research_slice_pass` 不重定义，旧 bundle 不迁移。

## Risks / Trade-offs

- [主体不确定事实被下游误用] → 投影执行封闭用途表；合并敏感指标在 `unclear` 时不进入合并模板。
- [Gold 放宽后掩盖真实语义错误] → 只允许列明的等价规则；物理锚点、指标对象和合同冲突仍严格检查。
- [usable_with_caveats 过度泛化] → 六个核心章节必须完成，§14 hold 清单仍优先于 caveat。
- [置信度变成新的复杂平台] → 只做现有字段的离散派生，不持久化自由权重或 LLM 概率。
- [研究结果被误认为生产数据] → 所有 bundle、projection 和状态保留 `accepted_for_review` 与 `not_authorized`。
