# 制造/材料公司画像审核日志

> 文档类型：append-only review log
> 版本：`manufacturing_materials_review.v1`
> 日期：2026-09-03
> 当前状态：`independent_review_complete_pending_user_acceptance`
> production authorization：`not_authorized`

## 1. 使用规则

本文件只追加审核事件，不覆盖或删除既有决定。Codex 负责初始研究与标注，用户安排的外部 AI 独立审核，用户裁决关键语义分歧和最终 acceptance；这是逻辑职责分离，不要求组建现实多人团队。

每条审核结论必须记录 `accepted`、`rejected` 或 `deferred`，并说明证据、理由、影响文件和解除条件。外部审核或用户验收未完成时，制造/材料行业包保持 `in_review`，不得登记为 `approved`，不得授权生产实现或恢复旧 backfill。

## 2. 待审核材料

- 研究入口与样本：`company_profile_manufacturing_materials_research_index.md`、`company_profile_manufacturing_materials_sample_manifest.v1.json`；
- 四份逐报告 dossier：宁德时代、璞泰来、锦华新材、中航成飞；
- 跨样本决策：`company_profile_manufacturing_materials_field_decision_ledger.md`；
- 行业需求：`company_profile_manufacturing_materials_requirements.md`；
- LLM 合同：`company_profile_manufacturing_materials_llm_contract.md`；
- Gold 与验收：`company_profile_manufacturing_materials_gold_annotations.v1.json`、`company_profile_manufacturing_materials_benchmark_acceptance.md`。

### 2.1 独立盲审交付顺序

1. 盲标阶段只提供四份原 PDF、冻结 field checklist、字段定义和中性 JSON 输出格式；不提供 Gold 预期标签、逐报告 dossier、field ledger 或已有口径结论；
2. 盲标结果提交后再揭示 Gold 和其余研究材料，逐条比较样本偏差、字段遗漏、语义、主体、期间、单位和页锚；
3. 每条差异登记 `accepted/rejected/deferred`、原 PDF 证据和解除条件；存在 blocker 时保持 `held`；
4. 第一阶段已经读取 Gold 的审核只能登记为独立复核，不能单独关闭 OpenSpec 8.1。

盲标交付文件固定为 `company_profile_manufacturing_materials_blind_review_brief.md` 和 `company_profile_manufacturing_materials_blind_review_output.template.json`；交接方不得额外附带本文件第 3 节、Gold、dossier、ledger 或 Benchmark。

## 3. 用户已裁决、仍需独立盲审的口径

1. “涂覆加工量（销量）”只映射 `processing_volume`，保留 source-native 双重叫法；另一表格锚点可独立为 `sales_volume`；
2. “合并抵消项”使用 `row_class=consolidation_adjustment` 行和独立 revenue/cost/margin Measurements，不新增对象；
3. `consolidated_group` 需要明文口径或与合并利润表完成并记录金额核对；只有“公司”时为 `unclear`；
4. 同一控制比较数使用四时钟和 `comparison_basis` 与 predecessor 原披露并列，不覆盖历史知识状态。

外部独立盲审仍需检查这些规则在原 PDF 上是否存在错标、遗漏或样本偏差；本节不是独立审核通过记录。

## 4. 审核事件

### 2026-09-02 — 初标包准备完成

- actor：Codex / primary annotator
- status：`handoff_ready`
- decision：阶段 3 的样本、dossier、字段账本、行业 requirements、LLM 合同、Gold 和 Benchmark 已形成，可提交外部独立审核。
- evidence：四份正式年报均已核验本地资产、SHA-256、页数和报告身份；Gold 包含成功事实、合法空值、歧义、不可读页、单位歧义和禁止推断负例。
- unresolved：本文件第 3 节四项问题。
- authorization：`not_authorized`；本事件不构成外部审核通过、用户验收或生产授权。

后续外部 AI 审核和用户裁决应在本节末尾继续追加，不修改上述历史事件。

### 2026-09-03 — 外部口径抽检与用户裁决

- actor：外部 AI（口径抽检）+ 用户（最终口径裁决）
- review class：`semantic_ruling_and_fact_spot_check`
- status：`accepted_with_required_corrections`
- accepted：加工量单指标、合并抵消调整行、主体肯定证据、同一控制比较数并列四项规则。
- corrections：Gold checklist 与行业 checklist 对齐；修正加工量 field identity；锦华新材在建产能改用 PDF 49–50 连续表格锚点；补 Activity 正例；固定一基 PDF 物理页坐标；未冻结 source verbs 不进入 v1 action enum。
- scope clarification：该事件包含事实抽检和用户语义裁决，`counts_as_independent_blind_review=false`，不得用于关闭 OpenSpec 8.1。
- remaining：由未参与初标的外部 AI 在不预载行业结论的情况下，从原 PDF 独立检查样本偏差、字段遗漏和标注错误。
- authorization：`not_authorized`；行业状态继续为 `in_review/hold`，不得启动阶段 4。

### 2026-09-03 — 盲审前 Gold 基线修正

- actor：Codex / primary annotator
- review class：`pre_blind_review_baseline_correction`
- status：`accepted`
- corrections：璞泰来库存、前五名空名及客户/供应商集中度因缺少明文合并口径或金额核对，`subject_scope` 从 `consolidated_group` 降为 `unclear`；新增中航成飞 PDF 第 14 页 2024 年营业收入合计正例，并以 PDF 第 8 页“同一控制下企业合并”作为 `same_control_restated` 比较口径证据；新增锦华新材 PDF 第 51 页“丁酮”原料输入正例。
- gold result：18 项 checklist、24 条真实标注、13 条阻塞负例；全部标注仍为 `pending`。
- protocol：8.1 使用本文件 2.1 的“盲标—揭示 Gold 对账”流程。
- scope clarification：`counts_as_independent_blind_review=false`，本事件不关闭 8.1 或 8.3。
- authorization：`not_authorized`；行业状态继续为 `in_review/hold`，不得启动阶段 4。

### 2026-09-03 — 独立盲标提交完成

- actor：`zcode-independent-blind-reviewer (GLM-5.3)`；
- review class：`independent_blind_annotation`；
- artifact：`company_profile_manufacturing_materials_blind_review_submission.zcode.20260903.json`（commit `ccbb4e5`）；
- independence：`gold_seen_before_submission=false`；审核方只读取盲审交接单、中性模板和四份原 PDF。开始前仅对另一份未跟踪提交做顶层结构、reviewer 和条数探测，未读取标注内容、引文或结论，且已在 artifact 中披露；
- completeness：4 份报告 × 18 个 checklist 字段全部覆盖，共 72 个检查位、74 条事实标注、68 条 bounded quotes；
- report blockers：四份报告的 `blocking_findings=[]`；
- findings：7 条 contract findings、7 条 cross-report findings；
- status：`accepted_as_independent_blind_submission`；
- authorization：`not_authorized`；盲标提交本身不等于 Gold 覆盖或行业包 approved。

### 2026-09-03 — 揭示 Gold 后逐项对账

- actor：Codex / research owner；
- review class：`blind_review_adjudication`；
- detail：`company_profile_manufacturing_materials_blind_review_adjudication_20260903.md`；
- accepted：`processing_volume` 方向收窄、`production_capacity.capacity_kind`、重述比较列强制 `comparison_basis`、库存脚注来源存在时保留、Activity actor 直接证据规则；
- accepted with clarification：仅合计披露时前五名 name coverage 仍为 `not_disclosed`，关联交易或报告内聚合身份只能形成独立 Relationship；未披露原因可细分，但保密/豁免必须有原文明示；
- rejected as Gold override：盲标中仅凭“公司”或惯例使用 `consolidated_group`、把中航成飞“无法分类统计”从 `not_applicable` 改成普通 `not_disclosed`、把璞泰来第 14 页加工量重新标成 sales volume、以关联交易关系填补前五名名单 coverage；
- deferred：`recycling_volume`、`order_backlog`、定性自给率和“仅产量无产能” holdout；
- rejected as new top-level fields：`related_party_counterparty`、境外收入、副产品客户；现有 Relationship/Segment 语义足够；
- status：`independent_review_complete_pending_user_acceptance`；OpenSpec 8.1 可关闭，8.3 保持未完成；
- authorization：`not_authorized`；用户接受本次新增裁决前不得登记 approved 或启动阶段 4。
