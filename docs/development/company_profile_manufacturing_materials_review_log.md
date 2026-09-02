# 制造/材料公司画像审核日志

> 文档类型：append-only review log
> 版本：`manufacturing_materials_review.v1`
> 日期：2026-09-02
> 当前状态：`pending_independent_review`
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

## 3. 需要独立审核和用户裁决的问题

1. 璞泰来“涂覆加工量（销量）”应只映射为 `processing_volume`，还是在该指标之外保留可查询的双重来源标签；
2. “合并抵消项”在未来 common model 中应如何表达，且必须保证它不被当作产品、活动或普通业务分部；
3. 管理层讨论表使用“公司”但未明确写“合并口径”时，认定 `consolidated_group` 所需的最小证据；
4. 同一控制合并产生的比较数如何与 predecessor 原历史报告按各自知识时点并列，避免用后来重述覆盖当时可知事实。

## 4. 审核事件

### 2026-09-02 — 初标包准备完成

- actor：Codex / primary annotator
- status：`handoff_ready`
- decision：阶段 3 的样本、dossier、字段账本、行业 requirements、LLM 合同、Gold 和 Benchmark 已形成，可提交外部独立审核。
- evidence：四份正式年报均已核验本地资产、SHA-256、页数和报告身份；Gold 包含成功事实、合法空值、歧义、不可读页、单位歧义和禁止推断负例。
- unresolved：本文件第 3 节四项问题。
- authorization：`not_authorized`；本事件不构成外部审核通过、用户验收或生产授权。

后续外部 AI 审核和用户裁决应在本节末尾继续追加，不修改上述历史事件。
