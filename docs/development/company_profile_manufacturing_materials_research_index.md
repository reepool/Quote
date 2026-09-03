# 制造/材料公司画像阶段 3 研究索引

> 文档类型：requirements research index
> 状态：`in_review`
> 日期：2026-09-03
> 行业包：`manufacturing_materials`
> production authorization：`not_authorized`
> 上位需求：`company_profile_product_and_industry_semantic_requirements.md`
> 研究方法：`company_profile_industry_research_method.md`
> OpenSpec change：`research-manufacturing-materials-profile-package`

## 1. 本阶段目标

本阶段通过多份正式年报研究制造/材料行业共性，形成可审核的行业需求、章节任务、字段检查清单、主体/期间/单位规则、确定性与 LLM 分工、Gold 标注和 Benchmark。阶段 3 不实现生产代码，不运行生产 LLM，不写数据库，也不恢复旧公司画像 backfill。

## 2. 样本与只读资产

权威样本清单：`company_profile_manufacturing_materials_sample_manifest.v1.json`。

| sample | report | exchange | local PDF | dossier |
|---|---|---|---|---|
| `manufacturing-materials-300750-2025` | 宁德时代 2025 年报 | SZSE | `data/filings/announcements/blobs/c1/c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9.pdf` | `company_profile_manufacturing_materials_dossier_300750_2025.md` |
| `manufacturing-materials-603659-2025` | 璞泰来 2025 年报 | SSE | `data/filings/announcements/blobs/4e/4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6.pdf` | `company_profile_manufacturing_materials_dossier_603659_2025.md` |
| `manufacturing-materials-920015-2025` | 锦华新材 2025 年报 | BSE | `data/filings/announcements/blobs/4d/4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a.pdf` | `company_profile_manufacturing_materials_dossier_920015_2025.md` |
| `manufacturing-materials-302132-2025-regime` | 中航成飞 2025 年报 | SZSE | `data/filings/announcements/blobs/60/605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020.pdf` | `company_profile_manufacturing_materials_dossier_302132_2025_regime.md` |

四份报告已于 2026-09-02 通过数据库身份、公告标题、PDF 完整性、SHA-256 和实际页数复核。中航成飞报告明确记录重大资产重组、2025 年 1 月 6 日股权过户和上市公司主营结构转型，已补齐阶段 3 的 regime 样本覆盖缺口。

## 3. 研究产物及权威关系

研究必须按以下顺序推进，后层只能引用前层已记录证据：

1. sample manifest：确定为什么研究这些报告和仍缺什么边界；
2. report dossiers：每份报告独立记录事实、合法空值、失败和未决问题；
3. field decision ledger：仅在前三份 dossier 完成后进行跨样本归纳；
4. manufacturing/materials requirements：冻结行业边界、字段义务和章节任务；
5. task-specific LLM contract：定义 `extract/repair/verify`，不生成生产 prompt 代码；
6. Gold annotations：记录正例、反例、合法空、歧义和失败；
7. Benchmark acceptance：按 blocker 和分项指标给出 `pass/hold`；
8. blind-review brief/output template：隔离 Gold 预期，提供四份 PDF、冻结 checklist、字段定义和中性提交格式；
9. append-only review log：保存外部独立审核和用户最终决策。

已形成的权威文件：

- `company_profile_manufacturing_materials_field_decision_ledger.md`；
- `company_profile_manufacturing_materials_requirements.md`；
- `company_profile_manufacturing_materials_llm_contract.md`；
- `company_profile_manufacturing_materials_gold_annotations.v1.json`；
- `company_profile_manufacturing_materials_benchmark_acceptance.md`；
- `company_profile_manufacturing_materials_blind_review_brief.md`；
- `company_profile_manufacturing_materials_blind_review_output.template.json`；
- `company_profile_manufacturing_materials_review_log.md`。

## 4. 研究职责

- Codex：研究 owner、逐报告初标和需求编辑；
- 用户安排的外部 AI：独立审核样本、字段、标注和验收结论；
- 用户：裁决关键语义分歧并作最终 acceptance。

这是逻辑职责分离，用于避免同一上下文自标、自审并自行宣布通过，不要求现实中组建多人团队。

## 5. 当前状态

- 初始样本复核：`pass`；
- 四份报告 dossier：`initial_annotation_complete`；
- 转型/regime 样本：`covered`；
- 行业 requirements：`complete_pending_review`；
- LLM 合同：`complete_pending_review`；
- Gold/Benchmark：`complete_pending_review`；
- 外部独立审核：`pending`；
- 用户口径验收：`accepted_2026-09-03`；
- 研究状态：`in_review`；
- 生产授权：`not_authorized`。

四项口径裁决见 `company_profile_manufacturing_materials_review_log.md`；盲标阶段只向审核方提供四份 PDF、`company_profile_manufacturing_materials_blind_review_brief.md` 和 `company_profile_manufacturing_materials_blind_review_output.template.json`。在未参与初标的外部 AI 完成独立盲审、且所有 blocker 关闭前，本行业包不得登记为 `approved`，也不得据此启动阶段 4 实现。
