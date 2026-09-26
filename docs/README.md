# Quote 文档索引

本文件是当前文档的唯一总索引。当前架构、运行手册、开发约束和历史需求必须分开维护；未列入本索引的开发文档不自动具有当前规范效力。

## 开发总纲

- `docs/development/project_development_governance.md`：长期开发、架构边界、复用、单一执行链和生产稳定性约束
- `docs/development/framework_refactoring_program.md`：框架改造总需求、工作流、依赖、验收和 OpenSpec change 矩阵
- `docs/development/README.md`：当前开发入口和文档使用规则
- `AGENTS.md`：所有 AI 和开发任务必须遵守的仓库级规则

## 当前系统

- `docs/architecture.md`：按当前代码和配置维护的系统架构概览
- `docs/api/restful_api.md`：RESTful API 参考
- `docs/configuration/config_file.md`：分文件配置参考
- `docs/database_guide.md`：SQLite 存储布局与调优
- `docs/financial_data_system.md`：财务数据系统
- `docs/features/scheduler_system.md`：调度任务和生产时间表
- `docs/telegram_task_manager.md`：Telegram 运维入口

## 行情与主数据

- `docs/features/quote_maintenance.md`：日线行情维护、历史回补、单标的下载和缺口处理
- `docs/features/trading_calendar_management.md`：交易日历
- `docs/development/instrument_master_sync.md`：证券主数据同步与治理
- `docs/development/stock_adjustment_factor_framework.md`：复权因子当前框架

## 研究数据域

- `docs/development/research_data_engine_execution.md`：研究数据引擎实施与现状
- `docs/development/company_profile_product_and_industry_semantic_requirements.md`：全 A 股通用骨架、事实级交付、report_default_group_scope、基础商品暴露与 M1—M4 路线的唯一产品总需求
- `openspec/changes/archive/2026-09-17-deliver-a-share-core-profiles-and-commodity-exposure/`：已归档的全 A 股通用骨架与基础商品暴露实施；入口为 `company_profile_common_core`，生产仍为 not_authorized；2026-09-17 首次观察未过 4.1 门槛
- `openspec/changes/archive/2026-09-17-interpret-owned-page-company-total-and-bank-income-mix/`：已验收并归档的 `owned_page_facts=v4`；重新核原文后 recall 7/7、accuracy 7/7，4.1 数字门槛此次算过；独立 Review 已通过；不是生产授权
- `openspec/specs/company-profile-common-core-company-total-and-income-mix/spec.md`：公司级营业收入总额与银行收入构成的当前合同
- `openspec/changes/archive/2026-09-24-read-as-of-shenwan-l1-for-profile-strata/`：已归档的 as-of 申万一级名称读取。正式历史行只有股票代码、行业代码、计入日期和更新时间；一级名称沿同版本活动 taxonomy 父链解析；顶层 `sw_l1_name` 优先；不读 `classification.levels.sw_l1.industry_name`；断链为 `other`；不回退 cutoff 后的当前 membership。2026-09-17 `service_available_count=0` 只是该修复前的历史基线。固定样本已重新探查并执行：600004.SH / service、600006.SH / manufacturing。不是生产授权
- `openspec/changes/archive/2026-09-25-expand-company-profile-m4-first-expansion/`：已于 2026-09-25 执行、关闭并归档的首次扩大。固定样本为 600004.SH / service 与 600006.SH / manufacturing。原始观察 recall 0/9、accuracy unassessed、mode completed，正式 closure v2 已写入；checkpoint 没有正式 v1 快照，不得补造。`expansion_gates_met=false`，`scale_quality_claim_allowed=false`，生产仍为 `not_authorized`，不授权下一轮扩大
- `openspec/specs/company-profile-m4-first-expansion/spec.md`：首次扩大的当前合同；剩余 M4 待办以正式 closure v2 为准
- `openspec/changes/archive/2026-09-26-repair-common-core-first-expansion-owned-page-gaps/`：已于 2026-09-26 归档的 common-core repair。当前 identity 为 `owned_page_facts=v8`。v8 recall 8/9、accuracy 8/8、critical numeric errors 0、`expansion_gates_met=false`。v6、v7 的 8/8 快照保留并已被后续 Review 否决。不授权下一轮扩大、规模质量声明或生产
- `openspec/specs/common-core-owned-page-gap-repair/spec.md`：owned 业务标题下的主营及服务投影，以及正式收入表和组合表组单位作用域的当前合同；当前 identity 为 v8
- `openspec/specs/company-profile-a-share-delivery/spec.md`：全市场目标合同；规范存在不等于能力上线
- `docs/development/company_profile_industry_research_method.md`：行业增强的研究方法；不阻塞通用基础画像
- `docs/development/company_profile_manufacturing_materials_requirements.md`、`company_profile_manufacturing_materials_research_index.md`：已通过阶段 3 研究验收的制造/材料行业合同与证据索引；不代表生产授权
- `docs/development/company_profile_common_semantic_model.md`：阶段 4 历史实现与可复用模型参考；当前政策以总需求为准
- `openspec/changes/archive/2026-09-17-deliver-a-share-core-profiles-and-commodity-exposure/replan-record.md`：旧执行change退役、失效审计删除和恢复位置；含16/19归档计数说明及29个旧business-profile计划退役清单，历史记录不作为当前生产入口
- `openspec/changes/archive/2026-09-06-slice-manufacturing-materials-company-profile/`：已归档的阶段 5 隔离竖切合同；不恢复旧生产链
- `openspec/changes/archive/2026-09-08-close-manufacturing-materials-profile-research-slice/`：阶段 5.5 研究切片闭包合同；权威结果为 `research_slice_usable`、Gold 14/24，生产仍未授权
- `openspec/changes/archive/2026-09-08-refine-company-profile-period-and-event-semantics/`：已归档的离线期间、qualifier、Gold 主体严格度和成飞事件裁决 change；权威运行仍为 `stage55-closure-four-20260907-a`，离线 Gold 为 18/24，生产仍未授权且未启动阶段 6
- `docs/development/company_profile_industry_requirements_template.md`：独立行业 requirements 标准模板
- `docs/development/company_profile_industry_sample_manifest.template.json`、`company_profile_industry_gold_annotation.template.json`：仅用于研究合同的样本与 gold 标注清单模板
- `docs/development/company_profile_industry_benchmark_acceptance_template.md`：行业 benchmark 验收报告模板
- `docs/development/business_profile_semantic_production_runbook.md`：已冻结的旧公司画像语义生产手册，仅用于本地审计、停止与历史排障，不得继续启动旧回补
- `docs/development/shared_pdf_processing.md`：共享 PDF 解析、选择性 OCR、CPU/GPU canary 与回滚
- `docs/development/common_llm_gateway_architecture.md`：公共 LLM 网关
- `docs/development/fx_market_data_requirements.md`：外汇数据域
- `docs/development/commodity_futures_market_data_requirements.md`：商品期货数据域
- `docs/development/special_commodity_market_data_requirements.md`：特殊商品数据域
- `docs/development/professional_dcf_requirements.md`：专业 DCF 需求与口径

## 运维与问题

- `docs/DATABASE_BACKUP_IMPLEMENTATION.md`：数据库备份 runbook
- `docs/troubleshooting/faq.md`：故障排查
- `docs/development/open_issues_backlog.md`：尚未解决的数据能力和源端问题
- `docs/CHANGELOG.md`：版本变更记录

## 文档生命周期

- current 文档描述当前代码和稳定边界；
- runbook 必须包含当前可执行命令；
- 未完成 requirements 由对应 OpenSpec change 管理；
- 已完成的需求稿、调查、回执和迁移记录在有效内容合并后删除；
- 文档清理与合并由 `consolidate-project-documentation` change 跟踪。
