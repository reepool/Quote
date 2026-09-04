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
- `docs/development/company_profile_product_and_industry_semantic_requirements.md`：公司画像产品、通用对象、分行业语义、LLM 分工与分阶段实施的唯一权威总需求
- `docs/development/company_profile_industry_research_method.md`：公司画像阶段 2 分行业研究方法、样本选择、标注、benchmark 与阶段 3 进入门
- `docs/development/company_profile_manufacturing_materials_requirements.md`、`company_profile_manufacturing_materials_research_index.md`：已通过阶段 3 研究验收的制造/材料行业合同与证据索引；不代表生产授权
- `docs/development/company_profile_common_semantic_model.md`：阶段 4 通用语义模型、受控内存工作流和研究员读取投影；生产授权仍为 `not_authorized`
- `openspec/changes/slice-manufacturing-materials-company-profile/`：阶段 5 在途 change；仅对四份已批准制造/材料 2025 年报执行隔离证据准备、bounded semantic workflow、研究视图和 benchmark，不恢复旧生产链
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
