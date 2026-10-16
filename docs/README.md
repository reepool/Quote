# Quote 文档索引

本文件是当前文档的唯一总索引。当前架构、运行手册、开发约束和历史需求必须分开维护；未列入本索引的开发文档不自动具有当前规范效力。

## 开发总纲

- `docs/development/project_development_governance.md`：长期开发、架构边界、复用、单一执行链和生产稳定性约束
- `docs/development/ab_role_development_workflow.md`：current；A/B 职责、自主修复、独立业务审核和交接流程；下次审核推进时告知并写入后续任务卡
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
- [公司画像产品总需求](development/company_profile_product_and_industry_semantic_requirements.md)：唯一产品目标；全A股公司画像、产品暴露、供应链定位；真实LLM语义与程序硬约束分工。
- [验收计划v2](development/company_profile_acceptance_program.md)：唯一数量、质量/成本门槛、容错/补救和实际进度；4家开发、12家新链回归、20家盲测、100家试运行，再全市场分波。
- [交付路径与任务卡](development/company_profile_delivery_plan.md)：当前首要卡为1家公司真实LLM纵向闭环；后续顺序、清理依赖和每次审核检查。
- [当前实施change](../openspec/changes/restore-company-profile-llm-led-generalization/proposal.md)：目标delta和实施待办；文档具备不代表运行完成或生产授权。
- [公司画像规范适用说明](../openspec/README.md)：主spec与历史逐样本要求的适用范围；旧轮次/closure不再作为下一开发入口。
- `docs/development/company_profile_industry_research_method.md`：通用链通过后的行业增强，不能阻塞基础交付。
- `docs/development/company_profile_common_semantic_model.md`：历史实现和可复用模型参考，现行语义以产品总需求为准。
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
