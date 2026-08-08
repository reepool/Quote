## Why

Quote 的公告解析和公司画像已经统一使用公共 LLM 网关，但一个逻辑 profile 仍只能绑定一个实际模型，单模型慢响应、限流或故障会直接拖慢整个语义流水线。现在需要在公共后端增加多个实际 LLM 的共享总并发、加权分流和故障转移，同时让业务继续只依赖稳定的逻辑 profile 和候选结果契约。

## What Changes

- 增加后端 LLM pool 和逻辑 route 配置，使多个逻辑 profile 可以共享一个总并发预算，并将请求映射到 Pipio `grok-4.5` 与 `gpt-5.6-luna` 实际 profile。
- 增加确定性的加权公平调度和可借用空闲容量，成员权重控制长期正常流量比例，现有 provider/account 并发、RPM 和自适应拥塞控制继续作为实际 quota 约束。
- 增加成员健康状态、熔断、半开探测和同一逻辑 deadline 内的有界故障转移；全部成员失败时保持 fail closed。
- 扩展公共响应和分类失败 lineage，返回稳定 `source_label`、逻辑/实际 profile、route fingerprint 和完整脱敏尝试记录。
- 增加公共逻辑 profile 查询接口，迁移应用代码中对 `llm_config.profiles`、单一 model identity 和实际 provider 配置的直接访问。
- 复用 `utils/logging_manager.py`、`config/01_log.json` 和 `LLM` domain logger，为路由、排队、调度、attempt、retry、failover、熔断、完成和关闭过程增加脱敏日志；过程细节使用 `DEBUG`，重要运行节点使用 `INFO`，异常按 `WARNING`/`ERROR` 分级。
- 将 CNInfo 标题分类、公司行动正文抽取/独立复核、公司画像语义抽取/复核、旧画像适配器和应用生命周期纳入兼容性检查及回归验收。
- **BREAKING**：部署配置从 `config/11_llm.json` 迁移到 `config/13_llm.json`，并以 `QUOTE_LLM_PIPIO_GROK_API_KEY`、`QUOTE_LLM_PIPIO_LUNA_API_KEY` 替代通用 `QUOTE_LLM_API_KEY`；业务 `LlmRequest.profile` 和提示词/schema 契约保持兼容。

## Capabilities

### New Capabilities

- `weighted-llm-pool-routing`: 定义共享总并发、实际来源权重、可借用容量、健康状态、故障转移、路由身份、来源审计及现有 LLM 业务兼容验收。

### Modified Capabilities

- `common-llm-gateway`: 将公共 profile 契约扩展为逻辑 profile/实际 profile 分层，扩展响应来源 lineage，并禁止业务适配器直接依赖实际 profile 配置。

## Impact

- 公共模块：`utils/llm/models.py`、`client.py`、`errors.py`、`orchestration/`、配置加载和 fake transport 测试。
- 配置与部署：LLM JSON 文件名、两个 Pipio Key 环境变量、实际 profile、provider resource、pool/route 和非敏感模板/文档。
- CNInfo 业务：标题分类、公司行动抽取/复核、异步 pipeline、恢复、持久化、审计和自动 promotion 的来源透传与兼容检查。
- 公司画像：语义抽取、结构化抽取、独立复核、runtime identity、checkpoint/artifact、旧适配器和生产脚本。
- 数据：公司行动 LLM 分析与公司画像审计需要兼容持久化实际来源和路由 lineage；历史缺失来源不得猜测。
- 测试与运维：扩展公共网关/编排单测，运行所有现有 LLM 业务回归、静态调用扫描和受控双模型 smoke/分级并发验证。
- 日志与可观测性：沿用系统 task-domain handler、格式、轮转和模块级日志配置，扩展 `LLM` 日志事件、pool/provider 快照和脱敏测试，不新增独立日志后端。
