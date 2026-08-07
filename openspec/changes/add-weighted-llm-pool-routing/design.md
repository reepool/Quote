## Context

现有 `LlmClient.complete()` 已统一处理实际 profile、结构化输出、profile limiter、provider/account coordinator、重试、deadline 和响应 envelope。CNInfo 标题分类、公司行动抽取/复核以及公司画像语义生产都使用这个入口，但部分应用编排仍直接读取 `llm_config.profiles` 来判断启用状态、模型身份和有效 RPM。

本变更需要在不复制 transport、限流和金融业务逻辑的前提下，引入逻辑 profile、共享 LLM pool 和实际 profile 三层配置。首期两个实际来源均使用 Pipio OpenAI-compatible Base URL，但分别使用 `QUOTE_LLM_PIPIO_GROK_API_KEY`/`grok-4.5` 和 `QUOTE_LLM_PIPIO_LUNA_API_KEY`/`gpt-5.6-luna`。相同 URL 只表示 transport 地址相同，不表示 quota bucket 相同。

关键约束：

- pool、profile 和 provider 协调均只保证单进程共享；
- 所有 LLM 输出仍是不可信候选，不能改变业务证据门禁；
- 现有 `LlmRequest.profile` 逻辑名称必须保持稳定；
- 故障转移必须共享一个逻辑执行 deadline；
- response/source lineage 需要进入业务审计，但不能污染模型业务 schema；
- 当前工作区已有其他业务改动，实施时需要继续精确隔离改动。

详细需求基线见：

```text
docs/development/common_llm_multi_source_routing_requirements.md
```

## Goals / Non-Goals

**Goals:**

- 在 `utils/llm` 内提供逻辑 profile 到实际 profile 的后端透明路由。
- 多个逻辑 profile 共享一个 pool 的总并发和成员权重。
- 使用确定性加权公平调度，支持健康成员借用空闲容量。
- 在同一请求预算内执行有界故障转移和成员熔断。
- 保持现有 profile/provider limiter、RPM 和自适应拥塞控制有效。
- 扩展成功/失败 envelope，提供稳定实际来源和完整脱敏尝试 lineage。
- 用公共逻辑配置 facade 替换应用层实际 profile 访问。
- 完整验证现有 CNInfo、公司画像、应用生命周期和开发脚本调用。

**Non-Goals:**

- 不实现跨进程/跨主机分布式协调。
- 不实现 hedging、竞速、多模型投票或结果融合。
- 不新增供应商 SDK；首期继续使用 OpenAI-compatible transport。
- 不自动决定两个 Pipio Key 是否共享 quota。
- 不修改提示词、金融字段解释、证据批准或生产事实写入策略。
- 不将认证失败默认静默切换，以免长期隐藏部署错误。

## Decisions

### 1. 使用逻辑 route、LLM pool、实际 profile 三层配置

`LlmRequest.profile` 继续表示逻辑业务能力。`routes` 将逻辑 profile 映射到 pool，pool 成员再将逻辑 profile 映射到实际 profile。实际 profile 继续拥有 URL、Key 环境变量、模型、结构化输出能力和局部限制。

这样一个 pool 可以同时承载标题、抽取和复核的总并发，而每个来源仍能为不同逻辑 profile 声明不同参数。没有 route 的 profile 走现有直接路径。

替代方案：让业务传入实际 profile。拒绝，因为会把模型选择、故障状态和密钥配置泄漏到每个业务，并使增删模型需要修改业务代码。

替代方案：仅在 `provider_resources` 增加模型权重。拒绝，因为 provider resource 表示 quota bucket，不表示业务可替换能力；同一资源也可能承载多个不可互换 profile。

### 2. 在 `LlmClient.complete()` 内解析 route，内部拆分实际调用方法

公共入口保留 `complete(request)`。实现将现有单 profile 主体提取为不再次解析 route 的私有实际调用路径，例如 `_complete_concrete(request, profile, budget)`。逻辑入口负责：

1. 解析逻辑 profile；
2. 无 route 时调用兼容直接路径；
3. 有 route 时进入 pool coordinator；
4. 对选择的实际 profile调用私有路径；
5. 聚合 route lineage 并返回最终响应。

避免通过递归 `complete()` 调用实际 profile，以免实际 profile 名与 route 冲突、重复创建逻辑 request ID 或再次进入 pool。

替代方案：在每个业务外包一层 `RoutedLlmClient`。拒绝，因为项目有多个构造入口和注入路径，容易出现部分调用绕过 route；统一公共入口更符合当前架构。

### 3. 增加进程级 `LlmPoolCoordinatorRegistry`

与现有全局 profile limiter/provider coordinator registry 相同，所有 `LlmClient` 默认共享一个进程级 pool registry。测试可注入独立 registry 和 fake clock。registry 以经过验证的 pool 配置身份为键，配置冲突必须 fail closed，而不是复用旧状态。

pool coordinator 维护：

- 有界逻辑请求队列；
- 总活动请求计数；
- 每成员活动、等待、dispatch、成功和失败；
- 加权公平游标/deficit；
- circuit 状态和定时探测；
- failover 和 route 延迟指标；
- 生命周期关闭及等待者取消。

client 关闭不默认关闭全局 registry；应用 shutdown 通过公共 registry lifecycle 或已有 application-owned client 统一停止新准入并清理等待者。测试 registry 可显式 `clear/close`。

### 4. 使用确定性 weighted deficit round robin

正常 dispatch 使用 weighted deficit round robin。每个健康成员按正整数权重累积 credit；有待处理请求且具备对应逻辑 profile 映射的成员按 credit 获得选择机会。成功或失败都视为一次 dispatch 消耗，故障转移不会回补正常流量统计。

选择时排除：

- 当前 logical profile 无实际映射；
- actual profile disabled 或配置无效；
- circuit open；
- 已被本逻辑请求尝试；
- 达到成员/actual profile 明确并发上限。

`borrow_idle_capacity=true` 时，正常权重候选不可用后可选择其他健康成员，pool 总并发仍是硬上限。成员恢复后 credit 算法逐步回到长期比例，不做瞬时抢占。

替代方案：无状态随机权重。拒绝，因为小批次波动大、难以测试并可能饿死低权重成员。

替代方案：按权重永久切割 semaphore。拒绝，因为慢/故障成员会留下空闲槽，降低吞吐并削弱故障转移价值。

### 5. pool 总并发覆盖一个逻辑调用，provider lease 仍只覆盖实际 attempt

pool activity 从逻辑请求 dispatch 开始，覆盖同源重试、解析/repair 和故障转移，直到成功或终态失败。这样总并发表示业务同时执行的逻辑 LLM 工作数，不因切换瞬间重复计算。

provider coordinator 的 lease 继续只覆盖每次实际 HTTP attempt，并按实际 resource 分别计入 RPM、cooldown 和自适应结果。pool 不复制 provider 拥塞算法，也不把 schema failure 上报为 provider 拥塞。

逻辑请求尚未 dispatch 时留在 pool 有界队列，不占 pool activity。下游 provider 排队可能暂时占一个 pool active slot，这是保证严格总逻辑并发的保守取舍；快照会区分 pool activity 与 provider activity，便于发现容量配置不匹配。

### 6. 使用共享绝对预算对象治理 queue、execution 和 failover

引入内部不可变预算上下文，至少包含：

- pool queue deadline；
- 第一次实际发送许可后建立的逻辑 execution deadline；
- 当前剩余执行预算；
- 最小下一 attempt 预算；
- cancellation 状态。

实际调用路径接受外部预算，而不是为每个 source 重新开始完整 `timeout_seconds`。第一次来源的初始 profile/provider 准入仍遵循既有“排队不消耗执行预算”；一旦第一次 HTTP attempt 获得发送许可，后续同源重试、退避、repair、failover 选择和其他来源准入都消耗同一 execution deadline。

实际 attempt timeout 取 concrete profile `attempt_timeout_seconds`、route 剩余预算和请求覆盖值的最小值。剩余预算不足时不启动新的 failover。

替代方案：每个 actual profile 各自重新计算 deadline。拒绝，因为最坏时延会随成员数线性增长，并违背调用方 timeout 契约。

### 7. 同源重试先于跨源 failover

现有 actual profile `max_retries` 和 schema repair 先执行。actual 调用返回终态分类错误后，route 根据 `failover.on`、剩余预算、`max_hops` 和未尝试成员决定是否切换。

默认允许：429、transient transport、retryable provider、已耗尽 repair 的 parse/schema failure。默认禁止：cancelled、公共请求/route 配置错误和预算耗尽。认证错误只有配置 `allow_auth_failover=true` 才切换，并产生高优先级安全告警。

首期配置应使用低同源重试次数，避免慢成员消耗全部 route deadline。业务层不得再嵌套无界模型切换循环。

### 8. circuit breaker 与 provider 自适应控制分层

每个 pool 成员使用 `closed/open/half_open` 状态。达到配置故障阈值后 open；冷却后仅准入有限 half-open probe；成功关闭 circuit，失败重新 open。429 可立即触发成员 cooldown，具体阈值由配置模型定义并用 fake clock 测试。

provider coordinator 仍处理 quota bucket 级 RPM、共享 cooldown 和并发降档。pool circuit 处理“该替代来源当前是否适合接收新逻辑请求”。两层可同时生效，但一次实际失败只能向 provider coordinator 上报一次。

### 9. 建立逻辑请求与实际尝试双层身份

逻辑 route fingerprint 由规范化 pool/route 配置、成员 source label、逻辑到实际映射和与输出契约有关的实际 profile能力组成，不包含 Key 值、瞬时 health 或统计计数。

业务 input/runtime identity 使用逻辑 profile、route fingerprint、业务 payload、prompt/schema/parser 版本。实际 attempt identity 额外包含 selected profile/source/model/attempt。这样负载均衡和故障转移不会改变同一业务输入身份，而配置能力变化会使 checkpoint/resume 正确失效。

调用方 idempotency key 在跨源切换时保持，实际本地 request ID/provider request ID 分开记录。公共层不声称不同 Key 共享上游幂等实现。

### 10. 扩展公共响应而不修改模型业务数据

`LlmResponse` 增加 `source_label`、`logical_profile`、`selected_profile`，并在 lineage 增加 pool、route fingerprint、failover count 和安全 attempts。分类错误的 lineage 使用同一结构。

`response.data` 仍是通过业务 schema 的模型数据，不能注入来源字段。CNInfo/company-profile 适配器将公共来源复制到各自 audit/envelope/数据库字段。

新增 dataclass 字段使用兼容默认值或同步更新所有仓库内 fixture。历史数据缺失来源时使用 `null`/`legacy_unknown`，不从可变 model 字符串推断。

### 11. 通过公共 facade 隐藏实际 profiles

`LlmConfig` 提供逻辑查询：是否启用、非敏感描述、route fingerprint、有效限制和实际能力集合。无 route 的旧 profile 也通过该 facade 返回统一描述。

迁移以下直接访问模式：

```text
llm_config.profiles.get(logical_name)
configured_profile.model
resource_for_profile(logical_profile)
```

业务只记录逻辑描述和实际响应来源。实际 profile 解析、Key、URL、model、resource 选择限制在 `utils/llm`。

替代方案：保留一个同名“虚拟 profile”供业务读取。拒绝，因为容易让调用方继续误认为只有一个模型，并产生错误 runtime identity。

### 12. provider resource 按真实 quota 建模

首期为 Grok/Luna 配置显式 resource 名，不再依赖 `provider:api_key_env` 自动名字。两个 Key 是否映射同一 resource 取决于供应商确认和受控 quota 验证，而不是 Base URL。

若额度独立：两个 resource 各自协调。若共享：两个 actual profile 映射同一 resource，确保合计并发/RPM不超限。配置文档和 smoke 报告必须披露该判断。

### 13. 配置文件和密钥迁移

将 `config/11_llm.json` 重命名为 `config/13_llm.json`，更新所有文档、测试和配置说明路径。配置加载器继续发现全部 `*.json`，应用代码不应绑定文件名。

实际 Key 只由部署环境提供：

```text
QUOTE_LLM_PIPIO_GROK_API_KEY
QUOTE_LLM_PIPIO_LUNA_API_KEY
```

仓库不读取或迁移真实 `.env` 值。部署步骤是先注入新变量，再切换 config，验证后删除旧 `QUOTE_LLM_API_KEY`。`load_project_environment(override=False)` 不变。

### 14. 业务来源持久化分阶段完成

CNInfo 标题 lineage、公司行动主要抽取和独立 verifier 分别记录来源。`corporate_action_llm_analyses` 增加主要来源和受控 route lineage，迁移保持历史可读。公司画像 `SemanticRunAudit`、抽取 envelope、artifact/checkpoint 报告实际来源。

业务 schema、证据引用和确定性 gate 不增加来源字段。自动 promotion 只消费既有验证结论，不基于具体模型放宽规则。

### 15. 现有业务调用检查是实施完成条件

实施任务包含静态扫描和明确测试矩阵，覆盖：

- `data_sources/cninfo_announcement_title_llm.py`；
- `data_sources/cninfo_corporate_action_llm.py` 和 async pipeline；
- `data_manager.py` 的 enable/RPM/resolver/resume/persist/lifecycle；
- `research/business_profile_semantic_extraction.py`；
- `research/business_profile_semantic_runtime.py`；
- `research/business_profile_production_rollout.py`；
- `research/business_profile_async_production.py`；
- `research/business_profile_llm.py` legacy adapter；
- scheduler/production scripts/live validation/benchmark；
- 所有 `LlmClient`、`LlmClientProtocol`、`LlmRequest` 和 `llm_config.profiles` 引用。

公共 fake transport 测试先完成，再运行业务回归。真实 Key 测试必须是显式受控 smoke，不进入普通单元测试。

### 16. 配置校验使用显式 fail-closed 契约

配置模型不得只校验字段存在。`total_concurrency`、`queue_size` 和成员 `weight` 必须是非布尔正整数，首期 strategy 只允许 `weighted_fair`。pool、route 和 source label 必须非空且在各自作用域唯一；route 必须引用启用的 pool；每个成员必须为 route 映射存在且启用的实际 profile，且成员/实际 profile 的 source label 一致。

配置加载还需要验证：

- 每个实际 profile 显式声明 structured output、stream、timeout、retry、token、局部并发和 RPM 等既有能力字段；
- 所有可选成员满足逻辑 profile 的 structured-output 契约；
- pool 总并发不突破项目硬上限；
- provider resource、profile 和 workload RPM 的继承关系不因 route 改变；
- 已确认共享的 quota bucket 不得通过不同 resource 名称绕过总限制；
- route 名称不得与实际 profile 名称产生歧义；
- 正式配置文件中只能有一个顶层 `llm` 所有者，避免配置管理器的浅合并覆盖。

所有分支都使用离线配置测试覆盖，包括未知引用、重复标签、布尔/小数数值、能力不兼容、独立/共享 quota 和无 route 兼容路径。

### 17. LLM 日志复用系统日志架构并按事件重要性分级

LLM 不创建独立 handler、日志目录或轮转机制。公共模块继续使用 `logging.getLogger("LLM")`，由 `utils/logging_manager.py` 将其路由到 task-domain 文件，格式、console、模块级别和 size/time rotation 由 `config/01_log.json` 统一管理。实施需在该配置的 `modules` 中加入可配置的 `LLM` 条目；生产默认可保持 `INFO`，诊断时切换为 `DEBUG`。

日志事件采用稳定事件名和参数化字段，不拼接完整 prompt/response：

- `DEBUG`：入队/出队、候选成员及排除原因、weight/deficit 游标、借用判断、provider/profile lease 等待与释放、attempt payload 非敏感摘要、retry/repair/backoff 计划、剩余 deadline、half-open probe 细节、快照采样和关闭清理细节；
- `INFO`：route admitted、source selected、首次 provider attempt 开始、failover selected、route completed、route exhausted、circuit opened/half-open/recovered、pool/registry 启动与正常关闭；
- `WARNING`：可恢复 429/5xx/timeout、parse/schema failure、认证 failover、成员熔断、deadline 接近耗尽、回退或部分来源不可用；
- `ERROR`：配置无法启动、全部来源终态失败、生命周期关闭失败、状态/租约泄漏或破坏公共契约的内部异常，并保留 `exc_info` 供非业务敏感异常诊断。

每条相关日志按可用范围携带 logical profile、pool、source label、selected profile、local/provider request ID、request hash、workload、run/stage、business item、attempt、failover count、queue/elapsed/remaining 时间和分类错误码。日志禁止 API Key、Authorization、Cookie、完整正文、完整 prompt/response 和原始 provider 错误体；测试通过捕获 logger 验证级别、字段和脱敏。

pool 快照扩展为可运维合同：报告 pool 配置/有效上限及当前瓶颈、active/waiting/oldest wait；各来源 weight/dispatch/active/waiting/ratio、success/error/429/5xx/timeout/parse/schema；circuit/cooldown/probe；failover requested/succeeded/exhausted 和错误分类；queue/execution/failover/total latency；logical profile/workload/run/stage/business item 关联；provider resource concurrency/RPM/cooldown。日志用于过程诊断，快照计数用于聚合观测，二者不能相互替代。

### 18. 数据迁移、业务回归和分级放量使用显式门禁

来源字段数据库迁移必须幂等、可重复执行并具备回滚验证；不得覆盖既有分析结果。历史记录使用 `null`/`legacy_unknown`，route 回滚不得删除已保存 lineage。API、任务报告和审计查询返回 LLM 结果时均要保留来源标签。

业务回归必须逐项验证需求文档第 14 节的既有契约，不能只验证模块可导入或请求成功：标题分类保留分块/隔离重试/乱序身份/schema/applicability；公司行动保留 `source_event_key`、resume 去重、确定性证据和 promotion gate；公司画像保留 runtime identity、checkpoint/rework/promotion manifest/source revision、network/scope/candidate gate，并区分业务 structured-source fallback 和模型 failover；legacy adapter 保留同步/异步、fake client、来源 envelope 和关闭行为。

真实验证必须使用合成非敏感中文输入。先逐个来源验证认证、实际 model、stream、usage、structured output、timeout 和 quota；再运行小批量逻辑路由并显式禁用/模拟一个成员验证 failover；最后按 10、25、50 并发逐级放量。每一级记录成功率、429/5xx、首事件/总耗时、权重比例、failover、内存、连接数和关闭耗时，低一级未通过不得进入高一级。所有 live 结果仍只作为候选，不绕过 holdout 或 promotion gate。

## Risks / Trade-offs

- [相同 Base URL 是共同故障域] → 将本变更定位为模型/Key 冗余而非跨供应商灾备；lineage 披露实际来源，未来可加入不同 provider。
- [quota bucket 判断错误] → 启用前验证两个 Key 的共享/独立额度；resource 映射显式配置且运行快照显示合计压力。
- [第一个慢成员耗尽 failover 时间] → 共享绝对 execution deadline、低同源重试和最小剩余 attempt 预算。
- [权重比例与瞬时在途不一致] → 权重定义为长期 dispatch 比例；快照同时报告 dispatch、active、latency、熔断和借用。
- [多层 limiter 造成排队复杂] → 明确 pool、profile、provider 的职责和租约顺序；使用取消/deadline/异常测试验证无死锁与泄漏。
- [不同模型结构合法但语义质量不同] → 保留业务 schema、证据门禁、holdout 和 promotion gate；不实现结果融合或模型特权。
- [故障转移增加成本] → 配置 max hops、总 deadline 和指标；默认不 hedging。
- [实际 profile 迁移破坏 resume identity] → 使用 route fingerprint，历史 identity 保持 legacy 可读，不按实际随机来源分裂业务输入。
- [应用代码仍绕过 facade] → 静态扫描直接 profile 访问并加入验收 gate。
- [响应/数据库字段扩展影响旧 fixture] → 兼容默认、显式迁移和历史缺失来源语义，禁止伪造。
- [进程内协调被误解为全局] → 文档、日志和部署检查明确单执行进程约束；跨进程另立 change。

## Migration Plan

1. 新增配置数据模型、逻辑 facade、source label 和 route fingerprint，保持 route 关闭并通过旧配置回归。
2. 新增 pool registry/coordinator、加权调度、快照、fake clock 和并发测试。
3. 重构 `LlmClient.complete()` 为逻辑入口与实际调用内核，接入共享预算。
4. 增加 failover、circuit breaker 和成功/失败 route lineage。
5. 迁移应用直接 profile 访问，先保持单成员 route 验证行为等价。
6. 扩展 CNInfo/company-profile 来源透传、持久化和历史兼容迁移。
7. 将 LLM 配置重命名为 `13_llm.json`，增加两个实际 profile 和部署变量说明；默认 pool/新成员在未验证时保持保守关闭或低并发。
8. 运行公共测试、完整业务测试矩阵、静态调用扫描、OpenSpec validate 和未提交 diff 审核。
9. 分别对 Grok/Luna 做单源受控 smoke，确认 structured output、stream、usage、model 和 quota 映射。
10. 开启双源 10 并发，验证权重和故障转移，再分级到 25、50。

回滚策略：关闭 route/pool，将逻辑 profile 重新映射到一个已验证实际 profile或兼容单 profile；保留已写入的 source/route lineage和数据库迁移，不删除历史分析，不回退 Key 或覆盖业务结果。若新配置文件导致启动失败，可恢复上一版本 `11_llm.json` 配置引用和旧部署变量，但不得通过禁用配置校验绕过错误。

## Open Questions

- 两个 Pipio Key 的 hard concurrency 和 RPM 是独立 quota 还是共享账号 quota？必须在真实放量前确认。
- 首次生产权重采用 1:1、3:1 还是基于单源质量/吞吐基准确定？实现只提供可配置能力，不在代码中写死。
- `gpt-5.6-luna` 是否与 Grok 一样稳定支持当前 `json_object`、stream usage 和 `max_completion_tokens`？必须通过单源 capability smoke。
- 公司行动数据库来源字段采用独立列加 route lineage JSON，还是复用现有 JSON 并新增索引列？实施时应优先满足可查询性和历史兼容，不能只考虑最小迁移。
