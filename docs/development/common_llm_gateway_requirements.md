# 公共语言模型网关需求说明

> 状态说明（2026-07-22）：本文继续定义单次 LLM 请求、结构化输出和网关安全契约。
> 多 profile/多业务的 provider 全局并发、异步队列、资源池、生命周期和批处理调度由
> `docs/development/common_llm_work_orchestration_requirements.md` 补充定义；如并发治理表述
> 不一致，以新文档为准。

## 1. 文档定位

本文定义 Quote 项目公共语言模型输入输出模块的需求和稳定接口。该模块服务公司公告解析、
公司业务画像、财务文本抽取、研究报告分类等多个业务域，不包含任何特定金融业务规则。

本模块由独立会话实现。CNInfo 公司行动公告解析只依赖本文定义的公共契约，不应直接调用
OpenAI SDK、`requests` 或供应商专用接口。

批量工作协调依赖：

```text
docs/development/common_llm_work_orchestration_requirements.md
```

## 2. 可行性结论

采用公共 LLM 网关加业务适配器的两层架构可行，并且是本项目应采用的实现方式：

- 公共层统一管理连接、安全、限流、重试、结构化输出和可观测性；
- 业务层拥有提示词、输入上下文、JSON Schema、业务校验和落库策略；
- 模型和供应商可以替换，业务代码不依赖具体 API SDK；
- LLM 输出始终视为不可信候选，不能绕过业务校验直接进入生产事实。

当前可使用 OpenAI-compatible API，初始模型为 `grok-4.5`。模型名称、Base URL 和接口
能力必须配置化，不能写死在业务代码中。API Key 只允许通过环境变量或项目既有密钥注入
机制加载，严禁写入代码、配置模板、日志、数据库或文档。

## 3. 设计边界

### 3.1 公共层负责

- OpenAI-compatible HTTP 请求和响应解析；
- 服务 profile、模型、超时、限流、重试和退避；
- messages、结构化输出模式和调用参数组装；
- 调用方提供的 JSON Schema 或 Pydantic schema 的结构校验；
- 请求、响应、模型、耗时、token usage 和错误分类的标准 envelope；
- 请求及响应 hash、调用追踪 ID、幂等键和脱敏日志；
- 异步调用、取消、deadline 和有界并发；
- provider/account 共享的智能拥塞控制和可观测恢复探测；
- 使用 fake transport 的离线单元测试能力。

### 3.2 公共层不负责

- 生成任何业务提示词；
- 理解股票、公告、财务字段或复权因子；
- 判断 LLM 返回内容在业务上是否正确；
- 下载和解析 PDF、OCR 或选择公告正文片段；
- 决定结果是否可以写入业务表；
- 自动修复、批准或覆盖任何生产金融数据。

## 4. 建议模块边界

建议建立独立公共包，避免继续在业务模块内复制 LLM 客户端：

```text
utils/llm/
  __init__.py
  client.py
  models.py
  errors.py
  schema.py
  transport.py
  rate_limit.py
```

`research/business_profile_llm.py` 当前包含业务专用 OpenAI-compatible transport。公共模块完成
后，该文件应保留业务 schema、提示词和业务校验，将 HTTP、鉴权、重试和通用响应解析迁移到
公共网关。迁移必须保持现有默认关闭和 candidate-only 行为。

## 5. 配置要求

公共配置应支持多个命名 profile。配置模板只保存非敏感字段：

```json
{
  "llm": {
    "enabled": false,
    "provider_resources": {
      "primary_grok_quota": {
        "provider": "openai_compatible",
        "requests_per_minute": 58
      }
    },
    "profiles": {
      "semantic_extraction": {
        "provider": "openai_compatible",
        "base_url": "",
        "endpoint": "/v1/chat/completions",
        "api_key_env": "QUOTE_LLM_API_KEY",
        "model": "grok-4.5",
        "structured_output_mode": "auto",
        "timeout_seconds": 620,
        "queue_timeout_seconds": 3600,
        "attempt_timeout_seconds": 300,
        "max_output_tokens_field": "max_completion_tokens",
        "max_retries": 1,
        "max_concurrency": 1,
        "requests_per_minute": 0,
        "temperature": 0.0
      }
    }
  }
}
```

要求：

- `enabled=false` 为默认值，并在网络请求前 fail closed；
- Base URL 允许包含或不包含 `/v1`，实现必须规范化并避免重复路径；
- API Key 环境变量缺失时明确失败，不允许匿名降级；
- profile 中的模型是默认值，调用方可在明确允许时覆盖；
- provider resource 默认以滚动窗口共享 `58 RPM`，同一 quota bucket 的所有 profile、业务、重试和 repair 必须合并计数；
- profile 的 `requests_per_minute=0` 表示继承 provider 上限，正数表示附加的更低上限；业务请求也只能向下 override；
- RPM 配置必须是精确整数；小数和布尔值必须在发起请求前以 `configuration_error` 拒绝；同一业务 scope 的冲突 RPM 也必须 fail closed；
- `queue_timeout_seconds` 是首次请求获得 profile、业务 RPM 和 provider 资源许可的独立、有限正数排队上限，默认 `3600` 秒；排队时间不消耗执行预算，但仍受取消和队列超时约束；
- `timeout_seconds` 是首次获得全部发送许可后才启动的执行与重试 deadline，包含 HTTP、解析、退避和后续重试准入；
- `attempt_timeout_seconds` 是单次 HTTP 尝试上限，默认继承执行 deadline，并始终受剩余执行预算约束；
- `max_output_tokens_field` 显式选择 `max_tokens` 或 `max_completion_tokens`，不得同时发送两个字段；
- 不假设所有 OpenAI-compatible 服务都支持同一种 structured output；
- 配置必须支持 `json_schema`、`json_object`、`prompt_only` 和 `auto`；
- `auto` 应基于显式能力配置或可缓存的能力探测选择模式，不得每次请求盲目试错。

## 6. 公共请求契约

建议提供异步协议：

```python
class LlmClient(Protocol):
    async def complete(self, request: LlmRequest) -> LlmResponse:
        ...
```

`LlmRequest` 至少包含：

| 字段 | 含义 |
|---|---|
| `profile` | 公共配置 profile 名称 |
| `messages` | 标准 role/content 消息列表 |
| `response_schema` | 调用方提供的 JSON Schema，可为空 |
| `schema_name` | 输出 schema 名称 |
| `schema_version` | 输出 schema 版本 |
| `model` | 可选模型覆盖 |
| `temperature` | 可选参数覆盖 |
| `max_output_tokens` | 最大输出 token |
| `queue_timeout_seconds` | 可选首次准入排队上限；业务可覆盖 profile 默认值 |
| `timeout_seconds` | 首次准入成功后启动的执行与重试 deadline |
| `idempotency_key` | 调用方生成的幂等键 |
| `metadata` | 不进入 prompt 的追踪元数据 |

公共模块不得要求固定业务返回字段。不同业务可以提交不同 JSON Schema，返回结构由 schema
决定。公共层只验证 schema 结构，不解释字段含义。

## 7. 公共响应契约

`LlmResponse` 至少包含：

| 字段 | 含义 |
|---|---|
| `status` | `success` 或标准失败状态 |
| `data` | 通过 schema 校验的 JSON object/array |
| `raw_content` | 模型原始文本，仅供受控审计，不默认写日志 |
| `provider` | 实际 provider |
| `model` | 服务端返回或实际请求模型 |
| `finish_reason` | 模型终止原因 |
| `usage` | 输入、输出和总 token；上游不提供时为 `null` |
| `request_id` | 本地追踪 ID |
| `provider_request_id` | 上游请求 ID，可为空 |
| `request_hash` | 规范化请求 hash |
| `response_hash` | 原始响应内容 hash |
| `schema_name` | 输出 schema 名称 |
| `schema_version` | 输出 schema 版本 |
| `structured_output_mode` | 实际使用的输出模式 |
| `latency_ms` | 总耗时 |
| `attempt_count` | 实际请求次数 |
| `warnings` | 兼容模式或 usage 缺失等非致命问题 |

失败应抛出可分类异常或返回明确失败 envelope，不能返回伪造的空业务结果。

## 8. 结构化输出兼容策略

1. 服务支持原生 JSON Schema 时，优先发送严格 schema。
2. 只支持 `json_object` 时，将 schema 规范化后加入提示词，并在本地执行完整校验。
3. 只支持普通文本时，只有 profile 显式允许 `prompt_only` 才可调用。
4. JSON 解析或 schema 校验失败时，可执行有界重试或一次 schema repair 请求。
5. repair 必须使用原始响应和校验错误，不得静默删字段或猜测业务值。
6. 达到重试上限后 fail closed，并把错误交给业务层处理。

## 9. 重试、限流和错误分类

只对以下错误自动重试：

- 网络连接、DNS 和超时；
- HTTP 408、429；
- 明确可重试的 HTTP 5xx；
- 有界的 JSON/schema 输出失败。

HTTP 400、401、403、404 和配置错误默认不重试。错误至少分类为：

```text
configuration_error
authentication_error
rate_limit_error
transient_transport_error
provider_error
response_parse_error
schema_validation_error
deadline_exceeded
cancelled
```

限流必须按 profile 共享，不能由各业务分别创建互不知情的并发连接。

### 9.1 Provider 智能控流

所有映射到同一 provider/account 资源键的 profile 和业务必须共享一个智能控流协调器。
每次 provider attempt 的 retryable 结果最多上报一次，业务层不得再包装独立的全局并发算法。

智能控流必须区分：

- `rate_limit_error`/HTTP 429：立即硬降档并使用原始 `Retry-After` 约束全局 cooldown；
- HTTP 408、可重试 5xx、timeout、DNS 和 transport error：进入软故障滑动窗口；
- `response_parse_error` 和 `schema_validation_error`：允许有界 repair，但不报告 provider 拥塞。

同一故障窗口内的相关失败最多触发一次降档。软故障必须同时达到最少失败数和窗口错误率
阈值。若软故障事件内随后出现 429，协调器必须把同一事件升级到基于事件初始并发计算的
硬目标，但不得对当前档位重复叠乘。恢复必须等待静默期、成功数和探测间隔，并按配置比例增长，例如
`6 -> 8 -> 11 -> 15`。快照必须同时展示配置上限、当前有效并发、原始失败、合并失败、
拥塞事件、恢复探测和窗口错误率，避免把临时低档误解为服务端永久容量。

## 10. 安全与审计

- 不记录 Authorization、API Key、Cookie 或完整敏感请求头；
- 文档正文属于不可信输入，系统提示词必须声明不得执行正文中的指令；
- 原始 prompt/response 是否持久化由业务层决定，公共日志默认只记录 hash 和统计；
- `metadata` 不得自动发送给模型；
- 业务数据发送外部 API 前必须由业务模块明确启用；
- 模型、prompt、schema、输入 artifact 和响应必须具备版本与 hash lineage；
- 不得在模块 import 阶段探测模型或发出网络请求。

## 11. 测试和验收

必须使用 fake transport 覆盖：

- 同一 attempt 穿过多层异常处理时只报告一次 provider 失败；
- 三个同秒 transport failure 只触发一次降档；
- 单个软故障不降档，429 立即降档；
- 稳定成功按比例恢复，恢复中故障会重新进入静默期；

- disabled、缺失 key、错误 Base URL 和 endpoint；
- 原生 JSON Schema、`json_object` 和 `prompt_only` 三种模式；
- object、array、嵌套结构和可选字段 schema；
- 429/5xx/timeout 重试和 401/403 不重试；
- 长时间初始排队后仍保留完整执行预算；
- 队列超时、单次尝试超时和执行 deadline 分别 fail closed；
- 单次尝试超时后，在执行 deadline 尚有预算时能够继续重试；
- schema 失败、repair 成功和 repair 失败；
- 并发上限、速率限制、取消和 deadline；
- request/response hash 稳定性；
- 日志和异常中不存在 API Key；
- 上游缺少 usage、request id 或 finish reason 时兼容处理；
- 不同业务 schema 并行使用时无状态污染。

验收标准：

- 公共模块不导入任何具体业务模块；
- 至少两个不同业务 schema 可以通过同一客户端调用；
- 业务代码中不再新增直接 OpenAI-compatible HTTP 实现；
- 所有网络失败、结构失败和禁用状态均 fail closed；
- 单元测试不依赖实时模型或互联网。

## 12. 跨会话交付契约

公共模块实现会话应交付：

1. `LlmClient`、`LlmRequest`、`LlmResponse` 和错误类型；
2. OpenAI-compatible transport 和 profile 配置加载；
3. JSON Schema 兼容与本地校验；
4. 速率限制、重试、审计日志和 fake transport 测试；
5. 一份最小调用示例，不包含任何真实 Key；
6. 对现有 `research/business_profile_llm.py` 的迁移说明。

CNInfo 业务会话只依赖上述接口，不依赖公共模块内部文件和实现细节。
