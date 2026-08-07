# 公共 LLM 多源加权路由与故障转移需求

## 1. 文档定位

本文定义 Quote 项目公共 LLM 后端从单一实际 profile 演进为多源加权路由的完整需求。目标是让公告解析、公司画像和后续语义业务继续只依赖稳定的逻辑 profile，由公共网关统一完成实际模型选择、总并发治理、故障转移、来源标记和调用审计。

本文补充并服从以下既有文档中的安全、结构化输出、候选结果和业务隔离约束：

```text
docs/development/common_llm_gateway_requirements.md
docs/development/common_llm_gateway_architecture.md
docs/development/common_llm_work_orchestration_requirements.md
docs/development/cninfo_corporate_action_async_pipeline_requirements.md
docs/development/cninfo_corporate_action_llm_resolution_requirements.md
```

如本文与旧文档中的“一个逻辑 profile 对应一个实际模型”假设冲突，以本文为准。现有业务证据门禁、确定性校验、candidate-only 和禁止 LLM 直接覆盖生产金融事实的规则不变。

## 2. 当前基线

项目已经具备以下公共能力：

- `utils.llm.LlmClient.complete()` 作为统一异步调用入口；
- 命名 profile、OpenAI-compatible transport 和环境变量密钥解析；
- profile 并发/RPM 限制；
- provider/account 级全局协调、workload 公平调度、共享 cooldown 和自适应并发；
- 结构化输出选择、本地 JSON Schema 校验和有界 repair；
- 队列超时、执行 deadline、单次 attempt timeout、重试和取消；
- request/response hash、usage、latency、request ID 和业务 lineage；
- CNInfo 公告标题分类、公司行动正文抽取、独立语义复核；
- 公司画像普通语义抽取、结构化语义抽取和独立语义复核。

当前限制：

1. 一个业务逻辑 profile 只能绑定一个实际模型；
2. 应用编排代码仍有直接访问 `llm_config.profiles` 的情况；
3. workload 权重只解决同一 provider resource 内不同业务的公平性，不能表达多个实际 LLM 对一个总并发预算的分配权重；
4. 单个实际模型失败后只能在该 profile 内重试，不能切换到其他模型；
5. `LlmResponse` 有 provider/model，但没有稳定、可配置的实际 LLM 来源标签；
6. 业务运行身份和部分恢复逻辑仍假设逻辑 profile 只有一个 model identity。

## 3. 首期部署条件

首期配置两个 Pipio OpenAI-compatible 模型：

| 来源标签 | 模型 | API Key 环境变量 | Base URL |
| --- | --- | --- | --- |
| `pipio:grok-4.5` | `grok-4.5` | `QUOTE_LLM_PIPIO_GROK_API_KEY` | 与 Pipio 当前地址相同 |
| `pipio:gpt-5.6-luna` | `gpt-5.6-luna` | `QUOTE_LLM_PIPIO_LUNA_API_KEY` | 与 Grok profile 相同 |

要求：

- `.env` 只保存真实 Key，不得进入版本库、日志、数据库、OpenSpec 或测试 fixture；
- 配置文件只保存 `api_key_env` 名称；
- 两个模型使用相同 Base URL 不代表共享或独立 quota；
- provider resource 必须按真实 quota bucket 建模；
- 若两个 Key 的额度独立，应配置两个 provider resource；
- 若供应商确认两个 Key 共享账号并发/RPM，应让两个实际 profile 映射到同一个 provider resource；
- 启用前必须通过受控验证确认各 Key 的模型权限、structured-output 能力、并发/RPM 和实际返回 model 字段。

## 4. 目标与非目标

### 4.1 目标

- 业务继续使用 `semantic_extraction`、`corporate_action_title_classification` 等稳定逻辑 profile；
- 一个逻辑 profile 可以映射到多个实际模型；
- 多个逻辑 profile 可以共享一个 LLM pool 的总并发预算；
- pool 成员按可配置权重获得正常流量；
- 一个成员空闲或不可用时，健康成员可以接管可借用容量；
- 发生允许切换的故障时，在同一逻辑请求预算内切换到其他成员；
- 成功和失败结果均能识别实际 LLM 来源和完整路由尝试；
- 应用业务模块不得直接访问实际 profile、Key、URL、模型或路由内部状态；
- 无 pool 配置时保持现有单 profile 行为；
- 现有所有 LLM 业务必须通过离线回归和受控 smoke 验证。

### 4.2 非目标

- 不实现跨进程或跨主机的分布式并发协调；
- 不同时请求多个模型并竞速返回；
- 不做多模型投票、结果融合或事实仲裁；
- 不改变公告、公司行动和公司画像的业务提示词或证据规则；
- 不允许故障转移绕过 JSON Schema、业务验证、人工复核或生产写入门禁；
- 不在公共网关中解释任何金融业务字段；
- 不自动探测或猜测供应商 quota 归属；
- 不把模型历史回测或一次 smoke 成功解释为长期质量保证。

## 5. 术语和配置层次

### 5.1 逻辑 profile

业务调用的稳定能力名称，例如：

```text
semantic_extraction
corporate_action_title_classification
business_profile_semantic_verification
```

逻辑 profile 表示业务所需的调用契约，不表示某个供应商或模型。

### 5.2 实际 profile

绑定具体 provider、Base URL、API Key 环境变量、模型、structured-output 能力、timeout、重试和局部限额的配置，例如：

```text
semantic_extraction__pipio_grok
semantic_extraction__pipio_luna
corporate_action_title_classification__pipio_grok
corporate_action_title_classification__pipio_luna
```

实际 profile 只能由公共 LLM 模块解析和调用，业务模块不得直接依赖其名称。

### 5.3 Provider resource

代表真实 provider/account quota bucket，继续由现有 `ProviderCoordinator` 管理硬并发、批量并发、RPM、cooldown 和自适应降档。

### 5.4 LLM pool

多个实际 LLM 来源组成的逻辑资源池。pool 负责：

- 跨来源总并发；
- 成员权重；
- 可借用容量；
- 成员健康状态；
- 故障转移；
- pool 级排队和指标。

### 5.5 来源标签

`source_label` 是运维配置的稳定、非敏感标识，例如 `pipio:grok-4.5`。它不能包含 API Key、账号 Cookie 或其他秘密，也不能只依赖供应商响应中的可变 model 名称。

## 6. 总体架构

```text
业务适配器
  LlmRequest(profile=<逻辑 profile>)
          |
          v
公共 LLM 路由解析
  logical profile -> pool
          |
          v
LLM Pool Coordinator
  总并发 / 加权公平 / 健康状态 / 故障转移
          |
          +-----------------------------+
          |                             |
          v                             v
实际 profile: Pipio Grok       实际 profile: Pipio Luna
          |                             |
          v                             v
现有 profile limiter + provider/account coordinator
          |                             |
          +-------------+---------------+
                        v
               OpenAI-compatible transport
```

公共层必须保持以下边界：

- pool 路由位于 `utils/llm`；
- provider resource 仍是实际外部 quota 的最终约束；
- 业务 pipeline 的 worker 数仍是业务提交上限；
- pool 不导入 `data_manager`、`data_sources`、`research`、scheduler 或数据库模型；
- 业务模块只构造请求、解释业务数据并透传公共响应血缘。

## 7. 配置要求

### 7.1 配置文件

实施时应将：

```text
config/11_llm.json
```

重命名为：

```text
config/13_llm.json
```

原因：当前已有 `10_research.json`、`11_futures.json` 和 `12_fx.json`。配置加载器按文件名排序并对顶层键执行浅合并；重复数字不会造成当前运行错误，但唯一前缀有助于治理。重命名必须同步修改测试、文档和配置说明中的显式路径引用。

系统必须继续保证只有一个正式 JSON 文件拥有顶层 `llm` 键，避免浅合并导致整个 LLM 配置被后加载文件覆盖。

### 7.2 建议配置结构

以下示例只展示结构和非敏感名称：

```json
{
  "llm": {
    "enabled": true,
    "provider_resources": {
      "pipio:grok": {
        "provider": "openai_compatible",
        "hard_max_concurrency": 60,
        "default_bulk_concurrency": 50,
        "requests_per_minute": 58
      },
      "pipio:luna": {
        "provider": "openai_compatible",
        "hard_max_concurrency": 60,
        "default_bulk_concurrency": 50,
        "requests_per_minute": 58
      }
    },
    "pools": {
      "shared_semantic": {
        "enabled": true,
        "total_concurrency": 50,
        "queue_size": 200,
        "strategy": "weighted_fair",
        "borrow_idle_capacity": true,
        "members": [
          {
            "source_label": "pipio:grok-4.5",
            "weight": 3,
            "profiles": {
              "semantic_extraction": "semantic_extraction__pipio_grok",
              "corporate_action_title_classification": "corporate_action_title_classification__pipio_grok"
            }
          },
          {
            "source_label": "pipio:gpt-5.6-luna",
            "weight": 1,
            "profiles": {
              "semantic_extraction": "semantic_extraction__pipio_luna",
              "corporate_action_title_classification": "corporate_action_title_classification__pipio_luna"
            }
          }
        ],
        "failover": {
          "enabled": true,
          "max_hops": 1,
          "failure_threshold": 3,
          "open_seconds": 60,
          "allow_auth_failover": false,
          "on": [
            "rate_limit_error",
            "transient_transport_error",
            "provider_error",
            "response_parse_error",
            "schema_validation_error"
          ]
        }
      }
    },
    "routes": {
      "semantic_extraction": {"pool": "shared_semantic"},
      "corporate_action_title_classification": {"pool": "shared_semantic"}
    },
    "profiles": {
      "semantic_extraction__pipio_grok": {
        "enabled": true,
        "provider": "openai_compatible",
        "provider_resource": "pipio:grok",
        "source_label": "pipio:grok-4.5",
        "base_url": "https://pipio.io/v1",
        "endpoint": "/v1/chat/completions",
        "api_key_env": "QUOTE_LLM_PIPIO_GROK_API_KEY",
        "model": "grok-4.5"
      },
      "semantic_extraction__pipio_luna": {
        "enabled": true,
        "provider": "openai_compatible",
        "provider_resource": "pipio:luna",
        "source_label": "pipio:gpt-5.6-luna",
        "base_url": "https://pipio.io/v1",
        "endpoint": "/v1/chat/completions",
        "api_key_env": "QUOTE_LLM_PIPIO_LUNA_API_KEY",
        "model": "gpt-5.6-luna"
      }
    }
  }
}
```

完整实际 profile 仍必须显式声明 structured-output、stream、timeout、retry、token 字段、局部并发和 RPM 等既有字段，不能依赖示例中的省略项。

### 7.3 配置校验

配置加载必须 fail closed 校验：

- pool 名称和逻辑 route 名称非空且唯一；
- `total_concurrency`、`queue_size` 和成员 `weight` 为正整数，布尔值和小数不作为整数接受；
- `strategy` 首期只允许 `weighted_fair`；
- `source_label` 在同一 pool 内唯一且不含敏感信息；
- 每个 route 引用已启用 pool；
- 每个 pool 成员必须为所路由的逻辑 profile 映射一个存在且已启用的实际 profile；
- 实际 profile 的 `source_label` 与 pool 成员标签一致；
- 所有成员都能满足逻辑 profile 所需的 structured-output 契约；
- pool 总并发不突破项目硬上限；
- provider resource、profile 和业务 RPM 的继承规则保持不变；
- 同一实际 quota bucket 不得通过不同 resource 名称绕过共享限制；
- 没有 route 的现有 profile 继续按单 profile 路径工作；
- route 名称与实际 profile 同名时必须拒绝歧义配置。

## 8. 加权调度和总并发

### 8.1 权重语义

权重定义健康成员在持续有请求时的长期正常流量比例，而不是每个成员独占的永久槽位。例如总并发为 50，权重为 3:1 时，长期分配目标约为 75%:25%。

首期必须使用可测试、确定性的加权公平算法，例如 deficit round robin 或等价算法。不得仅使用无状态随机数，因为随机分配难以保证小批次公平、复现测试和故障后的容量解释。

### 8.2 可借用容量

`borrow_idle_capacity=true` 时：

- 一个成员没有可处理能力、进入熔断、达到自身上限或暂时无对应 profile 时，健康成员可以接管空闲容量；
- 借用不能突破 pool 总并发、实际 profile 限制或 provider resource 限制；
- 成员恢复后，调度应逐步回到配置权重；
- 借用只改变实时容量利用，不修改配置权重。

### 8.3 有效并发

一次实际调用同时受以下上限约束：

```text
业务阶段 worker 上限
pool total_concurrency
实际 profile max_concurrency
provider resource 当前有效并发
HTTP connection pool
```

最小值决定实际在途能力。配置和运行快照必须能说明哪个限制正在生效，避免把业务 worker 只有 15 误解为 pool 50 未工作。

### 8.4 公平性层次

- pool 成员权重控制“请求发给哪个实际 LLM”；
- 现有 provider `workload_weights` 控制同一 quota bucket 内不同业务的公平准入；
- 两类权重不能复用同一个配置字段；
- 历史回补、日更、标题分类、正文抽取和语义复核仍必须通过现有 workload 身份参与公平调度。

## 9. 故障转移和健康状态

### 9.1 基本流程

1. 逻辑请求进入 pool 有界队列；
2. 调度器从健康、尚未尝试且具备该逻辑 profile 映射的成员中选择来源；
3. 实际 profile 在现有 profile/provider 限流下执行；
4. 成功时返回第一个通过公共解析和 schema 校验的响应；
5. 失败且错误允许切换时，记录尝试并选择另一个成员；
6. 达到 `max_hops`、没有健康成员或总 deadline 不足时停止；
7. 全部失败时抛出分类异常并附带安全的路由 lineage，不返回空业务对象。

同一逻辑请求不得再次选择已失败的成员，除非进入新的业务级重试或恢复任务。

### 9.2 错误策略

默认允许故障转移：

- `rate_limit_error`；
- `transient_transport_error`；
- 明确 retryable 的 `provider_error`；
- 已耗尽该实际 profile repair/retry 的 `response_parse_error`；
- 已耗尽该实际 profile repair/retry 的 `schema_validation_error`。

默认不允许故障转移：

- `cancelled`；
- 调用方请求或公共配置本身无效；
- route 总 deadline 已耗尽；
- 与所有成员共同请求契约有关的 schema 定义错误。

认证失败默认不自动切换。只有 `allow_auth_failover=true` 时才可切换，并且必须记录高优先级配置告警，防止 Key 失效长期被备用模型掩盖。

### 9.3 重试与切换顺序

- 实际 profile 继续拥有有限的同源重试；
- pool 只在同源调用最终失败后切换；
- 首期实际 profile 的 `max_retries` 应保持较低，避免一个慢模型耗尽全部故障转移预算；
- pool 必须使用一个逻辑请求总 deadline，并为后续成员保留最小可执行预算；
- 初始 pool 排队上限与执行 deadline 必须区分，延续现有 queue/execution deadline 语义；
- 故障转移的每个 provider attempt 继续消耗对应 provider resource 的 RPM；
- 不允许业务层在外部再实现无界模型切换循环。

### 9.4 熔断

每个 pool 成员维护进程内健康状态：

```text
closed -> open -> half_open -> closed
```

- `closed`：正常参与权重调度；
- `open`：达到故障阈值后，在 `open_seconds` 内不接收普通请求；
- `half_open`：冷却后只允许有界探测；
- 探测成功恢复，失败重新 open；
- 429 可以立即触发成员 cooldown，但 provider resource 的共享 cooldown 和自适应降档仍由现有协调器负责；
- schema/parse 质量故障可以影响成员健康，但不能上报为 provider 拥塞；
- health 状态只在进程内共享，不宣称跨进程一致。

## 10. 请求身份、幂等和 deadline

### 10.1 双层身份

必须区分：

- 逻辑请求身份：业务输入、逻辑 profile、route/pool revision、prompt/schema/parser 版本；
- 实际尝试身份：逻辑请求身份加实际 profile、来源标签、实际 model 和 attempt 序号。

业务输入哈希不能包含本次随机选择的实际来源，否则同一公告发生故障转移会被误判为不同业务输入。route 配置变化必须通过稳定 `route_fingerprint` 进入运行身份，防止成员集合或能力变化后错误复用旧结果。

### 10.2 幂等键

- 同一逻辑请求跨来源切换时沿用调用方提供的业务 `idempotency_key`；
- 实际 provider request ID 和本地 request ID 每次尝试独立记录；
- 网关不能宣称不同供应商或不同 Key 共享持久幂等语义；
- 本项目 LLM 调用只生成候选结果，故障转移不得直接触发生产事实副作用。

### 10.3 总时间预算

路由层必须确保：

- pool 排队时间受有限 queue timeout 约束；
- 第一次实际发送许可后启动逻辑执行 deadline；
- 同源重试、退避、后续成员准入、HTTP、解析和 repair 全部计入逻辑执行 deadline；
- 剩余时间不足以完成下一次最小 attempt 时停止切换；
- 取消会终止排队、当前调用和后续切换，并释放所有租约。

## 11. 响应和来源血缘

### 11.1 公共响应

`LlmResponse` 必须新增稳定字段：

```python
source_label: str
logical_profile: str
selected_profile: str
```

现有 `provider` 和 `model` 继续记录协议/供应商类型和服务端实际模型，不替代 `source_label`。

### 11.2 路由 lineage

成功和分类失败必须包含安全 lineage：

```json
{
  "pool": "shared_semantic",
  "logical_profile": "semantic_extraction",
  "selected_profile": "semantic_extraction__pipio_luna",
  "llm_source": "pipio:gpt-5.6-luna",
  "route_fingerprint": "...",
  "failover_count": 1,
  "attempts": [
    {
      "source_label": "pipio:grok-4.5",
      "selected_profile": "semantic_extraction__pipio_grok",
      "request_id": "...",
      "error_code": "rate_limit_error",
      "attempt_count": 2
    },
    {
      "source_label": "pipio:gpt-5.6-luna",
      "selected_profile": "semantic_extraction__pipio_luna",
      "request_id": "...",
      "status": "success",
      "attempt_count": 1
    }
  ]
}
```

lineage 不得包含 API Key、Authorization、Cookie、完整 prompt 或完整 raw response。

### 11.3 业务透传

- CNInfo 标题分类 lineage 必须保存实际来源；
- `CorporateActionAnalysis` 必须保存抽取来源；
- `_semantic_verifier` 必须单独保存复核来源，因为抽取和复核可能由不同模型完成；
- `corporate_action_llm_analyses` 必须持久化主要抽取来源和完整受控路由 lineage；
- 公司画像 `SemanticRunAudit`、抽取 envelope、运行 artifact 和发布审计必须保存实际来源；
- API、任务报告或审计查询返回 LLM 结果时必须包含来源标签；
- 模型输出的业务 JSON Schema 不增加来源字段，来源属于公共调用 envelope，不属于公告或公司事实。

## 12. 应用层透明边界

### 12.1 允许的应用依赖

应用模块可以：

- 构造 `LlmRequest` 并使用逻辑 profile；
- 调用 `LlmClientProtocol.complete()`；
- 读取 `LlmResponse` 的公共字段和来源 lineage；
- 查询逻辑 route 是否启用、逻辑有效限额和 route fingerprint；
- 根据业务 schema、证据和金融规则拒绝候选结果。

### 12.2 禁止的应用依赖

`data_manager`、`research`、`data_sources`、scheduler、API 和脚本不得：

- 直接访问 `llm_config.profiles` 判断逻辑能力是否启用；
- 读取某个实际 profile 的 model 作为逻辑运行身份；
- 自行选择 Grok 或 Luna；
- 直接读取 `api_key_env`、Base URL 或 provider resource；
- 实现另一套权重、熔断或故障转移；
- 根据具体模型名改变金融业务规则。

### 12.3 公共配置查询接口

公共模块应提供稳定接口，名称可在实现中结合现有风格确定，但至少表达：

```python
is_logical_profile_enabled(name)
describe_logical_profile(name)
route_fingerprint(name)
effective_route_limits(name)
```

`describe_logical_profile` 返回非敏感逻辑能力摘要，不返回 Key 值。无 route 的单 profile 应通过同一接口返回兼容描述。

### 12.4 兼容要求

- `LlmRequest.profile` 字段保留；
- 现有业务默认逻辑 profile 名称保留；
- 无 route 配置时单 profile 请求和测试 fixture 保持现有行为；
- fake/injected `LlmClientProtocol` 继续可用于离线业务测试；
- 公共响应新增字段应提供兼容默认值或同步更新所有构造 fixture；
- 配置改名不能改变应用配置发现方式；
- 不允许为了迁移路由而修改业务 schema 预期或降低证据门禁。

## 13. 可观测性

pool 快照至少报告：

- pool 名称、配置总并发、当前活动数、等待数和最老等待时间；
- 每个来源的配置权重、实际分发数、活动数、等待数和长期实际比例；
- 每个来源的成功、失败、429、5xx、timeout、parse/schema failure；
- circuit 状态、open 剩余时间、half-open 探测；
- failover 请求数、成功数、耗尽数和按错误类别统计；
- route queue wait、实际执行、切换和总耗时；
- 逻辑 profile、workload、run、stage 和 business item 的关联身份；
- provider resource 现有并发/RPM/cooldown 快照。

日志必须在关键节点记录：

```text
route admitted
source selected
source attempt failed
failover selected
route completed
route exhausted
circuit opened/half-open/recovered
```

所有日志继续执行密钥和正文脱敏。

## 14. 现有业务调用检查

实施不得只完成公共模块单元测试。必须盘点并验证仓库中所有现有 LLM 调用者。

### 14.1 CNInfo 公告标题分类

涉及：

```text
data_sources/cninfo_announcement_title_llm.py
data_manager.py 中标题分类构造和 enable 检查
```

必须验证：

- 继续使用逻辑 profile；
- 批量分块、隔离重试和乱序身份不变；
- 每个事件 lineage 含实际来源；
- 单成员失败可切换，不能导致同一事件串线；
- 标题业务 schema 和 applicability 校验不变。

### 14.2 CNInfo 公司行动正文抽取与语义复核

涉及：

```text
data_sources/cninfo_corporate_action_llm.py
data_sources/cninfo_corporate_action_pipeline.py
data_manager.py 中 resolver、恢复、持久化和自动 promotion
```

必须验证：

- 抽取与复核各自记录来源；
- 两阶段可能使用不同来源但仍关联同一 `source_event_key`；
- 输入哈希使用 route fingerprint，不依赖随机实际来源；
- resume 和已完成结果复用不因负载均衡产生重复分析；
- failover 后全部确定性证据门禁仍执行；
- 自动 promotion 不因来源变化放宽；
- 数据库迁移、旧记录读取和审计查询兼容。

### 14.3 公司画像通用语义生产

涉及：

```text
research/business_profile_semantic_extraction.py
research/business_profile_semantic_runtime.py
research/business_profile_production_rollout.py
research/business_profile_async_production.py
scripts/research_business_profile_semantic_production.py
```

必须验证：

- 普通原子活动/关系抽取、结构化表格抽取和独立复核均通过逻辑 profile；
- runtime identity 使用 route fingerprint；
- `SemanticRunAudit` 和 artifact 保存实际来源；
- checkpoint/resume、rework、promotion manifest 和 source revision 保持稳定；
- 网络 kill switch、scope gate、candidate-only 和发布门禁不变；
- 现有 structured fallback 不被模型故障转移误判为业务 structured-source fallback。

### 14.4 旧公司画像适配器

涉及：

```text
research/business_profile_llm.py
```

必须验证同步兼容入口、异步入口、注入 fake client、关闭生命周期和 envelope 来源字段。该适配器不得继续单独维护供应商配置或绕开 pool。

### 14.5 应用生命周期和开发验证

涉及：

```text
data_manager.py 的 application-owned LlmClient
main.py / api/app.py 的显式 .env 加载
scripts/dev_validation/validate_common_llm_gateway_live.py
scripts/dev_validation/benchmark_llm_orchestration.py
```

必须验证：

- 一个应用生命周期内共享 pool/coordinator；
- `close()` 后没有等待任务、租约、socket 或 HTTP client 泄漏；
- `.env` 使用 `override=False`，进程环境优先；
- live validation 可以明确指定逻辑 profile并报告实际来源；
- benchmark 能验证总并发和权重，而不输出密钥。

## 15. 测试要求

### 15.1 配置单元测试

- 两个 Pipio 环境变量名正确解析，不读取真实 `.env`；
- 同 Base URL、不同 Key 和 model 可形成两个实际 profile；
- 独立 quota 和共享 quota 两种 provider-resource 映射均可表达；
- 未知 pool、未知实际 profile、重复来源标签、缺失逻辑映射和非法权重 fail closed；
- route/实际 profile 同名歧义被拒绝；
- 无 route 的旧配置保持兼容；
- `13_llm.json` 可被配置管理器发现，旧路径引用全部清理。

### 15.2 调度和并发测试

- 多个逻辑 profile 合计不超过 pool 总并发；
- 3:1 权重在确定性批次中达到可解释比例；
- 小批次不会永久饿死低权重成员；
- `borrow_idle_capacity=true` 时健康成员可接管容量；
- provider/profile/pool 多层限制同时生效且不死锁；
- 排队取消、deadline 和异常释放全部租约；
- 多 client 实例共享同一进程级 pool 状态。

### 15.3 故障转移测试

- Grok 429 后 Luna 成功，最终来源为 Luna；
- Luna transport timeout 后 Grok 成功；
- parse/schema repair 耗尽后按配置切换；
- 配置错误、取消和耗尽 deadline 不切换；
- 认证错误遵守 `allow_auth_failover`；
- 同一逻辑请求不重复选择失败成员；
- 全部成员失败返回分类错误和完整安全 lineage；
- circuit open、half-open 探测和恢复可用 fake clock 测试；
- 故障转移不重复上报 provider 拥塞结果。

### 15.4 响应和审计测试

- 成功响应包含 `source_label`、logical/selected profile 和 route fingerprint；
- 抽取与复核来源分别保存；
- API Key、Authorization 和完整正文不进入日志、异常或 lineage；
- 同一业务输入跨不同实际来源保持逻辑输入哈希稳定；
- route 配置 revision 变化会改变 runtime identity；
- 旧数据库记录和缺少来源字段的历史 artifact 可读，并标记为 legacy/unknown，而非伪造来源。

### 15.5 业务回归测试

必须运行并按改动补充：

```text
tests/unit/test_utils/test_llm_gateway.py
tests/unit/test_utils/test_llm_orchestration.py
tests/unit/test_cninfo_corporate_action_llm.py
tests/unit/test_scheduler_cninfo_corporate_action_llm.py
tests/unit/test_research/test_business_profile_llm.py
tests/unit/test_research/test_business_profile_semantic_extraction.py
tests/unit/test_research/test_business_profile_semantic_runtime.py
tests/unit/test_research/test_business_profile_async_production.py
tests/unit/test_research/test_scheduler_business_profile_semantic_maintenance.py
```

还必须静态扫描仓库中的 `LlmClient`、`LlmClientProtocol`、`LlmRequest` 和 `llm_config.profiles` 调用，确认没有遗漏业务调用者或新的应用层实际 profile 依赖。

### 15.6 受控线上验证

真实模型验证必须人工启用，不能作为普通单元测试：

1. 分别以两个实际 Key 验证模型权限、实际 model、stream、usage 和 structured output；
2. 对同一合成非敏感中文文本分别运行单源 smoke；
3. 以逻辑 profile 运行小批量加权分流；
4. 在测试环境模拟或临时禁用一个成员，验证故障转移；
5. 分级验证 10、25、50 总并发；
6. 记录成功率、429/5xx、首字/总耗时、权重实际比例、failover、内存、连接数和关闭耗时；
7. 未通过低并发等级不得进入更高等级；
8. 线上结果仍只作为候选，不跳过业务质量 holdout 和 promotion gate。

## 16. 数据和兼容迁移

- 配置文件从 `11_llm.json` 重命名为 `13_llm.json`；
- 现有 `QUOTE_LLM_API_KEY` 迁移为两个显式环境变量，不在代码中读取或复制旧值；
- 部署时先注入新变量，再切换 `api_key_env`，验证后删除旧变量；
- 业务逻辑 profile 名称保留，实际 profile 使用带来源后缀的新名称；
- 应用直接访问 `llm_config.profiles` 的位置迁移到公共逻辑查询接口；
- 响应、业务 envelope、数据库和 artifact 增加来源字段；
- 历史记录缺少来源时保留 `null` 或明确 `legacy_unknown`，不得根据 model 字符串猜测；
- 数据库迁移必须可重复、可回滚，并不得覆盖已有分析结果；
- 回滚时可关闭 route/pool 并恢复单实际 profile，但已保存来源 lineage 必须保留。

## 17. 验收标准

全部满足才可认为实施完成：

1. 业务调用仍只使用逻辑 profile；
2. 应用业务模块不再直接访问 `llm_config.profiles`；
3. 两个 Pipio 模型可在同一 Base URL 下使用各自环境变量 Key；
4. pool 总并发和成员权重可配置且通过确定性测试；
5. 任一成员的允许故障可切换到另一成员；
6. 全部失败时 fail closed；
7. 所有成功、失败、数据库审计和业务报告都可识别实际来源；
8. provider resource 的并发/RPM/自适应控制未被绕过；
9. CNInfo 标题、正文、复核和公司画像全部通过兼容性检查；
10. prompt、业务 schema、证据门禁和 candidate-only 行为没有回归；
11. 配置文件已迁移为 `config/13_llm.json` 且所有路径引用更新；
12. 离线测试不依赖互联网或真实 Key；
13. 受控 live smoke 披露实际来源、权重和 failover 结果；
14. client、transport、queue、coordinator 和 circuit 状态在关闭后无泄漏。

## 18. 实施顺序

1. 增加配置模型、逻辑 profile 查询和 source label，不启用路由；
2. 增加 pool coordinator、加权公平调度和快照；
3. 增加 route-aware `LlmClient.complete()` 和双层请求身份；
4. 增加故障转移、成员健康状态和 circuit breaker；
5. 扩展公共响应、错误 lineage 和配置文档；
6. 迁移应用层所有直接 profile 访问；
7. 迁移 CNInfo 标题、正文、复核和持久化来源；
8. 迁移公司画像语义生产、runtime identity 和审计来源；
9. 重命名 LLM 配置文件并完成 Key 名称迁移说明；
10. 运行公共模块、全部现有 LLM 业务回归、静态调用扫描和 OpenSpec 校验；
11. 通过受控单源 smoke 后启用双源低并发；
12. 按 10、25、50 并发逐级放量，并保留单源配置回滚路径。

## 19. 风险与限制

- Grok 与 Luna 对同一 schema 的质量可能不同，结构合法不代表金融语义等价；
- 故障转移会增加单请求成本和尾延迟，必须使用总 deadline；
- 同 Base URL 可能形成共同故障域，双模型不能替代跨供应商容灾；
- 两个 Key 的 quota 归属若判断错误，可能造成超限或不必要限速；
- 权重是调度目标，受响应时长、成员上限、熔断和业务 worker 限制后，瞬时在途比例不一定等于权重；
- 当前协调器只保证单进程范围，多 worker/多主机部署仍需另行设计分布式协调；
- 来源字段扩展涉及历史数据库和 artifact 兼容，不能通过删除旧数据简化；
- 任何模型切换均不得被解释为投资结论质量提升，必须继续使用业务 holdout、证据门禁和人工复核。
