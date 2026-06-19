# 系统日志分流需求

## 背景

当前系统主要通过 `log/sys.log` 记录运行日志。调度任务、Telegram 手工任务、数据源下载、数据库写入、API 访问流水和服务生命周期事件混在同一个文件中。随着 A 股、港股、财务、估值、期货等任务增多，`sys.log` 体积快速膨胀，排查单个问题时需要在大量无关日志中筛选。

近期 API 访问量和日更任务并行运行时，日志混杂带来两个具体问题：

- 任务排查时，API 扫描、客户端请求和爬虫访问流水会干扰任务链路分析。
- API 访问审计时，任务内部数据源请求、调度器状态和数据写入日志会干扰访问统计。

因此，日志体系需要从单一 `sys.log` 调整为职责明确的三类日志文件。

## 目标

日志文件分为：

| 文件 | 定位 | 主要内容 |
|---|---|---|
| `log/sys.log` | 系统生命周期日志 | 进程启动/停止、配置加载、日志系统初始化、服务启动失败、数据库连接池初始化、调度器/API/Telegram 生命周期、未捕获异常、跨模块基础设施问题 |
| `log/task.log` | 任务执行日志 | scheduler 自动任务、Telegram `/run` 手工任务、数据同步、行情回补、主数据治理、财务/估值/研究任务、任务内数据源下载、任务内数据库写入摘要、任务报告生成 |
| `log/access.log` | API 访问流水 | 客户端 HTTP 请求、method、path/query、client IP、user-agent、状态码、耗时、request id、限流/准入结果 |

核心原则：

- `sys.log` 不是全集日志，不应继续无差别收集所有任务和访问流水。
- `task.log` 是任务排查主入口，任务执行过程中的 INFO/WARNING/ERROR 应集中落入该文件。
- `access.log` 是客户/API 访问审计主入口，不记录任务内部数据源下载和数据库处理细节。
- 三类日志可以共享格式、级别和轮转策略，但路由边界必须清晰。

## 非目标

- 不迁移或删除历史 `sys.log`、`sys.log.N` 文件。
- 不改变业务任务、API 响应、数据库 schema 或调度时间。
- 不引入外部日志系统、ELK、OpenTelemetry 或新依赖。
- 不在本阶段实现结构化 JSON 日志；保留当前文本格式。
- 不把第三方库内部日志完全重分类；只治理项目自有 logger 和 middleware 输出。

## 日志边界

### `sys.log`

`sys.log` 只记录系统运行状态和基础设施问题，包括：

- 日志系统初始化和配置读取结果。
- 进程启动、关闭、信号处理和主入口异常。
- API 服务、Scheduler、Telegram Bot 的启动/停止生命周期。
- 数据库连接池初始化、连接池配置错误、全局资源初始化失败。
- 未被任务上下文或访问上下文捕获的异常。
- 跨模块基础设施告警，例如配置缺失、运行目录异常、服务端口占用。

`sys.log` 不应包含：

- 每只股票、指数或期货的行情下载流水。
- 每个任务的逐步执行日志。
- 每个 HTTP API 请求和响应。

### `task.log`

`task.log` 记录任务执行链路，包括：

- APScheduler 自动触发任务。
- Telegram `/run`、`/backfill`、`/find_gap_and_repair` 等手工触发任务。
- `main.py run job` 或本地 CLI 触发的同类任务。
- 任务内部的 DataManager、DataSource、Database 写入摘要和异常。
- 任务报告生成和 Telegram 发送结果。
- 任务相关的官方源、BaoStock、AkShare、CNIndex、CSIndex、HKEX 等数据源请求诊断。

`task.log` 应足够支持回答：

- 任务是否启动、是否完成、耗时多久。
- 哪些阶段耗时较长。
- 哪些数据源返回空、超时、降级或 fallback。
- 哪些标的写入、跳过、失败或进入待复核。

### `access.log`

`access.log` 记录 HTTP 客户端访问流水，包括：

- 请求开始和请求完成。
- `method`、`path`、`query` 或完整 URL。
- client IP。
- user-agent。
- HTTP status code。
- 请求耗时。
- request id。
- 限流、排队、拒绝和异常响应。

`access.log` 不应记录：

- API 路由内部的业务计算细节。
- 数据库 SQL 细节。
- 任务执行日志。
- 数据源下载日志。

## 配置要求

日志配置应保持向后兼容：

- 现有 `logging_config.file_config.filename` 可以继续作为旧字段读取。
- 新配置应支持显式声明：
  - `system_filename`: 默认 `sys.log`
  - `task_filename`: 默认 `task.log`
  - `access_filename`: 默认 `access.log`
- 三类文件默认共用当前 rotation 配置：按大小轮转、`max_bytes_mb`、`backup_count`。
- 如果新字段缺失，系统应使用默认文件名，而不是退回单文件全集日志。

## 实现约束

- 使用项目现有 `utils.logging_manager.LoggingManager` 扩展，不新增重量级日志依赖。
- API middleware 的访问流水必须使用独立 access logger，并关闭 propagation，避免写入 `sys.log` 或 `task.log`。
- 任务相关 logger 应通过 logger 名称或显式 handler 路由写入 `task.log`。
- `sys.log` 应保留系统生命周期 logger，不作为 root logger 的全集 dump。
- Uvicorn/FastAPI 启动日志如果可控，应归入系统生命周期；客户请求流水必须进入 `access.log`。
- 测试环境应避免污染生产日志，可继续使用测试临时目录或 mock handler。

## 验收标准

- 启动系统后会创建或可写入 `log/sys.log`、`log/task.log`、`log/access.log`。
- 一次 API 请求只在 `access.log` 产生访问流水，不在 `task.log` 产生同一条访问流水。
- 一次手工 `/run daily_data_update` 或 scheduler 任务在 `task.log` 能看到任务启动、阶段执行、完成/失败信息。
- `sys.log` 能看到系统启动、配置、服务生命周期信息，但不会持续写入大量逐标的行情下载流水。
- 旧文档、脚本和排障命令中的 `log/sys.log` 引用被更新为对应的新日志入口。
- 单元测试覆盖日志 handler 配置、access logger 不传播、任务 logger 路由和旧配置兼容。
