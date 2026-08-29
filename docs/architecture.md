# Quote 当前架构

> 文档类型：current
>
> 本文描述当前代码和配置中的稳定事实。框架迁移目标、依赖顺序和未完成事项以
> [框架改造总纲](development/framework_refactoring_program.md) 为准；目标结构不等于已经完成的实现。

## 1. 系统定位

Quote 是本地优先的量化研究模块化单体，负责行情、证券主数据、财务、估值、行业、股东、公司行为、期货、外汇、特殊商品和研究报告等数据的采集、维护、查询与分析。

系统主要通过 Python 3.10+ 运行，使用 FastAPI 提供 API，APScheduler 执行配置化任务，SQLite 按数据域隔离存储。数据源、网络请求、调度和本地数据库均属于运行时依赖。当前仍有少量历史生产路径反向依赖 `scripts/dev_validation`，该依赖已登记为 W3/W8 的清理目标；在清理完成前不得新增此类依赖。

## 2. 当前入口

```text
CLI (main.py)          API (api/app.py)          Scheduler (scheduler/scheduler.py)
       |                       |                              |
       +-----------------------+------------------------------+
                               v
                   当前应用/兼容门面与域服务
                               |
              +----------------+----------------+
              v                                 v
       数据库/Repository                    Provider/HTTP
```

当前入口包括：

- `main.py`：命令行解析、轻量初始化、API/调度器/完整系统启动，以及历史下载、更新、缺口和任务命令；
- `api/app.py`：FastAPI 应用生命周期、中间件和路由装配；
- `api/routes.py`、`api/announcement_asset_routes.py`：当前 API 路由入口；前者仍是混合路由文件，后者承载共享公告资产端点；
- `scheduler/scheduler.py`：APScheduler 初始化、配置解析、job 注册、依赖执行和运行状态；
- `scheduler/tasks.py`：当前任务适配和部分历史业务编排，处于按域拆分前的兼容阶段；
- `utils/task_manager/`：Telegram 运维入口和人工任务触发；
- `scripts/`：人工 operator、回填、迁移和开发验证工具，不能被生产模块反向 import。

同一业务动作可以有多个入口，但必须汇聚到一个权威应用服务和一个写入 owner。兼容门面只能做参数归一化和委托，不能新增业务循环。

## 3. 当前核心模块

| 模块 | 当前职责 | 迁移状态 |
|---|---|---|
| `data_manager.py` | 行情下载/更新、缺口维护、主数据和部分研究/公司行为编排 | 受控收敛；新业务不得继续堆入，按 W2-W6 迁移 |
| `database/operations.py` | `quotes.db` 的主数据、行情、交易日历和写入协调 | W3 先登记 quote-storage owner，再决定拆分边界 |
| `research/storage.py` | 研究数据库连接、事务、scope、建表/迁移和跨域存储门面 | W5 按数据库/表 owner 拆 repository |
| `research/` | 研究查询、同步、财务、估值、行业、股东、公司画像和市场数据服务 | 已有域服务逐步成为应用服务 owner |
| `scheduler/tasks.py` | 配置任务的参数适配、执行和报告衔接 | W7 按域拆 task adapter；模块级函数也纳入迁移 |
| `api/routes.py` | HTTP 参数处理、响应映射及部分历史业务调用 | W9 按路由族拆分，最终只保留装配/兼容职责 |
| `data_sources/`、`research/providers/` | 外部来源协议、请求和源数据解析 | 保持 source-specific，不在入口复制 provider 逻辑 |

## 4. 数据存储边界

生产数据按现有配置和代码使用的数据库文件隔离。常见存储包括：

- `data/quotes.db`：证券主数据、行情、交易日历和行情维护状态；
- `data/research.db`：研究域通用记录、行业、股东、报告和画像相关研究数据；
- `data/financials.db`：财务事实、来源清单、映射和可得日信息；
- `data/valuation.db`：估值输入、历史和模型相关记录；
- `data/interests.db`：关注标的及相关研究记录；
- `data/fx.db`、商品/期货等域数据库：按对应配置和域模块管理。

数据库路径、表名、canonical key、连接 scope、WAL/超时和事务语义属于兼容约束。框架重构不得合并数据库、改变目录或在迁移期引入第二个写入实现。

## 5. 业务执行链

行情维护的目标单链为：

```text
入口参数
  -> canonical instrument/universe
  -> 权威交易日历
  -> source routing 与限流
  -> 规范化与保存验证
  -> 单一写入 owner
  -> 结构化结果/报告
```

研究和公司行为按各自应用服务、repository 和 provider port 组合。Provider 只负责来源协议和解析；应用服务负责跨阶段编排；repository 负责所属数据库/表的持久化。

公司画像当前仍在开发和验收过程中。画像写入、发布和回放链路保持现有 owner；W4 的画像切片必须等相关 change 完成归档、生产验收通过且无同域工作区改动后再启动。

## 6. 运行与验证约束

- API、CLI、Scheduler、Telegram 和 operator 工具必须保持现有公共契约，除非有独立变更；
- 调度 job id、启用状态、触发器、依赖、并发和通知配置不得因重构改变；
- 行情、财务和公司行为必须保持交易日、实际可得日、生命周期和复权语义；
- 重构前后使用临时数据库、冻结 fixture 和 no-write 解析验证，不在生产数据库试验；
- 生产切换前确认受影响 job 空闲且只有一个 writer，首次自然运行后核对 watermark、业务 key、报告和错误；
- 新增共享抽象必须有至少两个当前真实调用方，并在同一 change 中删除重复实现；
- 旧门面新增代码必须登记移动目标例外、替代 owner 和最晚清理工作流。

## 7. 权威文档

- 开发和长期治理：[project_development_governance.md](development/project_development_governance.md)；
- 框架改造计划：[framework_refactoring_program.md](development/framework_refactoring_program.md)；
- 开发入口：[development/README.md](development/README.md)；
- API 参考：[api/restful_api.md](api/restful_api.md)；
- 调度运行手册：[features/scheduler_system.md](features/scheduler_system.md)；
- 完整文档索引：[README.md](README.md)。
