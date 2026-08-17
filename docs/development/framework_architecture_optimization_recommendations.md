# Quote 项目框架审计复核与架构优化建议

> 复核日期：2026-08-17
>
> 复核对象：`docs/development/framework_audit_and_improvement_plan.md` 及当前工作区代码、配置、测试和 OpenSpec 状态
>
> 文档性质：架构决策建议，不包含业务代码改动
>
> 目标：完善模块边界、整合功能、提高复用、清除已确认废弃代码，并确保同一业务能力只有一条权威执行链

## 0. 结论

原审计的核心方向基本正确，但严重度和若干技术结论需要修正。Quote 目前不是“框架已经失效”，而是一个持续扩展的模块化单体积累了明显的应用层和演进治理债务：底层数据能力大多可用，域服务也已经存在，但入口、编排、兼容门面和阶段性工具没有随功能完成而及时收口。

最需要优先处理的不是文件数量，而是以下五个问题：

1. `DataManager` 和 `ScheduledTasks` 同时承担门面、装配、业务编排和报告逻辑，修改半径过大。
2. 缺口修复存在多套真正不同的检测、过滤和写入实现，Telegram 还能绕过调度器直接启动脚本。
3. 生产模块反向导入 `scripts/dev_validation`，依赖方向已经倒置。
4. 证券代码的 canonical 格式没有形成唯一契约，现有公共转换器与实际 `.BJ`、`.HK` 存储格式存在不一致。
5. 架构文档、任务分类和 OpenSpec 生命周期没有跟上系统演进，导致新功能容易沿旧地图继续复制路径。

以下判断不应按原审计直接实施：

- 不应把同步 SQLite、异步 SQLAlchemy、公告资产 repository 的存在本身视为错误，更不应合并数据库或统一成一个 ORM。
- 不应强制 `aiohttp`、`requests`、`httpx` 使用同一个客户端；它们服务不同并发和协议边界，只需共享代理、TLS、超时等配置语义。
- 不应统一股票、期货、外汇和发布日历的业务语义；应统一接口形状，保留各域规则。
- 不应把所有 `_parse_date`、`RateLimiter`、`StorageManager` 或文件数量相似项直接判定为重复代码。
- CNInfo 和业务画像已经使用 `utils.llm` 公共网关，不能按“LLM 未复用”重新改造。
- `interests.db` 会被 `database_backup_config.include_globs = ["data/*.db"]` 纳入统一备份，不能认定为漏备份。
- `logging.getLogger(__name__)` 与根日志配置兼容，不是绕过统一日志系统的证据。

因此，本项目应采用“先收口业务主链，再拆大门面，最后抽取经验证的共享原语”的渐进式优化，不进行全仓重写。

## 1. 复核依据与边界

### 1.1 当前规模事实

| 对象 | 当前事实 | 说明 |
|---|---:|---|
| `data_manager.py` | 38,439 行，`DataManager` 433 个直接方法 | 117 个方法不超过 20 行，但仍有 72 个方法超过 100 行，既是门面也包含大量业务逻辑 |
| `scheduler/tasks.py` | 13,414 行，`ScheduledTasks` 111 个方法 | 20 个方法超过 100 行，不只是薄调度包装 |
| `research/storage.py` | 14,190 行 | 多研究子域共享的同步存储实现 |
| `database/operations.py` | 9,716 行 | 行情库异步数据访问入口 |
| `api/routes.py` | 4,827 行 | 主路由仍集中，但公告资产已开始拆出独立路由 |
| `research/` | 178 个 Python 文件 | 46 个文件名以 `business_profile_` 开头 |
| Scheduler 配置 | 93 个 job 定义 | 64 个启用，40 个 `manual_only`，实际自动启用且非手工的为 29 个 |
| OpenSpec | 89 个未归档 change | 83 个状态为 complete，6 个仍 in-progress |
| `implementation_plan.md` | 210,637 字节 | 已更接近演进记录，不适合作为当前架构入口 |

这些数字用于定位热点，不能单独证明设计错误或代码垃圾。判断是否重复必须继续核对输入、输出、时间语义、状态所有权和调用方。

### 1.2 复核限制

本次以静态代码、配置、测试和 OpenSpec 工件为依据，没有对所有 29 个自动任务做生产运行验证，也没有执行真实网络和数据库写入。因此，文档将“已确认的结构问题”和“实施前仍需做调用链核对的候选项”分开表达。

当前 `establish-shared-announcement-asset-management` 仍有 88/91 个任务完成，且相关代码存在未提交改动。公告资产和业务画像的删除、合并、迁移应等该 change 完成并归档后再进行，不能与当前在途改动交叉重构。

## 2. 对原问题清单的逐项复核

### 2.1 框架问题

| ID | 复核结论 | 依据与处理建议 |
|---|---|---|
| A1 `DataManager` 神对象 | **属实，高优先级** | 它不是“唯一应用层”，因为 `research.query_service`、各类 sync/read service 已存在；但 72 个超 100 行方法证明它仍持有大量领域编排。应逐域迁出业务逻辑，保留限期兼容门面。 |
| A2 双存储底座 | **现象属实，问题定义不准确** | 行情异步 ORM 与研究同步 SQLite 是有意隔离。真正问题是连接策略、迁移、备份纳管和只读约束不一致。先定义存储生命周期合同，再决定是否抽小型 helper，不先造通用 `SqliteStore`。 |
| A3 调度器膨胀 | **属实，中高优先级** | 93 是定义数，不是 93 个自动生产任务；实际自动非手工为 29。问题是任务类型、业务执行和报告格式集中在一个 13K 行类中，应按域拆 handler，并从配置生成任务目录。 |
| A4 纵向切片未回收原语 | **部分属实** | 公共 LLM、HTTP transport、调度依赖 DAG、统一备份等已经完成复用；但大量 completed OpenSpec 未归档，阶段性脚本和兼容路径没有系统退出。重点应是生命周期收尾，不是继续建设共享平台。 |
| A5 生产代码依赖 `scripts/dev_validation` | **属实，最高优先级** | `data_manager.py`、财务增量同步和财务维护直接 import 校验脚本。应把仍属生产合同的函数迁入 `research/`，脚本只作 CLI 适配器；建立禁止生产包 import `scripts` 的架构测试。 |

### 2.2 多入口与多实现

| ID | 复核结论 | 依据与处理建议 |
|---|---|---|
| B1 缺口检测/修复多链 | **属实，最高优先级** | `DataManager`、scheduler、`smart_fill_gaps.py`、`find_gap_and_repair.py` 的算法、universe、跳过策略和因子同步不完全一致，Telegram 直接 subprocess 启动脚本。必须收口成一个写入服务。 |
| B2 日更/补数多链 | **部分属实** | CLI、API、scheduler 大多已经汇到 `DataManager.update_daily_data`；Telegram `/backfill` 也调用 scheduler 任务。历史下载、目标日补数、区间回补本来就有不同语义。应统一命令合同和参数模型，而不是强行合并为一个巨型函数。 |
| B3 复权与公司行为多链 | **阶段多，不能等同重复** | observation、resolution、canonical selection、promotion、factor rebuild 是数据生命周期阶段。真实缺口是状态流不易读，且 `DataManager` 持有多个千行方法。应先定义状态机和唯一权威表，再按阶段迁出。 |
| B4 财务同步重叠 | **需逐对象核对** | summary、statement、disclosure、catch-up、reconciliation 处理的对象和时点不同。命名确实混乱，但不能仅凭 job 名合并。先产出“对象 × 模式 × 写表 × 水位”矩阵，再删除真正同义路径。 |
| B5 各研究域同步族 | **模式相似，不等于实现重复** | incremental、reconcile、full/backfill 是合理的运维模式。可统一报告字段与触发参数，但不要建设跨域通用 ingestion engine。 |
| B6 备份出口 | **部分属实** | `DatabaseBackupService` 是现行统一入口，旧 `backup_database`/`DataManager.backup_data` 仍存在兼容路径，应确认无调用后退役。`interests.db` 已由 `data/*.db` glob 覆盖，原审计的漏备份判断不成立。 |
| B7 公告获取四层 | **大部分是合理分层** | provider、通用 acquisition、资产生命周期、公司行为消费是不同职责。当前公告资产 OpenSpec 也明确要求复用现有 provider 边界。优化重点是完成消费者切换后删除 legacy writer，不是合并这些层。 |
| B8 主数据隐式刷新 | **行为属实，是否拆分取决于合同** | 日更前强制刷新主数据是正确的业务前置条件。应将其声明为日更命令的显式子步骤并进入报告；只有需要独立失败/重试时才拆成依赖 job。 |

### 2.3 复用问题

| ID | 复核结论 | 依据与处理建议 |
|---|---|---|
| C1 证券代码规范化 | **属实，最高优先级** | `utils/code_utils.py` 把 BSE/HKEX 映射成 `.BSE`/`.HKEX`，而实际存储和测试广泛使用 `.BJ`/`.HK`；`BaseDataSource` 又有另一套 `.BJ`/`.HK` 映射。应建立唯一标识值对象和边界转换。 |
| C2 HTTP 客户端三套 | **技术事实，不是缺陷本身** | 行情异步、研究同步、LLM transport 的运行模型不同。继续保留客户端差异，只统一代理、TLS、CA、超时、重试错误分类等配置合同。 |
| C3 限流三套 | **部分属实** | 数据源、API、LLM 的限流对象不同，不应合并算法。可统一命名和配置结构，尤其避免两个无关的 `RateLimiter` 造成误导。 |
| C4 交易日判断分叉 | **属实，高优先级** | `DateUtils.is_trading_day` 仍可回退到启发式日历，scheduler 也保留 fallback。任何写入/缺口判断必须使用权威交易日历；启发式只能用于展示或明确的 degraded 模式。 |
| C5 LLM 网关未复用 | **不属实** | 两个 CNInfo LLM 模块与业务画像均已 import `utils.llm`。大文件主要包含业务决议编排，不应误搬进公共网关。只需继续禁止直接发模型 HTTP 请求。 |
| C6 StorageManager 复制 | **部分属实** | futures/fx/special commodity 的连接和 WAL 初始化相似，但 schema、迁移和领域操作差异大。先提炼连接策略函数或 protocol；只有真实重复代码能被删除时才抽基类。 |
| C7 Telegram 脚本旁路 | **属实，最高优先级** | `_run_script` 直接执行三类脚本，绕过任务互斥、工作负载上下文和统一参数校验。Telegram 应只调用 command service 或 scheduler command。 |
| C8 日期解析重复 | **结论过度** | report period、available date、aware timestamp、交易日和公告时间不是同一种日期。应复用严格的基础解析器，但保留带业务语义的边界函数，禁止“万能日期解析器”。 |
| C9 日志未收口 | **证据不足** | `logging.getLogger(__name__)` 会继承根 handler，是推荐用法。真正需要检查的是脚本内 `basicConfig`、日志上下文字段和任务关联 ID，而不是替换所有 logger 获取方式。 |
| C10 代理安装器重复 | **属实但有启动约束** | bootstrap 必须在导入项目包前运行，runtime 还支持 yfinance 和请求 fallback。可以共享纯配置/安装算法，但必须保留轻量早期入口，不能简单让 bootstrap import 完整 runtime。 |

### 2.4 冗余与文档

| ID | 复核结论 | 依据与处理建议 |
|---|---|---|
| D1 画像模块过多 | **复杂度属实，垃圾结论未证实** | 文件按 acquisition、artifact、semantic、review、rollout 等职责拆分，其中很多是稳定业务阶段。先生成生产调用图并标状态，只有无调用、无配置、无恢复价值的文件才删除。 |
| D2 公司行为按 job 切文件 | **部分属实** | 更严重的问题其实是大量逻辑仍留在 `DataManager`。先迁出领域逻辑并形成状态流，再根据内聚性合并模块；不要先按文件名做机械合并。 |
| D3 `dev_validation` 堆积 | **属实** | 72 个文件混合了生产依赖、现场探针、迁移和历史验收。先解除生产 import，再把仍需人工运行的工具移到明确的 `scripts/research_ops` 或同类目录；已无用途的直接删除，不长期建立 `legacy/` 垃圾场。 |
| D4 仓库卫生 | **部分属实** | 根目录 6 个 `test_*.py` 是未被 `pytest.ini` 收集的现场脚本，其中还有 import 时联网/读库行为，应删除或改名迁入明确运维目录。多套 OpenSpec skill 是不同 AI 工具的集成文件，不是生产重复代码，不能无依据删除。 |
| E1 架构文档过期 | **属实，高优先级** | `docs/architecture.md` 仍写 8 个任务、不存在的 `utils/gap_manager/`，且重复列出 YFinance；开发指南仍写 Python 3.8、25K 行/77 文件。应重写为当前模块化单体地图。 |
| E2 演进文档过长 | **属实** | `implementation_plan.md` 已超过 210 KB。应保留为历史记录，当前架构、运行手册和未完成问题分别进入短文档，避免继续把流水账当系统规范。 |

## 3. 目标架构

### 3.1 架构形态：边界清晰的模块化单体

本项目继续保持单进程、本地优先和多 SQLite 数据库，不拆微服务、不引入消息队列，也不建设通用数据平台。目标调用关系如下：

```text
CLI / FastAPI / Scheduler / Telegram / operator scripts
                         |
                         v
              application command/query
        （一个业务动作只有一个权威执行合同）
                         |
            +------------+------------+
            |                         |
            v                         v
      domain service             read service
  （规则、阶段、状态流）       （稳定只读投影）
            |                         |
            +------------+------------+
                         v
             repository / provider ports
                         |
                         v
     SQLite / SQLAlchemy / HTTP source / filesystem / LLM
```

这里的“唯一”指唯一业务语义，不是唯一外部入口，也不是要求所有模式塞进一个函数：

- CLI、API、scheduler、Telegram 可以同时存在。
- 它们必须构造同一种 command/query，并调用同一个应用服务。
- daily、target-date、range、backfill 可以是不同 command，也可以是显式 `mode`，但各自只能有一个写入 owner。
- dry-run、reconcile、full rebuild 若有不同副作用，必须在合同中明确，不能靠 job 名暗示。

### 3.2 模块职责

| 层/角色 | 可以做 | 不可以做 |
|---|---|---|
| 外部适配器 | 参数解析、鉴权、任务提交、结果格式化 | 直接拼写业务 SQL、直接调用 provider 写库、复制业务循环 |
| 应用服务 | 编排一个业务用例、事务/幂等边界、调用域服务和 repository | 包含 Telegram/FastAPI 特有格式；依赖 `scripts` |
| 域服务 | 金融规则、状态机、权威选择、时间语义 | 读取 CLI 参数、直接初始化全局应用 |
| Repository | 表结构、查询、事务、迁移 | 决定业务模式和调度策略 |
| Provider | 上游协议、源字段映射、源错误语义 | 决定 canonical 写入或跨域业务状态 |
| 共享基础设施 | 已证明相同的配置、连接策略、错误类型、传输能力 | 建设万能 ingestion/store/provider 框架 |

### 3.3 `DataManager` 的目标

短期内保留 `DataManager` 以维持 API 和脚本兼容，但它只能逐步收敛为 composition facade：

- 初始化并持有应用服务；
- 对旧调用方做参数兼容和转发；
- 不再新增超过简单转发范围的领域逻辑；
- 每迁完一个域，新入口直接依赖对应应用服务；
- 兼容方法必须标明替代入口和删除条件，不能永久保留双路径。

第一批不应搬公告资产。该域仍有在途 change 和未提交改动。更稳妥的顺序是：缺口修复、生产脚本依赖、研究只读查询、财务编排，最后再处理公告资产和画像消费者收尾。

### 3.4 Scheduler 的目标

Scheduler 只负责触发、并发约束、依赖、重试和报告发送。具体业务执行交给应用服务。

建议把当前概念拆成三部分：

1. `JobCatalog`：从 `config/05_scheduler.json` 读取任务元数据，区分 `scheduled`、`manual`、`migration`、`deprecated_alias`。
2. 域 task handler：把 job 参数转换为应用 command，调用服务，返回结构化结果。
3. Report formatter：把结构化结果转换为 Telegram/日志报告，不参与写库。

不需要新增复杂注册中心。现有配置和 Python 映射足够，只需按域拆文件并保持 job id 兼容。

## 4. 应先确立的权威路径

| 业务能力 | 当前建议 owner | 要收口的旁路 |
|---|---|---|
| A/HK/US 行情日更 | 当前 `DataManager.update_daily_data`，迁移后为行情更新应用服务 | 入口自行刷新、下载和保存的循环 |
| 指定日/区间回补 | 与日更共享保存、主数据和源路由原语，但使用独立显式 command | 名称不同但语义相同的 backfill 包装 |
| 行情缺口修复 | 新的单一 Gap Repair 应用服务 | 两个脚本内联实现、scheduler 内联修复循环、Telegram subprocess |
| 股票交易日判断 | `quotes.db` 权威交易日历，经统一端口读取 | `holidays` 启发式参与写入决策 |
| 股票主数据 | 现有 instrument master governance 入口 | 各域自行刷新 active universe |
| 证券标识 | 一个 `InstrumentKey(symbol, exchange)` 语义模型及明确 renderer | 各域 suffix map、仅 `upper()` 的伪规范化 |
| 研究只读查询 | `ResearchQueryService` 及各域 read service | API/`DataManager` 中新增拼 SQL 或业务投影 |
| 公告传输 | `research.announcements` provider/acquisition 边界 | 消费域新增 CNInfo/交易所下载器 |
| 公告资产生命周期 | `research.announcement_assets` | 消费方自己的重复归档 writer；须等当前 cutover 完成后删除 |
| LLM 调用 | `utils.llm` | 业务模块直接使用 `httpx`/OpenAI SDK 发请求 |
| 数据库备份 | `DatabaseBackupService` | 旧 `DataManager.backup_data` 和单库备份入口在无调用后退役 |

复权、财务、画像暂不在本表指定单个函数。它们需要先完成“状态/数据对象 × 模式 × 权威表 × 水位 × 允许触发器”清单，否则仓促指定入口会把不同业务阶段错误合并。

## 5. 共享代码原则

### 5.1 该共享什么

满足以下条件才抽共享代码：

1. 至少两个当前生产调用方的语义真正相同；
2. 输入、输出、异常和生命周期一致；
3. 抽取后能删除实际重复代码；
4. 不要求调用方暴露大量无关参数；
5. 有覆盖所有调用方的合同测试。

当前适合共享的候选包括：

- 证券标识解析与格式渲染；
- SQLite 连接 PRAGMA、busy timeout、只读打开等纯基础策略；
- HTTP 代理/TLS/CA/timeout 配置模型；
- 应用命令的标准执行结果，如 status、counts、warnings、errors、watermark；
- 股票交易日历读取端口。

### 5.2 不该共享什么

- 股票交易日与期货交易日、外汇观察日、宏观发布日期的具体规则；
- API 限流、源端 pacing、LLM RPM 的算法；
- 财务报告期、公告时间、可得日和普通日期字符串的业务校验；
- 不同数据库的 schema/migration 业务逻辑；
- 仅因类名都叫 `StorageManager` 就创建继承体系。

### 5.3 证券标识的推荐模型

不要继续把 `.SZSE`、`.SZ`、`.XSHE` 之间的关系当作字符串替换。内部应先得到结构化身份：

```text
InstrumentKey(symbol="000001", exchange="SZSE")
    -> storage_id: 000001.SZ
    -> exchange_id: 000001.SZSE
    -> vendor aliases: 000001.XSHE / sz000001 / ...
```

数据库主键格式仍保持现状，不做全库迁移。所有别名只在边界解析，进入业务层后使用 canonical `InstrumentKey` 或 canonical storage id。BSE、HKEX、指数 metadata-only key 和美股 ticker 必须有表驱动测试。

## 6. 垃圾代码和兼容路径的清理规则

用户要求“不留垃圾”是正确目标，但必须通过可证明的删除流程实现，而不是按文件名或行数删除。

每个候选文件/入口只能处于一种状态：

| 状态 | 含义 | 处理 |
|---|---|---|
| `production` | 自动或稳定人工生产路径 | 保留并明确 owner |
| `operator` | 仍需要的人工运维工具 | 放入明确运维目录，调用应用服务，不复制逻辑 |
| `migration` | 一次性迁移尚未完成 | 绑定 change 和删除条件，完成后删除 |
| `compatibility` | 临时旧接口 | 标替代入口、调用方和移除版本 |
| `obsolete` | 无调用、无恢复用途、已有替代 | 直接删除，Git 历史就是归档 |

删除前至少证明：

1. 不在生产 import 图中；
2. 不在 scheduler/config/API/Telegram/文档命令中；
3. 不承担尚未结束的迁移或回滚；
4. 替代路径通过等价或业务验收测试；
5. 删除后相关测试和静态边界检查通过。

不要长期建立 `legacy/` 目录。真正废弃的代码应删除；需要保留的运维工具应有清晰名称、owner 和运行说明。

首批明确清理候选：

- 根目录 `test_akshare.py`、`test_db.py`、`test_db2.py`、`test_data_api.py`、`test_env.py`、空 `test_requests_v8.py`；
- 旧备份兼容入口，在全仓和外部运维确认无调用后删除；
- 缺口脚本的内联业务实现，在统一服务落地后改成薄入口或删除；
- 已完成并验证替代的公告/画像 legacy writer，在当前公告资产 change 的 cutover 和 rollback 条件满足后删除。

## 7. 实施路线

### 阶段 0：保护在途工作并建立基线

交付：

- 完成并归档当前 88/91 的公告资产 change，期间不做公告/画像结构重排；
- 复核 6 个 in-progress change，关闭或重定界长期停滞项；
- 将 83 个 complete change 按现行规范归档；
- 生成当前入口、job、数据库、写表 owner 和外部命令清单。

验收：每个在途 change 有唯一业务目标，不再与框架清理交叉修改同一批文件。

### 阶段 1：建立当前架构地图和约束

交付：

- 重写 `docs/architecture.md`，反映模块化单体、多数据库、应用服务和现行主链；
- 从 scheduler 配置生成任务目录，不再手写“8 个/93 个任务”这种易过期数字；
- 建立 production/manual/migration/compatibility 状态清单；
- 加最小架构测试：生产模块不得 import `scripts`，配置 job 必须能解析到 handler。

验收：新人能在文档中找到某业务能力的 owner、写入库、权威表、自动任务和手工入口。

### 阶段 2：先消除真实双写风险

交付：

- 建立 Gap Repair 应用服务，统一检测、生命周期 universe、skip policy、写入验证和因子同步；
- scheduler、API、CLI、Telegram 全部调用该服务；
- 删除 Telegram `_run_script` 对缺口和因子审计的生产旁路，改走 scheduler/application command；
- 把生产依赖的 `dev_validation` 逻辑迁入正式模块，脚本只做参数解析。

验收：同一输入在不同入口产生相同候选缺口、相同写入集合和相同失败语义；生产包对 `scripts` 的 import 数为零。

### 阶段 3：统一关键边界语义

交付：

- 落地 `InstrumentKey`/canonical renderer，兼容现有数据库格式；
- 所有 API、主数据、公告资产、研究和回测入口使用同一解析合同；
- 写入和缺口路径只使用权威交易日历；启发式 fallback 明确降级且不写库；
- 统一 HTTP/代理/TLS 配置模型，但保留三类客户端实现。

验收：`.SZSE/.SZ/.XSHE`、`.BSE/.BJ`、`.HKEX/.HK` 等别名进入系统后得到唯一 canonical id；调休日 fixture 不产生假缺口。

### 阶段 4：按域缩小大门面

每次只迁一个业务域，并保持行为兼容：

1. 研究只读查询：`DataManager` 只转发到已经存在的 `ResearchQueryService`/read service；API 新代码直接调用 read service。
2. 财务同步：应用编排迁入 `research/`，明确 summary/statement/disclosure 的对象和水位。
3. 缺口与行情更新：形成独立应用服务，`DataManager` 仅保留兼容方法。
4. 公司行为/复权：先建立状态图和权威表合同，再迁移千行级方法。
5. 公告资产和画像：等待当前 change 完成后，按其 consumer cutover 规则删除 legacy writer。

Scheduler 与 API 路由按相同域边界拆文件，但拆文件必须与 owner 迁移一起做，不能只把一个大文件机械切成多个互相调用的小文件。

验收：迁出的域不再向 `DataManager` 增加业务逻辑；新入口不经过兼容门面；旧门面有明确删除计划。

### 阶段 5：抽取已证明的共享原语并清理仓库

交付：

- 对 futures/fx/special commodity/announcement repository 做连接策略 diff，只抽完全相同部分；
- 统一任务结果模型和报告格式化；
- 删除已确认 obsolete 的根目录脚本、兼容入口和迁移代码；
- 把 `implementation_plan.md` 固化为历史文档，不再追加当前规范；
- 为 completed OpenSpec 建立及时 archive 的完成条件。

验收：共享抽取必须伴随重复代码删除；不存在只增加抽象、不减少调用方复杂度的改造。

## 8. 架构质量门槛

后续功能和框架优化应持续满足以下规则：

1. 一个业务写入动作只有一个 application command owner。
2. 外部入口可以多，但不得复制业务循环、SQL 和 provider fallback。
3. 生产代码不得 import `scripts`、测试或 OpenSpec evidence。
4. `DataManager`、scheduler handler 和 API route 中不得新增大段领域逻辑。
5. 新增共享抽象必须能指出至少两个当前调用方和被删除的重复实现。
6. 新增 compatibility path 必须同时写明替代入口和删除条件。
7. completed OpenSpec 必须归档；未完成 change 不应无限期与新 change 重叠。
8. 文档中的 job、数据库和入口清单尽量从配置或代码生成，避免手工数字失真。
9. 删除以调用链和验收证据为准，不以文件大小、命名或“看起来旧”为准。
10. 任何重构都不得改变复权、可得日、交易日、生命周期和回测时点语义，除非 change 明确要求并有金融语义测试。

建议把前 3 条做成轻量静态测试，其余作为 change review checklist。不要为此建设新的通用治理平台。

## 9. 最终优先级

| 顺序 | 工作 | 原因 |
|---:|---|---|
| 1 | 完成在途公告资产 change，清理 OpenSpec 状态 | 先稳定当前工作边界，避免交叉重构 |
| 2 | 当前架构图、权威路径和任务分类 | 低成本阻止继续复制错误路径 |
| 3 | 缺口修复单链 + 移除 Telegram subprocess | 直接降低并发写入和语义分叉风险 |
| 4 | 消除生产对 `scripts/dev_validation` 的依赖 | 修正依赖方向，明确生产代码所有权 |
| 5 | 证券标识 canonical 合同 | 解决静默空查询和跨域不一致风险 |
| 6 | 权威交易日历进入所有写入路径 | 防止假缺口和错误日更判断 |
| 7 | 按域迁出 `DataManager`/`ScheduledTasks` 逻辑 | 持续降低修改半径和回归成本 |
| 8 | 复权、财务、画像的状态/模式收口 | 复杂度高，必须在主链清晰后进行 |
| 9 | 小型存储原语与仓库卫生 | 有价值，但不应抢在业务写入正确性之前 |

## 10. 总体判断

项目确实需要一次持续、分阶段的框架优化，但不需要推倒重来。真正要建立的是清晰的所有权和退出机制：功能开发完成后，入口汇聚、兼容路径退役、迁移工具删除、change 归档、架构文档更新。只要坚持“多适配器、单业务命令、单写入 owner、域内规则、有限共享”，现有代码可以逐域变干净，而不必承担一次全仓重构的高风险。

原审计可作为问题线索保留，本文件应作为后续框架 change 的决策基线；具体实施仍应拆成独立 OpenSpec change，每个 change 只解决一个可验收的业务或架构问题。
