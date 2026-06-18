# SQLite 底层架构与调优指南

本文档全面记录了本项目当前使用的数据库架构、表结构设计理念，以及针对海量（量化行情级别）数据的 SQLite 性能并发调优机制。本技术说明与当前项目代码严格同步（截止 **2026年4月最新重构版**）。

---

## 🏗️ 核心架构与模式选择

项目中采用了轻量级但被极致压榨性能的 **SQLite 3** 作为底层存储。

1. **核心库位置**：`data/quotes.db`
   - 运行环境中 `data/` 应挂载为独立本地数据卷：
     `/dev/sda3 -> /home/python/Quote/data`。
   - 研究库 `data/research.db`、后续财务库 `data/financials.db` 和
     XBRL/结构化财报归档 `data/filings/` 也应放在该数据卷下。
   - NAS 备份目录继续作为子挂载保留在 `data/PVE-Bak` 与
     `data/QuoteBak`，不要把 NAS 内容复制进本地数据卷。
2. **连接池设计**：
    - **同步操作引擎**: 使用 `StaticPool` 单开连接防冲突，主要用于少量初始化建库和少量同步读写兼容路径。
    - **任务异步引擎**: `task_async_pool` 使用 `aiosqlite + SQLAlchemy QueuePool`，专供调度器、数据维护、历史回补、Telegram 任务收尾等后台任务使用。当前生产建议为 `pool_size=2, max_overflow=0`，形成 2 条任务侧专用异步 DB 通道。
    - **API 异步引擎**: `api_async_pool` 使用独立的 `aiosqlite + SQLAlchemy QueuePool`，专供 FastAPI 外部请求使用。当前生产建议为 `pool_size=2, max_overflow=6`，由 API admission control 控制进入后端的并发请求。
    - **路由机制**: `api.middleware.RateLimitMiddleware` 在已准入请求进入路由前设置 `db_workload_context("api")`；`DatabaseOperations.get_async_session()` 通过 `DatabaseManager.get_async_session()` 根据上下文选择 API pool 或 task pool。非 API 执行路径默认使用 `task` pool；管理 API 触发的数据生产任务通过路由层 helper 显式切回 `task` pool。
3. **高并发 WAL 机制（ Write-Ahead Logging ）**：
    - 在每一次发起的读/写 Engine 会话启动生命周期（`event.listen` "connect"）时，系统自动挂载以下 PRAGMA 配置：
      ```sql
      PRAGMA journal_mode=WAL; -- 放弃旧的表级排他锁，允许 1 个写者和外围并发 N 个跨协程读者。
      PRAGMA synchronous=NORMAL; -- 内存快于刷盘，仅在极低概率停电时会丢失最近几个事务数据，换取10倍写入性能。
      PRAGMA cache_size=-64000; -- 在所有连接通道共享一个 64MB 的 LRU 热数据内存高速缓存。
      ```

4. **读写冲突边界**：
    - API/task 双池隔离解决的是连接池耗尽和任务饥饿问题，不改变 SQLite 的底层并发模型。
    - WAL 模式支持一个写者与多个读者并发；API 主要读、任务主要写时，正常短事务不会形成数据库死锁。
    - 长写事务、`VACUUM`、`ANALYZE`、大批量 DDL/索引维护仍可能造成文件级锁等待，因此写密集维护任务必须继续通过调度器 `max_instances`、任务级互斥和批量提交粒度控制。
    - 外部 API 请求必须经过 admission control 有界排队；即便 API pool 被占满，task pool 仍保留给后台任务完成状态写入、报告收尾和数据维护。

---

## 财务域与未来数据库迁移原则

专项说明见 [财务数据系统专项文档](financial_data_system.md)，其中记录了当前 L1/L2/L3 财务服务分层、`data/financials.db` 写入边界、字段映射原则和全量导入脚本 `scripts/research_financial_l1_full_import.py` 的使用方法。

当前 Quote Core 和 research 域仍以 SQLite 为现实底座，但财务域已经开始引入官方结构化披露文件、全数值事实长表、hot/cold tier、多期 backfill/catchup 和更复杂的 coverage/readiness 查询。为避免未来迁移到更高性能数据库时重写业务逻辑，财务相关实现必须遵守以下边界：

1. **存储隔离**
   - `2026-05-20` 起，财务域生产读写必须通过 `financials_db_path` 路由到 `data/financials.db`；`data/research.db` 中已有的 `financial_*` 表只作为历史兼容/迁移来源，不再作为新增财务事实的目标库。
   - 财务仓表包括 `financial_summaries`、`financial_statements_raw`、`financial_facts`、`financial_indicator_snapshots`、`financial_source_files`、`financial_core_facts_hot/history`、`financial_numeric_facts_hot/history`、`financial_indicator_snapshots_hot/history`、`financial_source_field_mappings` 和 `financial_source_mapping_audits`。`financial_numeric_facts` 在优化后只作为 `hot UNION ALL history` 兼容视图，不再作为重复物理表写入。
   - 官方 XBRL 或等价结构化文件归档放在 `data/filings/`，归档路径写入 `financial_source_files.archive_path`。
   - `quotes.db` 不承载财务事实表，避免行情主库被低频大字段写入放大。
   - 财务库离线优化必须保持最终库名 `data/financials.db`；如需备份生产库，默认写入 `/home/python/Quote/data/PVE-Bak/QuoteBak`。
   - `2026-05-25` 已完成 `data/financials.db` 离线优化：生产库从约 `79G` 降至约 `53G`，`financial_numeric_facts` 已改为兼容视图，`financial_numeric_facts_hot/history` 为权威物理表，NAS 备份保留为 `/home/python/Quote/data/PVE-Bak/QuoteBak/financials.db.20260525_011603.bak`。

2. **访问抽象**
   - `DataManager`、API 路由、provider 和 sync service 只依赖 storage/repository 方法。
   - SQLite 的 `PRAGMA`、`ATTACH`、`ON CONFLICT`、JSON text 查询、批量 insert 优化等必须封装在存储层。
   - 如果未来切换到 DuckDB/PostgreSQL，优先替换连接配置、迁移脚本和 storage adapter，不改变 API 响应模型和财务语义。

3. **可迁移逻辑模型**
   - 财务表必须以 `instrument_id`、`report_period`、`report_type`、`source_file_id`、`source`、`source_mode`、`parser_version`、`data_available_date` 等逻辑字段为核心。
   - 不依赖 SQLite `rowid` 作为跨表业务主键；内部自增 id 只用于本库引用。
   - JSON 字段用于保留 diagnostics 和原始扩展信息，但常用查询字段必须结构化落列。

4. **热/冷分层**
   - 常用财务事实和派生指标可以维护 hot tier，默认保留最近 `12` 个季度及必要 anchor periods。
   - 超过 hot window 的财务事实进入 history tier；在 SQLite 阶段可实现为两张表，在 PostgreSQL 阶段可实现为分区表，在 DuckDB 阶段可实现为热表 + 冷历史列式表。
   - source manifest、raw archive、payload audit 不按 hot/cold 拆散，保持全量唯一，便于修订报表追踪。
   - 查询合并逻辑必须在 storage/repository 层实现，业务代码不直接 union 物理表。

5. **配置化**
   - 官方源 URL、归档路径、报告期 baseline、rolling quarters、限流、重试、并发、parser version、字段 alias、fallback policy 不能写死在业务代码里。
   - hot window、anchor policy、tier maintenance 开关也必须配置化。
   - 这些参数应来自 `config/10_research.json`、scheduler 参数或后续独立 financial config，并在 `ingestion_runs` 或 source manifest 中记录本次运行的解析结果；当前 backfill/catchup 已记录 resolved periods、sync mode、checkpoint、写入计数、tier maintenance 和 coverage gaps。

---

## 📊 数据核心表结构设计 

利用 `SQLAlchemy ORM` 映射，数据分为几类不同职责的核心隔离表：

### 1. `daily_quotes` (高频重型数据表)
**职责**：存储每天多达 5000+ 支股票的完整日线面阵及附加标签属性。单表常规在 1~3 年内会轻易达到几千万行级别（目前近 2000 万行）。

**字段要点**：
- `open, high, low, close, volume, amount, turnover`: 常温字段。
- `tradestatus`: 高价值标记，用于标注当日是否开盘、停牌（`1` / `0`）。
- `quality_score, is_complete`: 自动化的数据完整性信赖评分标注。

**极端瘦身索引设计**：
为了抗拒高频更新带来的写入放大拖累（Write Amplification），我们抹除了大量自动化产生的冗余排序索引。
目前该表**有且只有两个索引**：
1. `sqlite_autoindex_daily_quotes_1`：不可变的聚合首列 **`PrimaryKey(time, instrument_id)`**。
2. `idx_daily_quotes_instrument_time`：**`Index(instrument_id, time)`**。这是专为 `Query.WHERE(instrument_id='xx').BETWEEN(time_start, time_end)` 这种最高频的连续行情回溯读取查询所量身打造的最佳二阶 B-Tree 联合覆盖索引。

### 2. `adjustment_factors` (权息衍生表)
**职责**：剥离行情数据，独立按发生日期保存每一家公司的现金分红、送股、配股以及最终算定的除权因子。计算过程实现了**数据面与计算面的逻辑隔离**。

### 3. `gap_skip_list` (网络与源空缺豁免表)
**职责**：跳过黑洞！当底层回补进程发起请求，确信 `bao_stock` / `akshare` 等数据源中确实**不存在**这根K线数据（如长假休市被非法请求、股票尚未上市前区间），系统会记录起缺口边界日期，在接下来的一个月内再也不会浪费宝贵的 http 请求额度和执行线程锁来试图补它。

### 4. `instruments` 与 `data_source_status`
用于存储静态的资产信息主键映射字典与目前各个爬虫 API 网关的防拉黑并发冷却标记。

---

## 🧹 关于无用索引回收的历史提示

SQLite 有一个物理特性需要维护者知晓：**删除冗余数据后硬盘空间并不会缩小**。
在执行 `scripts/optimize_sqlite.py` 将大量历史遗留垃圾索引删除后。约 8.5 GB 的硬盘占用虽然没消失，但变成了一个 `Free List` （物理文件挂载中的空数据海绵区）。未来系统下载的新股票日线会优先被回填在这 8.5GB 海绵区里。

在未来数据量庞大（突破 50GB）或者你需要释放 VPS 硬盘挪作他用时，可用下面命令手动将海绵地挤干：
```bash
# 警告：会挂起整个数据库大约3至5分钟长锁，需产生同等大小临时虚拟临时文件。
python3 scripts/optimize_sqlite.py --vacuum
```
