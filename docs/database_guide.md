# SQLite 底层架构与调优指南

本文档全面记录了本项目当前使用的数据库架构、表结构设计理念，以及针对海量（量化行情级别）数据的 SQLite 性能并发调优机制。本技术说明与当前项目代码严格同步（截止 **2026年4月最新重构版**）。

---

## 🏗️ 核心架构与模式选择

项目中采用了轻量级但被极致压榨性能的 **SQLite 3** 作为底层存储。

1. **核心库位置**：`data/quotes.db`
2. **连接池设计**：
    - **同步操作引擎**: 使用 `StaticPool` 单开连接防冲突，主要用于少量初始化建库。
    - **异步操作引擎**: 引入了 `aiosqlite` 底层驱动配合 SQLAlchemy 的 `QueuePool(pool_size=1, max_overflow=4)`，利用异步协程达到读写的高效排队流通。
3. **高并发 WAL 机制（ Write-Ahead Logging ）**：
    - 在每一次发起的读/写 Engine 会话启动生命周期（`event.listen` "connect"）时，系统自动挂载以下 PRAGMA 配置：
      ```sql
      PRAGMA journal_mode=WAL; -- 放弃旧的表级排他锁，允许 1 个写者和外围并发 N 个跨协程读者。
      PRAGMA synchronous=NORMAL; -- 内存快于刷盘，仅在极低概率停电时会丢失最近几个事务数据，换取10倍写入性能。
      PRAGMA cache_size=-64000; -- 在所有连接通道共享一个 64MB 的 LRU 热数据内存高速缓存。
      ```

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
