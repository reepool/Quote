# 加速财务就绪查询：让周度对账回到 5 小时窗口内

## Why

`financial_disclosure_reconciliation_sync`（周日 09:30 全量对账）已连续无法在 5 小时上限内完成：2026-11-08 运行在扫描阶段 ~90% 处（26,483/约 28k 候选）被超时终止，**5 小时只读扫描全部作废**（扫描零写入，应用阶段从未开始）。当前已把该任务上限临时调至 8 小时（`e4771ffb`），治本必须把单候选就绪查询成本降下来。

实测证据（2026-11-08，生产库）：

- 扫描实测速率 **0.64 秒/候选**（心跳 14:10→14:15：516 候选/300s）→ 全扫描 ~5.5h + 应用 ~1h ≈ 6.5h；
- 就绪查询现行形态 `SELECT *`（35 列肥行，含 JSON/文本）：**每标的每期取回 573 行**，热缓存 10ms/标的；
- 收窄形态（显式列投影 + canonical 事实过滤下推）：**取回 7 行，<1ms/标的**——**行数降 98.8%，查询快 10~20 倍**；
- 每次存储调用新建 SQLite 连接 + PRAGMA：**5ms/次**，每候选多次调用叠加；
- 页缓存 19GB < 热表规模：扫描态持续 9~12MB/s 磁盘读，冷缓存回表是主要物理成本。

## What Changes

- **新增窄口径读取方法**（不动现有 `get_financial_numeric_facts`，其他调用方零影响）：只投影就绪判定所需列、canonical 事实过滤下推 SQL，专供对账/增量就绪路径；
- **覆盖索引**：`(instrument_id, report_period, canonical_fact_name, fact_value)` 上建复合索引使就绪查询走 index-only scan（冷缓存收益最大）；DDL 在维护窗口执行（WAL 读者不受影响），保留回滚路径（DROP INDEX）；
- **扫描循环连接复用**：消除每候选多次 connect+PRAGMA 的 5ms 叠加；
- **可观测**：对账报告与心跳暴露每候选速率与阶段耗时，性能回退可见；
- **等价性保障**：新旧实现结果必须逐字节一致（含银行 profile、零数据标的、生命周期豁免样本），等价性测试是合入门槛。

## Capabilities

### New Capabilities

- `financial-readiness-query-performance`：就绪查询的资源边界（投影/过滤/索引）、性能目标、等价性与索引运维安全。

### Modified Capabilities

无。`financial-operations-scheduler` 的任务契约不变（同一任务、同一判定语义，只是更快）；本 change 不触碰任何写入路径与业务语义。

## Impact

- 代码影响集中在 `research/storage.py`（新增窄方法 + 索引 DDL 入口）与就绪调用点（`research/financial_disclosure_incremental_sync.py`、`research/financial_statement_maintenance_repair.py`）；
- 生产 DDL 一次：77GB 库上建索引（估 1~2GB 空间），写者阻塞限于维护窗口；
- 明确非目标：不缩减 77GB 库本身（历史表治理另立卡）、不改任何就绪判定语义、不引入缓存表/物化视图等写路径复杂度、不动数据库拆分；
- 与既有登记项衔接：本 change 即 isolation change 附录中登记的"77GB 就绪查询性能"改进项的落地。
