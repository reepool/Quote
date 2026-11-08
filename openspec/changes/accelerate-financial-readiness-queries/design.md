# Design: 加速财务就绪查询

## 1. 问题分析（全部为 2026-11-08 生产实测）

### 1.1 成本去向分解

每候选 0.64s 的构成（按证据强度排序）：

| 成本项 | 实测 | 证据 |
|---|---|---|
| 肥行取回与解码 | 每标的每期 573 行 × 35 列（含 JSON/文本列），热缓存 10ms | A/B 基准：现行 10ms vs 收窄 <1ms |
| 无用行占比 | 573 行中就绪判定只需要 ~7 行（5 个核心事实 + 变体） | 行数对比 98.8% 冗余 |
| 冷缓存回表 | 页缓存 19GB < 热表规模，扫描态持续 9~12MB/s 磁盘读 | /proc/io 采样；索引回表为随机页读 |
| 连接开销 | 每次存储调用 connect+PRAGMA 5ms，每候选多次调用 | 微基准 |
| 每候选多次存储调用 | 就绪判定 = local core facts（宽形态查询）+ **映射注册查询（每候选一次、当前无缓存、表仅 527 行）** + 缺口候选条件触发的 CNInfo 官方事实合并读取 + repair target 组装 | 代码路径（`readiness_for_target` → `get_financial_local_core_facts` → `get_financial_source_field_mappings`；`_merge_official_cninfo_readiness` 仅在缺事实时触发）；精确计数归 M1 |

附两项代码核证（2026-11-08）：`physical_table` 合成列**全库零消费方**（grep 无引用）——窄方法直接不产出该列；热/历史同键优先级由跨表 `updated_at DESC` 决定（无表级优先规则）。

### 1.2 现状查询形态（`research/storage.py` get_financial_numeric_facts）

- 真实调用为 `include_history=True`（`readiness_for_target` 固定传）：内层 `SELECT *, '<table>' AS physical_table FROM hot UNION ALL SELECT *, ... FROM history`，外层再按 instrument+period 过滤——**A/B 基准测的是热表单表直查形态**，浪费结论（573 vs 7 行）不变，但绝对耗时以 M1 在真实形态上重测为准；
- 历史表全表仅 **10,978 行**（实测），union 体积可忽略；成本主体是热表肥行解码；
- `SELECT *`（35 列全部解码为 Python 对象，含大文本列），并附加合成列 `physical_table`；
- canonical 过滤在评估器侧逐行判断（573 行取回后丢弃绝大多数），SQL 未下推；
- `ORDER BY report_period DESC, fact_name ASC, updated_at DESC` 触发 TEMP B-TREE；
- 已有 `idx_financial_numeric_facts_hot_instrument(instrument_id, ...)` 支撑定位——**不缺查找索引，缺的是"少取数据"**；
- 附：就绪链上的 `wrapped` 装饰器（storage.py:14796）只是 financial 库路由（进入 `financial_database_scope`），**无缓存层**——L4 连接复用的收益前提成立。

### 1.3 为什么现在立项

5 小时上限下该任务**每周日必然白扫**；临时 8 小时上限只是兜底（占用周日白天全程）。Q3 数据量与库增长使速率从历史可完成退化到当前水平，属持续性代价而非偶发。

## 2. 目标与非目标

**目标（全部可验收）**

1. 肥数据标的（如 000001.SZ）单候选就绪判定端到端 **≤0.15s**（现 ~0.64s）；
2. 全量对账扫描段 **≤75 分钟**（现 ~5.5h）；
3. 周日全量对账总时长（扫描+应用+清退）**≤2.5 小时**，回到 18000s 原上限内从容完成；
4. 新旧就绪判定结果**逐字段等价**（样本集覆盖银行/非银 profile、零数据标的、生命周期豁免）；
5. 其他 `get_financial_numeric_facts` 调用方行为零变化。

**非目标**

- 不缩减 77GB 库体积（历史/冷表治理另立卡）；
- 不改就绪判定语义、required facts 口径、profile 规则；
- 不引入物化/缓存表（写路径失效维护复杂度不值）；不拆库、不加内存、不换盘。

## 3. 方案与决策

| 杠杆 | 内容 | 预期收益 | 决策 |
|---|---|---|---|
| L1 投影收窄 | 新方法只 SELECT 就绪所需列（period、canonical_fact_name、fact_value、unit 等少量列） | 肥列（JSON/长文本）不再解码，IO 与解码同降 | **做**（A/B 已证 10→<1ms 量级） |
| L2 过滤下推 | canonical 事实 IN (...) 进 SQL WHERE | 573 行→~7 行 | **做**（与 L1 同一方法体内） |
| L3 覆盖索引 | **降级为条件项**（2026-11-08 EXPLAIN 实证：既有 `idx_financial_numeric_facts_hot_canonical` 已支撑 (instrument_id, report_period, canonical_fact_name) 三列 SEARCH，历史表分支同样有 canonical 索引；且 `raw_fact_json` 必须回表，index-only 目标对该查询形态不可达） | L1+L2 后每候选仅 ~7 行回表，边际收益小 | **条件项**：M4.1 实测不达标才执行（脚本已备 `scripts/financial_readiness_covering_index.py`，窗口要求见 runbook） |
| L4 连接复用 + 注册记忆化 | 就绪路径在同一库作用域内复用连接（现每调用 5ms×N）；映射注册查询按 (mapping_version, profile) 进程内记忆化——**空结果一律不缓存**（V6 事故即"进程运行中目录从空被装载"，空结果缓存会把恢复窗口钉死到下次重启，此规则使其自愈） | 每候选省 15~30ms 连接开销 + 每候选一次注册查询 | **做**（最小：readiness 循环级连接；不建常驻池；键含 mapping_version，版本变化自动失效） |
| L5 按标的批量化 | 一次查询取回该标的全部 11 期就绪行（11 次查询→1 次） | 查询次数降 11 倍 | **条件项**：M1 实测 L1-L4 后仍不达标才做 |
| 物化就绪表 | 写路径维护每标就绪摘要 | 读端 O(1) 但写端失效逻辑复杂 | **否决**（违反最小充分实现） |

**实施形态**：新增 `get_financial_core_fact_readiness_rows(...)` 窄方法（storage 层），就绪调用点（`financial_statement_maintenance_repair.readiness_for_target` → storage）切换到新方法；**现有宽方法原样保留**，其余调用方（PIT、导出、审计等）零影响。

## 4. 等价性策略（合入门槛）

- M1 建立**双跑等价工具**：对样本集（≥200 标的 × 全报告期，必含银行 profile、BSE、零数据标的、生命周期豁免分类）逐候选比较新旧实现的 missing_fields/ready/facts 判定，要求 100% 一致；
- 样本集冻结入仓库（tests fixture），回归永久保留；
- 单元测试覆盖：过滤下推后"多版本同名事实取最新"语义不变（updated_at 排序语义在窄方法中保持）。

## 5. 索引 DDL 运维（L3——条件项，仅当 M4.1 实测不达标）

（2026-11-08 EXPLAIN 更新：既有 `idx_financial_numeric_facts_hot_canonical` 已支撑窄查询三列过滤，历史表亦有同类索引；覆盖升级仅在窄化后实测仍超标时执行，脚本与窗口 runbook 已备。）

- `CREATE INDEX` 在 77GB 库上为写事务：WAL 下**读者不受影响**，**写者阻塞建索引全程**；
- 窗口选择：夜间增量（21:45 起、上限 4h，最坏 01:45 结束——2026-11-02 实际发生过）之后、且无其他 financials.db 写任务时段（建议 **≥02:00** 的凌晨空档，M3 出具精确窗口核查清单排除当日在跑任务）；
- 时长预估：77GB 顺序构建估 20~60 分钟（M3 在备份副本上实测后写死进 runbook）；
- 空间：估 1~2GB（29.7M 行 × 窄键），建前查剩余空间；
- 回滚：`DROP INDEX` 即可，无数据影响；
- 部署物：索引创建进 `scripts/`（operator script，显式执行），不挂自动任务。

## 6. 生产不变量

1. 就绪判定结果与现实现 100% 等价（§4 双跑证明）；
2. 写入路径零改动（导入、upsert、状态机不动）；
3. 其他宽方法调用方零影响；
4. 对账任务对外契约（报告字段、状态分类、幂等语义）不变；
5. DDL 只在授权窗口人工执行，回滚路径先行验证。

## 7. 风险

| 风险 | 缓解 |
|---|---|
| 窄方法漏列导致判定偏差 | 等价性双跑 100% 一致为合入门槛；样本含全部 profile |
| 索引构建期间写任务撞窗 | M3 窗口清单 + runbook 中断/续做说明；构建时长先在副本实测 |
| ORDER BY 语义在新形态偏差 | 单测钉住"同期同名取 updated_at 最新" |
| 收益低于预期（0.64s 大头不在查询） | M1 profile 先行：先测每候选存储调用分解，再定 L4/L5 取舍；目标不达不关 change |
| 热/历史同键且 updated_at 相同时，宽实现依赖 UNION ALL 分支顺序（排序并列不稳定） | 等价性工具遇到该并列时按黄金基线固化具体行为并单独评审（可能暴露既有非确定性），不静默放行 |
| 扫描速率回退不可见 | 心跳/报告暴露每候选速率（M4 落地） |

## 8. 验证与度量

1. M1 基线工具：单候选端到端计时 + 每候选存储调用计数（连接次数/查询次数/行数），产出基线报告入库（docs/development）；
2. 等价性双跑（§4）；
3. 预发验证：副本库上全量对账 dry-run 计时（扫描段 ≤75min）；
4. 生产验收：下一个周日 09:30 自然调度运行，总时长 ≤2.5h、报告正常、`max_runtime_seconds` 回调 18000；
5. 回归：storage/对账/增量全部既有单测 + 新增等价与窄方法单测。
