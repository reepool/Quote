# Design: 消除财报披露同步的读放大

## 1. Context

执行链（现状）：

```text
scheduler (worker loop, FR-18 已隔离)
  └─ FinancialDisclosureIncrementalSyncService.sync
       ├─ _clear_stale_ready_local_gap_states        # 逐状态全量就绪度查询（6b985707 后有检查点，成本未变）
       └─ _readiness_for_candidate (逐候选)
            └─ financial_statement_maintenance_repair.readiness_for_target
                 ├─ _official_cninfo_facts_by_canonical
                 │    └─ storage.get_numeric_facts(instrument, period, include_history=True)
                 │         # SELECT * FROM hot UNION history；每行 dict + 2×JSON 反序列化；每次连接 _apply_pragmas
                 ├─ _load_profile_industry_membership (逐候选)
                 └─ …（M1 盘点补全清单）
```

已核实事实：

| 事实 | 证据 |
|---|---|
| 热表 2,970 万行，库 77GB | `SELECT COUNT(*)` = 29,699,529（2026-11-03） |
| 单查询有索引 | `EXPLAIN QUERY PLAN` 走 `idx_financial_numeric_facts_hot_canonical (instrument_id, report_period)` |
| 放大在调用次数×每行解码，不在单查询计划 | py-spy 2026-11-02 栈：`_decode_financial_numeric_fact_row` / `listcomp (storage.py:2490)` 常驻 |
| stale 清理逐状态全量查询 | commit `6b985707` 事故记录（Q3 积压、~2h） |
| 对账全量 14.1h | run `20261101_180537`，50,803s |
| 单行 JSON 载荷中等 | 抽样 avg ~394B / max ~994B（`raw_fact_json`） |

结论：瓶颈不是缺索引，而是**每候选/每状态重复读取 + 全列解码**的乘法效应。修复方向是减少调用次数与解码范围，不是新建索引或改表。

## 2. Goals / Non-Goals

**Goals**

- 同一 run 内就绪度输入零重复读取（`instrument × period` 维度）。
- 就绪度路径不解码其不需要的大 JSON 列。
- stale-state 清理不再独立做全量就绪度查询。
- 以受控 fixture 的性能与查询次数上限锁住成果。
- 判定输出与写入语义逐字段等价。

**Non-Goals**

- 不处理 `financials.db` 体积（77GB）治理、归档、分区。
- 不建跨域缓存/共享 memo 抽象（当前只有一个域内场景；总纲 §4 两个真实调用方门槛未满足）。
- 不改 FR-18 的 worker 隔离、检查点、`/stop` 契约。
- 不做 W5 全量 repository 拆分；`storage.py` 只做列级投影参数的局部扩展。

## 3. Decisions

### D1 run 级就绪度输入复用（域服务层）

在 `FinancialMaintenanceRepair`（或其调用方 service）内引入 **run 作用域** 的输入装配：任务开始时（或惰性首批）按候选清单的 `(instrument_id, report_period)` 集合批量取数，装配为 run 私有的 readiness 输入表；`readiness_for_target` 与 `_clear_stale_ready_local_gap_states` 都从该表取数。

- 形态：run 级 dict 缓存 + 批量预取，生命周期=单次 run，随 run 结束释放。不引入 LRU/TTL 等通用缓存结构。
- 批量粒度：按 run 的候选集合分批（如每 500 候选一批）预取，避免一次性物化全部。
- 未命中兜底：缓存未命中时单点回读并回填，语义与现状一致。

### D2 投影收窄（repository 读方法）

`get_numeric_facts` 增加 `columns`/`include_payloads` 类列级参数（命名以实现为准），默认值保持现状全列全解码；就绪度路径传窄投影（不含 `raw_fact_json`、`dimensions_json`），仅在确需载荷的合并分支（`_merge_official_cninfo_readiness` 命中补齐时）按行回读载荷。

- 兼容：既有调用方零改动；返回 dict 的字段集合在窄投影下明确减少（仅内部新调用方使用），公共契约不变。
- 就绪度判定只需 `canonical_fact_name / fact_name / report_period / value / unit / source 等标量列`——M1 盘点确认精确清单后固化。

### D3 stale-state 清理降本

`_clear_stale_ready_local_gap_states` 复用 D1 的 run 输入表判定"当前已就绪"，不再逐状态独立执行 readiness 查询；仅对输入表未覆盖的孤儿状态做单点回读。保留 `6b985707` 的检查点让出与计数（它是 FR-18 取消契约的一部分，不动）。

### D4 性能合同测试

- fixture：临时库，注入 2,000 候选 × 10 报告期 × 每期 N 行 facts（含大 JSON 行），对照组=现状读取行为（等价性断言用），实验组=新路径。
- 断言：就绪度阶段查询次数 ≤ 候选数 × 小常数（首版 ≤3，含 industry）；耗时上限在 CI 可容忍量级（如 <60s，按 CI 实测定值）；输出与对照逐字段一致。
- 回归防线：现有 financial disclosure 47 项单测 + 新增等价性测试全绿。

### D5 先测量后定型（M1）

M1 产出一次受控 instrumented run 的"读路径 × 调用次数 × 耗时"表（写入本 design 附录 A），若实测推翻上述任一放大源假设（例如真凶是 `_apply_pragmas` 连接开销而非行数），D1–D3 的优先级按实测重排，但 Goals 与验收不变。

## 4. Risks / Mitigations

| 风险 | 缓解 |
|---|---|
| 批量预取自身变重（单批过大） | 分批上限 + 批间沿用 FR-18 检查点让出 |
| 窄投影漏列导致就绪度误判 | M1 固化列清单 + 等价性对照测试（窄投影 vs 全列判定结果一致） |
| run 缓存与中途写入产生陈旧读 | 就绪度输入均为只读快照语义；run 内候选写入后不回读自身判定结果（与现状一致），等价性测试覆盖 |
| stale 清理与主循环共享输入后行为漂移 | 清理只消费与主循环相同的只读输入，判定函数复用同一实现 |

## 5. 生产不变量

- job id、cron、enabled、参数、报告字段不变；
- `financials.db` 表结构、canonical key、写入 owner、水位与幂等语义不变；
- 对账扫描期零写语义不变；
- 运行中进程需重启加载（沿用仓库既有条款）。

## 6. 切片与验收映射

见 tasks.md。M1 测量 → M2/M3 修复 → M4 合同与真实夜间观察（运行时长回到分钟级，实测记录进 tasks）。

## 附录 A（M1 填充）

读路径清单与实测计数：待 M1。
