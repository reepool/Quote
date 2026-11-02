# 消除财报披露同步的读放大

## Why

财报披露增量同步（`financial_disclosure_incremental_sync`）与对账（`financial_disclosure_reconciliation_sync`）的候选就绪度评估存在系统性读放大，夜间运行已达小时级：

- 2026-11-02 实测：增量任务 21:45 启动后，`_clear_stale_ready_local_gap_states` 对 Q3 季报积压状态逐条执行全量就绪度查询（每条状态一次 `financials.db` 查询，库 77GB、`financial_numeric_facts_hot` 约 2,970 万行），纯 CPU 段持续约 2 小时（commit `6b985707` 事故记录与 py-spy 取证）；
- 同族对账任务上一晚完整运行 50,803 秒（约 14.1 小时，run `20261101_180537`）；
- 放大源在代码中可指认：就绪度判定链 `_readiness_for_candidate → readiness_for_target → _merge_official_cninfo_readiness → _official_cninfo_facts_by_canonical → get_numeric_facts` 逐候选执行；`get_numeric_facts` 为 `SELECT *`（含 `raw_fact_json`、`dimensions_json` 大 JSON 列）+ 每行两次 JSON 反序列化 + 每次连接 `_apply_pragmas`；另有 `_load_profile_industry_membership` 逐候选查询；stale-state 清理与主候选循环不共享任何读取结果。

`isolate-long-tasks-from-api-event-loop`（FR-18）已让这两个任务在 worker 循环执行并具备检查点与 `/stop`，主循环不再被饿死；但该 change 明确将"扫描查询成本"列为非目标（design §2 非目标：不处理 `financials.db` 体积与扫描查询索引，另记改进建议）。本 change 即该遗留项的落实：**任务不再卡死应用之后，还要让任务本身在分钟级完成**。当前状态下即使已隔离，小时级运行仍意味着 `max_runtime_seconds` 高概率中途终止任务、CPU 长期占满一个 worker、结果延迟到次日早晨才可用。

## What Changes

- 就绪度输入的 run 级复用：同一次任务运行内，同一 `(instrument_id, report_period)` 的数值事实与行业隶属等就绪度输入只读取一次；stale-state 清理与主候选循环共享同一份输入，不再逐状态、逐候选重复全量查询。
- 读取投影收窄：就绪度判定不需要大 JSON 列的路径不在 SQL/解码层拉取 `raw_fact_json`、`dimensions_json`（列级选列或惰性解码）；`get_numeric_facts` 现有返回契约保持不变，完整字段的既有调用方不受影响。
- stale-state 清理降本：`_clear_stale_ready_local_gap_states` 判断"当前已就绪可删除"所执行的查询不得比主循环单个候选更贵；按批共享读取或直接复用主循环结果。
- 可测性能合同：以受控 fixture（临时库，非生产库）固定候选规模基准（首版 2,000 候选 × 10 个报告期），就绪度评估阶段的耗时与查询次数设定上限并纳入回归测试，防止放大以新形态回归。
- 语义零变化：就绪度判定结果、missing 字段集合、`maintenance_source_routing` 标注、写入与水位语义逐字段等价；以现有 financial 单测全集 + 新增等价性对比测试作为验收依据。

## Capabilities

### New Capabilities

- `financial-disclosure-read-efficiency`: 财报披露同步的就绪度评估在数据量增长下保持有界读取成本，且判定语义与字段级输出不因性能优化改变。

### Modified Capabilities

无。`scheduler-task-loop-isolation`（FR-18 change）的执行隔离契约不受影响；本 change 只改变读取次数与解码范围，不改变执行位置、调度与报告合同。

## Impact

- 受影响代码：
  - `research/financial_statement_maintenance_repair.py`（就绪度输入的 run 级复用、stale 清理读取路径）；
  - `research/financial_disclosure_incremental_sync.py`（stale-state 清理与主循环共享读取）；
  - `research/storage.py`（`get_numeric_facts` 增加列级投影参数，默认行为不变——属总纲 §5 允许的"修复当前业务错误"级局部改动，不新增大段逻辑）。
- 程序映射：FR-07（W5 研究存储 repository 边界）读路径切片；与 FR-18（`isolate-long-tasks-from-api-event-loop`）互补——隔离解决"卡死谁"，本 change 解决"跑多久"。
- 生产不变量：数据库路径、表结构、canonical key、写入 owner、job 配置与调度时间全部不变；对账扫描期零写语义不变；运行中进程需重启加载新代码。
- 非目标：不做 `financials.db` 瘦身/归档/分区（另行登记）；不建通用缓存平台或跨域 memo 框架（复用仅限本次两个调用方场景，不满足总纲 §4 共享抽象门槛的留在域内）；不改执行隔离与检查点机制；不整体拆分 `research/storage.py`（W5 全量另行）。
