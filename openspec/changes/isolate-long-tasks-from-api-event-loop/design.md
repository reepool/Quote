# Design: 隔离长任务与 API 共享事件循环

## 1. 问题分析

### 1.1 现状架构

```text
进程: python main.py full (systemd: quote-system.service)
  └── 单一 asyncio 事件循环
        ├── uvicorn API server (main.py: asyncio.create_task(server.serve()))
        ├── AsyncIOScheduler (scheduler/scheduler.py:42)
        │     └── job 协程直接 await 在主循环 (scheduler/scheduler.py:305)
        │           └── 任务内部: 同步 sqlite3 (research/storage.py:14) + pandas + httpx await
        └── Telethon Telegram bot (utils/task_manager/handlers.py)
```

### 1.2 故障机理

任务协程内的同步重活（77GB 库候选扫描，实测 CPU ~84%、持续读盘 9–12.5MB/s）占住解释器与循环：API 的 accept/路由协程、bot 的 update 处理、以及 `asyncio.wait_for` 的超时回调都只能挤任务 await 的间隙运行。2026-11-01 实测：API LISTEN 积压 172、curl 8s 超时；bot 仅在 15:41/15:42/15:44/16:16 零星醒来；`/stop` 不可达，只能重启进程。

超时依赖被饿住的循环（`scheduler/scheduler.py:302-305`），意味着"5 小时强制超时"在饿住期间是尽力而为而非保证——这与"任务可能永不结束"叠加，构成当前最危险组合。

### 1.3 与既有能力的关系

- `api-db-resource-isolation`：保护任务不受 API 负载挤占 DB 容量（反方向）。本 change 不修改其任何需求。
- `scheduler`（任务配置与依赖组编排契约）：`pre_success`/`post_success`/`post_always`、parallel/serial、`manual_only` 语义不变，编排仍留在主循环包装器；注意 parallel 依赖组在 worker 化后从"单循环交错"变为真实并行，其写协调由 §3.6 约束。
- `financial-operations-scheduler`（财务任务报告契约）：Telegram 报告的内容与渠道契约不变，本 change 只改变执行位置与投递路径（见 §3.4）。
- 治理总纲 §2.1 多入口单实现：本 change 只改"执行位置"，不改业务实现；任务仍由唯一调度器链路触发。
- 覆盖范围：本 change 收口调度包装器执行路径（cron、bot `/run`、依赖组触发共用）。`api/routes.py` 存在直接调用 `data_manager.run_*` 的长任务入口（special commodity 系列同步、futures calendar backfill 等 5 处），不经过该包装器；M1 盘点后要么收敛到同一执行路径（优先，符合治理总纲 §2.1），要么显式记录为后续 change，不在本 change 静默留白。

## 2. 目标与非目标

**目标**

1. 长任务运行期间：API health ≤500ms、读接口 p95 ≤1s（10 分钟窗口）、bot 命令 ack ≤10s（阈值可配置）。
2. 新增停止命令与 `max_runtime_seconds` 在配置上限（默认 30s）内于下一个检查点生效，并产生对应报告。
3. 长阶段心跳日志 ≤5 分钟一次；bot/API 可查询当前 run 状态。
4. worker 失败可上报、可诊断，API/bot 不受影响。
5. 全部为包装器层与少量服务检查点改动，业务语义零变化。

**非目标**

- 不拆分 `scheduler/tasks.py`（FR-08 独立推进）；
- 不引入 Celery/消息队列等通用任务平台（防过度工程）；
- 不处理 `financials.db` 77GB 体积与扫描查询索引（另记改进建议）；
- 不改变任何任务的业务验收口径与数据写入 owner。

## 3. 方案比较与决策

| 方案 | 说明 | 结论 |
|---|---|---|
| A. worker loop（选定） | 包装器把任务协程提交到独立线程 + 独立事件循环；主循环仅 await Future | 改动集中在 `scheduler/scheduler.py` 一处收口；业务代码零改动；GIL 下主循环仍按切换间隔（~5ms）获得时间片，响应性有界 |
| B. 进程隔离（条件升级） | 每个长任务在子进程运行，结果 pickle 返回，超时/停止经 kill 收口 | 隔离最强（防 OOM/失控 CPU），但需子进程内重建 DataManager/服务实例、`job_config` 传递改造；保留为 M4 升级路径 |
| C. 仅协作式让出 | 各服务内插 `await asyncio.sleep(0)` 检查点 | 需逐服务改造、无强制边界、CPU 段不可控；**否决为独立方案**，其检查点要求并入方案 A |

**决策：方案 A 为主**，落在 `parameterized_task` 包装器；方案 B 的设计要点保留于 §7，仅当 §6 度量不达标时按 M4 另立 change 实施。

### 3.1 执行模型（方案 A）

- 收口点（已代码核实）：cron 注册路径的 `parameterized_task`（`scheduler/scheduler.py:147` 起，内部经 `dependency_executor.run_job` 并以 `_SCHEDULER_ALREADY_TRACKED_PARAM` 防重复跟踪）、`/run` 立即执行（`run_job_now` → `scheduler.modify_job(next_run_time=now)`，仍由 APScheduler 触发同一 `parameterized_task`）、依赖组执行（`raw_job_runner=_run_configured_task_raw`）三条入口全部汇聚到 `_run_configured_task_raw`（`scheduler/scheduler.py:255` 起）。隔离只替换该函数内对 `task_func(**all_parameters)` 的调用与既有 `wait_for`（`scheduler/scheduler.py:302-305`）两处；`pre_run_notify`、依赖编排与依赖报告留在主循环 `parameterized_task`，不进 worker。
- worker 执行模型：每个 run 使用独立线程 + 独立事件循环（`asyncio.new_event_loop()` + `run_forever`，随 run 创建销毁）；提交采用"启动器协程"模式——worker loop 内 `create_task` 创建 asyncio.Task 并把句柄线程安全回传主循环，再 `await` 该 Task；主循环以 `asyncio.wrap_future` 转为 asyncio Future 后沿用 `asyncio.wait_for(timeout=max_runtime_seconds)` 外层超时。不设共享线程池容量上限，避免无关任务因池满互相排队；任务间真实并发度由调度器现有并发语义与 §3.6 写协调约束。
- 取消传导：不得依赖 `concurrent.futures.Future.cancel()`（任务开跑后恒返回 False）；超时与停止命令统一经已回传的 worker 侧 asyncio.Task 句柄执行 `worker_loop.call_soon_threadsafe(handle.cancel)`，在任务下一个 await 检查点落地。若 worker 线程卡死到取消无法落地，主循环超时仍按期判定该 run 失败并记录孤儿 worker 告警日志（可观测、不阻塞主循环；线程随进程重启回收）。
- 停止命令为**新增**操作入口（现状盘点：bot 仅有 /start /status /help /detail /run /backfill，无停止命令，运行中任务今天只能靠进程重启终止）：新增 bot 停止命令作用于 `running_tasks` 中的在跑 run，经上述同一取消通道注入；权限沿用现有 chat 白名单。
- `max_instances`、`run_id`、依赖组（dependency_results）、`job_config` 跟踪全部保持在主循环包装器内，语义不变。
- per-job escape hatch：确有主 loop 资源依赖、暂无法改造的任务可在 `config/05_scheduler.json` 中配置保留主 loop 执行，理由记录在配置注释与资源盘点文档；默认全部走 worker。
- 优雅停机：SIGTERM/systemctl 重启时 `shutdown()`（现有信号处理 `main.py _setup_signal_handlers` 之后）对在跑 worker 执行取消并有界等待；等待超时后随进程退出，由 SQLite 事务/WAL 保证中断安全（扫描期零写，apply 期未提交事务回滚）。

### 3.2 loop-bound 资源盘点（M1 spike 必产）

worker loop 上禁止使用主 loop 资源。M1 必须产出清单并逐任务标注（worker-safe / 需代理 / 主 loop 保留）：

| 资源 | 归属 | 处理 |
|---|---|---|
| Telethon 客户端 | 主 loop | 报告/通知一律经 `run_coroutine_threadsafe` 送回主 loop 执行（`_send_task_report`、`_send_task_report` 失败分支、scheduler 通知） |
| aiosqlite/SQLAlchemy task 池（api-db-resource-isolation 的 task workload） | 主 loop | worker 内需要 async DB 会话的任务：在 worker loop 内经现有连接管理自建 per-run 会话，或配置为主 loop 保留 |
| 共享 HTTP 会话（全局 aiohttp/httpx 客户端，若有） | 主 loop | per-run 实例化优先；确属全局单例的走代理或主 loop 保留 |
| 同步 sqlite3（`research/storage.py` contextmanager，per-call 连接） | 无 loop 绑定 | 线程安全，无需迁移；spike 中逐调用方确认连接生命周期 |

### 3.3 检查点契约

- 长阶段（候选扫描、批量循环）必须提供检查点：默认每 ≤30 秒或每 200 个候选至少一次 `await`（`asyncio.sleep(0)` 即可），同时更新进度状态；参数进配置。
- 首批真实调用方：`financial_disclosure_incremental_sync`（对账扫描）、`shareholder_reconciliation_sync`。两处接入后再抽最小共享 helper（满足治理总纲"两个真实调用方"门槛），不预先平台化。
- 取消语义：取消在下一检查点落地；最坏延迟 = 最长同步段 ≤ 检查点上限。此契约写进任务服务文档与测试。

### 3.4 报告与通知通道

`_send_task_report`（`scheduler/tasks.py:5219`）与 scheduler 错误/错过通知（`scheduler/scheduler.py:440-455` 的 `asyncio.create_task(bot.send_...)`）当前隐式假设主 loop。改造后：

- 任务侧 `_send_task_report` 调用经线程安全网关送回主 loop 执行，对任务代码保持原签名；
- 主 loop 侧通知路径不变；
- 网关等待模式：worker 侧协程必须以 `await asyncio.wrap_future(run_coroutine_threadsafe(...))` 非阻塞等待，禁止在 worker loop 协程内 `.result()` 阻塞（会冻结 worker loop，把取消延迟放大到整个投递时长——现有配置 Telegram 发送超时可达 45s，直接击穿取消上限）；主循环侧投递独立执行，不构成死锁。任务被取消时，已提交主循环的报告投递可自然完成，不回压 worker。

### 3.5 可观测性与失败隔离

- worker 侧维护 run 进度状态对象（task、run_id、phase、counters、elapsed、last_progress_at）；主 loop 侧定时器（≤5min）读取并打心跳日志。
- 状态查询：bot `/status` 与 API 运行状态端点暴露当前 run（含 max_runtime 剩余）。
- worker 内异常 → 包装器捕获 → 复用 `_format_job_exception`（`scheduler/scheduler.py:451`）与现有错误报告路径；API/bot 不受影响。worker 进程级崩溃（OOM 等）由 systemd 拉起整体进程，现状行为保留。

### 3.6 并行引入的写协调

现状单事件循环下各任务事实串行；worker 线程模型引入真实并行，必须防止新问题：

- M1 盘点"任务 × 数据库写入"矩阵；同一数据库的写动作按现有单一写入 owner 语义串行（同库排队或互斥），并复核 sqlite `busy_timeout`；
- 读路径依赖 WAL 并发读；跨数据库写入天然并行，不受限；
- 该矩阵同时是 §6 并发测试的输入。

### 3.7 与 enforce-sqlite-connection-closing 的协调

worker/loop 随 run 创建销毁，退出路径必须复用该 change 固化的 finally-close 连接工厂释放全部 sqlite 连接与 HTTP 会话，禁止引入新的裸连接；两 change 的测试互为补充——"连接数不随 run 泄露"列为本 change 的回归断言之一（同一故障根因：资源生命周期治理）。

## 4. 生产不变量（review 时逐条核对）

1. 单一业务实现：隔离只在包装器层；任务函数、业务服务不复制、不改语义。
2. `max_instances=1`、`run_id`、依赖组、`job_config` 语义不变。
3. 对账任务扫描期零写、只写变化或缺失的幂等语义不变；kill worker 不产生部分写入。
4. 配置驱动且归属正确：心跳间隔、检查点参数、escape hatch、取消等待上限属于 `config/05_scheduler.json` job 级配置，不改代码可调；API/bot 延迟 SLO 数值属于验收与观测口径（测试断言与监控文档），不写入调度器配置文件。
5. 日志沿用现有 logging 体系，不引入新 observability 框架。

## 5. 风险

| 风险 | 缓解 |
|---|---|
| GIL 争用导致纯 CPU 段 API p95 仍超标 | §6 度量把关；不达标走 M4 进程隔离（方案 B 要点已保留） |
| 个别任务隐藏的主 loop 资源依赖 | M1 盘点清单 + per-job escape hatch 兜底 |
| 报告/通知跨 loop 使用 telethon | 统一 marshalling 网关 + 并发测试覆盖 |
| 检查点遗漏导致取消延迟 | 检查点契约进测试（合成卡死任务断言 ≤30s） |
| 真实并行引入同库写竞争（现状单循环为事实串行） | §3.6 任务×数据库写入矩阵 + 同库写串行/busy_timeout；并发测试覆盖 |
| worker 线程卡死形成孤儿线程 | 主循环超时仍判定失败 + 孤儿 worker 告警日志（§3.1）；线程随进程重启回收 |

## 6. 验证与度量

1. 合成阻塞任务（CPU 密集 + 同步 sqlite 两类）并发测试：长任务运行期间断言 health/read/bot SLO（pytest-asyncio）。
2. 取消/超时测试：注入卡死任务，断言 `/stop` 与 `max_runtime_seconds` ≤30s 生效并产生对应报告。
3. 真实回放：`financial_disclosure_reconciliation_sync` 以有界 `target_symbols` 范围运行，期间实测 API p95、bot 响应、心跳日志与扫描期零写。
4. 数据安全：worker 中断后核对 `financials.db` 无部分写入、重跑幂等。
5. 全量现有单测回归通过。
6. 并发与停机：两个不同任务并行运行互不阻塞；任务运行中 `systemctl restart quote-system`，核对有界取消、无部分写入、重跑幂等。

## 7. M4 升级路径（仅当 §6 不达标）

scheduler 与 API 拆分为两个进程（`main.py` 拆分或双 systemd 单元）：

- 任务在调度进程内执行，bot 与调度器同进程（/run、/stop 需要的共享状态最小化），API 进程只做查询/提交（跨进程提交经调度进程暴露的内部触发接口，不复制业务循环，符合治理总纲 §2.1）；
- 子进程执行任务时：子进程内经同一工厂重建 DataManager/服务实例，结果 dict 经 pickle 返回；超时/停止经 kill 收口，WAL 机制保证中断安全；
- 注意：这是同一实现的执行位置隔离，不构成总纲 §2.1 禁止的"通过 subprocess 启动另一套生产实现"。
