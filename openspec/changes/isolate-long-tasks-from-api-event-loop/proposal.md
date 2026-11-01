# 隔离长任务与 API 共享事件循环

## Why

`python main.py full` 将 uvicorn API、AsyncIOScheduler 调度器和 Telethon Telegram bot 放在同一进程的同一个 asyncio 事件循环上（`main.py` `start_full_system` 中 `asyncio.create_task(server.serve())` 与后台调度器共存；`scheduler/scheduler.py:42` 使用 `AsyncIOScheduler`）。调度任务协程直接 `await` 在该循环上执行（`scheduler/scheduler.py:305` `await asyncio.wait_for(task_func(...))`），任务内部的同步 sqlite3/pandas 工作（`research/storage.py:14` 使用同步 `sqlite3`）会长时间占住循环。

2026-11-01 实测（任务 `financial_disclosure_reconciliation_sync` 对 77GB `financials.db` 做候选扫描，持续 2 小时以上、CPU ~84%）：

- API 完全不可用：`:8000` LISTEN 队列积压 172 个未 accept 连接，`curl /openapi.json` 8 秒超时；
- Telegram bot 基本失去响应：`/run`、`/stop` 均不可达，日志仅在任务 await 间隙零星出现（15:41、15:42、15:44、16:16）；
- `max_runtime_seconds` 超时用 `asyncio.wait_for` 实现且依赖同一循环（`scheduler/scheduler.py:302-305`），循环被饿住时超时自身不可靠；
- 任务无进度输出、无法停止，只能进程重启恢复。

现有 capability `api-db-resource-isolation` 解决的是反方向问题（防止 API 负载饿死调度任务的 DB 容量），不覆盖本问题。两者互补，本 change 新建独立 capability。

## What Changes

- 调度器任务包装器层统一改造：任务协程在独立 worker 事件循环（独立线程）中执行，主循环只等待结果；业务任务代码不复制、不改语义、执行位置单一收口。
- 报告与通知通道回收主循环：任务内 `_send_task_report` 及 bot 通知经线程安全 marshalling 回主循环，telethon 客户端、aiosqlite 引擎等 loop-bound 资源不跨 loop 使用。
- 停止与超时改造：`/stop` 与 `max_runtime_seconds` 通过线程安全 cancel 注入 worker loop；长同步段保证有 await 检查点，取消在配置上限（≤30s）内生效。
- 长任务可观测性：长阶段心跳日志（≤5 分钟）；当前运行状态（run_id、阶段、耗时、最近进度）可经 bot 与 API 查询。
- 失败隔离：worker 异常时任务标记 failed 并复用现有错误报告路径，API 与 bot 不受影响；worker 卡死成孤儿线程时告警可观测。
- 并行与停机正确性：同库写按现有单一写入 owner 串行（防止 worker 真实并行引入写竞争）；优雅停机对在跑 worker 取消并有界等待。
- 覆盖盘点：`api/routes.py` 绕过调度包装器直接调用 `data_manager.run_*` 的长任务入口（5 处）纳入 M1 盘点，收敛到同一执行路径或显式另立 change，不静默留白。

## Capabilities

### New Capabilities

- `scheduler-task-loop-isolation`: 长任务执行不得饿死共享事件循环；停止、超时、进度可观测与失败上报在隔离执行下仍然成立。

### Modified Capabilities

无。既有 capability 语义全部保持，本 change 只改变执行位置：`api-db-resource-isolation`（API 准入与 DB 池隔离，互补方向）、`scheduler`（任务配置与依赖组编排契约，编排仍留在主循环包装器）、`financial-operations-scheduler`（财务任务报告内容与渠道契约）。

## Impact

- 代码影响集中在 `scheduler/scheduler.py` 任务包装器与报告通道、`utils/task_manager/handlers.py` 停止/状态路径；长任务服务仅需提供检查点与心跳，首批真实调用方为 `financial_disclosure_reconciliation_sync` 与 `shareholder_reconciliation_sync`。
- 生产不变量保持：max_instances、run_id、依赖组与 job_config 语义不变；业务实现单一路径不变（治理总纲 §2.1）；对账扫描期零写与幂等语义不变；DB 写入 owner 不变。
- 与 `enforce-sqlite-connection-closing`（同日 fd 耗尽事故 change）协调：worker/loop 生命周期退出必须复用其固化的 finally-close 连接工厂，连接数不随 run 泄露列为本 change 回归断言。
- 非目标：不拆分 `scheduler/tasks.py`（FR-08 另行处理）；不引入 Celery 等通用任务平台；不处理 `financials.db` 体积与扫描索引问题（另记改进建议）；不改变任何任务的业务语义与验收口径。
