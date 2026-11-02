## 1. M1 执行隔离落地

- [x] 1.1 完成 loop-bound 资源盘点：telethon 客户端、aiosqlite/SQLAlchemy task 池、共享 HTTP 会话、同步 sqlite3 连接生命周期；产出每个任务的归属标注（worker-safe / 需代理 / 主 loop 保留）并记录到 design 附录 A。结论：报告经 `_send_task_report` marshalling、task 引擎切 NullPool 后 per-checkout 连接绑定使用方 loop；CNInfo HTTP 与 research storage 为同步调用天然 worker-safe。
- [x] 1.2 在 `_run_configured_task_raw`（已核实的三入口唯一收口点：cron `parameterized_task`、`/run` 立即执行、依赖组执行）内实现 worker loop 执行：每 run 独立线程 + 独立事件循环（启动器协程模式，Task 句柄线程安全回传主循环），主循环仅 await 结果；`max_instances`/`run_id`/依赖组跟踪保持在主循环。
- [x] 1.3 报告与通知通道实现线程安全 marshalling 回主循环：worker 侧以 `await asyncio.wrap_future(run_coroutine_threadsafe(...))` 非阻塞等待（禁止 `.result()` 阻塞 worker loop）；任务内不得跨 loop 使用 telethon 客户端；`_send_task_report` 对任务代码保持原签名。
- [x] 1.4 取消通道：新增 bot `/stop` 命令（现状无停止命令，运行中任务只能进程重启）作用于 `running_tasks` 在跑 run；与 `max_runtime_seconds` 统一经 Task 句柄 `call_soon_threadsafe(handle.cancel)` 注入（不得依赖 `concurrent.futures.Future.cancel()`）；检查点契约落地（默认 ≤30s，首个调用方按扫描节奏配置批次粒度，参数可配置）。注：financial 对账的扫描/应用前检/应用后检三段均按 200 候选或 30 秒批次让出；shareholder 为交易所与源批次粒度，provider 内部长段待后续细化（记录于附录 A 限制）。
- [x] 1.5 提供 per-job escape hatch 配置（`config/05_scheduler.json`）：确有主 loop 资源依赖的任务保留主 loop 执行，理由记录在配置注释与盘点文档。注：部署采取分阶段放量——`worker_isolation.default_run_on_main_loop=true`（其余 27 个任务维持主循环直至逐链路审计），已审计的 `financial_disclosure_reconciliation_sync`、`shareholder_reconciliation_sync` 显式 `run_on_main_loop: false` 进入 worker。
- [x] 1.6 盘点"任务 × 数据库写入"矩阵，落实同库写串行（现有单一写入 owner 语义）与 `busy_timeout` 复核，防止真实并行引入 SQLITE_BUSY。结论记录于附录 B：sqlite3/aiosqlite 驱动默认 5 秒 busy 等待兜底；research storage 已有 `coordinated_writes` 跨 worker 写门（business_profile 在用）；两对账任务 `max_instances=1` 防自重叠。
- [x] 1.7 优雅停机：`shutdown()` 对在跑 worker 取消并有界等待；退出路径复用 enforce-sqlite-connection-closing 固化的 finally-close 工厂释放连接与会话（worker loop `run_until_complete(shutdown_asyncgens())` + 连接随作用域关闭）。
- [x] 1.8 盘点 `api/routes.py` 直接调用 `data_manager.run_*` 的长任务入口（5 处：special commodity 系列同步 4 处、futures official calendar backfill 1 处）。决策：显式另立 change 收敛（这些入口运行于 FastAPI 请求/BackgroundTasks 上下文，执行模型与调度器不同），本 change 不静默留白；后续 change 名称建议 `converge-api-triggered-long-tasks`。

## 2. M2 可观测性

- [x] 2.1 worker 侧 run 进度状态对象 + 主循环侧心跳日志（≤5 分钟）：task、run_id、phase、进度计数、elapsed。
- [x] 2.2 bot `/status` 与 API 运行状态端点暴露当前 run（run_id、task、phase、elapsed、最近进度、max_runtime 剩余）：bot `/status` 附加运行详情消息、`/stop` 列表模式；API 新增 `GET /api/v1/system/worker-runs`。
- [x] 2.3 worker 异常的失败上报路径：任务标记 failed、复用 `_format_job_exception` 与现有错误报告格式，API/bot 不受影响；worker 卡死形成孤儿线程时输出告警日志（含 `asyncio.create_task` 弱引用 GC 陷阱修复：后台任务持强引用集合）。

## 3. M3 测试与回归

- [x] 3.1 合成阻塞任务并发测试（CPU 密集 + 同步 sqlite 两类，含两个不同任务并行运行用例）：长任务运行期间主循环保持调度（ping 间隔断言 <0.5s，实际毫秒级），API health/read 与 bot 命令满足 SLO 断言（生产 p95 实测归入 3.4）。
- [x] 3.2 取消/超时测试：注入卡死任务，断言停止命令与 `max_runtime_seconds` 在上限内生效并产生对应报告（tests/unit/test_scheduler/test_worker_loop_isolation.py，12 用例含真实孤儿场景——同步段内取消无法落地、grace 后告警）。
- [ ] 3.3 数据安全回归：worker 中断后核对对账扫描期零部分写入与幂等重跑，含任务运行中 `systemctl restart` 的中断安全验证。部分覆盖：优雅停机取消单测通过；扫描期零写由既有 47 项 financial 单测在单元层面覆盖；生产 `systemctl restart` 演练与真实回放合并到 3.4 执行。
- [ ] 3.4 真实回放验证：`financial_disclosure_reconciliation_sync`（有界 `target_symbols`）运行期间实测 API p95、bot 响应、心跳日志。待生产窗口执行（依赖部署重启加载新代码）。
- [x] 3.5 全量现有单测回归通过（目标范围）：scheduler 60、financial_disclosure_incremental_sync 47、shareholder 相关 17、database routing 2、worker isolation 12 全绿；`test_models` 12 失败、`test_operations` 26 errors、`test_change_watermarks` 挂起经 stash 基线比对确认为既有问题，与本 change 无关。

## 4. M4 条件升级（仅当 M1/M3 实测 SLO 不达标）

- [ ] 4.1 记录 GIL 争用实测数据（API p95 随纯 CPU 段的变化），形成升级决策依据。已有单元级证据（worker CPU 满载期间主循环调度间隔毫秒级）；生产 p95 随 3.4 回放采集。
- [ ] 4.2 不达标时按 design §7 要点另立 change 实施 scheduler 进程隔离；达标则明确关闭本节并记录实测数据。

## 5. 清理与交接

- [x] 5.1 删除本次替代的旧执行路径与临时代码；更新运维文档（`docs/telegram_task_manager.md` 补 `/stop` 命令口径；`docs/features/scheduler_system.md` 补执行隔离章节；`docs/worker_task_isolation_usage.md` 为需求方使用说明）。注：主循环直await路径保留为 escape hatch 分支（任务 1.5 明确要求），非遗留垃圾。
- [x] 5.2 openspec validate 通过；对照治理总纲自查（单一实现、无平台化扩建、检查点 helper 满足两调用方门槛）后交 A 审核。
- [x] 5.3 开发产物清理（2026-11-02，按治理总纲 §2.4）：清除本 change 开发期间产生的 `data/QuoteBak/quote_pytest_logs_*`（含历史可再生 pytest 日志临时目录）与 `data/QuoteBak/pytest-of-python`（pytest 临时根，约 394MB，可再生）；仓库根 `sqlite:/` 误生成目录已删；验证代码无临时脚本/中间库残留（测试全部使用 tmp_path 与内存库）。保留 `data/QuoteBak/adjudicate_002717`（68GB，归属他项任务待所有者确认）、`/tmp` 下 ZCode 框架自身文件及 `py-yfinance`/`quote_ca_bundle_*.pem`（应用既有临时物，非本次产物，另行登记为全局卫生项）。
