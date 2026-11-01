## 1. M1 执行隔离落地

- [ ] 1.1 完成 loop-bound 资源盘点：telethon 客户端、aiosqlite/SQLAlchemy task 池、共享 HTTP 会话、同步 sqlite3 连接生命周期；产出每个任务的归属标注（worker-safe / 需代理 / 主 loop 保留）并记录到 design 附录。
- [ ] 1.2 在 `_run_configured_task_raw`（已核实的三入口唯一收口点：cron `parameterized_task`、`/run` 立即执行、依赖组执行）内实现 worker loop 执行：每 run 独立线程 + 独立事件循环（启动器协程模式，Task 句柄线程安全回传主循环），主循环仅 await 结果；`max_instances`/`run_id`/依赖组跟踪保持在主循环。
- [ ] 1.3 报告与通知通道实现线程安全 marshalling 回主循环：worker 侧以 `await asyncio.wrap_future(run_coroutine_threadsafe(...))` 非阻塞等待（禁止 `.result()` 阻塞 worker loop）；任务内不得跨 loop 使用 telethon 客户端；`_send_task_report` 对任务代码保持原签名。
- [ ] 1.4 取消通道：新增 bot 停止命令（现状无停止命令，运行中任务只能进程重启）作用于 `running_tasks` 在跑 run；与 `max_runtime_seconds` 统一经 Task 句柄 `call_soon_threadsafe(handle.cancel)` 注入（不得依赖 `concurrent.futures.Future.cancel()`）；检查点契约落地（默认 ≤30s，首个调用方按扫描节奏配置批次粒度，参数可配置）。
- [ ] 1.5 提供 per-job escape hatch 配置（`config/05_scheduler.json`）：确有主 loop 资源依赖的任务保留主 loop 执行，理由记录在配置注释与盘点文档。
- [ ] 1.6 盘点"任务 × 数据库写入"矩阵，落实同库写串行（现有单一写入 owner 语义）与 `busy_timeout` 复核，防止真实并行引入 SQLITE_BUSY。
- [ ] 1.7 优雅停机：`shutdown()` 对在跑 worker 取消并有界等待；退出路径复用 enforce-sqlite-connection-closing 固化的 finally-close 工厂释放连接与会话。
- [ ] 1.8 盘点 `api/routes.py` 直接调用 `data_manager.run_*` 的长任务入口（5 处），收敛到调度执行路径或显式另立 change，不静默留白。

## 2. M2 可观测性

- [ ] 2.1 worker 侧 run 进度状态对象 + 主循环侧心跳日志（≤5 分钟）：task、run_id、phase、进度计数、elapsed。
- [ ] 2.2 bot `/status` 与 API 运行状态端点暴露当前 run（run_id、task、phase、elapsed、最近进度、max_runtime 剩余）。
- [ ] 2.3 worker 异常的失败上报路径：任务标记 failed、复用 `_format_job_exception` 与现有错误报告格式，API/bot 不受影响；worker 卡死形成孤儿线程时输出告警日志。

## 3. M3 测试与回归

- [ ] 3.1 合成阻塞任务并发测试（CPU 密集 + 同步 sqlite 两类，含两个不同任务并行运行用例）：长任务运行期间 API health/read 与 bot 命令满足 SLO 断言。
- [ ] 3.2 取消/超时测试：注入卡死任务，断言 `/stop` 与 `max_runtime_seconds` 在 ≤30s 内生效并产生对应报告。
- [ ] 3.3 数据安全回归：worker 中断后核对对账扫描期零部分写入与幂等重跑，含任务运行中 `systemctl restart` 的中断安全验证。
- [ ] 3.4 真实回放验证：`financial_disclosure_reconciliation_sync`（有界 `target_symbols`）运行期间实测 API p95、bot 响应、心跳日志。
- [ ] 3.5 全量现有单测回归通过。

## 4. M4 条件升级（仅当 M1/M3 实测 SLO 不达标）

- [ ] 4.1 记录 GIL 争用实测数据（API p95 随纯 CPU 段的变化），形成升级决策依据。
- [ ] 4.2 不达标时按 design §7 要点另立 change 实施 scheduler 进程隔离；达标则明确关闭本节并记录实测数据。

## 5. 清理与交接

- [ ] 5.1 删除本次替代的旧执行路径与临时代码；更新运维文档（长任务运行/停止/重启操作口径）。
- [ ] 5.2 openspec validate 通过；对照治理总纲自查（单一实现、无平台化扩建、检查点 helper 满足两调用方门槛）后交 A 审核。
