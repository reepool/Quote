# 长任务执行隔离使用说明

> 适用对象：运维与任务操作者（Telegram bot 用户、API 使用方）
>
> 对应能力：openspec change `isolate-long-tasks-from-api-event-loop`（scheduler-task-loop-isolation）
>
> 状态：代码已合入 master；**需重启 `quote-system` 服务后生效**。

## 1. 这是什么

此前 `python main.py full` 中 API、调度器、Telegram bot 共用同一个事件循环。长任务（如财务周度对账扫描 77GB 库）会把循环占住，导致：

- API 完全无响应（实测积压 172 个连接、curl 8 秒超时）；
- bot `/run`、`/status` 失灵；
- 任务超时兜底失效，唯一手段是重启整个服务（2026-11-01 事故）。

现在的行为：**已启用隔离的调度任务在独立线程 + 独立事件循环上运行**，API 与 bot 全程保持响应；任务可被 `/stop` 停止、有进度心跳、超时可可靠生效。

## 2. 哪些任务已启用

| 任务 | 状态 | 说明 |
|---|---|---|
| `financial_disclosure_reconciliation_sync` | ✅ worker 隔离 | 财务周度对账（77GB 库扫描的肇因任务） |
| `shareholder_reconciliation_sync` | ✅ worker 隔离 | 股东周度复核 |
| 其余全部任务 | 主循环（现状不变） | 逐链路审计后逐个启用（见 §7） |

判断依据：`config/05_scheduler.json` 中任务的 `parameters.run_on_main_loop`。未配置时跟随全局默认 `scheduler_config.worker_isolation.default_run_on_main_loop`（当前 `true` = 主循环）。

## 3. 常用操作

### Telegram

| 命令 | 作用 |
|---|---|
| `/run <task_id>` | 立即执行（与之前一致；worker 任务自动隔离运行） |
| `/run <task_id> key=value` | 带参数执行；未覆盖的键继承任务配置 `parameters` 默认值 |
| `/stop` | 列出当前 worker 隔离运行中的任务（run_id、阶段、耗时） |
| `/stop <task_id>` | 请求停止；**取消在任务下一个检查点生效（默认 ≤30 秒）**，完成后推送通知 |
| `/status` | 任务总览 + worker 运行详情（阶段、进度计数、耗时、超时剩余） |

注意：**主循环上运行的任务不受 `/stop` 支持**（回复会明确提示）。这类任务卡住时的处置仍是 `/restart_system` 或等待自然结束——这也是逐任务放量隔离的动机。

### API

```
GET /api/v1/system/worker-runs
```

返回当前所有 worker 运行实例：`run_id`、`phase`、`progress`、`elapsed_seconds`、`max_runtime_seconds`。

### 日志观测

长阶段超过 5 分钟时，`log/task.log` 出现心跳行（含阶段与进度计数）：

```
[Scheduler][Heartbeat] job=... run_id=... phase=reconciliation_scan elapsed=...s progress={'scanned': 4200, ...}
```

取消无法落地的卡死任务在 30s grace 后出现 `Orphaned worker thread` ERROR（仅告警，不影响 API/bot；线程随进程重启回收）。

## 4. 停止与超时语义（重要）

- worker 任务内的长循环按批次让出控制权（财务对账：每 200 个候选或 30 秒，可配置 `scan_checkpoint_*`）；`/stop` 与超时都在让出点生效。
- **最坏取消延迟 = 当前批次剩余时长**，正常 ≤30 秒；个别同步段（如 shareholder 单源批次）可能超过 30 秒，属已知边界。
- `max_runtime_seconds` 到期：取消注入 + 告警日志 + 任务按超时失败上报（报告渠道与格式不变）。
- 中断安全：对账任务扫描期零写入；应用期按候选逐个事务提交，中断后重跑幂等补齐。

## 5. 服务重启

`systemctl restart quote-system` 时，运行中的 worker 任务会被先取消并有界等待（默认 30s），再执行原有停机流程；数据库无部分写入风险（WAL + 幂等）。重启后用以下命令快速验收：

```
# 1) API 秒回
curl -m 3 http://127.0.0.1:8000/openapi.json

# 2) 有界回放（几分钟级，验证隔离链路 + 拿到报告）
# Telegram: /run financial_disclosure_reconciliation_sync target_symbols=600036,600519
# 期间另开终端验证 API 仍秒回、/status 显示 worker 运行详情、日志出现心跳
```

## 6. 配置速查（`config/05_scheduler.json`）

```jsonc
"scheduler_config": {
  "worker_isolation": {
    "default_run_on_main_loop": true,   // 未显式 opt-in 的任务维持主循环
    "heartbeat_seconds": 300,           // 心跳间隔
    "shutdown_grace_seconds": 30        // 停机/超时后的取消等待上限
  },
  "jobs": {
    "<task_id>": {
      "parameters": {
        "run_on_main_loop": false       // 显式启用 worker 隔离
      }
    }
  }
}
```

财务对账扫描批次粒度在 `config/03_data.json` 的 `financial_statements.disclosure_incremental_sync` 下：`scan_checkpoint_every_candidates`（默认 200）、`scan_checkpoint_interval_seconds`（默认 30）。

## 7. 如何给新任务启用隔离

按仓库治理协议（`framework_refactoring_program.md` FR-18），启用前完成该任务**调用链审计**并满足两条硬性检查：

1. 任务体及其调用链**不得直接调用 `self.bot.send_*`**（报告统一走 `_send_task_report` 的 marshalling；现存 17 处直接调用分布在 health_check、hkex 等任务体内，须先改道）；
2. 长同步段接入 `worker_context.yield_checkpoint`（禁止手写 `sleep(0)`+上报对）。

然后在 `05_scheduler.json` 该任务 `parameters` 中加 `"run_on_main_loop": false`，重启生效，并在该 change 的 design 附录 A 登记审计结论。

## 8. 已知边界

- `shareholder_reconciliation_sync` 的取消粒度为交易所/源批次，provider 单批次内部可能超 30 秒；
- `api/routes.py` 有 5 处直连 `data_manager.run_*` 的入口（special commodity 系列、futures calendar backfill），不经调度包装器、不受本能力约束，收敛由后续 change `converge-api-triggered-long-tasks` 承接；
- GIL 下极端纯 CPU 任务仍可能与主循环争用时间片（当前实测主循环调度间隙毫秒级，p95 达标；生产 p95 随首次回放采集，不达标按 design §7 升级进程隔离）。

## 9. 问题排查

| 现象 | 检查 |
|---|---|
| `/stop` 回复"主循环上运行" | 该任务未启用隔离——按 §7 流程审计后 opt-in |
| worker 运行但报告未收到 | 查 `log/task.log` 是否有 `report disabled or Telegram not enabled`；报告投递失败有独立告警行 |
| 怀疑任务卡死 | `/status` 看 `seconds_since_progress`；超 30s 无进展且非批次边界 → 日志搜 `Orphaned worker thread` |
| API 又变慢 | `/status` 确认卡住的是否主循环任务（未放量任务）；是则按既有方式处置，同时登记该任务进入审计队列 |
