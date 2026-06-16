# Quote System - 全系统配置参数名录大全

这份配置速查百科依据系统最新迭代标准全量提取。展示了系统的每一根执行筋骨和行为配置，可供你在修改 `config.json` 或者 `0x_*.json` 单体模块注入的时候仔细校准对照。

系统会自动在初始化阶段为各种为空或未标注的情况装载最安全预设妥协。
---

## sys_config

```json
{
  "sys_config": {
    "pid_file": "log/process.pid",
    "lock_file": "log/process.lock",
    "single_instance": true
  }
}
  ...
```

- **`pid_file`**: `str` (默认: `log/process.pid`) —— *系统 PID 文件存放路径，用于检测进程存活*
- **`lock_file`**: `str` (默认: `log/process.lock`) —— *系统实例锁文件路径，防止多个进程同时启动引发数据冲突*
- **`single_instance`**: `bool` (默认: `True`) —— *是否开启单实例强制锁（确保同一台机器只运行一个实例）*
- **`cleanup_on_startup`**: `bool` (默认: `True`) —— *如果是，将在引擎启动时清理上一次未正常退出的状态*
### sys_config.persistent_pids

```json
{
  "persistent_pids": {
    "scheduler": "log/scheduler.pid",
    "api_server": "log/api_server.pid"
  }
}
```

- **`scheduler`**: `str` (默认: `log/scheduler.pid`) —— *调度器专用 PID 记录路径*
- **`api_server`**: `str` (默认: `log/api_server.pid`) —— *API 服务器专用 PID 记录路径*
## logging_config

```json
{
  "logging_config": {
    "level": "INFO",
    "format": "[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d] - %(message)s",
    "date_format": "%Y-%m-%d %H:%M:%S"
  }
}
  ...
```

- **`level`**: `str` (默认: `INFO`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`format`**: `str` (默认: `[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d] - %(message)s`) —— *标准输出日志格式模板*
- **`date_format`**: `str` (默认: `%Y-%m-%d %H:%M:%S`) —— *日志日期时间打印的模式字符串*
### logging_config.file_config

```json
{
  "file_config": {
    "enabled": true,
    "directory": "log",
    "filename": "sys.log"
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`directory`**: `str` (默认: `log`) —— *文件写入主目录*
- **`filename`**: `str` (默认: `sys.log`) —— *日志文件相对名称*
#### file_config.rotation

```json
{
  "rotation": {
    "enabled": true,
    "type": "size",
    "max_bytes_mb": 50
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`type`**: `str` (默认: `size`) —— *归档轮转模式：如 size 表示依据体积轮转，time 表示依据日期轮转*
- **`max_bytes_mb`**: `int` (默认: `50`) —— *触发单体归档文件截断的最大兆字节数*
- **`backup_count`**: `int` (默认: `10`) —— *历史归档文件可保留的最大文件个数限制*
### logging_config.console_config

```json
{
  "console_config": {
    "enabled": true,
    "level": "INFO"
  }
}
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`level`**: `str` (默认: `INFO`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
### logging_config.modules

```json
{
  "modules": {
    "DataManager": {
      "level": "INFO",
      "enabled": true
    },
    "DataSource": {
      "level": "INFO",
      "enabled": true
    },
    "Database": {
      "level": "WARNING",
      "enabled": true
    }
  }
}
  ...
```

#### modules.DataManager

```json
{
  "DataManager": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
#### modules.DataSource

```json
{
  "DataSource": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
#### modules.Database

```json
{
  "Database": {
    "level": "WARNING",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `WARNING`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
#### modules.API

```json
{
  "API": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
#### modules.Scheduler

```json
{
  "Scheduler": {
    "level": "DEBUG",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `DEBUG`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
#### modules.Monitor

```json
{
  "Monitor": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
#### modules.TaskManager

```json
{
  "TaskManager": {
    "level": "DEBUG",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `DEBUG`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
#### modules.TelegramBot

```json
{
  "TelegramBot": {
    "level": "DEBUG",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `DEBUG`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
#### modules.AkShareSource

```json
{
  "AkShareSource": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
#### modules.YFinanceSource

```json
{
  "YFinanceSource": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
#### modules.Tushare

```json
{
  "Tushare": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
#### modules.Baostock

```json
{
  "Baostock": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
#### modules.tg_task_manager

```json
{
  "tg_task_manager": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
#### modules.Report

```json
{
  "Report": {
    "level": "DEBUG",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `DEBUG`) —— *控制日志的输出等级，可选 DEBUG/INFO/WARNING/ERROR*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
### logging_config.performance_monitoring

```json
{
  "performance_monitoring": {
    "enabled": true,
    "slow_operation_threshold_seconds": 2.0,
    "log_memory_usage": true
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`slow_operation_threshold_seconds`**: `float` (默认: `2.0`) —— *慢操作告警的延迟阈值（单位：秒）*
- **`log_memory_usage`**: `bool` (默认: `True`) —— *是否连带打印当前机器内存的耗散详情*
- **`collect_metrics`**: `bool` (默认: `True`) —— *是否持续统计日志埋点的 QPS 指标*
## telegram_config

```json
{
  "telegram_config": {
    "enabled": true,
    "api_id": "",
    "api_hash": ""
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`api_id`**: `str` (默认: ``) —— *Telegram 机器人客户端身份 ID（需向 Telegram 服务端申请）*
- **`api_hash`**: `str` (默认: ``) —— *与 api_id 成对绑定的数字摘要凭证*
- **`bot_token`**: `str` (默认: ``) —— *Telegram BotFather 提供给机器人的访问 Token*
- **`chat_id`**: `List` (默认: `['']`) —— *允许接收推送报警通知的授权用户 ID 或群组 ID 列表*
- **`session_name`**: `str` (默认: `MsgBot`) —— *持久化会话名称（会自动落地 .session 文件）*
- **`connection_retries`**: `int` (默认: `3`) —— *若网路异常或不可达，客户端尝试重拨次数*
- **`retry_delay`**: `int` (默认: `5`) —— *重播间的基础冷却等待时间（秒）*
- **`flood_sleep_threshold`**: `int` (默认: `60`) —— *超过 429 Too Many Requests 时最大允许睡眠重试时限（秒）*
- **`auto_reconnect`**: `bool` (默认: `True`) —— *网络丢包是否执行客户端层面静默重连*
- **`timeout`**: `int` (默认: `30`) —— *基础网络收发报文超时限制*
### telegram_config.intervals

```json
{
  "intervals": {
    "tg_msg_retry_interval": 3,
    "tg_msg_retry_times": 5,
    "tg_connect_timeout": 30
  }
}
  ...
```

- **`tg_msg_retry_interval`**: `int` (默认: `3`) —— *发送消息失败时下一次重试冷却时间*
- **`tg_msg_retry_times`**: `int` (默认: `5`) —— *一条消息最多尝试重发的次数上限*
- **`tg_connect_timeout`**: `int` (默认: `30`) —— *特定操作网络探测的超时设定*
- **`tg_auto_reconnect`**: `bool` (默认: `True`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`tg_max_reconnect_attempts`**: `int` (默认: `5`) —— *断口层级重试重连总最大次数*
- **`tg_reconnect_delay`**: `int` (默认: `10`) —— *连接失效状态下重连递延等候期*
## database_config

```json
{
  "database_config": {
    "db_path": "data/quotes.db",
    "backup_enabled": false,
    "backup_interval_days": 7
  }
}
```

- **`db_path`**: `str` (默认: `data/quotes.db`) —— *系统中主数据 SQLite 物理文件存放地点*
- **`backup_enabled`**: `bool` (默认: `False`) —— *是否开启独立文件定点自动硬备份*
- **`backup_interval_days`**: `int` (默认: `7`) —— *硬备份循环的默认时长周期*
## data_config

```json
{
  "data_config": {
    "data_dir": "data",
    "batch_size": 50,
    "download_chunk_days": 0
  }
}
  ...
```

- **`data_dir`**: `str` (默认: `data`) —— *各数据源产生的历史中间缓存与资源统一回收存放主目录*
- **`batch_size`**: `int` (默认: `50`) —— *分批读写或并发拉取记录时的内存切片单元大小*
- **`download_chunk_days`**: `int` (默认: `0`) —— *历史单次追溯拉取最大断点的跨度天数，设为 0 表示一次性加载所有不分阶段*
- **`max_concurrent_downloads`**: `int` (默认: `1`) —— *同时并发执行拉取任务的协程数上限限制（请考虑机器 IP 并发拦截率）*
- **`retry_times`**: `int` (默认: `3`) —— *遭遇各类异常时总体默认的尝试拉取重放拦截次数*
- **`retry_interval_seconds`**: `int` (默认: `5`) —— *下载出现报错异常时候的基础退坡缓冲等待期（秒）*

### data_config.daily_update_catchup

> 普通行情日更的小窗口追补配置。该配置接受新股主数据或行情源 T+1/T+2 滞后，但要求标的进入本地主表后，日更自动补齐上市日至当前日附近的小范围缺口，避免首日行情永久缺失。

```json
{
  "daily_update_catchup": {
    "enabled": true,
    "exchanges": ["SSE", "SZSE", "BSE"],
    "new_instrument_catchup_days": 10,
    "short_gap_catchup_days": 5,
    "sample_limit": 10
  }
}
```

- **`enabled`**: 是否启用普通日更追补逻辑。
- **`exchanges`**: 启用追补的市场，默认覆盖 A 股 `SSE/SZSE/BSE`。
- **`new_instrument_catchup_days`**: 对本地还没有任何行情的标的，从 `max(listed_date, target_date - N days)` 开始追补；避免新股迟到入库后漏掉上市首日行情。
- **`short_gap_catchup_days`**: 对已有行情但最新日期落后普通日更窗口的标的，从 `max(latest_quote_date, target_date - N days)` 开始追补。
- **`sample_limit`**: 日更报告中保留的追补样例数量。追补窗口被截断或缺少 `listed_date` 时会在报告中暴露，超过窗口的大缺口仍交给 `/backfill` 或 `find_gap_and_repair`。

### data_config.repair_universe_governance

> 历史缺口修复、月度完整性检查和区间回补的本地生命周期过滤配置。该配置只使用本地主数据和本地生命周期证据约束 repair universe，默认不刷新上游当前主数据。

```json
{
  "repair_universe_governance": {
    "enabled": true,
    "default_mode": "historical_backfill",
    "sample_limit": 10,
    "allow_lifecycle_filter_override": true,
    "max_override_instruments": 50,
    "max_override_limit": 50,
    "current_repair_requires_tradable": true,
    "allow_inactive_pre_lifecycle_history": true,
    "skip_index_lifecycle_states": [
      "calculation_terminated",
      "inactive",
      "stale_no_quote"
    ],
    "enable_local_stale_no_quote": true,
    "stale_no_quote_trading_days": 10,
    "max_index_no_data_failures_per_instrument": 1,
    "max_no_data_failures_per_instrument": 3,
    "stale_governance_continue_policy": "warn"
  }
}
```

- **`enabled`**: 是否在 repair/backfill universe 选择前启用本地生命周期过滤。
- **`default_mode`**: 默认运行模式。`historical_backfill` 允许在生命周期有效窗口内补历史；`current_repair` 会额外要求 active/tradable；`dry_run` 只输出过滤诊断；`override` 必须有明确边界。
- **`sample_limit`**: 报告中保留的生命周期跳过样例数量。
- **`allow_lifecycle_filter_override`**: 是否允许绕过生命周期过滤。开启后仍必须提供 `instrument_ids` 或小范围 `repair_universe_limit`。
- **`max_override_instruments` / `max_override_limit`**: override 模式的最大标的数或最大 limit，防止全市场绕过过滤后直接打到行情源。
- **`current_repair_requires_tradable`**: `current_repair` 模式下是否要求 `trading_status=1`。
- **`allow_inactive_pre_lifecycle_history`**: 历史模式下是否允许有明确 `delisted_date` 或指数生命周期边界的 inactive 标的补边界前历史。
- **`skip_index_lifecycle_states`**: 指数生命周期状态黑名单；命中后会按 evidence effective/last quote date 裁剪，完全越界则跳过。
- **`enable_local_stale_no_quote`**: 是否用本地最新行情日期识别长期无行情的 A 股指数，避免停编或失效指数反复触发 CNIndex/CSIndex/BaoStock/AkShare fallback。
- **`stale_no_quote_trading_days`**: 本地 stale-no-quote 判断阈值，默认与指数主数据治理保持一致。
- **`max_index_no_data_failures_per_instrument`**: `find_gap_and_repair` 中同一指数连续无数据失败后的本次任务熔断阈值。默认 1，成功补回任意 gap 后清零。该项不改变主数据状态，也不写生命周期证据，只避免健康性未知的指数在同一次运行中被拆成大量 gap 后反复请求外部源。
- **`max_no_data_failures_per_instrument`**: `find_gap_and_repair` 中同一非指数标的连续无数据失败后的本次任务熔断阈值。默认 3，成功补回任意 gap 后清零。
- **`stale_governance_continue_policy`**: 本地 lifecycle/evidence 不完整时的继续策略；当前实现以 warning 形式进入 `repair_universe.warnings`。

### data_config.instrument_master_sync

> A 股证券主数据同步底层配置。普通日更和通用主数据治理最终都复用这套 `sync_instrument_master()` 源优先级与写入规则。

```json
{
  "instrument_master_sync": {
    "enabled": true,
    "run_before_daily_update": true,
    "skip_for_backfill": true,
    "continue_on_failure": true,
    "timeout_sec": 180,
    "freshness_threshold_hours": 48,
    "pytdx_validation_enabled": false,
    "exchanges": ["SSE", "SZSE", "BSE"]
  }
}
```

- **`enabled`**: 是否允许 A 股主数据同步。
- **`run_before_daily_update`**: 普通行情日更是否前置主数据治理。
- **`skip_for_backfill`**: 历史补数是否默认跳过当前主数据刷新，避免当前股票池污染历史语义。
- **`continue_on_failure`**: 同步失败时是否允许下游任务继续使用本地已有股票池。
- **`timeout_sec`**: 单市场主数据同步超时秒数。
- **`freshness_threshold_hours`**: 本地主数据新鲜度窗口。
- **`pytdx_validation_enabled`**: 是否启用 pytdx 当前列表差异诊断；pytdx 不作为权威状态源。
- **`exchanges`**: 当前启用的 A 股市场，默认 `SSE/SZSE/BSE`。

### data_config.instrument_master_governance

> 通用证券主数据治理配置。该层覆盖行情、研究、财务和当前快照任务，统一调用 `DataManager.ensure_instrument_master_fresh()`；它不另起一套行情外的主数据源逻辑，而是包装并复用 `instrument_master_sync`。

```json
{
  "instrument_master_governance": {
    "enabled": true,
    "reuse_fresh_master": true,
    "skip_for_backfill": true,
    "continue_on_failure": true,
    "timeout_sec": 180,
    "freshness_threshold_hours": 48,
    "pytdx_validation_enabled": false,
    "supported_exchanges": ["SSE", "SZSE", "BSE", "HKEX"],
    "force_refresh_job_names": ["daily_data_update", "industry_standard_sync"],
    "current_job_names": [
      "daily_data_update",
      "hk_daily_data_update",
      "financial_summary_shadow_sync",
      "financial_statements_shadow_sync",
      "financial_disclosure_incremental_sync",
      "financial_disclosure_reconciliation_sync",
      "valuation_history_rebuild",
      "valuation_input_sync"
    ]
  }
}
```

- **`enabled`**: 是否启用通用主数据治理入口。
- **`reuse_fresh_master`**: 本地 `instruments.updated_at` 在新鲜度窗口内时，前置治理可以复用本地状态，不重复请求上游主数据列表。
- **`force_refresh_job_names`**: 强制刷新策略列表，不是“有主数据前置的任务列表”。任务名命中该列表时，即使本地主数据仍在新鲜度窗口内，也会跳过本地复用并执行实际主数据同步；`daily_data_update` 默认在此列表内，保证 A 股行情主日更使用最新 current universe；`industry_standard_sync` 默认在此列表内，避免新股进入股票池晚于申万 source file 变更时造成 current membership 缺口。
- **`skip_for_backfill`**: 历史、回补和 point-in-time 类任务是否默认跳过当前主数据刷新。
- **`continue_on_failure`**: 治理失败后是否允许业务任务继续使用本地股票池，并在报告中暴露 warning/error。
- **`supported_exchanges`**: 已启用主数据策略的市场；`SSE/SZSE/BSE` 走 A 股既有策略，`HKEX` 走港股专用策略，不复用 A 股 `sync_instrument_master()` 规则。
- **`current_job_names`**: 参与前置治理的当前任务清单，用于配置审计和运维可读性；是否真实刷新上游仍取决于 freshness 与 `force_refresh_job_names`。

### data_config.master_governance（模块化配置）

> 模块化主数据治理的当前配置形态。业务任务通过 `job_requirements` 声明需要哪些主数据 scope；`instrument_master_governance` 保留为兼容配置。同一任务同时存在新旧配置时，以 `master_governance.job_requirements` 为准。

```json
{
  "master_governance": {
    "enabled": true,
    "policies": {
      "a_share_stock": {"enabled": true},
      "a_share_index": {"enabled": true},
      "hkex_instrument": {"enabled": true}
    },
    "job_requirements": {
      "daily_data_update": [
        {
          "scope": "a_share_stock",
          "mode": "force_refresh",
          "exchanges": ["SSE", "SZSE", "BSE"],
          "instrument_types": ["stock"],
          "continue_on_error": true
        },
        {
          "scope": "a_share_index",
          "mode": "freshness_gated",
          "exchanges": ["SSE", "SZSE"],
          "instrument_types": ["index"],
          "continue_on_error": true
        }
      ],
      "hk_daily_data_update": [
        {
          "scope": "hkex_instrument",
          "mode": "lifecycle_write",
          "exchanges": ["HKEX"],
          "instrument_types": ["stock"],
          "continue_on_error": true
        }
      ]
    }
  }
}
```

- **`policies`**: 可用治理能力，而不是 scheduler job 清单。第一阶段包括 `a_share_stock`、`a_share_index`、`hkex_instrument`。
- **`job_requirements`**: 业务任务的主数据前置需求。一个任务可以声明多个 scope，例如 A 股普通日更同时需要股票和指数主数据。
- **`scope`**: 治理能力标识。`a_share_stock` 包含 `SSE/SZSE/BSE` 股票；BSE 不需要独立 scheduler 任务才能被纳入 A 股股票治理。
- **`mode`**: 执行策略，按 scope 校验。A 股 stock/index 支持 `force_refresh`、`freshness_gated`、`audit_only`、`skip_for_backfill`；HKEX 支持 `audit_only`、`safe_write`、`lifecycle_write`、`skip_for_backfill`。`safe_write/lifecycle_write` 只允许用于 `hkex_instrument`，不能用于 A 股 scope；`force_refresh/freshness_gated` 也不会覆盖 HKEX 的生命周期写入模式。
- **兼容优先级**: 新配置存在时使用新配置；缺失时回退到 `instrument_master_governance.force_refresh_job_names/current_job_names`，并在治理结果中标记 legacy fallback。

### data_config.index_master_governance

> A 股指数主数据治理配置。该层复用日更前置治理入口，但生命周期证据、停编状态和官方指数源独立于股票主数据同步，避免把指数停编误当作股票退市。

```json
{
  "index_master_governance": {
    "enabled": true,
    "run_before_daily_update": true,
    "exchanges": ["SSE", "SZSE"],
    "official_sources": ["cnindex", "csindex"],
    "freshness_threshold_hours": 48,
    "stale_no_quote_trading_days": 10,
    "skip_stale_no_quote": true,
    "write_stale_no_quote": false,
    "allow_series_inference": true,
    "master_admission": {
      "canonical_key": "instrument_id",
      "duplicate_key_policy": "skip_ambiguous",
      "ambiguous_duplicate_action": "skip",
      "collapse_identical_duplicates": true,
      "conflict_signature_fields": [
        "name",
        "market",
        "industry",
        "sector",
        "metadata.cni_code",
        "metadata.full_name"
      ]
    },
    "sample_limit": 10,
    "timeout_sec": 120
  }
}
```

- **`enabled`**: 是否启用指数主数据治理。
- **`run_before_daily_update`**: 普通日更读取 active universe 前是否运行指数治理。
- **`official_sources`**: 官方源优先级。当前 `cnindex` 覆盖国证/深证指数清单、公告证据和官方日线；`csindex` 先提供中证/SSE 基础信息适配点，完整清单能力未启用时会在报告中降级提示。
- **`stale_no_quote_trading_days`**: 本地最新行情超过该窗口时进入 stale 诊断。默认只报告，不直接写停用。
- **`write_stale_no_quote`**: 是否把长期无行情指数写成 `stale_no_quote` 并从日更 universe 排除。默认 `false`，避免仅凭行情缺口误杀临时停发或源异常。
- **`allow_series_inference`**: 是否允许用官方公告直接代码和全收益/价格指数配对关系做保守推断，例如 `980055` 停编且 `480055` 官方行情止于生效日前后时，将 `480055.SZ` 标记为 `series_inferred`。
- **CNIndex 主键语义**: `指数代码` 不是本地行情主键。只有官方列表提供 `深交所行情代码` 时，指数才写入行情型 `<行情代码>.SZ` 并参与日更；没有行情代码的 CNIndex 行写为 `.CNI` 或 `CNI<指数代码>.SZ` metadata-only 身份，保留主数据但不触发每日行情空抓，也不得覆盖同代码股票。治理前快照或本地已存官方 metadata 证明无行情代码时，同源、同 6 位代码的遗留行情型 key 会被标记为 `status=metadata_only,is_active=0,trading_status=0` 并从 `tradable_only` 日更 universe 排除。`metadata_only` 不是停编结论，而是“无交易所行情代码”的 eligibility 状态；若官方后续补充行情代码，下一次治理应按新的行情型 key 恢复。
- **`master_admission`**: 官方指数主数据写入 `instruments` 前的准入规则。默认以 `instrument_id` 作为 canonical key；同 key 且冲突签名不同的官方行视为代码命名空间歧义，按 `ambiguous_duplicate_action=skip` 跳过，不把多个不同指数压成一个本地主键。`conflict_signature_fields` 支持点路径，例如 `metadata.cni_code`。
- **`sample_limit`**: Telegram 日报中的指数治理样例数量上限，详细证据保存在 `index_lifecycle_evidence`。

### 任务前后置依赖配置

任务前后置关系通过 `config/05_scheduler.json` 的 job-level `dependencies` 配置声明，由调度器统一执行。当前券商专项财务任务 `broker_risk_control_incremental_sync` 已作为 `financial_disclosure_incremental_sync` 成功后的 `post_success` 后置节点运行；该任务本身保持 `manual_only=true`，不会单独注册 cron，但允许被依赖执行器触发。详见 `docs/development/scheduler_task_dependency_dag.md` 与 OpenSpec change `configure-scheduler-task-dependency-dag`。

配置形态：

```json
{
  "scheduler_config": {
    "jobs": {
      "financial_disclosure_incremental_sync": {
        "dependencies": {
          "post_success": [
            {
              "group_id": "financial_industry_supplements",
              "mode": "parallel",
              "jobs": [
                {
                  "job_id": "broker_risk_control_incremental_sync",
                  "inherit": ["exchanges", "dry_run"],
                  "timeout_seconds": 7200,
                  "failure_policy": "degrade_parent"
                }
              ]
            }
          ]
        }
      }
    }
  }
}
```

语义要求：

- `pre_success`：前置任务成功后才启动主任务。
- `post_success`：主任务成功后启动后置任务；主任务失败则不启动。
- `post_always`：主任务成功或失败后均执行，适合清理和审计。
- `mode=parallel`：同组任务并行执行。
- `mode=serial`：同组任务按配置顺序串行执行。
- `manual_only=true` 任务不注册独立 cron，但可被依赖图自动触发。

### data_config.hkex_instrument_master_sync

> HKEX 专用主数据策略配置。当前生产配置为 `lifecycle_write`，由官方 active/delisted/suspension 证据驱动 active/suspended/delisted 状态；`audit_only` 仍用于人工复核 API 和只读诊断。

```json
{
  "hkex_instrument_master_sync": {
    "enabled": true,
    "mode": "lifecycle_write",
    "timeout_sec": 60,
    "quote_stale_days": 14,
    "official_securities_list_url": "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx",
    "official_securities_list_file": "",
    "hkexnews_active_list_url": "https://www.hkexnews.hk/ncms/script/eds/activestock_sehk_e.json",
    "hkexnews_active_list_file": "",
    "hkexnews_delisted_list_url": "https://www.hkexnews.hk/ncms/script/eds/inactivestock_sehk_e.json",
    "hkexnews_delisted_list_file": "",
    "hkexnews_suspension_main_board_url": "https://www2.hkexnews.hk/-/media/HKEXnews/Homepage/Exchange-Reports/Prolonged-Suspension-Status-Report/psuspenrep_mb.pdf",
    "hkexnews_suspension_gem_url": "https://www2.hkexnews.hk/-/media/HKEXnews/Homepage/Exchange-Reports/Prolonged-Suspension-Status-Report/psuspenrep_gem.pdf",
    "hkexnews_suspension_main_board_file": "",
    "hkexnews_suspension_gem_file": "",
    "manual_review_file": "data/hkex_manual_review.json",
    "akshare_spot_file": "",
    "eastmoney_profile_file": "",
    "fetch_supplemental_live": false,
    "write_review_discrepancies": true,
    "allowed_product_types": ["ordinary_equity", "reit", "etf"]
  }
}
```

- **`mode`**: `audit_only` 只出差异，`safe_write` 写官方 in-scope 新标的、安全范围内存量标的的非破坏性字段和辅助 metadata，`lifecycle_write` 才允许官方证据驱动 active/suspended/delisted 状态变更。
- **`official_*_url/file`**: 官方 HKEX / HKEXnews 快照来源；`file` 用于本地审计或离线 fixture，`url` 用于带超时的线上获取。当前主 active 源为 HKEX `ListOfSecurities.xlsx`，辅 active 源为 HKEXnews `activestock_sehk_e.json`，delisted 源为 HKEXnews `inactivestock_sehk_e.json`。
- **`hkexnews_suspension_*_url/file`**: HKEXnews 月度 prolonged suspension PDF，作为停牌复核来源；自动解析依赖 `pypdf`，失败时报告 warning，不作为停牌证据。
- **`manual_review_file`**: 人工复核结论回灌文件，默认 `data/hkex_manual_review.json`，支持 JSON/CSV，文件不存在时按空 review 处理。字段建议包含 `instrument_id`/`code`、`action`、`effective_date`、`reason`、`evidence_url`、`reviewed_by`；`action=delisted/suspended/active` 会在 `lifecycle_write` 下作为 reviewed lifecycle evidence 生效。API 与 Telegram `/hkex_review` 命令都会写入该文件。
- **`akshare_spot_file` / `eastmoney_profile_file` / `fetch_supplemental_live`**: 补充源配置，仅用于候选发现、字段补充和差异诊断，不作为生命周期权威。
- **`allowed_product_types`**: 进入研究 universe 的 HKEX 产品类型；默认保留普通股、REIT、ETF。
- **`quote_stale_days`**: 本地行情过旧诊断窗口；yfinance/本地行情诊断不会产生生命周期 mutation candidates。

### data_config.default_start_years

> **💠 节点释源**: 若无强制历史拉取点，新部署拉取各类股票/指数必须强制从哪一年起跑

```json
{
  "default_start_years": {
    "SSE": 1990,
    "SZSE": 1990,
    "BSE": 2014
  }
}
  ...
```

- **`SSE`**: `int` (默认: `1990`) —— *上海证券交易所启动参数/预设映射标签*
- **`SZSE`**: `int` (默认: `1990`) —— *深圳证券交易所启动参数/预设映射标签*
- **`BSE`**: `int` (默认: `2014`) —— *北京证券交易所启动参数/预设映射标签*
- **`HKEX`**: `int` (默认: `1990`) —— *香港交易所联交所*
- **`NASDAQ`**: `int` (默认: `1990`) —— *纳斯达克挂牌市场*
- **`NYSE`**: `int` (默认: `1990`) —— *纽约证券交易所相关版块*
### data_config.market_presets

> **💠 节点释源**: 快捷的组合版块查询映射：一键调用所有包含的证券市场标签

```json
{
  "market_presets": {
    "a_shares": [
      "SSE",
      "SZSE",
      "BSE"
    ],
    "hk_stocks": [
      "HKEX"
    ],
    "us_stocks": [
      "NASDAQ",
      "NYSE"
    ]
  }
}
  ...
```

- **`a_shares`**: `List` (默认: `['SSE', 'SZSE', 'BSE']`) —— *中国 A 股或相关混合市场打包快捷调取字眼*
- **`hk_stocks`**: `List` (默认: `['HKEX']`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`us_stocks`**: `List` (默认: `['NASDAQ', 'NYSE']`) —— *美、港、及海外权益市场群组包装映射定义类*
- **`all_markets`**: `List` (默认: `['SSE', 'SZSE', 'BSE', 'HKEX', 'NASDAQ', 'NYSE']`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`mainland`**: `List` (默认: `['SSE', 'SZSE', 'BSE']`) —— *中国 A 股或相关混合市场打包快捷调取字眼*
- **`overseas`**: `List` (默认: `['HKEX', 'NASDAQ', 'NYSE']`) —— *美、港、及海外权益市场群组包装映射定义类*
- **`chinese`**: `List` (默认: `['SSE', 'SZSE', 'BSE', 'HKEX']`) —— *中国 A 股或相关混合市场打包快捷调取字眼*
- **`global`**: `List` (默认: `['NASDAQ', 'NYSE']`) —— *美、港、及海外权益市场群组包装映射定义类*
## data_sources

```json
{
  "data_sources": {
    "a_stock": {
      "enabled": true
    },
    "hk_stock": {
      "enabled": false
    },
    "us_stock": {
      "enabled": false
    }
  }
}
```

### data_sources.a_stock

```json
{
  "a_stock": {
    "enabled": true
  }
}
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
### data_sources.hk_stock

```json
{
  "hk_stock": {
    "enabled": false
  }
}
```

- **`enabled`**: `bool` (默认: `False`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
### data_sources.us_stock

```json
{
  "us_stock": {
    "enabled": false
  }
}
```

- **`enabled`**: `bool` (默认: `False`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
## data_sources_config

```json
{
  "data_sources_config": {
    "pytdx": {
      "enabled": true,
      "exchanges_supported": [
        "a_stock"
      ],
      "instrument_types_supported": [
        "stock"
      ],
      "max_requests_per_minute": 6000,
      "max_requests_per_hour": 300000,
      "max_requests_per_day": 5000000,
      "retry_times": 3,
      "retry_interval": 1.0
    },
    "baostock": {
      "enabled": true,
      "exchanges_supported": [
        "a_stock"
      ],
      "instrument_types_supported": [
        "stock",
        "index"
      ],
      "max_requests_per_minute": 600,
      "max_requests_per_hour": 8000,
      "max_requests_per_day": 80000,
      "retry_times": 5,
      "retry_interval": 5.0
    },
    "akshare": {
      "enabled": true,
      "exchanges_supported": [
        "a_stock",
        "hk_stock",
        "us_stock"
      ],
      "max_requests_per_minute": 120,
      "max_requests_per_hour": 3800,
      "max_requests_per_day": 50000,
      "retry_times": 5,
      "retry_interval": 3.0
    },
    "yfinance": {
      "enabled": true,
      "exchanges_supported": [
        "hk_stock",
        "us_stock"
      ],
      "max_requests_per_minute": 30,
      "max_requests_per_hour": 1500,
      "max_requests_per_day": 10000,
      "retry_times": 3,
      "retry_interval": 2.0
    },
    "tushare": {
      "enabled": false,
      "exchanges_supported": [
        "a_stock"
      ],
      "max_requests_per_minute": 200,
      "max_requests_per_hour": 1000,
      "max_requests_per_day": 2000,
      "retry_times": 3,
      "retry_interval": 1.0,
      "token": ""
    }
  }
}
  ...
```

`data_sources_config` 现在只描述单个 source 的能力边界和连接/限流参数，不再承担“谁是主源”的路由职责。  
日线、品种列表、交易日历、复权因子等主备顺序统一由 `routing` 配置决定。

通用字段说明：

- **`enabled`**: `bool`，是否启用该 source
- **`exchanges_supported`**: `List[str]`，该 source 会为哪些 region 实例化，例如 `a_stock`、`hk_stock`、`us_stock`
- **`instrument_types_supported`**: `List[str]`，可选能力边界；声明后会参与启动期路由校验，例如 `stock`、`index`
- **`max_requests_per_minute` / `max_requests_per_hour` / `max_requests_per_day`**: 速率限制
- **`retry_times` / `retry_interval`**: 默认重试参数
- 其余字段如 `token`、`proxy`、`connection_timeout`、`batch_size` 等，属于 source 自身连接参数

### data_sources_config.pytdx

```json
{
  "pytdx": {
    "enabled": true,
    "exchanges_supported": [
      "a_stock"
    ],
    "instrument_types_supported": [
      "stock"
    ]
  }
}
  ...
```

- **`instrument_types_supported`**: 当前建议为 `['stock']`，因为指数日线不走 `pytdx`

### data_sources_config.baostock

```json
{
  "baostock": {
    "enabled": true,
    "exchanges_supported": [
      "a_stock"
    ],
    "instrument_types_supported": [
      "stock",
      "index"
    ]
  }
}
  ...
```

- **`instrument_types_supported`**: 当前建议包含 `stock` 和 `index`，以支持 A 股指数日线

### data_sources_config.akshare

```json
{
  "akshare": {
    "enabled": true,
    "exchanges_supported": [
      "a_stock",
      "hk_stock",
      "us_stock"
    ],
    "hk_adjustment_factors": {
      "api": "stock_hk_daily",
      "factor_adjust": "qfq-factor",
      "base_date": "1900-01-01",
      "require_base_date": true,
      "fallback_on_invalid_response": true,
      "rounding_tolerance": 0.0001,
      "diagnostic_hfq_check_enabled": false
    }
  }
}
  ...
```

- **`instrument_types_supported`**: 未声明时默认表示“不额外限制”
- **`hk_adjustment_factors`**: 港股复权因子算法配置，仅影响 `HKEX` 因子路径，不影响 A 股 AkShare 因子。默认使用新浪 `stock_hk_daily(adjust="qfq-factor")` 获取稀疏前复权乘法因子，再转换为项目统一的后复权累积因子。`fallback_on_invalid_response=true` 表示接口异常、缺少基准行或响应不可解析时返回 `None`，允许 `routing.factor.HKEX.fallback` 接管；有效空结果仍返回 `[]`，不触发 fallback。

### data_sources_config.yfinance

```json
{
  "yfinance": {
    "enabled": true,
    "proxy_patch": {
      "enabled": true,
      "gateway": "101.201.173.125",
      "auth_token": "从配置读取的 TOKEN",
      "retry": 30
    },
    "proxy": {
      "http": "",
      "https": ""
    },
    "exchanges_supported": [
      "hk_stock",
      "us_stock"
    ]
  }
}
  ...
```

- **`proxy_patch`**: 通过 `akshare_proxy_patch.install_yfinance_patch()` 为 `yfinance` 安装代理补丁；必须在 `import yfinance` 前执行。当前项目在 `data_sources/yfinance_source.py` 顶部调用统一 runtime 自动安装。一般不要为 yfinance 覆盖 `hook_domains`，保持上游默认值即可。
- **`proxy`**: 旧 HTTP 代理配置，仅对底层 Yahoo chart API fallback 生效；当 `proxy_patch.enabled=true` 时，`yfinance` 库路径优先使用 `proxy_patch`，避免两套代理叠加。
- 可用性验证：运行 `python scripts/validate_yfinance_proxy_patch.py --symbol AAPL --start 2017-01-01 --end 2017-04-30` 或兼容脚本 `python scripts/validate_yfinance_proxy_patch_1.py --symbol AAPL --start 2017-01-01 --end 2017-04-30`。
- 生产链路验证：运行 `python scripts/validate_yfinance_source_live.py --source-name yfinance_us_stock --symbol AAPL --exchange NASDAQ --start 2017-01-01 --end 2017-04-30`，确认项目内 `YFinanceSource` 也能通过统一 runtime 正常抓取。
- 统一运行时、影响边界与重启说明见 [proxy_patch_runtime.md](/home/python/Quote/docs/development/proxy_patch_runtime.md)。

## routing

```json
{
  "routing": {
    "daily": {
      "SSE": {
        "stock": ["pytdx", "baostock", "akshare"],
        "index": ["baostock", "akshare"]
      },
      "SZSE": {
        "stock": ["pytdx", "baostock", "akshare"],
        "index": ["baostock", "akshare"]
      },
      "BSE": {
        "stock": ["pytdx", "akshare"],
        "index": ["akshare"]
      },
      "HKEX": {
        "stock": ["akshare", "yfinance"]
      }
    },
    "daily_behavior": {
      "default": {
        "stock": {
          "skip_backup_on_empty_short_range": true
        },
        "index": {
          "skip_backup_on_empty_short_range": false
        }
      }
    },
    "instrument_list": {
      "a_stock": ["baostock"],
      "hk_stock": ["akshare"],
      "us_stock": ["akshare"]
    },
    "calendar": {
      "a_stock": ["baostock"],
      "hk_stock": ["akshare"],
      "us_stock": ["akshare"]
    },
    "factor": {
      "SSE": {"primary": "baostock", "validator": "tdx_xdxr", "fallback": "akshare", "daily_sync_enabled": true, "maintenance_sync_enabled": true},
      "SZSE": {"primary": "baostock", "validator": "tdx_xdxr", "fallback": "akshare", "daily_sync_enabled": true, "maintenance_sync_enabled": true},
      "BSE": {"primary": "akshare", "validator": "tdx_xdxr", "fallback": null, "daily_sync_enabled": true, "maintenance_sync_enabled": true},
      "HKEX": {"primary": "akshare", "validator": null, "fallback": "yfinance", "daily_sync_enabled": false, "maintenance_sync_enabled": true},
      "NASDAQ": {"primary": "yfinance", "validator": null, "fallback": null, "daily_sync_enabled": false, "maintenance_sync_enabled": false},
      "NYSE": {"primary": "yfinance", "validator": null, "fallback": null, "daily_sync_enabled": false, "maintenance_sync_enabled": false}
    }
  }
}
  ...
```

`routing` 是当前数据源选择的单一真相来源。

- **`routing.daily.<exchange>.<instrument_type>`**: `List[str]`
  第一个元素是主源，后续元素按顺序作为 fallback。缺失配置时，`DataSourceFactory` 会在启动或调用时抛出明确配置错误，不再按 region 做隐式推断。
- **`routing.daily_behavior.<default|exchange>.<instrument_type>.skip_backup_on_empty_short_range`**: `bool`
  控制短区间查询中“主源空结果是否跳过 fallback”。当前默认行为是 `stock=true`、`index=false`。
- **`routing.instrument_list.<region>`**: `List[str]`
  指定品种列表抓取链，按 region 配置。
- **`routing.calendar.<region>`**: `List[str]`
  指定交易日历抓取链，按 region 配置。
- **`routing.factor.<exchange>`**: `Object`
  指定复权因子路由，支持 `primary`、`validator`、`fallback`、`daily_sync_enabled`、`maintenance_sync_enabled`。其中 `primary` 是正式抓取与回补入口实际使用的主源；`validator=tdx_xdxr` 是特殊值，仅用于 A 股旁路审计，不参与主抓取结果选择。TDX 审计会把权威源累计因子先推导为单日因子，再与 TDX XDXR 单日因子比较；审计结果只写 `adjustment_factors_tdx`，不会覆盖生产 `adjustment_factors`。`daily_sync_enabled=false` 表示该交易所在日更任务中跳过 Phase 2 因子同步；`maintenance_sync_enabled=true` 表示该交易所会进入每周维护任务的周度因子同步。港股当前关闭日更因子、开启周维护因子，以避免交易日日更被 3000+ 标的扫描拖慢。

当前生产配置的关键路由示例：

- A 股股票日线：`SSE/SZSE stock = pytdx -> baostock -> akshare`
- A 股指数日线：`SSE index = csindex -> cnindex -> baostock -> akshare`，`SZSE index = cnindex -> csindex -> baostock -> akshare`；官方源临时不可用或不覆盖的 active 指数继续走 BaoStock/AkShare fallback。
- 北交所股票日线：`BSE stock = pytdx -> akshare`
- 港股股票日线：`HKEX stock = akshare -> yfinance`
- A 股品种列表与交易日历：`baostock`
- 港股/美股品种列表与交易日历：`akshare`
- A 股复权因子主源：`baostock`
- 港股复权因子主源：`akshare`（生产算法使用 `qfq-factor` 转换为乘法累积因子；`yfinance` 仅作为失败时的少量补位）
- 美股复权因子主源：`yfinance`（当前尚未正式上线）

## exchange_rules

```json
{
  "exchange_rules": {
    "exchange_mapping": {
      "SSE": "a_stock",
      "SZSE": "a_stock",
      "BSE": "a_stock",
      "HKEX": "hk_stock",
      "NASDAQ": "us_stock",
      "NYSE": "us_stock"
    },
    "symbol_rules": {
      "SZSE": 6,
      "SSE": 6,
      "BSE": 6,
      "HKEX": [
        4,
        5
      ],
      "NASDAQ": null,
      "NYSE": null
    },
    "symbol_start_with": {
      "SSE": [
        "600",
        "601",
        "603",
        "688",
        "900"
      ],
      "SZSE": [
        "000",
        "001",
        "002",
        "300",
        "200"
      ],
      "BSE": [
        "43",
        "83",
        "87"
      ],
      "HKEX": [
        "0",
        "00",
        "000"
      ]
    }
  }
}
```

### exchange_rules.exchange_mapping

> **💠 节点释源**: 将框架高阶指令传递的简拼交易所代号一律映射为底层执行代码类别组

```json
{
  "exchange_mapping": {
    "SSE": "a_stock",
    "SZSE": "a_stock",
    "BSE": "a_stock"
  }
}
  ...
```

- **`SSE`**: `str` (默认: `a_stock`) —— *上海证券交易所启动参数/预设映射标签*
- **`SZSE`**: `str` (默认: `a_stock`) —— *深圳证券交易所启动参数/预设映射标签*
- **`BSE`**: `str` (默认: `a_stock`) —— *北京证券交易所启动参数/预设映射标签*
- **`HKEX`**: `str` (默认: `hk_stock`) —— *香港交易所联交所*
- **`NASDAQ`**: `str` (默认: `us_stock`) —— *纳斯达克挂牌市场*
- **`NYSE`**: `str` (默认: `us_stock`) —— *纽约证券交易所相关版块*
### exchange_rules.symbol_rules

> **💠 节点释源**: 检查各个交易所市场股票代码的字符串有效长度定义规则，直接过筛无效品种

```json
{
  "symbol_rules": {
    "SZSE": 6,
    "SSE": 6,
    "BSE": 6
  }
}
  ...
```

- **`SZSE`**: `int` (默认: `6`) —— *深圳证券交易所启动参数/预设映射标签*
- **`SSE`**: `int` (默认: `6`) —— *上海证券交易所启动参数/预设映射标签*
- **`BSE`**: `int` (默认: `6`) —— *北京证券交易所启动参数/预设映射标签*
- **`HKEX`**: `List` (默认: `[4, 5]`) —— *香港交易所联交所*
- **`NASDAQ`**: `NoneType` (默认: `None`) —— *纳斯达克挂牌市场*
- **`NYSE`**: `NoneType` (默认: `None`) —— *纽约证券交易所相关版块*
### exchange_rules.symbol_start_with

> **💠 节点释源**: 各股票首段数字字首的严格界定群族，规避不相关逆回购和国退标的被错误纳管拉取

```json
{
  "symbol_start_with": {
    "SSE": [
      "600",
      "601",
      "603",
      "688",
      "900"
    ],
    "SZSE": [
      "000",
      "001",
      "002",
      "300",
      "200"
    ],
    "BSE": [
      "43",
      "83",
      "87",
      "92"
    ]
  }
}
  ...
```

- **`SSE`**: `List` (默认: `['600', '601', '603', '688', '900']`) —— *上海证券交易所启动参数/预设映射标签*
- **`SZSE`**: `List` (默认: `['000', '001', '002', '300', '200']`) —— *深圳证券交易所启动参数/预设映射标签*
- **`BSE`**: `List` (默认: `['43', '83', '87', '92']`) —— *北京证券交易所启动参数/预设映射标签*
- **`HKEX`**: `List` (默认: `['0', '00', '000']`) —— *香港交易所联交所*
## api_config

```json
{
  "api_config": {
    "host": "0.0.0.0",
    "port": 8000,
    "workers": 1
  }
}
  ...
```

- **`host`**: `str` (默认: `0.0.0.0`) —— *系统 Fast API 总控微服务监听对外提供的主机 IP 段协议端口落地点*
- **`port`**: `int` (默认: `8000`) —— *系统总微服务接收请求响应端口号*
- **`workers`**: `int` (默认: `1`) —— *处理请求的多重并行异步负载 UVicorn 虚拟打工节点扩容数*
- **`reload`**: `bool` (默认: `False`) —— *启用热更新检测探针系统（针对主进程开发过程有效，生产常关闭）*
- **`cors_origins`**: `List` (默认: `['*']`) —— *网页 AJAX 保护放行的浏览器请求原始域列表（填写特定域名有效避免 CSRF 隐患）*
## scheduler_config

```json
{
  "scheduler_config": {
    "enabled": true,
    "timezone": "Asia/Shanghai",
    "max_instances": 10
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`timezone`**: `str` (默认: `Asia/Shanghai`) —— *计算发车执行时点以及日历重计算时的全球标准地理参考体系依据*
- **`max_instances`**: `int` (默认: `10`) —— *针对并行触发的任务最多允许多少重并发任务队列并跑（1 限制表示严格串行单挑）*
- **`misfire_grace_time`**: `int` (默认: `300`) —— *当原定计划由于进程锁或忙碌延误错过了，允许它事后弥补执行的最大原谅时间宽度（秒）*
- **`coalesce`**: `bool` (默认: `True`) —— *如果同类型的发令积压多次错失，恢复后是否合并压缩命令为最新一次单次命令（防止突然雪崩喷发）*
- **`monitor_startup_delay`**: `int` (默认: `3`) —— *守护监控器在系统挂载多少秒以后才启动扫描探针*
- **`market_close_delay_minutes`**: `int` (默认: `15`) —— *等待真实市场正式收盘与数据整理推算完成后再触发抓取的时间差缓冲偏置*
### scheduler_config.jobs

> **💠 节点释源**: 自动化挂历的子分卷任务计划书列表集录

```json
{
  "jobs": {
    "daily_data_update": {
      "enabled": true,
      "description": "每日数据更新任务",
      "report": true,
      "trigger": {
        "type": "cron",
        "day_of_week": "mon-fri",
        "hour": 20,
        "minute": 0,
        "second": 0
      },
      "max_instances": 1,
      "misfire_grace_time": 600,
      "coalesce": true,
      "parameters": {
        "exchanges": [
          "SSE",
          "SZSE"
        ],
        "wait_for_market_close": true,
        "market_close_delay_minutes": 15,
        "enable_trading_day_check": true
      }
    },
    "system_health_check": {
      "enabled": true,
      "description": "系统健康检查",
      "report": true,
      "trigger": {
        "type": "interval",
        "hours": 1,
        "minutes": 0,
        "seconds": 0
      },
      "max_instances": 1,
      "misfire_grace_time": 300,
      "coalesce": true,
      "parameters": {
        "check_data_sources": true,
        "check_database": true,
        "check_disk_space": true,
        "check_memory_usage": true,
        "check_telegram": true,
        "disk_space_threshold_mb": 1000,
        "memory_threshold_percent": 85
      }
    },
    "weekly_data_maintenance": {
      "enabled": true,
      "description": "每周数据维护",
      "report": true,
      "trigger": {
        "type": "cron",
        "day_of_week": "sun",
        "hour": 2,
        "minute": 0,
        "second": 0
      },
      "max_instances": 1,
      "misfire_grace_time": 1800,
      "coalesce": true,
      "parameters": {
        "backup_database": true,
        "cleanup_old_logs": true,
        "log_retention_days": 30,
        "optimize_database": true,
        "validate_data_integrity": true
      }
    }
  }
}
  ...
```

#### jobs.daily_data_update

```json
{
  "daily_data_update": {
    "enabled": true,
    "description": "每日数据更新任务",
    "report": true
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`description`**: `str` (默认: `每日数据更新任务`) —— *用以展示在 Telegram 前端机器人等查询输出的具象化备注名称*
- **`report`**: `bool` (默认: `True`) —— *决定本任务在其成败生命周期结束后是否撰写推送简报向群组汇报分发*
- **`trigger`**: `Object` /* 挂历执行周期间隔触发器类型与形态定义结构：如 cron , interval 等 */
- **`max_instances`**: `int` (默认: `1`) —— *针对并行触发的任务最多允许多少重并发任务队列并跑（1 限制表示严格串行单挑）*
- **`misfire_grace_time`**: `int` (默认: `600`) —— *当原定计划由于进程锁或忙碌延误错过了，允许它事后弥补执行的最大原谅时间宽度（秒）*
- **`coalesce`**: `bool` (默认: `True`) —— *如果同类型的发令积压多次错失，恢复后是否合并压缩命令为最新一次单次命令（防止突然雪崩喷发）*
- **`parameters`**: `Object` /* 当按时激活任务方法时所需往下方传递的特征动作字典形参设定记录集 */
- **`parameters.instrument_types`**: `List[str]` —— 普通 A 股日更任务显式传入的品种类型。当前生产配置为 `["stock", "index"]`，因此指数主数据治理会在读取当前指数 universe 前执行；如临时只测股票，可在任务参数中覆盖为 `["stock"]`。
#### jobs.index_master_governance_sync

```json
{
  "index_master_governance_sync": {
    "enabled": true,
    "manual_only": true,
    "description": "A股指数主数据治理（仅手工触发，不下载日更行情）"
  }
}
```

- **`manual_only`**: `true`，该任务不会注册 cron，只能通过 `/run index_master_governance_sync` 手工触发。
- **`parameters.exchanges`**: 默认 `["SSE", "SZSE"]`。
- **`parameters.timeout_sec`**: 单次治理超时秒数，默认 `120`。
- **用途**: 非交易日或上线前冒烟测试时，只验证官方指数清单、公告证据、生命周期写入和报告摘要，不触发周末/节假日日线行情请求。
#### jobs.system_health_check

```json
{
  "system_health_check": {
    "enabled": true,
    "description": "系统健康检查",
    "report": true
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`description`**: `str` (默认: `系统健康检查`) —— *用以展示在 Telegram 前端机器人等查询输出的具象化备注名称*
- **`report`**: `bool` (默认: `True`) —— *决定本任务在其成败生命周期结束后是否撰写推送简报向群组汇报分发*
- **`trigger`**: `Object` /* 挂历执行周期间隔触发器类型与形态定义结构：如 cron , interval 等 */
- **`max_instances`**: `int` (默认: `1`) —— *针对并行触发的任务最多允许多少重并发任务队列并跑（1 限制表示严格串行单挑）*
- **`misfire_grace_time`**: `int` (默认: `300`) —— *当原定计划由于进程锁或忙碌延误错过了，允许它事后弥补执行的最大原谅时间宽度（秒）*
- **`coalesce`**: `bool` (默认: `True`) —— *如果同类型的发令积压多次错失，恢复后是否合并压缩命令为最新一次单次命令（防止突然雪崩喷发）*
- **`parameters`**: `Object` /* 当按时激活任务方法时所需往下方传递的特征动作字典形参设定记录集 */
#### jobs.weekly_data_maintenance

```json
{
  "weekly_data_maintenance": {
    "enabled": true,
    "description": "每周数据维护",
    "report": true,
    "parameters": {
      "backup_database": true,
      "cleanup_old_logs": true,
      "log_retention_days": 30,
      "cleanup_ghost_stocks": false,
      "ghost_stock_grace_days": 14,
      "zombie_stock_grace_days": 30,
      "sync_adjustment_factors": true,
      "factor_sync_exchanges": ["SSE", "SZSE", "BSE", "HKEX"],
      "factor_sync_days_back": 7,
      "validate_data_integrity": true,
      "optimize_database": true,
      "max_runtime_seconds": 18000
    }
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`description`**: `str` (默认: `每周数据维护`) —— *用以展示在 Telegram 前端机器人等查询输出的具象化备注名称*
- **`report`**: `bool` (默认: `True`) —— *决定本任务在其成败生命周期结束后是否撰写推送简报向群组汇报分发*
- **`trigger`**: `Object` /* 挂历执行周期间隔触发器类型与形态定义结构：如 cron , interval 等 */
- **`max_instances`**: `int` (默认: `1`) —— *针对并行触发的任务最多允许多少重并发任务队列并跑（1 限制表示严格串行单挑）*
- **`misfire_grace_time`**: `int` (默认: `1800`) —— *当原定计划由于进程锁或忙碌延误错过了，允许它事后弥补执行的最大原谅时间宽度（秒）*
- **`coalesce`**: `bool` (默认: `True`) —— *如果同类型的发令积压多次错失，恢复后是否合并压缩命令为最新一次单次命令（防止突然雪崩喷发）*
- **`parameters`**: `Object` /* 当按时激活任务方法时所需往下方传递的特征动作字典形参设定记录集 */
- **`parameters.cleanup_ghost_stocks`**: `bool` (当前生产默认: `False`) —— *兼容旧配置的废弃开关。周维护不再自动按“长期无行情”停用主数据；生命周期应由交易所主数据治理任务写入*
- **`parameters.sync_adjustment_factors`**: `bool` (默认: `True`) —— *是否在周维护中执行复权因子周度同步*
- **`parameters.factor_sync_exchanges`**: `List[str] | null` (默认: `null`) —— *周度因子同步的交易所范围；设为 `["SSE","SZSE","BSE","HKEX"]` 可显式纳入港股，设为 `null` 则使用系统默认启用市场*
- **`parameters.factor_sync_days_back`**: `int` (默认: `7`) —— *周度因子同步的回看天数*
- **`parameters.max_runtime_seconds`**: `int` (当前生产建议: `18000`) —— *周维护最大执行时间。港股周度因子扫描约需 2 小时以上，建议预留 5 小时窗口*
#### jobs.monthly_data_integrity_check

```json
{
  "monthly_data_integrity_check": {
    "enabled": true,
    "description": "月度数据完整性检查和缺口修复",
    "report": true
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`description`**: `str` (默认: `月度数据完整性检查和缺口修复`) —— *用以展示在 Telegram 前端机器人等查询输出的具象化备注名称*
- **`report`**: `bool` (默认: `True`) —— *决定本任务在其成败生命周期结束后是否撰写推送简报向群组汇报分发*
- **`trigger`**: `Object` /* 挂历执行周期间隔触发器类型与形态定义结构：如 cron , interval 等 */
- **`max_instances`**: `int` (默认: `1`) —— *针对并行触发的任务最多允许多少重并发任务队列并跑（1 限制表示严格串行单挑）*
- **`misfire_grace_time`**: `int` (默认: `3600`) —— *当原定计划由于进程锁或忙碌延误错过了，允许它事后弥补执行的最大原谅时间宽度（秒）*
- **`coalesce`**: `bool` (默认: `True`) —— *如果同类型的发令积压多次错失，恢复后是否合并压缩命令为最新一次单次命令（防止突然雪崩喷发）*
- **`parameters`**: `Object` /* 当按时激活任务方法时所需往下方传递的特征动作字典形参设定记录集 */
#### jobs.quarterly_cleanup

```json
{
  "quarterly_cleanup": {
    "enabled": true,
    "description": "季度数据清理",
    "report": true
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`description`**: `str` (默认: `季度数据清理`) —— *用以展示在 Telegram 前端机器人等查询输出的具象化备注名称*
- **`report`**: `bool` (默认: `True`) —— *决定本任务在其成败生命周期结束后是否撰写推送简报向群组汇报分发*
- **`trigger`**: `Object` /* 挂历执行周期间隔触发器类型与形态定义结构：如 cron , interval 等 */
- **`max_instances`**: `int` (默认: `1`) —— *针对并行触发的任务最多允许多少重并发任务队列并跑（1 限制表示严格串行单挑）*
- **`misfire_grace_time`**: `int` (默认: `1800`) —— *当原定计划由于进程锁或忙碌延误错过了，允许它事后弥补执行的最大原谅时间宽度（秒）*
- **`coalesce`**: `bool` (默认: `True`) —— *如果同类型的发令积压多次错失，恢复后是否合并压缩命令为最新一次单次命令（防止突然雪崩喷发）*
- **`parameters`**: `Object` /* 当按时激活任务方法时所需往下方传递的特征动作字典形参设定记录集 */
#### jobs.cache_warm_up

```json
{
  "cache_warm_up": {
    "enabled": true,
    "description": "缓存预热",
    "report": false
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`description`**: `str` (默认: `缓存预热`) —— *用以展示在 Telegram 前端机器人等查询输出的具象化备注名称*
- **`report`**: `bool` (默认: `False`) —— *决定本任务在其成败生命周期结束后是否撰写推送简报向群组汇报分发*
- **`trigger`**: `Object` /* 挂历执行周期间隔触发器类型与形态定义结构：如 cron , interval 等 */
- **`max_instances`**: `int` (默认: `1`) —— *针对并行触发的任务最多允许多少重并发任务队列并跑（1 限制表示严格串行单挑）*
- **`misfire_grace_time`**: `int` (默认: `600`) —— *当原定计划由于进程锁或忙碌延误错过了，允许它事后弥补执行的最大原谅时间宽度（秒）*
- **`coalesce`**: `bool` (默认: `True`) —— *如果同类型的发令积压多次错失，恢复后是否合并压缩命令为最新一次单次命令（防止突然雪崩喷发）*
- **`parameters`**: `Object` /* 当按时激活任务方法时所需往下方传递的特征动作字典形参设定记录集 */
#### jobs.trading_calendar_update

```json
{
  "trading_calendar_update": {
    "enabled": true,
    "description": "交易日历更新",
    "report": true
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`description`**: `str` (默认: `交易日历更新`) —— *用以展示在 Telegram 前端机器人等查询输出的具象化备注名称*
- **`report`**: `bool` (默认: `True`) —— *决定本任务在其成败生命周期结束后是否撰写推送简报向群组汇报分发*
- **`trigger`**: `Object` /* 挂历执行周期间隔触发器类型与形态定义结构：如 cron , interval 等 */
- **`max_instances`**: `int` (默认: `1`) —— *针对并行触发的任务最多允许多少重并发任务队列并跑（1 限制表示严格串行单挑）*
- **`misfire_grace_time`**: `int` (默认: `1800`) —— *当原定计划由于进程锁或忙碌延误错过了，允许它事后弥补执行的最大原谅时间宽度（秒）*
- **`coalesce`**: `bool` (默认: `True`) —— *如果同类型的发令积压多次错失，恢复后是否合并压缩命令为最新一次单次命令（防止突然雪崩喷发）*
- **`parameters`**: `Object` /* 当按时激活任务方法时所需往下方传递的特征动作字典形参设定记录集 */
#### jobs.database_backup

```json
{
  "database_backup": {
    "enabled": true,
    "description": "数据库备份任务",
    "report": true
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`description`**: `str` (默认: `数据库备份任务`) —— *用以展示在 Telegram 前端机器人等查询输出的具象化备注名称*
- **`report`**: `bool` (默认: `True`) —— *决定本任务在其成败生命周期结束后是否撰写推送简报向群组汇报分发*
- **`trigger`**: `Object` /* 挂历执行周期间隔触发器类型与形态定义结构：如 cron , interval 等 */
- **`max_instances`**: `int` (默认: `1`) —— *针对并行触发的任务最多允许多少重并发任务队列并跑（1 限制表示严格串行单挑）*
- **`misfire_grace_time`**: `int` (默认: `1800`) —— *当原定计划由于进程锁或忙碌延误错过了，允许它事后弥补执行的最大原谅时间宽度（秒）*
- **`coalesce`**: `bool` (默认: `True`) —— *如果同类型的发令积压多次错失，恢复后是否合并压缩命令为最新一次单次命令（防止突然雪崩喷发）*
- **`parameters`**: `Object` /* 当按时激活任务方法时所需往下方传递的特征动作字典形参设定记录集 */
## monitor_config

```json
{
  "monitor_config": {
    "enabled": true,
    "max_history_size": 1000,
    "startup_delay": 2
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`max_history_size`**: `int` (默认: `1000`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`startup_delay`**: `int` (默认: `2`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`min_wait_time`**: `int` (默认: `1`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
### monitor_config.alert_thresholds

```json
{
  "alert_thresholds": {
    "max_execution_time": 300,
    "max_failure_rate": 0.1,
    "max_consecutive_failures": 3
  }
}
```

- **`max_execution_time`**: `int` (默认: `300`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`max_failure_rate`**: `float` (默认: `0.1`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`max_consecutive_failures`**: `int` (默认: `3`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
## backup_config

```json
{
  "backup_config": {
    "enabled": true,
    "source_db_path": "data/quotes.db",
    "source_databases": [
      {"name": "quotes", "path": "data/quotes.db", "filename_pattern": "quotes_backup_{timestamp}.db"},
      {"name": "research", "path": "data/research.db", "filename_pattern": "research_backup_{timestamp}.db"},
      {"name": "financials", "path": "data/financials.db", "filename_pattern": "financials_backup_{timestamp}.db"},
      {"name": "valuation", "path": "data/valuation.db", "filename_pattern": "valuation_backup_{timestamp}.db"},
      {"name": "market_data", "path": "data/market_data.db", "filename_pattern": "market_data_backup_{timestamp}.db"}
    ],
    "include_extra_data_dbs": true,
    "extra_db_glob": "data/*.db",
    "backup_directory": "data/PVE-Bak/QuoteBak"
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`source_db_path`**: `str` (默认: `data/quotes.db`) —— *单库手工覆盖兼容项；正常自动备份使用 `source_databases`*
- **`source_databases`**: `List[dict]` —— *生产备份数据库清单，当前覆盖 `quotes.db / research.db / financials.db / valuation.db / market_data.db`*
- **`include_extra_data_dbs`**: `bool` (默认: `False`) —— *是否自动把 `extra_db_glob` 命中的新增 SQLite 数据库纳入备份*
- **`extra_db_glob`**: `str` (默认: `data/*.db`) —— *新增数据库自动发现范围*
- **`backup_directory`**: `str` (默认: `data/PVE-Bak/QuoteBak`) —— *NAS 备份输出路径；`data/PVE-Bak` 必须是 NAS 子挂载，不能退化为本地空目录*
- **`retention_days`**: `int` (默认: `30`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`compression_enabled`**: `bool` (默认: `False`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`notification_enabled`**: `bool` (默认: `True`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`filename_pattern`**: `str` (默认: `quotes_backup_{timestamp}.db`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`max_backup_files`**: `int` (默认: `10`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
## cache_config

```json
{
  "cache_config": {
    "enabled": true,
    "max_memory_mb": 512,
    "default_ttl_seconds": 3600
  }
}
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`max_memory_mb`**: `int` (默认: `512`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`default_ttl_seconds`**: `int` (默认: `3600`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
## report_config

```json
{
  "report_config": {
    "templates": {
      "download_report": {
        "name": "数据下载报告",
        "emoji": "📊",
        "output_format": "telegram",
        "enabled": true,
        "sections": [
          {
            "name": "header",
            "type": "static",
            "content": "📊 *{name}*"
          },
          {
            "name": "status",
            "type": "status",
            "data_source": "success_rate"
          },
          {
            "name": "summary",
            "type": "metrics",
            "fields": [
              "total_instruments",
              "success_count",
              "failure_count",
              "total_quotes",
              "success_rate"
            ]
          },
          {
            "name": "details",
            "type": "table",
            "data_source": "exchange_stats"
          },
          {
            "name": "footer",
            "type": "static",
            "content": "🔄 {source} - {time}"
          }
        ]
      },
      "daily_update_report": {
        "name": "每日数据更新报告",
        "emoji": "📈",
        "output_format": "telegram",
        "enabled": true,
        "sections": [
          {
            "name": "header",
            "type": "static",
            "content": "📈 *{name}*"
          },
          {
            "name": "summary",
            "type": "metrics",
            "fields": [
              "date",
              "updated_instruments",
              "new_quotes",
              "success_rate"
            ],
            "title": "更新摘要",
            "condition": "not non_trading_day"
          },
          {
            "name": "non_trading_day_info",
            "type": "static",
            "content": "🗓️ *非交易日通知*\n\n今天是 {date}，市场未开盘，跳过数据更新。\n\n*交易日历更新情况:*\n{calendar_updates}",
            "condition": "non_trading_day"
          },
          {
            "name": "error_info",
            "type": "static",
            "content": "❌ *更新失败*\n\n错误信息: {error_message}",
            "condition": "status == 'error'"
          },
          {
            "name": "status",
            "type": "status",
            "data_source": "status",
            "condition": "status != 'error'"
          },
          {
            "name": "exchange_stats_title",
            "type": "static",
            "content": "*交易所统计*",
            "condition": "not non_trading_day and status != 'error'"
          },
          {
            "name": "exchange_stats",
            "type": "table",
            "data_source": "exchange_stats"
          },
          {
            "name": "footer",
            "type": "static",
            "content": "🔄 {source} - {generated_at}"
          }
        ]
      },
      "gap_report": {
        "name": "数据缺口报告",
        "emoji": "🔍",
        "output_format": "telegram",
        "enabled": true,
        "sections": [
          {
            "name": "header",
            "type": "static",
            "content": "🔍 *{name}*"
          },
          {
            "name": "summary",
            "type": "metrics",
            "fields": [
              "total_gaps",
              "affected_stocks",
              "severity_distribution"
            ],
            "title": "缺口摘要",
            "condition": "status != 'error'"
          },
          {
            "name": "error_info",
            "type": "static",
            "content": "❌ *缺口检测/填补失败*\n\n错误信息: {error_message}",
            "condition": "status == 'error'"
          },
          {
            "name": "status",
            "type": "status",
            "data_source": "status",
            "condition": "status != 'error'"
          },
          {
            "name": "top_affected_stocks_title",
            "type": "static",
            "content": "*受影响最严重的股票*",
            "condition": "status != 'error'"
          },
          {
            "name": "details",
            "type": "list",
            "data_source": "top_affected_stocks",
            "display_format": "{symbol} - 缺失天数: {total_missing_days}, 严重分数: {severity_score}"
          },
          {
            "name": "footer",
            "type": "static",
            "content": "🔄 {source} - {generated_at}"
          }
        ]
      },
      "system_status": {
        "name": "系统状态报告",
        "emoji": "🏥",
        "output_format": "telegram",
        "enabled": true,
        "sections": [
          {
            "name": "header",
            "type": "static",
            "content": "🏥 *{name}*"
          },
          {
            "name": "status",
            "type": "status",
            "data_source": "system_status"
          },
          {
            "name": "summary",
            "type": "metrics",
            "fields": [
              "database_stats",
              "data_sources"
            ]
          },
          {
            "name": "details",
            "type": "metrics",
            "fields": [
              "database_stats",
              "data_sources"
            ]
          }
        ]
      },
      "backup_result": {
        "name": "数据库备份报告",
        "emoji": "💾",
        "output_format": "telegram",
        "enabled": true,
        "sections": [
          {
            "name": "header",
            "type": "static",
            "content": "💾 *{name}*"
          },
          {
            "name": "status",
            "type": "status",
            "data_source": "backup_result"
          },
          {
            "name": "details",
            "type": "metrics",
            "fields": [
              "backup_file",
              "file_size",
              "duration"
            ]
          }
        ]
      },
      "health_check_report": {
        "name": "系统健康检查报告",
        "emoji": "🏥",
        "output_format": "telegram",
        "enabled": true,
        "sections": [
          {
            "name": "header",
            "type": "static",
            "content": "🏥 *{name}*"
          },
          {
            "name": "summary",
            "type": "metrics",
            "fields": [
              "overall_status",
              "checks_performed",
              "issues_found"
            ],
            "title": "检查摘要"
          },
          {
            "name": "details",
            "type": "list",
            "data_source": "check_results",
            "display_format": "{status_icon} {check_name}: {result}",
            "title": "详细检查结果"
          }
        ]
      },
      "maintenance_report": {
        "name": "数据维护报告",
        "emoji": "🔧",
        "output_format": "telegram",
        "enabled": true,
        "sections": [
          {
            "name": "header",
            "type": "static",
            "content": "🔧 *{name}*"
          },
          {
            "name": "status",
            "type": "status",
            "data_source": "tasks_completed"
          },
          {
            "name": "summary",
            "type": "metrics",
            "fields": [
              "tasks_completed",
              "space_freed",
              "duration"
            ]
          },
          {
            "name": "details",
            "type": "list",
            "data_source": "maintenance_tasks",
            "display_format": "{task_name} - 状态: {status}"
          }
        ]
      },
      "cache_warm_up_report": {
        "name": "缓存预热报告",
        "emoji": "⚡",
        "output_format": "telegram",
        "enabled": true,
        "sections": [
          {
            "name": "header",
            "type": "static",
            "content": "⚡ *{name}*"
          },
          {
            "name": "status",
            "type": "status",
            "data_source": "stocks_warmed"
          },
          {
            "name": "summary",
            "type": "metrics",
            "fields": [
              "stocks_warmed",
              "cache_hit_rate",
              "duration"
            ]
          },
          {
            "name": "details",
            "type": "metrics",
            "fields": [
              "popular_stocks",
              "market_indices",
              "recent_data"
            ]
          }
        ]
      },
      "trading_calendar_report": {
        "name": "交易日历更新报告",
        "emoji": "📅",
        "output_format": "telegram",
        "enabled": true,
        "sections": [
          {
            "name": "header",
            "type": "static",
            "content": "📅 *{name}*"
          },
          {
            "name": "summary",
            "type": "metrics",
            "fields": [
              "exchanges_updated",
              "trading_days_added",
              "holidays_added"
            ],
            "title": "更新摘要",
            "condition": "status != 'error'"
          },
          {
            "name": "error_info",
            "type": "static",
            "content": "❌ *交易日历更新失败*\n\n错误信息: {error_message}",
            "condition": "status == 'error'"
          },
          {
            "name": "status",
            "type": "status",
            "data_source": "status",
            "condition": "status != 'error'"
          },
          {
            "name": "exchange_details_title",
            "type": "static",
            "content": "*交易所详情*",
            "condition": "status != 'error'"
          },
          {
            "name": "details",
            "type": "table",
            "data_source": "exchange_details"
          },
          {
            "name": "footer",
            "type": "static",
            "content": "🔄 {source} - {generated_at}"
          }
        ]
      }
    },
    "formats": {
      "telegram": {
        "max_length": 4000,
        "line_separator": "\n\n",
        "bold_format": "*{text}*",
        "code_format": "`{text}`"
      },
      "console": {
        "max_width": 100,
        "line_separator": "\n\n",
        "table_style": "grid"
      },
      "api": {
        "include_raw_data": true,
        "response_format": "json"
      }
    },
    "field_labels": {
      "total_instruments": "处理股票",
      "success_count": "成功数量",
      "failure_count": "失败数量",
      "quotes_added": "新增行情",
      "total_quotes": "行情数据",
      "success_rate": "成功率",
      "updated_instruments": "更新股票",
      "new_quotes": "新增行情",
      "total_gaps": "总缺口数",
      "affected_stocks": "受影响股票",
      "backup_file": "备份文件",
      "file_size": "文件大小",
      "duration": "耗时",
      "overall_status": "总体状态",
      "checks_performed": "检查数量",
      "issues_found": "问题数量"
    }
  }
}
```

### report_config.templates

```json
{
  "templates": {
    "download_report": {
      "name": "数据下载报告",
      "emoji": "📊",
      "output_format": "telegram",
      "enabled": true,
      "sections": [
        {
          "name": "header",
          "type": "static",
          "content": "📊 *{name}*"
        },
        {
          "name": "status",
          "type": "status",
          "data_source": "success_rate"
        },
        {
          "name": "summary",
          "type": "metrics",
          "fields": [
            "total_instruments",
            "success_count",
            "failure_count",
            "total_quotes",
            "success_rate"
          ]
        },
        {
          "name": "details",
          "type": "table",
          "data_source": "exchange_stats"
        },
        {
          "name": "footer",
          "type": "static",
          "content": "🔄 {source} - {time}"
        }
      ]
    },
    "daily_update_report": {
      "name": "每日数据更新报告",
      "emoji": "📈",
      "output_format": "telegram",
      "enabled": true,
      "sections": [
        {
          "name": "header",
          "type": "static",
          "content": "📈 *{name}*"
        },
        {
          "name": "summary",
          "type": "metrics",
          "fields": [
            "date",
            "updated_instruments",
            "new_quotes",
            "success_rate"
          ],
          "title": "更新摘要",
          "condition": "not non_trading_day"
        },
        {
          "name": "non_trading_day_info",
          "type": "static",
          "content": "🗓️ *非交易日通知*\n\n今天是 {date}，市场未开盘，跳过数据更新。\n\n*交易日历更新情况:*\n{calendar_updates}",
          "condition": "non_trading_day"
        },
        {
          "name": "error_info",
          "type": "static",
          "content": "❌ *更新失败*\n\n错误信息: {error_message}",
          "condition": "status == 'error'"
        },
        {
          "name": "status",
          "type": "status",
          "data_source": "status",
          "condition": "status != 'error'"
        },
        {
          "name": "exchange_stats_title",
          "type": "static",
          "content": "*交易所统计*",
          "condition": "not non_trading_day and status != 'error'"
        },
        {
          "name": "exchange_stats",
          "type": "table",
          "data_source": "exchange_stats"
        },
        {
          "name": "footer",
          "type": "static",
          "content": "🔄 {source} - {generated_at}"
        }
      ]
    },
    "gap_report": {
      "name": "数据缺口报告",
      "emoji": "🔍",
      "output_format": "telegram",
      "enabled": true,
      "sections": [
        {
          "name": "header",
          "type": "static",
          "content": "🔍 *{name}*"
        },
        {
          "name": "summary",
          "type": "metrics",
          "fields": [
            "total_gaps",
            "affected_stocks",
            "severity_distribution"
          ],
          "title": "缺口摘要",
          "condition": "status != 'error'"
        },
        {
          "name": "error_info",
          "type": "static",
          "content": "❌ *缺口检测/填补失败*\n\n错误信息: {error_message}",
          "condition": "status == 'error'"
        },
        {
          "name": "status",
          "type": "status",
          "data_source": "status",
          "condition": "status != 'error'"
        },
        {
          "name": "top_affected_stocks_title",
          "type": "static",
          "content": "*受影响最严重的股票*",
          "condition": "status != 'error'"
        },
        {
          "name": "details",
          "type": "list",
          "data_source": "top_affected_stocks",
          "display_format": "{symbol} - 缺失天数: {total_missing_days}, 严重分数: {severity_score}"
        },
        {
          "name": "footer",
          "type": "static",
          "content": "🔄 {source} - {generated_at}"
        }
      ]
    }
  }
}
  ...
```

#### templates.download_report

```json
{
  "download_report": {
    "name": "数据下载报告",
    "emoji": "📊",
    "output_format": "telegram"
  }
}
  ...
```

- **`name`**: `str` (默认: `数据下载报告`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`emoji`**: `str` (默认: `📊`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`output_format`**: `str` (默认: `telegram`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '📊 *{name}*'}, {'name': 'status', 'type': 'status', 'data_source': 'success_rate'}, {'name': 'summary', 'type': 'metrics', 'fields': ['total_instruments', 'success_count', 'failure_count', 'total_quotes', 'success_rate']}, {'name': 'details', 'type': 'table', 'data_source': 'exchange_stats'}, {'name': 'footer', 'type': 'static', 'content': '🔄 {source} - {time}'}]`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
#### templates.daily_update_report

```json
{
  "daily_update_report": {
    "name": "每日数据更新报告",
    "emoji": "📈",
    "output_format": "telegram"
  }
}
  ...
```

- **`name`**: `str` (默认: `每日数据更新报告`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`emoji`**: `str` (默认: `📈`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`output_format`**: `str` (默认: `telegram`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '📈 *{name}*'}, {'name': 'summary', 'type': 'metrics', 'fields': ['date', 'updated_instruments', 'new_quotes', 'success_rate'], 'title': '更新摘要', 'condition': 'not non_trading_day'}, {'name': 'non_trading_day_info', 'type': 'static', 'content': '🗓️ *非交易日通知*\n\n今天是 {date}，市场未开盘，跳过数据更新。\n\n*交易日历更新情况:*\n{calendar_updates}', 'condition': 'non_trading_day'}, {'name': 'error_info', 'type': 'static', 'content': '❌ *更新失败*\n\n错误信息: {error_message}', 'condition': "status == 'error'"}, {'name': 'status', 'type': 'status', 'data_source': 'status', 'condition': "status != 'error'"}, {'name': 'exchange_stats_title', 'type': 'static', 'content': '*交易所统计*', 'condition': "not non_trading_day and status != 'error'"}, {'name': 'exchange_stats', 'type': 'table', 'data_source': 'exchange_stats'}, {'name': 'footer', 'type': 'static', 'content': '🔄 {source} - {generated_at}'}]`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*

日报模板当前还支持以下紧凑诊断段落：

- `instrument_master_sync_summary`：证券主数据同步摘要。
- `index_master_governance_summary`：指数主数据治理摘要，限制样例数量，不渲染完整证据 URL，避免 Telegram 消息超长。
- `daily_catchup_summary`：新股和短缺口行情追补摘要。
#### templates.gap_report

```json
{
  "gap_report": {
    "name": "数据缺口报告",
    "emoji": "🔍",
    "output_format": "telegram"
  }
}
  ...
```

- **`name`**: `str` (默认: `数据缺口报告`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`emoji`**: `str` (默认: `🔍`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`output_format`**: `str` (默认: `telegram`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '🔍 *{name}*'}, {'name': 'summary', 'type': 'metrics', 'fields': ['total_gaps', 'affected_stocks', 'severity_distribution'], 'title': '缺口摘要', 'condition': "status != 'error'"}, {'name': 'error_info', 'type': 'static', 'content': '❌ *缺口检测/填补失败*\n\n错误信息: {error_message}', 'condition': "status == 'error'"}, {'name': 'status', 'type': 'status', 'data_source': 'status', 'condition': "status != 'error'"}, {'name': 'top_affected_stocks_title', 'type': 'static', 'content': '*受影响最严重的股票*', 'condition': "status != 'error'"}, {'name': 'details', 'type': 'list', 'data_source': 'top_affected_stocks', 'display_format': '{symbol} - 缺失天数: {total_missing_days}, 严重分数: {severity_score}'}, {'name': 'footer', 'type': 'static', 'content': '🔄 {source} - {generated_at}'}]`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
#### templates.system_status

```json
{
  "system_status": {
    "name": "系统状态报告",
    "emoji": "🏥",
    "output_format": "telegram"
  }
}
  ...
```

- **`name`**: `str` (默认: `系统状态报告`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`emoji`**: `str` (默认: `🏥`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`output_format`**: `str` (默认: `telegram`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '🏥 *{name}*'}, {'name': 'status', 'type': 'status', 'data_source': 'system_status'}, {'name': 'summary', 'type': 'metrics', 'fields': ['database_stats', 'data_sources']}, {'name': 'details', 'type': 'metrics', 'fields': ['database_stats', 'data_sources']}]`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
#### templates.backup_result

```json
{
  "backup_result": {
    "name": "数据库备份报告",
    "emoji": "💾",
    "output_format": "telegram"
  }
}
  ...
```

- **`name`**: `str` (默认: `数据库备份报告`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`emoji`**: `str` (默认: `💾`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`output_format`**: `str` (默认: `telegram`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '💾 *{name}*'}, {'name': 'status', 'type': 'status', 'data_source': 'backup_result'}, {'name': 'details', 'type': 'metrics', 'fields': ['backup_file', 'file_size', 'duration']}]`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
#### templates.health_check_report

```json
{
  "health_check_report": {
    "name": "系统健康检查报告",
    "emoji": "🏥",
    "output_format": "telegram"
  }
}
  ...
```

- **`name`**: `str` (默认: `系统健康检查报告`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`emoji`**: `str` (默认: `🏥`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`output_format`**: `str` (默认: `telegram`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '🏥 *{name}*'}, {'name': 'summary', 'type': 'metrics', 'fields': ['overall_status', 'checks_performed', 'issues_found'], 'title': '检查摘要'}, {'name': 'details', 'type': 'list', 'data_source': 'check_results', 'display_format': '{status_icon} {check_name}: {result}', 'title': '详细检查结果'}]`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
#### templates.maintenance_report

```json
{
  "maintenance_report": {
    "name": "数据维护报告",
    "emoji": "🔧",
    "output_format": "telegram"
  }
}
  ...
```

- **`name`**: `str` (默认: `数据维护报告`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`emoji`**: `str` (默认: `🔧`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`output_format`**: `str` (默认: `telegram`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '🔧 *{name}*'}, {'name': 'status', 'type': 'status', 'data_source': 'tasks_completed'}, {'name': 'summary', 'type': 'metrics', 'fields': ['tasks_completed', 'space_freed', 'duration']}, {'name': 'details', 'type': 'list', 'data_source': 'maintenance_tasks', 'display_format': '{task_name} - 状态: {status}'}]`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
#### templates.cache_warm_up_report

```json
{
  "cache_warm_up_report": {
    "name": "缓存预热报告",
    "emoji": "⚡",
    "output_format": "telegram"
  }
}
  ...
```

- **`name`**: `str` (默认: `缓存预热报告`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`emoji`**: `str` (默认: `⚡`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`output_format`**: `str` (默认: `telegram`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '⚡ *{name}*'}, {'name': 'status', 'type': 'status', 'data_source': 'stocks_warmed'}, {'name': 'summary', 'type': 'metrics', 'fields': ['stocks_warmed', 'cache_hit_rate', 'duration']}, {'name': 'details', 'type': 'metrics', 'fields': ['popular_stocks', 'market_indices', 'recent_data']}]`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
#### templates.trading_calendar_report

```json
{
  "trading_calendar_report": {
    "name": "交易日历更新报告",
    "emoji": "📅",
    "output_format": "telegram"
  }
}
  ...
```

- **`name`**: `str` (默认: `交易日历更新报告`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`emoji`**: `str` (默认: `📅`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`output_format`**: `str` (默认: `telegram`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '📅 *{name}*'}, {'name': 'summary', 'type': 'metrics', 'fields': ['exchanges_updated', 'trading_days_added', 'holidays_added'], 'title': '更新摘要', 'condition': "status != 'error'"}, {'name': 'error_info', 'type': 'static', 'content': '❌ *交易日历更新失败*\n\n错误信息: {error_message}', 'condition': "status == 'error'"}, {'name': 'status', 'type': 'status', 'data_source': 'status', 'condition': "status != 'error'"}, {'name': 'exchange_details_title', 'type': 'static', 'content': '*交易所详情*', 'condition': "status != 'error'"}, {'name': 'details', 'type': 'table', 'data_source': 'exchange_details'}, {'name': 'footer', 'type': 'static', 'content': '🔄 {source} - {generated_at}'}]`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
### report_config.formats

```json
{
  "formats": {
    "telegram": {
      "max_length": 4000,
      "line_separator": "\n\n",
      "bold_format": "*{text}*",
      "code_format": "`{text}`"
    },
    "console": {
      "max_width": 100,
      "line_separator": "\n\n",
      "table_style": "grid"
    },
    "api": {
      "include_raw_data": true,
      "response_format": "json"
    }
  }
}
```

#### formats.telegram

```json
{
  "telegram": {
    "max_length": 4000,
    "line_separator": "\n\n",
    "bold_format": "*{text}*"
  }
}
  ...
```

- **`max_length`**: `int` (默认: `4000`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`line_separator`**: `str` (默认: `

`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`bold_format`**: `str` (默认: `*{text}*`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`code_format`**: `str` (默认: ``{text}``) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
#### formats.console

```json
{
  "console": {
    "max_width": 100,
    "line_separator": "\n\n",
    "table_style": "grid"
  }
}
```

- **`max_width`**: `int` (默认: `100`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`line_separator`**: `str` (默认: `

`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`table_style`**: `str` (默认: `grid`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
#### formats.api

```json
{
  "api": {
    "include_raw_data": true,
    "response_format": "json"
  }
}
```

- **`include_raw_data`**: `bool` (默认: `True`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`response_format`**: `str` (默认: `json`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
### report_config.field_labels

```json
{
  "field_labels": {
    "total_instruments": "处理股票",
    "success_count": "成功数量",
    "failure_count": "失败数量"
  }
}
  ...
```

- **`total_instruments`**: `str` (默认: `处理股票`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`success_count`**: `str` (默认: `成功数量`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`failure_count`**: `str` (默认: `失败数量`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`quotes_added`**: `str` (默认: `新增行情`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`total_quotes`**: `str` (默认: `行情数据`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`success_rate`**: `str` (默认: `成功率`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`updated_instruments`**: `str` (默认: `更新股票`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`new_quotes`**: `str` (默认: `新增行情`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`total_gaps`**: `str` (默认: `总缺口数`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`affected_stocks`**: `str` (默认: `受影响股票`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`backup_file`**: `str` (默认: `备份文件`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`file_size`**: `str` (默认: `文件大小`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`duration`**: `str` (默认: `耗时`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`overall_status`**: `str` (默认: `总体状态`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`checks_performed`**: `str` (默认: `检查数量`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`issues_found`**: `str` (默认: `问题数量`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
