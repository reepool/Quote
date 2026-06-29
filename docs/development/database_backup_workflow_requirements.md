# 数据库备份工作流重构需求

## 1. 当前结论

当前数据库备份存在两个入口，但都不能作为可靠的生产备份工作流：

- 独立 `database_backup` 周任务读取 `backup_config.source_databases`，但配置中仍包含不存在的 `data/market_data.db`，任务会在预检阶段整体失败，导致任何数据库都不会开始备份。
- `weekly_data_maintenance` 内部仍保留旧的 `data_manager.backup_data()` 备份入口；该入口只面向旧的单库 `quotes.db` 备份路径，并且周维护报告会按参数显示“数据库备份成功”，不能证明实际生成了生产备份文件。

因此后续实现必须以一个独立、配置驱动、可观测的周度数据库备份任务替代上述分散逻辑。

## 2. 目标

新的数据库备份工作流必须满足：

1. 只有一个生产调度入口：`database_backup`。
2. 备份范围由配置声明，不再硬编码数据库文件列表。
3. 默认覆盖当前 `data/*.db` 里的生产 SQLite 数据库，但允许显式包含、排除和命名。
4. 每个数据库可以配置独立的保留数量，默认最多保留 3 个备份文件。
5. 支持配置目标目录、文件命名规则、是否启用、是否通知、是否跳过缺失文件。
6. 每个数据库完成备份后发送一次 Telegram 通知；整轮任务结束后发送汇总通知。
7. 备份过程应控制 I/O 和并发，允许与日常任务同时运行时尽量降低锁竞争和磁盘压力。
8. 清理旧的备份入口，避免周维护、脚本或报告继续暗示存在第二套生产备份机制。

## 3. 配置需求

建议统一配置命名为 `database_backup_config`，至少包含：

```json
{
  "database_backup_config": {
    "enabled": true,
    "backup_directory": "data/PVE-Bak/QuoteBak",
    "default_max_backup_files": 3,
    "default_filename_pattern": "{stem}_backup_{timestamp}.db",
    "timestamp_format": "%Y%m%d_%H%M%S",
    "include_globs": ["data/*.db"],
    "exclude_globs": ["data/*-wal", "data/*-shm"],
    "skip_missing": true,
    "continue_on_database_failure": true,
    "notification_enabled": true,
    "per_database_notification": true,
    "performance": {
      "mode": "sqlite_online_backup",
      "max_parallel_databases": 1,
      "chunk_pages": 1000,
      "chunk_sleep_seconds": 0.05,
      "busy_timeout_seconds": 30,
      "min_free_space_multiplier": 1.5
    },
    "databases": [
      {
        "name": "quotes",
        "path": "data/quotes.db",
        "enabled": true,
        "filename_pattern": "quotes_backup_{timestamp}.db",
        "max_backup_files": 3
      }
    ]
  }
}
```

配置语义：

- `databases` 是显式清单；`include_globs` 用于自动发现未来新增的 `data/*.db`。
- 显式清单优先于自动发现；同一路径只备份一次。
- `enabled=false` 的数据库不备份，也不计为失败。
- `skip_missing=true` 时，显式配置但不存在的数据库应记录 warning 并继续其他库；`skip_missing=false` 时整轮任务失败。
- `max_backup_files` 是每个数据库的保留数量；缺省使用 `default_max_backup_files=3`。
- 文件名模板至少支持 `{name}`、`{stem}`、`{timestamp}`。

## 4. 备份执行需求

### 4.1 预检

任务启动时必须：

- 解析并规范化备份源清单。
- 检查目标目录是否存在或可创建。
- 检查 `data/PVE-Bak/QuoteBak` 是否落在预期备份挂载下；如果检测到目标目录退化成本地普通目录，应失败并告警，避免大库写回本地数据卷。
- 实现中挂载强校验通过 `performance.require_backup_mount` 和 `performance.expected_mount_paths` 控制；开发/测试环境可关闭，生产环境建议开启。
- 按计划备份文件总大小估算可用空间，空间不足时失败或跳过本轮。
- 输出本轮计划：数据库数量、总大小、目标目录、保留策略。

### 4.2 复制方式

优先使用 SQLite online backup API，而不是直接 `shutil.copy2`：

- 避免复制正在写入的 SQLite 文件时产生不一致备份。
- 使用 chunk/page 分批复制，并在批次间 sleep，降低长时间大文件备份对日常任务的 I/O 冲击。
- 对大库按数据库串行执行，默认 `max_parallel_databases=1`。
- 对每个数据库设置 busy timeout，遇到锁等待时可重试或按配置失败。

如某个文件不是有效 SQLite 数据库，任务应失败该数据库并继续或终止，取决于 `continue_on_database_failure`。

### 4.3 验证

每个数据库备份完成后必须验证：

- 备份文件存在且大小大于 0。
- 可打开 SQLite 连接。
- `PRAGMA quick_check` 返回 `ok`。
- 记录源文件大小、备份文件大小、耗时、状态和错误摘要。

### 4.4 清理

清理必须按数据库独立执行：

- 按该数据库的命名规则匹配备份文件。
- 按修改时间或文件名时间戳保留最新 `max_backup_files` 个。
- 清理前后记录保留数量和删除文件。
- 清理失败不应掩盖备份失败，但必须进入汇总报告。

## 5. 调度与旧入口清理

- `database_backup` 是唯一生产备份周任务，默认每周六非交易主流程低峰时间执行。
- `weekly_data_maintenance` 不再直接调用 `data_manager.backup_data()`，也不再在报告里声明“数据库备份成功”。
- `data_manager.backup_data()` 如仍需保留，只能作为兼容或手动工具，并应明确不属于生产周备份工作流；更推荐迁移到统一备份服务后删除旧实现。
- 文档、配置样例、Telegram 任务说明必须同步到新入口和新配置。

## 6. 通知与可观测性

Telegram 通知要求：

- 每个数据库备份完成后发送通知，包含数据库名、状态、源路径、备份文件、文件大小、耗时、校验结果。
- 单库失败时发送失败通知，包含错误摘要和是否继续后续数据库。
- 整轮结束后发送汇总通知，包含成功数、失败数、跳过数、清理数、总耗时、下一次调度时间。

日志要求：

- 预检、每个数据库开始、分批进度、大库完成、校验、清理、汇总都应有阶段日志。
- 日志必须能回答“本周是否执行、备份了哪些库、失败在哪个库、是否清理过旧文件”。

## 7. 当前默认备份范围

截至 2026-06-29，本地 `data` 目录下当前应纳入默认备份的数据库为：

- `data/quotes.db`
- `data/research.db`
- `data/financials.db`
- `data/valuation.db`
- `data/futures.db`
- `data/fx.db`

不存在的旧配置项 `data/market_data.db` 不应阻断上述数据库备份。

## 8. 验收标准

实现完成后至少验证：

1. 配置解析测试覆盖显式清单、自动发现、缺失文件、排除规则和 per-db 保留数量。
2. 小型 SQLite fixture 使用 online backup 生成可通过 `PRAGMA quick_check` 的备份。
3. 单个数据库缺失时，在 `skip_missing=true` 下其他库继续备份。
4. 单个数据库备份失败时，按 `continue_on_database_failure` 控制继续或终止。
5. 每个数据库最多保留默认 3 个备份文件，单库 override 生效。
6. `weekly_data_maintenance` 不再执行旧备份逻辑，报告不再误报备份成功。
7. `database_backup` 调度配置、Telegram 文档、备份实施文档和配置样例保持一致。
