# 数据库备份工作流

## 状态

当前生产数据库备份由独立周任务 `database_backup` 统一负责。旧的 `backup_config` 和 `weekly_data_maintenance -> data_manager.backup_data()` 不再定义生产备份语义。

## 目标

- 统一备份入口，避免独立任务和周维护任务各自执行不同备份逻辑。
- 通过配置声明需要备份的数据库、目标目录、命名规则和保留数量。
- 默认覆盖当前 `data/*.db` 中的生产 SQLite 数据库：`quotes / research / financials / valuation / futures / fx`。
- 每个数据库默认最多保留 3 个备份文件。
- 使用 SQLite online backup，降低直接复制活跃 SQLite 文件导致不一致备份的风险。
- 每个数据库完成后发送 Telegram 通知，整轮结束后发送汇总报告。

## 配置

生产配置位于 `config/04_database.json` 的 `database_backup_config`：

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
      {"name": "quotes", "path": "data/quotes.db", "filename_pattern": "quotes_backup_{timestamp}.db", "max_backup_files": 3},
      {"name": "research", "path": "data/research.db", "filename_pattern": "research_backup_{timestamp}.db", "max_backup_files": 3},
      {"name": "financials", "path": "data/financials.db", "filename_pattern": "financials_backup_{timestamp}.db", "max_backup_files": 3},
      {"name": "valuation", "path": "data/valuation.db", "filename_pattern": "valuation_backup_{timestamp}.db", "max_backup_files": 3},
      {"name": "futures", "path": "data/futures.db", "filename_pattern": "futures_backup_{timestamp}.db", "max_backup_files": 3},
      {"name": "fx", "path": "data/fx.db", "filename_pattern": "fx_backup_{timestamp}.db", "max_backup_files": 3}
    ]
  }
}
```

## 调度

`config/05_scheduler.json` 中的 `database_backup` 是唯一生产周度备份任务，当前默认每周六 `03:30` 执行。

`weekly_data_maintenance` 不再调用旧备份入口，也不再在维护报告中宣称数据库备份成功；备份状态应以 `database_backup` 的每库通知和汇总报告为准。

## 执行流程

1. 解析 `database_backup_config`。
2. 合并显式数据库清单和 `include_globs` 自动发现结果。
3. 跳过 disabled 数据库；对缺失数据库按 `skip_missing` 决定跳过或失败。
4. 创建并校验目标目录，检查可用空间。
5. 默认串行备份每个 SQLite 数据库。
6. 使用 SQLite online backup 分批复制，按 `chunk_pages` 和 `chunk_sleep_seconds` 控制 I/O 压力。
7. 备份完成后打开备份文件并执行 `PRAGMA quick_check`。
8. 按每库 `max_backup_files` 清理旧备份。
9. 发送每库 Telegram 通知和整轮汇总报告。

## 存储前提

生产环境中 `data/` 是 Quote 本地数据卷；`data/PVE-Bak` 和 `data/QuoteBak` 是 NAS 子挂载点。备份任务写入 `data/PVE-Bak/QuoteBak` 前，应确认该路径落在预期备份挂载下，避免 NAS 不可用时把大库写回本地数据卷。

如果需要强制挂载校验，可在 `database_backup_config.performance` 中启用：

```json
{
  "require_backup_mount": true,
  "expected_mount_paths": ["data/PVE-Bak"]
}
```

## 结果判断

- 单库 `success`：备份文件生成、大小非零、SQLite 可打开、`PRAGMA quick_check=ok`。
- 单库 `skipped`：配置允许跳过且源库不存在或被禁用。
- 单库 `failed`：复制、校验、空间、锁等待或目标目录校验失败。
- 整轮 `success`：无 failed 数据库。
- 整轮 `failed`：任一必需数据库 failed；调度任务返回 `False`，不会只因为 APScheduler 包装层完成而被视作业务成功。

## 兼容说明

旧 `backup_config` 仍可被读取为迁移输入，但会记录 deprecation warning。新开发和生产配置必须使用 `database_backup_config`。
