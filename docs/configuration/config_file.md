# 系统配置参数详解

本文档自动整理自 `config/config.json` 或各模块的 `0*.json` 配置文件。以下是所有支持的配置节点及其参数说明。

## sys_config

```json
{
  "sys_config": {
    "pid_file": "log/process.pid",
    "lock_file": "log/process.lock"
  }
}...
```

- **`pid_file`**: `str` (默认: `log/process.pid`)
- **`lock_file`**: `str` (默认: `log/process.lock`)
- **`single_instance`**: `bool` (默认: `True`)
- **`cleanup_on_startup`**: `bool` (默认: `True`)
### sys_config.persistent_pids

```json
{
  "persistent_pids": {
    "scheduler": "log/scheduler.pid",
    "api_server": "log/api_server.pid"
  }
}
```

- **`scheduler`**: `str` (默认: `log/scheduler.pid`)
- **`api_server`**: `str` (默认: `log/api_server.pid`)
## logging_config

```json
{
  "logging_config": {
    "level": "INFO",
    "format": "[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d] - %(message)s"
  }
}...
```

- **`level`**: `str` (默认: `INFO`)
- **`format`**: `str` (默认: `[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d] - %(message)s`)
- **`date_format`**: `str` (默认: `%Y-%m-%d %H:%M:%S`)
### logging_config.file_config

```json
{
  "file_config": {
    "enabled": true,
    "directory": "log"
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`directory`**: `str` (默认: `log`)
- **`filename`**: `str` (默认: `sys.log`)
#### file_config.rotation

```json
{
  "rotation": {
    "enabled": true,
    "type": "size"
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`type`**: `str` (默认: `size`)
- **`max_bytes_mb`**: `int` (默认: `50`)
- **`backup_count`**: `int` (默认: `10`)
### logging_config.console_config

```json
{
  "console_config": {
    "enabled": true,
    "level": "INFO"
  }
}
```

- **`enabled`**: `bool` (默认: `True`)
- **`level`**: `str` (默认: `INFO`)
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
    }
  }
}...
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

- **`level`**: `str` (默认: `INFO`)
- **`enabled`**: `bool` (默认: `True`)
#### modules.DataSource

```json
{
  "DataSource": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`)
- **`enabled`**: `bool` (默认: `True`)
#### modules.Database

```json
{
  "Database": {
    "level": "WARNING",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `WARNING`)
- **`enabled`**: `bool` (默认: `True`)
#### modules.API

```json
{
  "API": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`)
- **`enabled`**: `bool` (默认: `True`)
#### modules.Scheduler

```json
{
  "Scheduler": {
    "level": "DEBUG",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `DEBUG`)
- **`enabled`**: `bool` (默认: `True`)
#### modules.Monitor

```json
{
  "Monitor": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`)
- **`enabled`**: `bool` (默认: `True`)
#### modules.TaskManager

```json
{
  "TaskManager": {
    "level": "DEBUG",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `DEBUG`)
- **`enabled`**: `bool` (默认: `True`)
#### modules.TelegramBot

```json
{
  "TelegramBot": {
    "level": "DEBUG",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `DEBUG`)
- **`enabled`**: `bool` (默认: `True`)
#### modules.AkShareSource

```json
{
  "AkShareSource": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`)
- **`enabled`**: `bool` (默认: `True`)
#### modules.YFinanceSource

```json
{
  "YFinanceSource": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`)
- **`enabled`**: `bool` (默认: `True`)
#### modules.Tushare

```json
{
  "Tushare": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`)
- **`enabled`**: `bool` (默认: `True`)
#### modules.Baostock

```json
{
  "Baostock": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`)
- **`enabled`**: `bool` (默认: `True`)
#### modules.tg_task_manager

```json
{
  "tg_task_manager": {
    "level": "INFO",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `INFO`)
- **`enabled`**: `bool` (默认: `True`)
#### modules.Report

```json
{
  "Report": {
    "level": "DEBUG",
    "enabled": true
  }
}
```

- **`level`**: `str` (默认: `DEBUG`)
- **`enabled`**: `bool` (默认: `True`)
### logging_config.performance_monitoring

```json
{
  "performance_monitoring": {
    "enabled": true,
    "slow_operation_threshold_seconds": 2.0
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`slow_operation_threshold_seconds`**: `float` (默认: `2.0`)
- **`log_memory_usage`**: `bool` (默认: `True`)
- **`collect_metrics`**: `bool` (默认: `True`)
## telegram_config

```json
{
  "telegram_config": {
    "enabled": true,
    "api_id": ""
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`api_id`**: `str` (默认: ``)
- **`api_hash`**: `str` (默认: ``)
- **`bot_token`**: `str` (默认: ``)
- **`chat_id`**: `List` (默认: `['']`)
- **`session_name`**: `str` (默认: `MsgBot`)
- **`connection_retries`**: `int` (默认: `3`)
- **`retry_delay`**: `int` (默认: `5`)
- **`flood_sleep_threshold`**: `int` (默认: `60`)
- **`auto_reconnect`**: `bool` (默认: `True`)
- **`timeout`**: `int` (默认: `30`)
### telegram_config.intervals

```json
{
  "intervals": {
    "tg_msg_retry_interval": 3,
    "tg_msg_retry_times": 5
  }
}...
```

- **`tg_msg_retry_interval`**: `int` (默认: `3`)
- **`tg_msg_retry_times`**: `int` (默认: `5`)
- **`tg_connect_timeout`**: `int` (默认: `30`)
- **`tg_auto_reconnect`**: `bool` (默认: `True`)
- **`tg_max_reconnect_attempts`**: `int` (默认: `5`)
- **`tg_reconnect_delay`**: `int` (默认: `10`)
## database_config

```json
{
  "database_config": {
    "db_path": "data/quotes.db",
    "backup_enabled": false
  }
}...
```

- **`db_path`**: `str` (默认: `data/quotes.db`)
- **`backup_enabled`**: `bool` (默认: `False`)
- **`backup_interval_days`**: `int` (默认: `7`)
## data_config

```json
{
  "data_config": {
    "data_dir": "data",
    "batch_size": 50
  }
}...
```

- **`data_dir`**: `str` (默认: `data`)
- **`batch_size`**: `int` (默认: `50`)
- **`download_chunk_days`**: `int` (默认: `0`)
- **`max_concurrent_downloads`**: `int` (默认: `1`)
- **`retry_times`**: `int` (默认: `3`)
- **`retry_interval_seconds`**: `int` (默认: `5`)
### data_config.default_start_years

```json
{
  "default_start_years": {
    "SSE": 1990,
    "SZSE": 1990
  }
}...
```

- **`SSE`**: `int` (默认: `1990`)
- **`SZSE`**: `int` (默认: `1990`)
- **`BSE`**: `int` (默认: `2014`)
- **`HKEX`**: `int` (默认: `1990`)
- **`NASDAQ`**: `int` (默认: `1990`)
- **`NYSE`**: `int` (默认: `1990`)
### data_config.market_presets

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
    ]
  }
}...
```

- **`a_shares`**: `List` (默认: `['SSE', 'SZSE', 'BSE']`)
- **`hk_stocks`**: `List` (默认: `['HKEX']`)
- **`us_stocks`**: `List` (默认: `['NASDAQ', 'NYSE']`)
- **`all_markets`**: `List` (默认: `['SSE', 'SZSE', 'BSE', 'HKEX', 'NASDAQ', 'NYSE']`)
- **`mainland`**: `List` (默认: `['SSE', 'SZSE', 'BSE']`)
- **`overseas`**: `List` (默认: `['HKEX', 'NASDAQ', 'NYSE']`)
- **`chinese`**: `List` (默认: `['SSE', 'SZSE', 'BSE', 'HKEX']`)
- **`global`**: `List` (默认: `['NASDAQ', 'NYSE']`)
## data_sources

```json
{
  "data_sources": {
    "a_stock": {
      "enabled": true
    },
    "hk_stock": {
      "enabled": false
    }
  }
}...
```

### data_sources.a_stock

```json
{
  "a_stock": {
    "enabled": true
  }
}
```

- **`enabled`**: `bool` (默认: `True`)
### data_sources.hk_stock

```json
{
  "hk_stock": {
    "enabled": false
  }
}
```

- **`enabled`**: `bool` (默认: `False`)
### data_sources.us_stock

```json
{
  "us_stock": {
    "enabled": false
  }
}
```

- **`enabled`**: `bool` (默认: `False`)
## data_sources_config

```json
{
  "data_sources_config": {
    "akshare": {
      "enabled": false,
      "exchanges_supported": [
        "a_stock"
      ],
      "primary_source_of": [],
      "max_requests_per_minute": 8,
      "max_requests_per_hour": 300,
      "max_requests_per_day": 5000,
      "retry_times": 5,
      "retry_interval": 3.0
    },
    "yfinance": {
      "enabled": false,
      "exchanges_supported": [
        "a_stock",
        "hk_stock",
        "us_stock"
      ],
      "primary_source_of": [],
      "max_requests_per_minute": 8,
      "max_requests_per_hour": 300,
      "max_requests_per_day": 5000,
      "retry_times": 3,
      "retry_interval": 2.0
    }
  }
}...
```

### data_sources_config.akshare

```json
{
  "akshare": {
    "enabled": false,
    "exchanges_supported": [
      "a_stock"
    ]
  }
}...
```

- **`enabled`**: `bool` (默认: `False`)
- **`exchanges_supported`**: `List` (默认: `['a_stock']`)
- **`primary_source_of`**: `List` (默认: `[]`)
- **`max_requests_per_minute`**: `int` (默认: `8`)
- **`max_requests_per_hour`**: `int` (默认: `300`)
- **`max_requests_per_day`**: `int` (默认: `5000`)
- **`retry_times`**: `int` (默认: `5`)
- **`retry_interval`**: `float` (默认: `3.0`)
### data_sources_config.yfinance

```json
{
  "yfinance": {
    "enabled": false,
    "exchanges_supported": [
      "a_stock",
      "hk_stock",
      "us_stock"
    ]
  }
}...
```

- **`enabled`**: `bool` (默认: `False`)
- **`exchanges_supported`**: `List` (默认: `['a_stock', 'hk_stock', 'us_stock']`)
- **`primary_source_of`**: `List` (默认: `[]`)
- **`max_requests_per_minute`**: `int` (默认: `8`)
- **`max_requests_per_hour`**: `int` (默认: `300`)
- **`max_requests_per_day`**: `int` (默认: `5000`)
- **`retry_times`**: `int` (默认: `3`)
- **`retry_interval`**: `float` (默认: `2.0`)
### data_sources_config.tushare

```json
{
  "tushare": {
    "enabled": false,
    "exchanges_supported": [
      "a_stock"
    ]
  }
}...
```

- **`enabled`**: `bool` (默认: `False`)
- **`exchanges_supported`**: `List` (默认: `['a_stock']`)
- **`primary_source_of`**: `List` (默认: `[]`)
- **`max_requests_per_minute`**: `int` (默认: `200`)
- **`max_requests_per_hour`**: `int` (默认: `1000`)
- **`max_requests_per_day`**: `int` (默认: `2000`)
- **`retry_times`**: `int` (默认: `3`)
- **`retry_interval`**: `float` (默认: `1.0`)
- **`token`**: `str` (默认: ``)
### data_sources_config.baostock

```json
{
  "baostock": {
    "enabled": true,
    "exchanges_supported": [
      "a_stock"
    ]
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`exchanges_supported`**: `List` (默认: `['a_stock']`)
- **`primary_source_of`**: `List` (默认: `['a_stock']`)
- **`max_requests_per_minute`**: `int` (默认: `60`)
- **`max_requests_per_hour`**: `int` (默认: `3000`)
- **`max_requests_per_day`**: `int` (默认: `60000`)
- **`retry_times`**: `int` (默认: `5`)
- **`retry_interval`**: `float` (默认: `5.0`)
- **`network_error_retry_interval`**: `float` (默认: `10.0`)
- **`connection_timeout`**: `float` (默认: `30.0`)
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
    }
  }
}...
```

### exchange_rules.exchange_mapping

```json
{
  "exchange_mapping": {
    "SSE": "a_stock",
    "SZSE": "a_stock"
  }
}...
```

- **`SSE`**: `str` (默认: `a_stock`)
- **`SZSE`**: `str` (默认: `a_stock`)
- **`BSE`**: `str` (默认: `a_stock`)
- **`HKEX`**: `str` (默认: `hk_stock`)
- **`NASDAQ`**: `str` (默认: `us_stock`)
- **`NYSE`**: `str` (默认: `us_stock`)
### exchange_rules.symbol_rules

```json
{
  "symbol_rules": {
    "SZSE": 6,
    "SSE": 6
  }
}...
```

- **`SZSE`**: `int` (默认: `6`)
- **`SSE`**: `int` (默认: `6`)
- **`BSE`**: `int` (默认: `6`)
- **`HKEX`**: `List` (默认: `[4, 5]`)
- **`NASDAQ`**: `NoneType` (默认: `None`)
- **`NYSE`**: `NoneType` (默认: `None`)
### exchange_rules.symbol_start_with

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
    ]
  }
}...
```

- **`SSE`**: `List` (默认: `['600', '601', '603', '688', '900']`)
- **`SZSE`**: `List` (默认: `['000', '001', '002', '300', '200']`)
- **`BSE`**: `List` (默认: `['43', '83', '87']`)
- **`HKEX`**: `List` (默认: `['0', '00', '000']`)
## api_config

```json
{
  "api_config": {
    "host": "0.0.0.0",
    "port": 8000
  }
}...
```

- **`host`**: `str` (默认: `0.0.0.0`)
- **`port`**: `int` (默认: `8000`)
- **`workers`**: `int` (默认: `1`)
- **`reload`**: `bool` (默认: `False`)
- **`cors_origins`**: `List` (默认: `['*']`)
## scheduler_config

```json
{
  "scheduler_config": {
    "enabled": true,
    "timezone": "Asia/Shanghai"
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`timezone`**: `str` (默认: `Asia/Shanghai`)
- **`max_instances`**: `int` (默认: `10`)
- **`misfire_grace_time`**: `int` (默认: `300`)
- **`coalesce`**: `bool` (默认: `True`)
- **`monitor_startup_delay`**: `int` (默认: `3`)
- **`market_close_delay_minutes`**: `int` (默认: `15`)
### scheduler_config.jobs

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
    }
  }
}...
```

#### jobs.daily_data_update

```json
{
  "daily_data_update": {
    "enabled": true,
    "description": "每日数据更新任务"
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`description`**: `str` (默认: `每日数据更新任务`)
- **`report`**: `bool` (默认: `True`)
- **`trigger`**: `Object`
- **`max_instances`**: `int` (默认: `1`)
- **`misfire_grace_time`**: `int` (默认: `600`)
- **`coalesce`**: `bool` (默认: `True`)
- **`parameters`**: `Object`
#### jobs.system_health_check

```json
{
  "system_health_check": {
    "enabled": true,
    "description": "系统健康检查"
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`description`**: `str` (默认: `系统健康检查`)
- **`report`**: `bool` (默认: `True`)
- **`trigger`**: `Object`
- **`max_instances`**: `int` (默认: `1`)
- **`misfire_grace_time`**: `int` (默认: `300`)
- **`coalesce`**: `bool` (默认: `True`)
- **`parameters`**: `Object`
#### jobs.weekly_data_maintenance

```json
{
  "weekly_data_maintenance": {
    "enabled": true,
    "description": "每周数据维护"
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`description`**: `str` (默认: `每周数据维护`)
- **`report`**: `bool` (默认: `True`)
- **`trigger`**: `Object`
- **`max_instances`**: `int` (默认: `1`)
- **`misfire_grace_time`**: `int` (默认: `1800`)
- **`coalesce`**: `bool` (默认: `True`)
- **`parameters`**: `Object`
#### jobs.monthly_data_integrity_check

```json
{
  "monthly_data_integrity_check": {
    "enabled": true,
    "description": "月度数据完整性检查和缺口修复"
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`description`**: `str` (默认: `月度数据完整性检查和缺口修复`)
- **`report`**: `bool` (默认: `True`)
- **`trigger`**: `Object`
- **`max_instances`**: `int` (默认: `1`)
- **`misfire_grace_time`**: `int` (默认: `3600`)
- **`coalesce`**: `bool` (默认: `True`)
- **`parameters`**: `Object`
#### jobs.quarterly_cleanup

```json
{
  "quarterly_cleanup": {
    "enabled": true,
    "description": "季度数据清理"
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`description`**: `str` (默认: `季度数据清理`)
- **`report`**: `bool` (默认: `True`)
- **`trigger`**: `Object`
- **`max_instances`**: `int` (默认: `1`)
- **`misfire_grace_time`**: `int` (默认: `1800`)
- **`coalesce`**: `bool` (默认: `True`)
- **`parameters`**: `Object`
#### jobs.cache_warm_up

```json
{
  "cache_warm_up": {
    "enabled": true,
    "description": "缓存预热"
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`description`**: `str` (默认: `缓存预热`)
- **`report`**: `bool` (默认: `False`)
- **`trigger`**: `Object`
- **`max_instances`**: `int` (默认: `1`)
- **`misfire_grace_time`**: `int` (默认: `600`)
- **`coalesce`**: `bool` (默认: `True`)
- **`parameters`**: `Object`
#### jobs.trading_calendar_update

```json
{
  "trading_calendar_update": {
    "enabled": true,
    "description": "交易日历更新"
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`description`**: `str` (默认: `交易日历更新`)
- **`report`**: `bool` (默认: `True`)
- **`trigger`**: `Object`
- **`max_instances`**: `int` (默认: `1`)
- **`misfire_grace_time`**: `int` (默认: `1800`)
- **`coalesce`**: `bool` (默认: `True`)
- **`parameters`**: `Object`
#### jobs.database_backup

```json
{
  "database_backup": {
    "enabled": true,
    "description": "数据库备份任务"
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`description`**: `str` (默认: `数据库备份任务`)
- **`report`**: `bool` (默认: `True`)
- **`trigger`**: `Object`
- **`max_instances`**: `int` (默认: `1`)
- **`misfire_grace_time`**: `int` (默认: `1800`)
- **`coalesce`**: `bool` (默认: `True`)
- **`parameters`**: `Object`
## monitor_config

```json
{
  "monitor_config": {
    "enabled": true,
    "max_history_size": 1000
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`max_history_size`**: `int` (默认: `1000`)
- **`startup_delay`**: `int` (默认: `2`)
- **`min_wait_time`**: `int` (默认: `1`)
### monitor_config.alert_thresholds

```json
{
  "alert_thresholds": {
    "max_execution_time": 300,
    "max_failure_rate": 0.1
  }
}...
```

- **`max_execution_time`**: `int` (默认: `300`)
- **`max_failure_rate`**: `float` (默认: `0.1`)
- **`max_consecutive_failures`**: `int` (默认: `3`)
## backup_config

```json
{
  "backup_config": {
    "enabled": true,
    "source_db_path": "data/quotes.db"
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`source_db_path`**: `str` (默认: `data/quotes.db`)
- **`backup_directory`**: `str` (默认: `data/PVE-Bak/QuoteBak`)
- **`retention_days`**: `int` (默认: `30`)
- **`compression_enabled`**: `bool` (默认: `False`)
- **`notification_enabled`**: `bool` (默认: `True`)
- **`filename_pattern`**: `str` (默认: `quotes_backup_{timestamp}.db`)
- **`max_backup_files`**: `int` (默认: `10`)
## cache_config

```json
{
  "cache_config": {
    "enabled": true,
    "max_memory_mb": 512
  }
}...
```

- **`enabled`**: `bool` (默认: `True`)
- **`max_memory_mb`**: `int` (默认: `512`)
- **`default_ttl_seconds`**: `int` (默认: `3600`)
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
    }
  }
}...
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
    }
  }
}...
```

#### templates.download_report

```json
{
  "download_report": {
    "name": "数据下载报告",
    "emoji": "📊"
  }
}...
```

- **`name`**: `str` (默认: `数据下载报告`)
- **`emoji`**: `str` (默认: `📊`)
- **`output_format`**: `str` (默认: `telegram`)
- **`enabled`**: `bool` (默认: `True`)
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '📊 *{name}*'}, {'name': 'status', 'type': 'status', 'data_source': 'success_rate'}, {'name': 'summary', 'type': 'metrics', 'fields': ['total_instruments', 'success_count', 'failure_count', 'total_quotes', 'success_rate']}, {'name': 'details', 'type': 'table', 'data_source': 'exchange_stats'}, {'name': 'footer', 'type': 'static', 'content': '🔄 {source} - {time}'}]`)
#### templates.daily_update_report

```json
{
  "daily_update_report": {
    "name": "每日数据更新报告",
    "emoji": "📈"
  }
}...
```

- **`name`**: `str` (默认: `每日数据更新报告`)
- **`emoji`**: `str` (默认: `📈`)
- **`output_format`**: `str` (默认: `telegram`)
- **`enabled`**: `bool` (默认: `True`)
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '📈 *{name}*'}, {'name': 'summary', 'type': 'metrics', 'fields': ['date', 'updated_instruments', 'new_quotes', 'success_rate'], 'title': '更新摘要', 'condition': 'not non_trading_day'}, {'name': 'non_trading_day_info', 'type': 'static', 'content': '🗓️ *非交易日通知*\n\n今天是 {date}，市场未开盘，跳过数据更新。\n\n*交易日历更新情况:*\n{calendar_updates}', 'condition': 'non_trading_day'}, {'name': 'error_info', 'type': 'static', 'content': '❌ *更新失败*\n\n错误信息: {error_message}', 'condition': "status == 'error'"}, {'name': 'status', 'type': 'status', 'data_source': 'status', 'condition': "status != 'error'"}, {'name': 'exchange_stats_title', 'type': 'static', 'content': '*交易所统计*', 'condition': "not non_trading_day and status != 'error'"}, {'name': 'exchange_stats', 'type': 'table', 'data_source': 'exchange_stats'}, {'name': 'footer', 'type': 'static', 'content': '🔄 {source} - {generated_at}'}]`)
#### templates.gap_report

```json
{
  "gap_report": {
    "name": "数据缺口报告",
    "emoji": "🔍"
  }
}...
```

- **`name`**: `str` (默认: `数据缺口报告`)
- **`emoji`**: `str` (默认: `🔍`)
- **`output_format`**: `str` (默认: `telegram`)
- **`enabled`**: `bool` (默认: `True`)
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '🔍 *{name}*'}, {'name': 'summary', 'type': 'metrics', 'fields': ['total_gaps', 'affected_stocks', 'severity_distribution'], 'title': '缺口摘要', 'condition': "status != 'error'"}, {'name': 'error_info', 'type': 'static', 'content': '❌ *缺口检测/填补失败*\n\n错误信息: {error_message}', 'condition': "status == 'error'"}, {'name': 'status', 'type': 'status', 'data_source': 'status', 'condition': "status != 'error'"}, {'name': 'top_affected_stocks_title', 'type': 'static', 'content': '*受影响最严重的股票*', 'condition': "status != 'error'"}, {'name': 'details', 'type': 'list', 'data_source': 'top_affected_stocks', 'display_format': '{symbol} - 缺失天数: {total_missing_days}, 严重分数: {severity_score}'}, {'name': 'footer', 'type': 'static', 'content': '🔄 {source} - {generated_at}'}]`)
#### templates.system_status

```json
{
  "system_status": {
    "name": "系统状态报告",
    "emoji": "🏥"
  }
}...
```

- **`name`**: `str` (默认: `系统状态报告`)
- **`emoji`**: `str` (默认: `🏥`)
- **`output_format`**: `str` (默认: `telegram`)
- **`enabled`**: `bool` (默认: `True`)
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '🏥 *{name}*'}, {'name': 'status', 'type': 'status', 'data_source': 'system_status'}, {'name': 'summary', 'type': 'metrics', 'fields': ['database_stats', 'data_sources']}, {'name': 'details', 'type': 'metrics', 'fields': ['database_stats', 'data_sources']}]`)
#### templates.backup_result

```json
{
  "backup_result": {
    "name": "数据库备份报告",
    "emoji": "💾"
  }
}...
```

- **`name`**: `str` (默认: `数据库备份报告`)
- **`emoji`**: `str` (默认: `💾`)
- **`output_format`**: `str` (默认: `telegram`)
- **`enabled`**: `bool` (默认: `True`)
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '💾 *{name}*'}, {'name': 'status', 'type': 'status', 'data_source': 'backup_result'}, {'name': 'details', 'type': 'metrics', 'fields': ['backup_file', 'file_size', 'duration']}]`)
#### templates.health_check_report

```json
{
  "health_check_report": {
    "name": "系统健康检查报告",
    "emoji": "🏥"
  }
}...
```

- **`name`**: `str` (默认: `系统健康检查报告`)
- **`emoji`**: `str` (默认: `🏥`)
- **`output_format`**: `str` (默认: `telegram`)
- **`enabled`**: `bool` (默认: `True`)
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '🏥 *{name}*'}, {'name': 'summary', 'type': 'metrics', 'fields': ['overall_status', 'checks_performed', 'issues_found'], 'title': '检查摘要'}, {'name': 'details', 'type': 'list', 'data_source': 'check_results', 'display_format': '{status_icon} {check_name}: {result}', 'title': '详细检查结果'}]`)
#### templates.maintenance_report

```json
{
  "maintenance_report": {
    "name": "数据维护报告",
    "emoji": "🔧"
  }
}...
```

- **`name`**: `str` (默认: `数据维护报告`)
- **`emoji`**: `str` (默认: `🔧`)
- **`output_format`**: `str` (默认: `telegram`)
- **`enabled`**: `bool` (默认: `True`)
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '🔧 *{name}*'}, {'name': 'status', 'type': 'status', 'data_source': 'tasks_completed'}, {'name': 'summary', 'type': 'metrics', 'fields': ['tasks_completed', 'space_freed', 'duration']}, {'name': 'details', 'type': 'list', 'data_source': 'maintenance_tasks', 'display_format': '{task_name} - 状态: {status}'}]`)
#### templates.cache_warm_up_report

```json
{
  "cache_warm_up_report": {
    "name": "缓存预热报告",
    "emoji": "⚡"
  }
}...
```

- **`name`**: `str` (默认: `缓存预热报告`)
- **`emoji`**: `str` (默认: `⚡`)
- **`output_format`**: `str` (默认: `telegram`)
- **`enabled`**: `bool` (默认: `True`)
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '⚡ *{name}*'}, {'name': 'status', 'type': 'status', 'data_source': 'stocks_warmed'}, {'name': 'summary', 'type': 'metrics', 'fields': ['stocks_warmed', 'cache_hit_rate', 'duration']}, {'name': 'details', 'type': 'metrics', 'fields': ['popular_stocks', 'market_indices', 'recent_data']}]`)
#### templates.trading_calendar_report

```json
{
  "trading_calendar_report": {
    "name": "交易日历更新报告",
    "emoji": "📅"
  }
}...
```

- **`name`**: `str` (默认: `交易日历更新报告`)
- **`emoji`**: `str` (默认: `📅`)
- **`output_format`**: `str` (默认: `telegram`)
- **`enabled`**: `bool` (默认: `True`)
- **`sections`**: `List` (默认: `[{'name': 'header', 'type': 'static', 'content': '📅 *{name}*'}, {'name': 'summary', 'type': 'metrics', 'fields': ['exchanges_updated', 'trading_days_added', 'holidays_added'], 'title': '更新摘要', 'condition': "status != 'error'"}, {'name': 'error_info', 'type': 'static', 'content': '❌ *交易日历更新失败*\n\n错误信息: {error_message}', 'condition': "status == 'error'"}, {'name': 'status', 'type': 'status', 'data_source': 'status', 'condition': "status != 'error'"}, {'name': 'exchange_details_title', 'type': 'static', 'content': '*交易所详情*', 'condition': "status != 'error'"}, {'name': 'details', 'type': 'table', 'data_source': 'exchange_details'}, {'name': 'footer', 'type': 'static', 'content': '🔄 {source} - {generated_at}'}]`)
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
    }
  }
}...
```

#### formats.telegram

```json
{
  "telegram": {
    "max_length": 4000,
    "line_separator": "\n\n"
  }
}...
```

- **`max_length`**: `int` (默认: `4000`)
- **`line_separator`**: `str` (默认: `

`)
- **`bold_format`**: `str` (默认: `*{text}*`)
- **`code_format`**: `str` (默认: ``{text}``)
#### formats.console

```json
{
  "console": {
    "max_width": 100,
    "line_separator": "\n\n"
  }
}...
```

- **`max_width`**: `int` (默认: `100`)
- **`line_separator`**: `str` (默认: `

`)
- **`table_style`**: `str` (默认: `grid`)
#### formats.api

```json
{
  "api": {
    "include_raw_data": true,
    "response_format": "json"
  }
}
```

- **`include_raw_data`**: `bool` (默认: `True`)
- **`response_format`**: `str` (默认: `json`)
### report_config.field_labels

```json
{
  "field_labels": {
    "total_instruments": "处理股票",
    "success_count": "成功数量"
  }
}...
```

- **`total_instruments`**: `str` (默认: `处理股票`)
- **`success_count`**: `str` (默认: `成功数量`)
- **`failure_count`**: `str` (默认: `失败数量`)
- **`quotes_added`**: `str` (默认: `新增行情`)
- **`total_quotes`**: `str` (默认: `行情数据`)
- **`success_rate`**: `str` (默认: `成功率`)
- **`updated_instruments`**: `str` (默认: `更新股票`)
- **`new_quotes`**: `str` (默认: `新增行情`)
- **`total_gaps`**: `str` (默认: `总缺口数`)
- **`affected_stocks`**: `str` (默认: `受影响股票`)
- **`backup_file`**: `str` (默认: `备份文件`)
- **`file_size`**: `str` (默认: `文件大小`)
- **`duration`**: `str` (默认: `耗时`)
- **`overall_status`**: `str` (默认: `总体状态`)
- **`checks_performed`**: `str` (默认: `检查数量`)
- **`issues_found`**: `str` (默认: `问题数量`)
