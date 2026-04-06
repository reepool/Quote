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
    },
    "tushare": {
      "enabled": false,
      "exchanges_supported": [
        "a_stock"
      ],
      "primary_source_of": [],
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

### data_sources_config.akshare

```json
{
  "akshare": {
    "enabled": false,
    "exchanges_supported": [
      "a_stock"
    ],
    "primary_source_of": []
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `False`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`exchanges_supported`**: `List` (默认: `['a_stock']`) —— *指明目前本接口数据源能够安全承接哪些宏观维度的查询（例如 a_stock 意味着包揽整个 A 股）*
- **`primary_source_of`**: `List` (默认: `[]`) —— *本接口在哪些市场拥有第一优先级霸主处理身份（不再走兜底查询链路）*
- **`max_requests_per_minute`**: `int` (默认: `8`) —— *单一节点 1 分钟区间由于网络防刷保护限制的查询总额设定*
- **`max_requests_per_hour`**: `int` (默认: `300`) —— *单一节点 1 小时区间防封拦截的查询最大总额设定*
- **`max_requests_per_day`**: `int` (默认: `5000`) —— *单一节点 全日上限阈值设定（超过即熔断当日操作）*
- **`retry_times`**: `int` (默认: `5`) —— *遭遇各类异常时总体默认的尝试拉取重放拦截次数*
- **`retry_interval`**: `float` (默认: `3.0`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
### data_sources_config.yfinance

```json
{
  "yfinance": {
    "enabled": false,
    "exchanges_supported": [
      "a_stock",
      "hk_stock",
      "us_stock"
    ],
    "primary_source_of": []
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `False`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`exchanges_supported`**: `List` (默认: `['a_stock', 'hk_stock', 'us_stock']`) —— *指明目前本接口数据源能够安全承接哪些宏观维度的查询（例如 a_stock 意味着包揽整个 A 股）*
- **`primary_source_of`**: `List` (默认: `[]`) —— *本接口在哪些市场拥有第一优先级霸主处理身份（不再走兜底查询链路）*
- **`max_requests_per_minute`**: `int` (默认: `8`) —— *单一节点 1 分钟区间由于网络防刷保护限制的查询总额设定*
- **`max_requests_per_hour`**: `int` (默认: `300`) —— *单一节点 1 小时区间防封拦截的查询最大总额设定*
- **`max_requests_per_day`**: `int` (默认: `5000`) —— *单一节点 全日上限阈值设定（超过即熔断当日操作）*
- **`retry_times`**: `int` (默认: `3`) —— *遭遇各类异常时总体默认的尝试拉取重放拦截次数*
- **`retry_interval`**: `float` (默认: `2.0`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
### data_sources_config.tushare

```json
{
  "tushare": {
    "enabled": false,
    "exchanges_supported": [
      "a_stock"
    ],
    "primary_source_of": []
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `False`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`exchanges_supported`**: `List` (默认: `['a_stock']`) —— *指明目前本接口数据源能够安全承接哪些宏观维度的查询（例如 a_stock 意味着包揽整个 A 股）*
- **`primary_source_of`**: `List` (默认: `[]`) —— *本接口在哪些市场拥有第一优先级霸主处理身份（不再走兜底查询链路）*
- **`max_requests_per_minute`**: `int` (默认: `200`) —— *单一节点 1 分钟区间由于网络防刷保护限制的查询总额设定*
- **`max_requests_per_hour`**: `int` (默认: `1000`) —— *单一节点 1 小时区间防封拦截的查询最大总额设定*
- **`max_requests_per_day`**: `int` (默认: `2000`) —— *单一节点 全日上限阈值设定（超过即熔断当日操作）*
- **`retry_times`**: `int` (默认: `3`) —— *遭遇各类异常时总体默认的尝试拉取重放拦截次数*
- **`retry_interval`**: `float` (默认: `1.0`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`token`**: `str` (默认: ``) —— *专属金融接口商业鉴权调用 Token（例如 Tushare 会员密匙）*
### data_sources_config.baostock

```json
{
  "baostock": {
    "enabled": true,
    "exchanges_supported": [
      "a_stock"
    ],
    "primary_source_of": [
      "a_stock"
    ]
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`exchanges_supported`**: `List` (默认: `['a_stock']`) —— *指明目前本接口数据源能够安全承接哪些宏观维度的查询（例如 a_stock 意味着包揽整个 A 股）*
- **`primary_source_of`**: `List` (默认: `['a_stock']`) —— *本接口在哪些市场拥有第一优先级霸主处理身份（不再走兜底查询链路）*
- **`max_requests_per_minute`**: `int` (默认: `60`) —— *单一节点 1 分钟区间由于网络防刷保护限制的查询总额设定*
- **`max_requests_per_hour`**: `int` (默认: `3000`) —— *单一节点 1 小时区间防封拦截的查询最大总额设定*
- **`max_requests_per_day`**: `int` (默认: `60000`) —— *单一节点 全日上限阈值设定（超过即熔断当日操作）*
- **`retry_times`**: `int` (默认: `5`) —— *遭遇各类异常时总体默认的尝试拉取重放拦截次数*
- **`retry_interval`**: `float` (默认: `5.0`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`network_error_retry_interval`**: `float` (默认: `10.0`) —— *遭遇服务器主动挂断拒绝或 Timeout 时延缓的重新探测冷却期（防IP 关小黑屋）*
- **`connection_timeout`**: `float` (默认: `30.0`) —— *往接口建连请求或 SSL 握手过程当中的限时放弃标准门槛*
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
      "87"
    ]
  }
}
  ...
```

- **`SSE`**: `List` (默认: `['600', '601', '603', '688', '900']`) —— *上海证券交易所启动参数/预设映射标签*
- **`SZSE`**: `List` (默认: `['000', '001', '002', '300', '200']`) —— *深圳证券交易所启动参数/预设映射标签*
- **`BSE`**: `List` (默认: `['43', '83', '87']`) —— *北京证券交易所启动参数/预设映射标签*
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
    "report": true
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
    "backup_directory": "data/PVE-Bak/QuoteBak"
  }
}
  ...
```

- **`enabled`**: `bool` (默认: `True`) —— *决定是否开启某子项/数据源/子系统的记录与服务开关*
- **`source_db_path`**: `str` (默认: `data/quotes.db`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
- **`backup_directory`**: `str` (默认: `data/PVE-Bak/QuoteBak`) —— *基本开闭设定属性（不填写或按内置模型自动约束处理默认值即可）*
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
