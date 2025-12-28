# 配置文件详解

## 📖 概述

当前版本使用 `config/` 目录下的**分文件配置**，启动时会按文件名顺序合并加载（例如 `00_sys.json`、`01_log.json`、`05_scheduler.json`）。无需维护单一巨大 `config.json`。

## 🏗️ 配置文件结构

常用配置文件如下（示例）：
```
config/
├── 00_sys.json          # 系统基础配置
├── 01_log.json          # 日志配置
├── 02_tg.json           # Telegram 配置
├── 03_data.json         # 数据下载与缓存配置
├── 04_database.json     # 数据库配置
├── 05_scheduler.json    # 调度器配置
├── 07_api.json          # API 配置
├── 08_cache.json        # 缓存配置
└── 09_report.json       # 报告模板配置
```

所有文件在启动时合并为统一配置树（`config_manager.get_*` 读取）。
   - 添加配置变更审计

### 模块化优势
- ✅ **维护性提升**: 每个模块职责单一，易于维护
- ✅ **协作效率**: 不同团队可并行修改不同模块
- ✅ **启动优化**: 按需加载配置，提升启动速度
- ✅ **版本管理**: 精确跟踪各模块配置变更
- ✅ **环境隔离**: 不同环境使用不同配置覆盖

```json
{
  "sys_config": {
    // 系统基础配置
  },
  "logging_config": {
    // 日志系统配置
  },
  "telegram_config": {
    // Telegram机器人和任务管理配置 ⭐
  },
  "database_config": {
    // 数据库配置
  },
  "data_config": {
    // 数据下载和缓存配置
  },
  "data_sources": {
    // 数据源启用配置
  },
  "data_sources_config": {
    // 各数据源详细配置
  },
  "exchange_rules": {
    // 交易所规则和映射配置
  },
  "api_config": {
    // RESTful API服务配置
  },
  "scheduler_config": {
    // 任务调度系统配置 ⭐
  },
  "backup_config": {
    // 自动备份配置 ⭐
  },
  "cache_config": {
    // 缓存系统配置
  },
  "report_config": {
    // 报告和通知模板配置
  }
}
```

## 📊 数据源配置 (data_sources)

### 配置示例
```json
{
  "data_sources": {
    "baostock_a_stock": {
      "enabled": true,
      "priority": 1,
      "exchanges": ["SSE", "SZSE"],
      "config": {
        "timeout": 30,
        "retry_times": 3,
        "retry_interval": 1.0
      }
    },
    "akshare_a_stock": {
      "enabled": false,
      "priority": 2,
      "exchanges": ["SSE", "SZSE"]
    },
    "tushare_a_stock": {
      "enabled": false,
      "priority": 3,
      "exchanges": ["SSE", "SZSE"],
      "config": {
        "token": "your_tushare_token"
      }
    }
  }
}
```

### 配置项说明

| 字段 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `enabled` | boolean | ✅ | 是否启用该数据源 |
| `priority` | integer | ✅ | 优先级（数字越小优先级越高） |
| `exchanges` | array | ✅ | 支持的交易所列表 |
| `config` | object | ❌ | 数据源特定配置 |

### 支持的数据源

#### 1. BaoStock (baostock_a_stock)
- **适用市场**: 中国A股（SSE、SZSE）
- **特点**: 免费、稳定、数据质量高
- **配置项**:
  - `timeout`: 请求超时时间（秒）
  - `retry_times`: 重试次数
  - `retry_interval`: 重试间隔（秒）

#### 2. AkShare (akshare_a_stock)
- **适用市场**: 中国A股、港股、美股
- **特点**: 开源、数据源丰富
- **注意**: 需要安装 akshare 包

#### 3. Tushare (tushare_a_stock)
- **适用市场**: 中国A股、港股、美股
- **特点**: 专业级、数据质量高
- **注意**: 需要申请 token

#### 4. YFinance (yfinance_us_stock)
- **适用市场**: 美股
- **特点**: 雅虎财经数据源

## ⚡ 限流配置 (rate_limit_config)

### 配置示例
```json
{
  "rate_limit_config": {
    "max_requests_per_minute": 60,
    "max_requests_per_hour": 1000,
    "max_requests_per_day": 10000,
    "retry_times": 3,
    "retry_interval": 1.0,
    "backoff_factor": 2.0,
    "circuit_breaker_threshold": 5,
    "circuit_breaker_timeout": 300
  }
}
```

### 配置项说明

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `max_requests_per_minute` | integer | 60 | 每分钟最大请求数 |
| `max_requests_per_hour` | integer | 1000 | 每小时最大请求数 |
| `max_requests_per_day` | integer | 10000 | 每日最大请求数 |
| `retry_times` | integer | 3 | 重试次数 |
| `retry_interval` | float | 1.0 | 重试间隔（秒） |
| `backoff_factor` | float | 2.0 | 退避因子 |
| `circuit_breaker_threshold` | integer | 5 | 熔断器阈值 |
| `circuit_breaker_timeout` | integer | 300 | 熔断器超时（秒） |

### 限流策略

系统使用多维度限流机制：
1. **分钟级限流**: 防止短时间内过度请求
2. **小时级限流**: 控制总体请求频率
3. **日级限流**: 避免超出日配额
4. **熔断器**: 连续失败时暂停请求

## 📈 数据配置 (data_config)

### 配置示例
```json
{
  "data_config": {
    "download_chunk_days": 2000,
    "batch_size": 50,
    "quality_threshold": 0.7,
    "enable_data_validation": true,
    "data_retention_days": 3650,
    "default_exchanges": ["SSE", "SZSE"],
    "precise_download_mode": true
  }
}
```

### 配置项说明

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `download_chunk_days` | integer | 2000 | 分块下载天数（0=一次性下载） |
| `batch_size` | integer | 50 | 批处理大小 |
| `quality_threshold` | float | 0.7 | 数据质量阈值 |
| `enable_data_validation` | boolean | true | 是否启用数据验证 |
| `data_retention_days` | integer | 3650 | 数据保留天数 |
| `default_exchanges` | array | ["SSE","SZSE"] | 默认交易所 |
| `precise_download_mode` | boolean | true | 精确下载模式 |

### 分块下载策略

- **download_chunk_days = 0**: 一次性下载所有数据
- **download_chunk_days > 0**: 按指定天数分块下载
- **推荐值**: 2000天（约5-6年）

## 🗄️ 数据库配置 (database_config)

### 配置示例
```json
{
  "database_config": {
    "url": "sqlite:///data/quotes.db",
    "pool_size": 10,
    "max_overflow": 20,
    "pool_timeout": 30,
    "pool_recycle": 3600,
    "echo": false,
    "backup_enabled": true,
    "backup_path": "data/backups",
    "backup_retention_days": 30
  }
}
```

### 配置项说明

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `url` | string | sqlite:///data/quotes.db | 数据库连接URL |
| `pool_size` | integer | 10 | 连接池大小 |
| `max_overflow` | integer | 20 | 最大溢出连接数 |
| `pool_timeout` | integer | 30 | 连接池超时（秒） |
| `pool_recycle` | integer | 3600 | 连接回收时间（秒） |
| `echo` | boolean | false | 是否输出SQL语句 |
| `backup_enabled` | boolean | true | 是否启用自动备份 |
| `backup_path` | string | data/backups | 备份路径 |
| `backup_retention_days` | integer | 30 | 备份保留天数 |

### 支持的数据库

- **SQLite**: 默认配置，适合小规模使用
- **PostgreSQL**: 推荐用于生产环境
- **MySQL**: 可选的数据库支持

#### PostgreSQL 配置示例
```json
{
  "database_config": {
    "url": "postgresql://user:password@localhost:5432/quotedb",
    "pool_size": 20,
    "max_overflow": 30
  }
}
```

## 📝 日志配置 (logging_config)

### 配置示例
```json
{
  "logging_config": {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "date_format": "%Y-%m-%d %H:%M:%S",
    "file_config": {
      "enabled": true,
      "path": "log",
      "filename": "sys.log",
      "rotation": {
        "max_bytes_mb": 50,
        "backup_count": 10
      }
    },
    "console_config": {
      "enabled": true,
      "level": "INFO"
    },
    "performance_monitoring": {
      "enabled": true,
      "log_slow_queries": true,
      "slow_query_threshold": 1.0
    }
  }
}
```

### 配置项说明

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `level` | string | INFO | 日志级别 |
| `format` | string | - | 日志格式 |
| `date_format` | string | %Y-%m-%d %H:%M:%S | 日期格式 |
| `file_config.enabled` | boolean | true | 是否启用文件日志 |
| `file_config.path` | string | log | 日志文件路径 |
| `file_config.filename` | string | sys.log | 日志文件名 |
| `file_config.rotation.max_bytes_mb` | integer | 50 | 单个文件最大大小（MB） |
| `file_config.rotation.backup_count` | integer | 10 | 保留的备份文件数 |
| `console_config.enabled` | boolean | true | 是否启用控制台日志 |
| `console_config.level` | string | INFO | 控制台日志级别 |
| `performance_monitoring.enabled` | boolean | true | 是否启用性能监控 |
| `performance_monitoring.log_slow_queries` | boolean | true | 是否记录慢查询 |
| `performance_monitoring.slow_query_threshold` | float | 1.0 | 慢查询阈值（秒） |

### 日志级别

- **DEBUG**: 详细的调试信息
- **INFO**: 一般信息（推荐）
- **WARNING**: 警告信息
- **ERROR**: 错误信息
- **CRITICAL**: 严重错误

## 🌐 API配置 (api_config)

### 配置示例
```json
{
  "api_config": {
    "host": "0.0.0.0",
    "port": 8000,
    "workers": 1,
    "reload": false,
    "cors_enabled": true,
    "cors_origins": ["*"],
    "cors_methods": ["GET", "POST", "PUT", "DELETE"],
    "cors_headers": ["*"],
    "rate_limiting": {
      "enabled": true,
      "requests_per_minute": 100
    },
    "authentication": {
      "enabled": false,
      "secret_key": "your-secret-key",
      "algorithm": "HS256",
      "access_token_expire_minutes": 30
    },
    "documentation": {
      "enabled": true,
      "title": "Quote System API",
      "description": "股票数据管理系统API",
      "version": "2.1.0"
    }
  }
}
```

### 配置项说明

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `host` | string | 0.0.0.0 | 服务器地址 |
| `port` | integer | 8000 | 服务器端口 |
| `workers` | integer | 1 | 工作进程数 |
| `reload` | boolean | false | 是否自动重载 |
| `cors_enabled` | boolean | true | 是否启用CORS |
| `cors_origins` | array | ["*"] | 允许的源 |
| `cors_methods` | array | HTTP方法 | 允许的HTTP方法 |
| `cors_headers` | array | ["*"] | 允许的请求头 |
| `rate_limiting.enabled` | boolean | true | 是否启用API限流 |
| `rate_limiting.requests_per_minute` | integer | 100 | API限流阈值 |
| `authentication.enabled` | boolean | false | 是否启用认证 |
| `authentication.secret_key` | string | - | JWT密钥 |
| `authentication.algorithm` | string | HS256 | JWT算法 |
| `authentication.access_token_expire_minutes` | integer | 30 | 访问令牌过期时间 |
| `documentation.enabled` | boolean | true | 是否启用API文档 |
| `documentation.title` | string | Quote System API | API标题 |
| `documentation.description` | string | - | API描述 |
| `documentation.version` | string | 2.1.0 | API版本 |

## 🤖 Telegram 任务管理配置 (telegram_config) ⭐ v2.3.0

### 完整配置示例
```json
{
  "telegram_config": {
    "enabled": true,
    "api_id": "your_api_id",
    "api_hash": "your_api_hash",
    "bot_token": "your_bot_token",
    "chat_id": ["your_chat_id"],
    "session_name": "MsgBot",
    "task_management": {
      "enabled": true,
      "authorized_users": ["user123", "user456"],
      "admin_users": ["admin123"],
      "commands": {
        "start": "显示主菜单和帮助信息",
        "status": "查看所有任务状态和下次执行时间",
        "detail": "查看指定任务的详细信息",
        "reload_config": "热重载配置文件",
        "help": "显示帮助信息"
      }
    },
    "intervals": {
      "tg_msg_retry_interval": 3,
      "tg_msg_retry_times": 5,
      "tg_connect_timeout": 30,
      "tg_auto_reconnect": true,
      "tg_max_reconnect_attempts": 5,
      "tg_reconnect_delay": 10
    },
    "notifications": {
      "download_completed": true,
      "download_failed": true,
      "system_errors": true,
      "daily_update": false,
      "task_executions": true,
      "backup_completed": true,
      "data_gaps_detected": true
    },
    "time_display": {
      "smart_format": true,
      "timezone": "Asia/Shanghai",
      "relative_threshold_hours": 24
    },
    "message_templates": {
      "task_status": "📋 **任务状态报告**\n\n{task_list}",
      "download_completed": "✅ 数据下载完成！\n成功: {success_count}，失败: {failed_count}",
      "system_error": "🚨 系统错误\n{error_message}",
      "backup_completed": "💾 数据库备份完成\n文件: {backup_file}"
    },
    "proxy": {
      "enabled": false,
      "url": "http://proxy-server:port",
      "username": "proxy-username",
      "password": "proxy-password"
    }
  }
}
```

### 配置项说明

#### 基础配置
| 字段 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `enabled` | boolean | ✅ | 是否启用Telegram功能 |
| `api_id` | integer | ✅ | Telegram API ID |
| `api_hash` | string | ✅ | Telegram API Hash |
| `bot_token` | string | ✅ | 机器人令牌 |
| `chat_id` | array | ✅ | 授权聊天ID列表 |
| `session_name` | string | ❌ | Telethon会话名称 |

#### 任务管理配置 ⭐ v2.3.0
| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `task_management.enabled` | boolean | true | 是否启用任务管理功能 |
| `task_management.authorized_users` | array | [] | 授权用户列表 |
| `task_management.admin_users` | array | [] | 管理员用户列表 |
| `task_management.commands` | object | {} | 自定义命令配置 |

#### 连接和重试配置
| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `intervals.tg_msg_retry_interval` | integer | 3 | 消息重试间隔（秒） |
| `intervals.tg_msg_retry_times` | integer | 5 | 消息重试次数 |
| `intervals.tg_connect_timeout` | integer | 30 | 连接超时（秒） |
| `intervals.tg_auto_reconnect` | boolean | true | 是否自动重连 |
| `intervals.tg_max_reconnect_attempts` | integer | 5 | 最大重连次数 |
| `intervals.tg_reconnect_delay` | integer | 10 | 重连延迟（秒） |

#### 智能时间显示 ⭐ v2.3.0
| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `time_display.smart_format` | boolean | true | 是否启用智能时间格式 |
| `time_display.timezone` | string | Asia/Shanghai | 时区设置 |
| `time_display.relative_threshold_hours` | integer | 24 | 相对时间显示阈值（小时） |

#### 通知配置
| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `notifications.download_completed` | boolean | true | 下载完成通知 |
| `notifications.download_failed` | boolean | true | 下载失败通知 |
| `notifications.system_errors` | boolean | true | 系统错误通知 |
| `notifications.task_executions` | boolean | true | 任务执行通知 |
| `notifications.backup_completed` | boolean | true | 备份完成通知 |
| `notifications.data_gaps_detected` | boolean | true | 数据缺口检测通知 |

### 权限管理
系统支持基于用户ID的权限控制：

- **管理员权限**: 可以执行所有操作，包括配置热重载
- **用户权限**: 可以查看任务状态和基本信息
- **访客权限**: 只能使用基础命令

### 使用示例
```bash
# 启动包含任务管理器的完整系统
python main.py full --host 0.0.0.0 --port 8000

# Telegram中的用户交互
/status                    # 显示所有任务状态
/detail daily_data_update  # 查看特定任务详情
/reload_config            # 热重载配置（需要管理员权限）
```

## 📄 报告系统配置 (report_config) ⭐ v2.3.1

### 配置示例
```json
{
  "report_config": {
    "enabled": true,
    "default_format": "telegram",
    "output_directory": "reports",
    "formats": {
      "telegram": {
        "enabled": true,
        "max_message_length": 4000,
        "parse_mode": "Markdown",
        "include_emojis": true
      },
      "console": {
        "enabled": true,
        "max_width": 100,
        "colors": true,
        "progress_bars": true
      },
      "api": {
        "enabled": true,
        "include_raw_data": false,
        "response_format": "json",
        "pagination_size": 100
      },
      "file": {
        "enabled": true,
        "formats": ["json", "csv", "html"],
        "compression": true,
        "timestamp_files": true
      }
    },
    "templates": {
      "system_status": {
        "name": "系统状态报告",
        "description": "系统运行状态和性能指标",
        "sections": ["overview", "database", "data_sources", "scheduler"],
        "schedule": "0 9 * * 1-5",
        "auto_generate": true
      },
      "data_quality": {
        "name": "数据质量报告",
        "description": "数据完整性和质量评估",
        "sections": ["completeness", "accuracy", "consistency", "gaps"],
        "schedule": "0 10 * * 1",
        "auto_generate": true
      },
      "task_summary": {
        "name": "任务执行摘要",
        "description": "调度器任务执行情况汇总",
        "sections": ["execution_summary", "failed_tasks", "performance_metrics"],
        "schedule": "0 18 * * 1-5",
        "auto_generate": true
      }
    },
    "delivery": {
      "telegram": {
        "enabled": true,
        "chat_ids": ["your_chat_id"],
        "split_long_messages": true
      },
      "email": {
        "enabled": false,
        "smtp_server": "smtp.example.com",
        "recipients": ["admin@example.com"]
      },
      "webhook": {
        "enabled": false,
        "url": "https://your-webhook.example.com/reports"
      }
    },
    "retention": {
      "keep_days": 30,
      "max_files_per_type": 100,
      "cleanup_schedule": "0 2 * * 0"
    }
  }
}
```

### 配置项说明

#### 输出格式配置
| 格式 | 说明 | 特性 |
|------|------|------|
| `telegram` | Telegram消息格式 | 支持Markdown、Emoji |
| `console` | 控制台输出 | 彩色显示、进度条 |
| `api` | API响应格式 | JSON结构化数据 |
| `file` | 文件输出 | 支持JSON/CSV/HTML |

#### 报告模板
| 模板 | 用途 | 自动生成 |
|------|------|----------|
| `system_status` | 系统状态监控 | 每工作日9:00 |
| `data_quality` | 数据质量评估 | 每周一10:00 |
| `task_summary` | 任务执行摘要 | 每工作日18:00 |

#### 传递方式
- **Telegram**: 实时推送到指定聊天
- **Email**: 邮件发送（可选）
- **Webhook**: HTTP回调（可选）

## 💾 自动备份配置 (backup_config) ⭐ v2.3.0

### 配置示例
```json
{
  "backup_config": {
    "enabled": true,
    "source_db_path": "data/quotes.db",
    "backup_directory": "data/PVE-Bak/QuoteBak",
    "retention_days": 30,
    "schedule": {
      "enabled": true,
      "cron": "0 6 * * 6"
    },
    "compression": {
      "enabled": true,
      "algorithm": "gzip",
      "level": 6
    },
    "notification": {
      "enabled": true,
      "telegram": true,
      "email": false
    },
    "verification": {
      "enabled": true,
      "checksum": true,
      "integrity_check": true
    },
    "cleanup": {
      "enabled": true,
      "max_backup_files": 50,
      "auto_delete": true
    },
    "filename_pattern": "quotes_backup_{timestamp}.db",
    "exclude_tables": [],
    "include_tables": ["*"]
  }
}
```

### 配置项说明

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | boolean | true | 是否启用自动备份 |
| `source_db_path` | string | data/quotes.db | 源数据库路径 |
| `backup_directory` | string | data/PVE-Bak/QuoteBak | 备份目录 |
| `retention_days` | integer | 30 | 备份保留天数 |
| `schedule.cron` | string | 0 6 * * 6 | 备份时间（每周六6:00） |
| `compression.enabled` | boolean | true | 是否压缩备份文件 |
| `notification.enabled` | boolean | true | 是否发送通知 |
| `verification.enabled` | boolean | true | 是否验证备份完整性 |

### 备份策略
- **自动备份**: 每周六6:00自动执行
- **增量备份**: 支持仅备份变更数据
- **压缩存储**: 使用gzip压缩节省空间
- **完整性验证**: 备份后自动验证文件完整性
- **自动清理**: 超过保留期的备份自动删除

### 配置项说明

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | boolean | false | 是否启用Telegram通知 |
| `bot_token` | string | - | 机器人令牌 |
| `chat_id` | string | - | 聊天ID |
| `proxy.enabled` | boolean | false | 是否使用代理 |
| `proxy.url` | string | - | 代理URL |
| `proxy.username` | string | - | 代理用户名 |
| `proxy.password` | string | - | 代理密码 |
| `notifications.download_completed` | boolean | true | 下载完成通知 |
| `notifications.download_failed` | boolean | true | 下载失败通知 |
| `notifications.system_errors` | boolean | true | 系统错误通知 |
| `notifications.daily_update` | boolean | false | 每日更新通知 |

## ⏰ 调度器配置 (scheduler_config)

### 配置示例
```json
{
  "scheduler_config": {
    "enabled": true,
    "timezone": "Asia/Shanghai",
    "max_instances": 10,
    "misfire_grace_time": 300,
    "coalesce": true,
    "jobs": {
      "daily_data_update": {
        "enabled": true,
        "trigger": {
          "type": "cron",
          "hour": 20,
          "minute": 30
        },
        "parameters": {
          "exchanges": ["SSE", "SZSE"],
          "wait_for_market_close": true,
          "market_close_delay_minutes": 15
        }
      },
      "trading_calendar_update": {
        "enabled": true,
        "trigger": {
          "type": "cron",
          "day": 1,
          "hour": 1,
          "minute": 0
        },
        "parameters": {
          "exchanges": ["SSE", "SZSE"],
          "update_future_months": 6
        }
      },
      "weekly_data_maintenance": {
        "enabled": true,
        "trigger": {
          "type": "cron",
          "day_of_week": "sun",
          "hour": 2,
          "minute": 0
        }
      },
      "find_gap_and_repair": {
        "enabled": true,
        "trigger": {
          "type": "cron",
          "day_of_week": "sun",
          "hour": 15,
          "minute": 0
        },
        "parameters": {
          "exchanges": ["SSE", "SZSE", "BSE"],
          "start_date": "2024-01-01"
        }
      }
    }
  }
}
```

### 配置项说明

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | boolean | true | 是否启用调度器 |
| `timezone` | string | Asia/Shanghai | 时区 |
| `max_instances` | integer | 10 | 任务最大实例数 |
| `misfire_grace_time` | integer | 300 | 错失任务的宽限时间 |
| `coalesce` | boolean | true | 是否合并相同任务 |

### 任务配置

每个任务包含以下配置：
- `enabled`: 是否启用
- `trigger`: 触发器配置
- `parameters`: 任务参数

#### 触发器类型

1. **cron**: Cron表达式
   ```json
   {
     "type": "cron",
     "day_of_week": "mon-fri",
     "hour": 9,
     "minute": 30
   }
   ```

2. **interval**: 间隔触发
   ```json
   {
     "type": "interval",
     "hours": 1,
     "minutes": 30
   }
   ```

3. **date**: 一次性触发
   ```json
   {
     "type": "date",
     "run_date": "2024-12-31T23:59:59"
   }
   ```

## 🔧 环境变量配置

除了配置文件，系统还支持通过环境变量进行配置：

### 数据库配置
```bash
export DATABASE_URL="postgresql://user:pass@localhost/db"
export DB_POOL_SIZE="20"
export DB_MAX_OVERFLOW="30"
```

### API配置
```bash
export API_HOST="0.0.0.0"
export API_PORT="8000"
export API_WORKERS="4"
```

### 日志配置
```bash
export LOG_LEVEL="INFO"
export LOG_PATH="/var/log/quote"
```

### Telegram配置
```bash
export TELEGRAM_BOT_TOKEN="your-bot-token"
export TELEGRAM_CHAT_ID="your-chat-id"
```

## 📊 配置验证

系统启动时会验证配置文件的正确性：

### 验证项
1. **JSON格式**: 检查配置文件是否为有效JSON
2. **必需字段**: 检查必需的配置项是否存在
3. **数据类型**: 验证配置项的数据类型
4. **值范围**: 检查配置值是否在合理范围内
5. **依赖关系**: 验证配置项之间的依赖关系

### 验证失败处理
- 记录错误日志
- 使用默认值
- 提示修复建议

## 🎯 配置最佳实践

### 1. 生产环境配置
```json
{
  "logging_config": {
    "level": "WARNING",
    "file_config": {
      "rotation": {
        "max_bytes_mb": 100,
        "backup_count": 30
      }
    }
  },
  "api_config": {
    "authentication": {
      "enabled": true
    }
  },
  "database_config": {
    "url": "postgresql://user:pass@localhost/quotedb"
  }
}
```

### 2. 开发环境配置
```json
{
  "logging_config": {
    "level": "DEBUG",
    "console_config": {
      "enabled": true
    }
  },
  "api_config": {
    "reload": true,
    "authentication": {
      "enabled": false
    }
  }
}
```

### 3. 高性能配置
```json
{
  "data_config": {
    "download_chunk_days": 0,
    "batch_size": 100
  },
  "database_config": {
    "pool_size": 50,
    "max_overflow": 100
  },
  "rate_limit_config": {
    "max_requests_per_minute": 120
  }
}
```

## 🔄 配置热更新

系统支持部分配置的热更新：

### 支持热更新的配置
- 日志级别
- 限流参数
- 数据源优先级
- 通知设置

### 热更新方法
```bash
# 发送重载信号
kill -HUP <pid>

# 或使用API
curl -X POST "http://localhost:8000/api/v1/config/reload"
```

## 📝 配置模板

系统提供了多个配置模板：

- `config/config.example.json`: 完整配置示例
- `config/config.dev.json`: 开发环境配置
- `config/config.prod.json`: 生产环境配置
- `config/config.minimal.json`: 最小配置

### 使用配置模板
```bash
# 复制模板
cp config/config.example.json config/config.json

# 根据环境选择模板
cp config/config.prod.json config/config.json
```

## 🚨 配置安全

### 敏感信息处理
- 使用环境变量存储敏感信息
- 配置文件权限控制
- 加密存储密码和令牌

### 权限设置
```bash
# 设置配置文件权限
chmod 600 config/config.json
chown app:app config/config.json
```

## 📞 技术支持

如果配置遇到问题：
1. 检查配置文件JSON格式
2. 查看系统启动日志
3. 验证必需配置项
4. 参考配置模板
5. 提交配置相关问题
