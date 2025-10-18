# 配置文件详解

## 📖 概述

Quote System 使用 `config/config.json` 作为主配置文件，支持数据源、限流、数据库、API、调度器、Telegram等多个方面的配置。本文档详细说明了所有配置项的含义和用法。系统采用完全配置化的设计，支持运行时热重载。

## 🏗️ 配置文件结构 (v2.3.0)

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

## 📱 通知配置 (telegram_config)

### 配置示例
```json
{
  "telegram_config": {
    "enabled": false,
    "bot_token": "your-bot-token",
    "chat_id": "your-chat-id",
    "proxy": {
      "enabled": false,
      "url": "http://proxy-server:port",
      "username": "proxy-username",
      "password": "proxy-password"
    },
    "notifications": {
      "download_completed": true,
      "download_failed": true,
      "system_errors": true,
      "daily_update": false
    },
    "message_templates": {
      "download_completed": "✅ 数据下载完成！\n成功: {success_count}，失败: {failed_count}",
      "download_failed": "❌ 数据下载失败！\n错误: {error_message}"
    }
  }
}
```

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
    "max_workers": 10,
    "job_defaults": {
      "coalesce": true,
      "max_instances": 1,
      "misfire_grace_time": 300
    },
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
| `max_workers` | integer | 10 | 最大工作线程数 |
| `job_defaults.coalesce` | boolean | true | 是否合并相同任务 |
| `job_defaults.max_instances` | integer | 1 | 任务最大实例数 |
| `job_defaults.misfire_grace_time` | integer | 300 | 错失任务的宽限时间 |

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