# RESTful API 接口文档

## 📖 概述

Quote System v2.3.1 提供了完整的 RESTful API 接口，支持股票数据查询、下载、系统状态监控、任务管理等功能。API 基于 FastAPI 0.115.0 框架构建，支持异步处理和自动文档生成。系统采用现代化架构，提供高性能的数据访问和管理能力，集成了 Telegram 任务管理系统和智能调度功能。

### 🆕 v2.3.1 新增功能
- **报告生成系统** - 支持多种输出格式的智能报告
- **数据缺口管理** - 自动检测和修复数据缺失
- **增强的任务调度** - 8种内置定时任务的完整API支持
- **智能时间处理** - 多时区支持和智能时间格式化
- **质量评分系统** - 实时数据质量评估

## 🚀 快速开始

### 启动 API 服务
```bash
# 启动 API 服务器
python main.py api --host 0.0.0.0 --port 8000

# 启动完整系统（包含 API）
python main.py full --host 0.0.0.0 --port 8000
```

### 访问 API 文档
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

## 📋 API 基础信息

### 基础 URL
```
http://localhost:8000/api/v1
```

### 通用响应格式
```json
{
  "success": true,
  "data": {},
  "message": "操作成功",
  "timestamp": "2024-10-11T16:00:00Z"
}
```

### 错误响应格式
```json
{
  "success": false,
  "error": {
    "code": "INVALID_PARAMETER",
    "message": "参数错误",
    "details": "股票代码格式不正确"
  },
  "timestamp": "2024-10-11T16:00:00Z"
}
```

## 🏢 健康检查接口

### GET /health
检查系统健康状态

**响应示例：**
```json
{
  "status": "healthy",
  "timestamp": "2025-01-25T16:00:00Z",
  "version": "2.3.1",
  "uptime": "2 days, 3 hours, 45 minutes",
  "components": {
    "database": "healthy",
    "data_sources": "healthy",
    "scheduler": "healthy",
    "telegram_bot": "healthy",
    "report_engine": "healthy"
  },
  "system_metrics": {
    "cpu_usage": 15.2,
    "memory_usage": 68.5,
    "disk_usage": 42.1,
    "active_connections": 3
  }
}
```

### GET /api/v1/status
获取详细系统状态

**响应示例：**
```json
{
  "system": {
    "status": "running",
    "version": "2.3.1",
    "uptime": "185400 seconds",
    "start_time": "2025-01-23T08:00:00Z"
  },
  "database": {
    "status": "connected",
    "instruments_count": 5159,
    "quotes_count": 10500000,
    "last_update": "2025-01-25T15:30:00Z",
    "backup_last_run": "2025-01-25T06:00:00Z",
    "data_quality_score": 0.995
  },
  "data_sources": {
    "baostock_a_stock": {
      "status": "connected",
      "last_request": "2025-01-25T15:45:00Z",
      "success_rate": 99.5,
      "requests_today": 2847,
      "rate_limit_remaining": 17153
    }
  },
  "scheduler": {
    "status": "running",
    "active_jobs": 8,
    "next_run": "2025-01-25T20:00:00Z",
    "last_execution": "2025-01-25T15:00:00Z"
  },
  "telegram_bot": {
    "status": "connected",
    "users_count": 3,
    "last_interaction": "2025-01-25T15:42:00Z"
  }
}
```

## 📊 股票数据接口

### GET /api/v1/quotes/{instrument_id}
获取单只股票的历史数据

**路径参数：**
- `instrument_id`: 股票代码（如：600000.SSE）

**查询参数：**
- `start_date` (string, 可选): 开始日期，格式：YYYY-MM-DD
- `end_date` (string, 可选): 结束日期，格式：YYYY-MM-DD
- `limit` (integer, 可选): 返回记录数限制，默认：1000
- `fields` (string, 可选): 返回字段，逗号分隔

**请求示例：**
```bash
curl "http://localhost:8000/api/v1/quotes/600000.SSE?start_date=2024-01-01&end_date=2024-12-31&limit=100"
```

**响应示例：**
```json
{
  "success": true,
  "data": {
    "instrument_id": "600000.SSE",
    "symbol": "浦发银行",
    "exchange": "SSE",
    "quotes": [
      {
        "time": "2024-01-02T00:00:00Z",
        "open": 9.50,
        "high": 9.65,
        "low": 9.45,
        "close": 9.60,
        "volume": 15432000,
        "amount": 148560000.00,
        "quality_score": 1.0
      }
    ],
    "total_count": 245,
    "start_date": "2024-01-01",
    "end_date": "2024-12-31"
  }
}
```

### GET /api/v1/quotes/batch
批量获取多只股票数据

**查询参数：**
- `instruments` (string, 必需): 股票代码列表，逗号分隔
- `start_date` (string, 可选): 开始日期
- `end_date` (string, 可选): 结束日期
- `limit` (integer, 可选): 每只股票的记录数限制

**请求示例：**
```bash
curl "http://localhost:8000/api/v1/quotes/batch?instruments=600000.SSE,000001.SZSE&start_date=2024-01-01&end_date=2024-01-31"
```

### GET /api/v1/quotes/latest
获取最新股票数据

**查询参数：**
- `instruments` (string, 可选): 股票代码列表，不指定则返回所有
- `exchange` (string, 可选): 交易所筛选

**请求示例：**
```bash
curl "http://localhost:8000/api/v1/quotes/latest?exchange=SSE&limit=10"
```

**响应示例：**
```json
{
  "success": true,
  "data": {
    "quotes": [
      {
        "instrument_id": "600000.SSE",
        "symbol": "浦发银行",
        "exchange": "SSE",
        "time": "2024-10-11T00:00:00Z",
        "price": 10.25,
        "change": 0.15,
        "change_percent": 1.48,
        "volume": 12345678,
        "amount": 126543210.00
      }
    ],
    "total_count": 1789,
    "update_time": "2024-10-11T15:30:00Z"
  }
}
```

## 📈 技术指标接口

### GET /api/v1/indicators/{instrument_id}
获取股票技术指标

**路径参数：**
- `instrument_id`: 股票代码

**查询参数：**
- `indicators` (string, 必需): 指标名称，逗号分隔
- `period` (integer, 可选): 计算周期，默认：20
- `start_date` (string, 可选): 开始日期
- `end_date` (string, 可选): 结束日期

**支持的指标：**
- `ma`: 移动平均线
- `ema`: 指数移动平均线
- `rsi`: 相对强弱指标
- `macd`: MACD指标
- `bollinger`: 布林带

**请求示例：**
```bash
curl "http://localhost:8000/api/v1/indicators/600000.SSE?indicators=ma,rsi&period=20"
```

**响应示例：**
```json
{
  "success": true,
  "data": {
    "instrument_id": "600000.SSE",
    "indicators": {
      "ma_20": [
        {
          "time": "2024-10-11T00:00:00Z",
          "value": 10.15
        }
      ],
      "rsi_14": [
        {
          "time": "2024-10-11T00:00:00Z",
          "value": 65.43
        }
      ]
    }
  }
}
```

## 🏢 交易所信息接口

### GET /api/v1/exchanges
获取支持的交易所列表

**响应示例：**
```json
{
  "success": true,
  "data": {
    "exchanges": [
      {
        "code": "SSE",
        "name": "上海证券交易所",
        "country": "CN",
        "timezone": "Asia/Shanghai",
        "status": "active",
        "instruments_count": 2284
      },
      {
        "code": "SZSE",
        "name": "深圳证券交易所",
        "country": "CN",
        "timezone": "Asia/Shanghai",
        "status": "active",
        "instruments_count": 2875
      }
    ]
  }
}
```

### GET /api/v1/exchanges/{exchange}/instruments
获取交易所股票列表

**路径参数：**
- `exchange`: 交易所代码

**查询参数：**
- `page` (integer, 可选): 页码，默认：1
- `limit` (integer, 可选): 每页数量，默认：100
- `active_only` (boolean, 可选): 只返回活跃股票，默认：true

**请求示例：**
```bash
curl "http://localhost:8000/api/v1/exchanges/SSE/instruments?page=1&limit=50"
```

## 📅 交易日历接口

### GET /api/v1/calendar/trading
获取交易日历

**查询参数：**
- `exchange` (string, 必需): 交易所代码
- `start_date` (string, 必需): 开始日期
- `end_date` (string, 必需): 结束日期
- `trading_only` (boolean, 可选): 只返回交易日，默认：true

**请求示例：**
```bash
curl "http://localhost:8000/api/v1/calendar/trading?exchange=SSE&start_date=2024-01-01&end_date=2024-12-31"
```

**响应示例：**
```json
{
  "success": true,
  "data": {
    "exchange": "SSE",
    "trading_days": [
      "2024-01-02",
      "2024-01-03",
      "2024-01-04"
    ],
    "holidays": [
      "2024-01-01",
      "2024-02-10",
      "2024-02-11"
    ],
    "total_trading_days": 245,
    "total_holidays": 120
  }
}
```

### GET /api/v1/calendar/trading/next
获取下一个交易日

**查询参数：**
- `exchange` (string, 必需): 交易所代码
- `date` (string, 可选): 参考日期，默认：今天

**请求示例：**
```bash
curl "http://localhost:8000/api/v1/calendar/trading/next?exchange=SSE"
```

## 📥 下载任务接口

### POST /api/v1/download/start
启动下载任务

**请求体：**
```json
{
  "exchanges": ["SSE", "SZSE"],
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "resume": true,
  "quality_threshold": 0.7
}
```

**响应示例：**
```json
{
  "success": true,
  "data": {
    "task_id": "download_20241011_160000",
    "status": "started",
    "estimated_duration": "2-3 hours",
    "total_instruments": 5159
  }
}
```

### GET /api/v1/download/status/{task_id}
获取下载任务状态

**响应示例：**
```json
{
  "success": true,
  "data": {
    "task_id": "download_20241011_160000",
    "status": "running",
    "progress": {
      "total_instruments": 5159,
      "processed_instruments": 2580,
      "successful_downloads": 2575,
      "failed_downloads": 5,
      "progress_percentage": 50.0
    },
    "current_exchange": "SZSE",
    "start_time": "2024-10-11T16:00:00Z",
    "estimated_completion": "2024-10-11T18:30:00Z"
  }
}
```

### POST /api/v1/download/stop/{task_id}
停止下载任务

**响应示例：**
```json
{
  "success": true,
  "data": {
    "task_id": "download_20241011_160000",
    "status": "stopped",
    "final_progress": {
      "processed_instruments": 2580,
      "total_quotes": 5000000
    }
  }
}
```

## 📊 统计分析接口

### GET /api/v1/statistics/market
获取市场统计数据

**查询参数：**
- `exchange` (string, 可选): 交易所代码
- `date` (string, 可选): 统计日期，默认：最新交易日

**响应示例：**
```json
{
  "success": true,
  "data": {
    "date": "2024-10-11",
    "exchange": "SSE",
    "market_stats": {
      "total_instruments": 2284,
      "trading_instruments": 1789,
      "total_volume": 1234567890,
      "total_amount": 15678901234.56,
      "gainers": 890,
      "losers": 679,
      "unchanged": 220
    },
    "top_gainers": [
      {
        "instrument_id": "600000.SSE",
        "symbol": "浦发银行",
        "change_percent": 10.0
      }
    ],
    "top_losers": [
      {
        "instrument_id": "600001.SSE",
        "symbol": "邯郸钢铁",
        "change_percent": -9.8
      }
    ]
  }
}
```

### GET /api/v1/statistics/data-quality
获取数据质量报告

**查询参数：**
- `exchange` (string, 可选): 交易所代码
- `start_date` (string, 可选): 开始日期
- `end_date` (string, 可选): 结束日期

**响应示例：**
```json
{
  "success": true,
  "data": {
    "period": {
      "start_date": "2024-10-01",
      "end_date": "2024-10-11"
    },
    "quality_metrics": {
      "total_records": 1000000,
      "complete_records": 995000,
      "quality_score": 0.995,
      "missing_data_count": 5000,
      "anomalies_count": 200
    },
    "quality_issues": [
      {
        "type": "missing_volume",
        "count": 100,
        "affected_instruments": ["600001.SSE"]
      }
    ]
  }
}
```

## 🔐 认证接口

### POST /api/v1/auth/token
获取访问令牌

**请求体：**
```json
{
  "username": "admin",
  "password": "password"
}
```

**响应示例：**
```json
{
  "success": true,
  "data": {
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "token_type": "bearer",
    "expires_in": 3600,
    "refresh_token": "refresh_token_here"
  }
}
```

### POST /api/v1/auth/refresh
刷新访问令牌

**请求头：**
```
Authorization: Bearer <access_token>
```

**请求体：**
```json
{
  "refresh_token": "refresh_token_here"
}
```

## 🔍 数据缺口管理接口

### GET /api/v1/gaps
获取数据缺口信息

**查询参数：**
- `exchange` (string, 可选): 交易所代码
- `start_date` (string, 可选): 开始日期
- `end_date` (string, 可选): 结束日期
- `severity` (string, 可选): 严重程度 (low, medium, high, critical)
- `detailed` (boolean, 可选): 详细信息，默认：false

**请求示例：**
```bash
curl "http://localhost:8000/api/v1/gaps?exchange=SSE&start_date=2025-01-01&severity=high&detailed=true"
```

**响应示例：**
```json
{
  "success": true,
  "data": {
    "summary": {
      "total_gaps": 45,
      "critical_gaps": 3,
      "high_gaps": 12,
      "medium_gaps": 20,
      "low_gaps": 10,
      "affected_instruments": 28
    },
    "gaps": [
      {
        "instrument_id": "600000.SSE",
        "gap_type": "missing_data",
        "start_date": "2025-01-15",
        "end_date": "2025-01-16",
        "severity": "high",
        "missing_days": 2,
        "estimated_impact": "medium"
      }
    ]
  }
}
```

### POST /api/v1/gaps/fill
填补数据缺口

**请求体：**
```json
{
  "exchange": "SSE",
  "start_date": "2025-01-01",
  "end_date": "2025-01-25",
  "severity_filter": ["high", "critical"],
  "auto_fix": true,
  "dry_run": false
}
```

**响应示例：**
```json
{
  "success": true,
  "data": {
    "task_id": "gap_fill_20250125_160000",
    "status": "started",
    "gaps_to_fix": 15,
    "estimated_duration": "30-45 minutes"
  }
}
```

### GET /api/v1/gaps/report
获取数据质量报告

**查询参数：**
- `detailed` (boolean, 可选): 详细报告，默认：false
- `format` (string, 可选): 输出格式 (json, csv, summary)

**响应示例：**
```json
{
  "success": true,
  "data": {
    "report_period": {
      "start_date": "2025-01-01",
      "end_date": "2025-01-25"
    },
    "quality_metrics": {
      "overall_score": 0.995,
      "completeness": 0.998,
      "accuracy": 0.992,
      "consistency": 0.996
    },
    "gap_analysis": {
      "total_records_expected": 128975,
      "total_records_found": 128340,
      "missing_records": 635,
      "gap_percentage": 0.49
    },
    "recommendations": [
      "建议对3个关键缺口进行优先修复",
      "考虑调整数据源获取频率"
    ]
  }
}
```

## ⏰ 任务调度管理接口

### GET /api/v1/scheduler/jobs
获取所有调度任务

**响应示例：**
```json
{
  "success": true,
  "data": {
    "jobs": [
      {
        "id": "daily_data_update",
        "name": "每日数据更新",
        "enabled": true,
        "schedule": "0 20 * * 1-5",
        "next_run": "2025-01-26T20:00:00Z",
        "last_run": "2025-01-25T20:00:00Z",
        "status": "scheduled",
        "execution_history": [
          {
            "run_time": "2025-01-25T20:00:00Z",
            "status": "success",
            "duration": "12m 34s"
          }
        ]
      },
      {
        "id": "database_backup",
        "name": "数据库备份",
        "enabled": true,
        "schedule": "0 6 * * 6",
        "next_run": "2025-02-01T06:00:00Z",
        "status": "scheduled"
      }
    ],
    "total_jobs": 8,
    "active_jobs": 6
  }
}
```

### POST /api/v1/scheduler/jobs/{job_id}/trigger
手动触发任务执行

**路径参数：**
- `job_id`: 任务ID

**响应示例：**
```json
{
  "success": true,
  "data": {
    "job_id": "daily_data_update",
    "execution_id": "exec_20250125_160000",
    "status": "started",
    "started_at": "2025-01-25T16:00:00Z"
  }
}
```

### PUT /api/v1/scheduler/jobs/{job_id}
更新任务配置

**请求体：**
```json
{
  "enabled": true,
  "schedule": "0 21 * * 1-5",
  "parameters": {
    "exchanges": ["SSE", "SZSE"],
    "quality_threshold": 0.8
  }
}
```

### GET /api/v1/scheduler/jobs/{job_id}/history
获取任务执行历史

**查询参数：**
- `limit` (integer, 可选): 返回记录数，默认：50
- `status` (string, 可选): 状态筛选 (success, failed, running)

**响应示例：**
```json
{
  "success": true,
  "data": {
    "job_id": "daily_data_update",
    "executions": [
      {
        "execution_id": "exec_20250125_200000",
        "start_time": "2025-01-25T20:00:00Z",
        "end_time": "2025-01-25T20:12:34Z",
        "status": "success",
        "duration": 754,
        "processed_instruments": 5159,
        "success_count": 5157,
        "error_count": 2
      }
    ],
    "total_executions": 45,
    "success_rate": 0.978
  }
}
```

## 📄 报告生成接口

### POST /api/v1/reports/generate
生成系统报告

**请求体：**
```json
{
  "report_type": "system_status",
  "format": "telegram",
  "include_charts": true,
  "time_range": {
    "start_date": "2025-01-01",
    "end_date": "2025-01-25"
  },
  "recipients": ["telegram"]
}
```

**支持的报告类型：**
- `system_status`: 系统状态报告
- `data_quality`: 数据质量报告
- `scheduler_summary`: 调度器执行摘要
- `market_analysis`: 市场分析报告
- `performance_metrics`: 性能指标报告

**响应示例：**
```json
{
  "success": true,
  "data": {
    "report_id": "report_20250125_160000",
    "status": "generated",
    "format": "telegram",
    "generated_at": "2025-01-25T16:00:00Z",
    "content_preview": "📊 系统状态报告 (2025-01-25)\n\n✅ 系统状态：正常运行...",
    "delivery_status": {
      "telegram": "sent",
      "email": "skipped",
      "api": "available"
    }
  }
}
```

### GET /api/v1/reports/{report_id}
获取生成的报告

**路径参数：**
- `report_id`: 报告ID

**查询参数：**
- `format` (string, 可选): 输出格式 (json, text, telegram)

**响应示例：**
```json
{
  "success": true,
  "data": {
    "report_id": "report_20250125_160000",
    "report_type": "system_status",
    "generated_at": "2025-01-25T16:00:00Z",
    "content": "完整的报告内容...",
    "metadata": {
      "generation_time": "2.3s",
      "data_points": 1247,
      "charts_included": 3
    }
  }
}
```

### GET /api/v1/reports/templates
获取报告模板列表

**响应示例：**
```json
{
  "success": true,
  "data": {
    "templates": [
      {
        "id": "daily_summary",
        "name": "每日摘要",
        "description": "每日系统运行摘要",
        "supported_formats": ["telegram", "console", "api"],
        "parameters": {
          "include_charts": {
            "type": "boolean",
            "default": true,
            "description": "是否包含图表"
          }
        }
      }
    ]
  }
}
```

## 🛠️ 配置接口

### GET /api/v1/config
获取系统配置

**响应示例：**
```json
{
  "success": true,
  "data": {
    "data_sources": {
      "baostock_a_stock": {
        "enabled": true,
        "priority": 1,
        "status": "connected",
        "rate_limits": {
          "max_requests_per_minute": 60,
          "max_requests_per_hour": 3000,
          "max_requests_per_day": 60000
        }
      }
    },
    "rate_limiting": {
      "max_requests_per_minute": 60,
      "max_requests_per_hour": 1000,
      "retry_times": 3,
      "retry_interval": 1.0
    },
    "download_config": {
      "batch_size": 50,
      "chunk_days": 2000,
      "quality_threshold": 0.7,
      "resume_enabled": true
    },
    "scheduler_config": {
      "enabled": true,
      "timezone": "Asia/Shanghai",
      "max_instances": 1,
      "misfire_grace_time": 300
    },
    "telegram_config": {
      "enabled": true,
      "task_management": true,
      "authorized_users": ["user123", "user456"]
    }
  }
}
```

### PUT /api/v1/config
更新系统配置

**请求体：**
```json
{
  "rate_limiting": {
    "max_requests_per_minute": 120
  }
}
```

## 🔍 搜索接口

### GET /api/v1/search/instruments
搜索股票

**查询参数：**
- `q` (string, 必需): 搜索关键词
- `exchange` (string, 可选): 交易所筛选
- `limit` (integer, 可选): 结果数量限制

**请求示例：**
```bash
curl "http://localhost:8000/api/v1/search/instruments?q=银行&exchange=SSE&limit=10"
```

**响应示例：**
```json
{
  "success": true,
  "data": {
    "query": "银行",
    "total_count": 15,
    "instruments": [
      {
        "instrument_id": "600000.SSE",
        "symbol": "浦发银行",
        "name": "浦发银行股份有限公司",
        "exchange": "SSE",
        "industry": "银行",
        "market_cap": 123456789000
      }
    ]
  }
}
```

## 🚨 错误代码

| 错误代码 | HTTP状态码 | 描述 |
|---------|-----------|------|
| INVALID_PARAMETER | 400 | 请求参数无效 |
| UNAUTHORIZED | 401 | 未授权访问 |
| FORBIDDEN | 403 | 禁止访问 |
| NOT_FOUND | 404 | 资源不存在 |
| RATE_LIMIT_EXCEEDED | 429 | 请求频率超限 |
| INTERNAL_ERROR | 500 | 服务器内部错误 |
| SERVICE_UNAVAILABLE | 503 | 服务不可用 |

## 📝 使用示例

### Python 示例
```python
import requests
import pandas as pd

# 获取股票数据
def get_stock_data(instrument_id, start_date, end_date):
    url = f"http://localhost:8000/api/v1/quotes/{instrument_id}"
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "limit": 1000
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data['data']['quotes'])
    else:
        raise Exception(f"API Error: {response.status_code}")

# 使用示例
df = get_stock_data("600000.SSE", "2024-01-01", "2024-12-31")
print(df.head())
```

### JavaScript 示例
```javascript
// 获取最新股票数据
async function getLatestQuotes(exchange) {
    const response = await fetch(
        `http://localhost:8000/api/v1/quotes/latest?exchange=${exchange}`
    );
    const data = await response.json();
    return data.data.quotes;
}

// 使用示例
getLatestQuotes('SSE').then(quotes => {
    console.log('Latest quotes:', quotes);
});
```

### cURL 示例
```bash
# 批量下载数据
curl -X POST "http://localhost:8000/api/v1/download/start" \
  -H "Content-Type: application/json" \
  -d '{
    "exchanges": ["SSE", "SZSE"],
    "start_date": "2024-01-01",
    "end_date": "2024-12-31"
  }'

# 检查下载状态
curl "http://localhost:8000/api/v1/download/status/download_20241011_160000"
```

## 🔧 开发和调试

### 启用调试模式
```bash
# 设置环境变量
export QUOTE_DEBUG=true
export QUOTE_LOG_LEVEL=DEBUG

# 启动服务
python main.py api --host 0.0.0.0 --port 8000
```

### 查看API日志
```bash
# 实时查看日志
tail -f log/api.log

# 查看错误日志
grep "ERROR" log/api.log
```

### 性能监控
```bash
# 查看API性能指标
curl "http://localhost:8000/api/v1/system/metrics"
```

## 📞 技术支持

如果API使用中遇到问题：
1. 查看 API 文档：http://localhost:8000/docs
2. 检查系统状态：/api/v1/status
3. 查看错误日志
4. 提交 Issue 反馈问题