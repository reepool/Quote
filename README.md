# Quote System

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)]())
[![Code Size](https://img.shields.io/badge/Code-25K+-lines-yellow.svg)]())
[![Files](https://img.shields.io/badge/Files-77+-purple.svg)]())

一个功能强大的金融数据下载和管理系统，支持多数据源、智能限流、断点续传、自动调度和实时监控。系统采用现代化的异步架构，提供完整的RESTful API和Telegram任务管理界面。

## ✨ 核心特性

### 📊 多数据源支持
- **BaoStock** - 中国A股市场历史数据（主要数据源，支持股票与指数）
- **AkShare** - 综合性金融数据接口（ETF 数据源，开发中）
- **Tushare** - 专业级金融数据服务
- **YFinance** - 雅虎财经数据接口

### 🚀 智能下载功能
- **精确下载模式** - 基于上市日期的智能数据范围计算
- **断点续传** - 支持中断后的自动恢复
- **分块下载** - 灵活的分块策略，支持 `download_chunk_days=0` 一次性下载
- **质量评估** - 实时数据质量评分和完整性检查
- **灵活日期选择** - 支持年份范围和精确日期范围下载
- **进度跟踪** - 详细的下载进度监控和统计信息

### 🤖 智能任务管理
- **Telegram机器人** - 完整的任务管理界面
- **实时状态监控** - 智能时间显示，支持相对时间和绝对时间
- **配置热重载** - 运行时动态更新任务配置无需重启
- **任务控制** - 启用/禁用/暂停/恢复定时任务
- **权限管理** - 基于用户ID的访问控制

### 🛡️ 高级限流机制
- **多维度限流** - 分钟/小时/日三级智能限流
- **自适应控制** - 根据网络状况动态调整请求频率
- **错误恢复** - 智能重试和故障自愈机制

### 📈 完整的生态系统
- **RESTful API** - 完整的 HTTP API 服务，支持多种数据格式
- **实时监控** - 系统健康检查和性能监控
- **智能调度** - 完全配置化的任务调度系统，支持8种内置任务
- **数据分析** - 内置数据统计和分析功能
- **数据缺口检测** - 自动识别和报告数据缺失，支持智能修复
- **自动备份** - 定时数据库备份和清理，支持Telegram通知
- **智能时间处理** - 多时区支持和交易日历集成

## 🚀 快速开始

### 安装依赖

```bash
# 克隆项目
git clone <repository-url>
cd Quote

# 安装依赖
pip install -r requirements.txt

# 复制配置文件
cp .env.example .env
# 编辑 .env 文件配置必要的参数
```

### 基本使用

#### 1. 下载历史数据

```bash
# 下载A股市场数据（默认所有年份）
python3 main.py download --exchanges SSE SZSE

# 下载指定年份的数据
python3 main.py download --exchanges SSE SZSE --years 2023 2024

# 下载指定日期范围的数据
python3 main.py download --exchanges SZSE --start-date 2024-01-01 --end-date 2024-12-31

# 下载从指定日期到今天的数据
python3 main.py download --exchanges SSE --start-date 2024-06-01

# 下载从开始到指定日期的数据
python3 main.py download --exchanges SSE SZSE --end-date 2024-12-31

# 使用市场预设
python3 main.py download --preset a_shares
python3 main.py download --preset hk_stocks
python3 main.py download --preset us_stocks

# 指定单个股票下载
python3 main.py download --instrument-id 000001.SZ

# 续传模式（中断后继续下载，默认启用）
python3 main.py download --exchanges SSE SZSE --resume

# 重置进度重新下载
python3 main.py download --exchanges SZSE --no-resume

# 按品种类型下载（stock=股票, index=指数, etf=ETF）
python3 main.py download --types index              # 仅下载指数
python3 main.py download --types stock index         # 股票+指数
python3 main.py download --types index --no-resume   # 全量重新下载指数

# 查看所有可用的市场预设
python3 main.py download --list-presets
```

#### 2. 数据更新和维护

```bash
# 更新当日日线数据
python3 main.py update --exchanges SSE SZSE

# 补充指定日期的缺失数据
python3 main.py update --target-date 2026-03-27
python3 main.py update --target-date 2026-03-27 --exchanges SSE

# 数据缺口检测
python3 main.py gap --exchanges SSE SZSE --start-date 2024-01-01 --end-date 2024-12-31

# 交互式下载模式
python3 main.py interactive --mode both
```

#### 3. 启动服务

```bash
# 启动API服务器
python3 main.py api --host 0.0.0.0 --port 8000

# 启动调度器（仅定时任务）
python3 main.py scheduler

# 启动完整系统（调度器 + API服务）
python3 main.py full --host 0.0.0.0 --port 8000
```

#### 4. Telegram任务管理

系统提供完整的Telegram机器人管理界面：

```bash
# 启动包含任务管理器的完整系统
python3 main.py full --host 0.0.0.0 --port 8000

# 或分别启动调度器和API服务
python3 main.py scheduler
python3 main.py api --host 0.0.0.0 --port 8000
```

**Telegram机器人命令**：
```
/start                                    # 显示主菜单和帮助信息
/status                                   # 查看所有任务状态和下次执行时间
/detail <任务ID>                           # 查看指定任务的详细信息
/run <任务ID>                              # 立即执行指定任务
/run daily_data_update 2026-03-27         # 补充指定日期的缺失数据
/backfill 2026-03-27                      # 补充指定日期的缺失数据（支持指定交易所）
/reload_config                            # 热重载配置文件
/help                                     # 显示帮助信息
```

**任务控制示例**：
```bash
# 在Telegram中直接发送命令
/status                                   # 显示任务状态（智能时间显示）
/detail daily_data_update                 # 查看每日数据更新任务详情
/backfill 2026-03-27 SSE                  # 仅补充上交所 3/27 数据
/reload_config                            # 重载任务配置
```

#### 5. 系统管理

```bash
# 显示系统状态
python3 main.py status

# 运行指定任务
python3 main.py job --job-id daily_data_update
python3 main.py job --job-id monthly_data_integrity_check
python3 main.py job --job-id database_backup

# 补充指定日期的缺失数据
python3 main.py update --target-date 2026-03-27 --exchanges SSE SZSE
```

## 📖 详细功能说明

### 调度系统

系统内置了丰富的定时任务，支持完全配置化管理：

#### 核心调度任务

1. **每日数据更新** (`daily_data_update`)
   - **执行时间**: 每周一至周五 20:00
   - **功能**: 自动更新当日股票和指数数据
   - **特点**: 支持交易日检查、市场收盘等待、品种类型过滤（通过 `instrument_types` 配置）

2. **系统健康检查** (`system_health_check`)
   - **执行时间**: 每小时执行一次
   - **功能**: 检查数据源状态、数据库健康、系统资源使用情况
   - **特点**: 自动告警、性能监控

3. **每周数据维护** (`weekly_data_maintenance`)
   - **执行时间**: 每周日 02:00
   - **功能**: 数据库优化、日志清理、备份验证
   - **包含**: 数据库备份、日志清理、数据库优化、数据完整性验证

4. **月度数据完整性检查** (`monthly_data_integrity_check`)
   - **执行时间**: 每月1日 03:00
   - **功能**: 检查上月数据缺口并自动修复
   - **特点**: 智能缺口检测、优先级修复、详细报告

5. **季度数据清理** (`quarterly_cleanup`)
   - **执行时间**: 每季度最后一天 04:00
   - **功能**: 清理过期数据和文件
   - **包含**: 旧行情数据清理、临时文件清理

6. **数据库备份** (`database_backup`) ⭐ **重要功能**
   - **执行时间**: 每周六 06:00
   - **功能**: 自动备份数据库文件
   - **特点**: 支持Telegram通知、自动清理过期备份

7. **缓存预热** (`cache_warm_up`)
   - **执行时间**: 每日 08:00
   - **功能**: 预加载热门数据到缓存
   - **包含**: 热门股票、市场指数预热

8. **交易日历更新** (`trading_calendar_update`)
   - **执行时间**: 每月1日 01:00
   - **功能**: 更新各交易所交易日历
   - **特点**: 支持节假日验证

#### Telegram任务管理界面 ⭐ **核心功能**

系统提供完整的Telegram机器人界面进行任务管理：

**智能时间显示**：
- 1小时内 → "X分钟后"
- 24小时内 → "今天 HH:MM"
- 7天内 → "周一 HH:MM"
- 超过7天 → "MM-DD HH:MM"

**实时任务控制**：
- 查看所有任务状态和下次执行时间
- 立即执行指定任务
- 启用/禁用/暂停任务
- 配置热重载无需重启

### 下载模式

#### 精确下载模式（默认）
- 基于每只股票的实际上市日期计算下载范围
- 自动过滤未上市期间的数据请求
- 避免无效请求，提高下载效率

#### 日期范围下载
- **灵活指定**：支持精确的开始日期和结束日期
- **部分范围**：可以只指定开始日期（到今天）或结束日期（从开始）
- **格式标准**：使用标准的 `YYYY-MM-DD` 日期格式
- **优先级**：直接指定的日期范围优先于年份参数

#### 分块下载策略
```python
# 配置文件中的分块设置
{
  "data_config": {
    "download_chunk_days": 2000,  # 每2000天一个块
    "batch_size": 50               # 每批处理50只股票
  }
}
```

#### 一次性下载模式
```bash
# 设置 download_chunk_days=0 实现一次性下载
# 或通过配置文件设置
```

### 市场预设

系统提供了多个市场预设组合，方便快速选择：

- **a_shares**: 中国A股市场 (SSE, SZSE, BSE)
- **hk_stocks**: 香港股市 (HKEX)
- **us_stocks**: 美国股市 (NASDAQ, NYSE)
- **mainland**: 中国大陆市场 (SSE, SZSE, BSE)
- **overseas**: 海外市场 (HKEX, NASDAQ, NYSE)
- **chinese**: 中文市场 (包含港股) (SSE, SZSE, BSE, HKEX)
- **global**: 全球市场 (NASDAQ, NYSE)

### API 接口 ⭐ 现代异步API

#### 系统管理
```bash
# 健康检查（应用根路径）
curl http://localhost:8000/health

# 系统状态
curl http://localhost:8000/api/v1/system/status

# 系统统计
curl http://localhost:8000/api/v1/stats
```

#### 交易品种管理
```bash
# 获取交易品种列表（支持过滤）
curl "http://localhost:8000/api/v1/instruments?exchange=SSE&limit=100"

# 根据ID获取品种信息
curl http://localhost:8000/api/v1/instruments/000001.SZSE

# 根据代码获取品种信息
curl http://localhost:8000/api/v1/instruments/symbol/000001
```

#### 行情数据接口
```bash
# 获取日线数据（默认前复权）
curl "http://localhost:8000/api/v1/quotes/daily?symbol=600519&start_date=2025-01-01&end_date=2025-12-31"

# 获取不复权原始数据
curl "http://localhost:8000/api/v1/quotes/daily?symbol=600519&start_date=2025-01-01&end_date=2025-12-31&adjust=none"

# 获取后复权数据
curl "http://localhost:8000/api/v1/quotes/daily?symbol=600519&start_date=2025-01-01&end_date=2025-12-31&adjust=hfq"

# 获取指数数据（指数无复权概念，adjust 参数被忽略）
curl "http://localhost:8000/api/v1/quotes/daily?instrument_id=000001.SH&start_date=2024-01-01&end_date=2024-01-31"

# 获取最新行情数据
curl "http://localhost:8000/api/v1/quotes/latest?instrument_ids=000001.SZ&instrument_ids=600000.SH"
```

#### 数据缺口管理
```bash
# 获取数据缺口
curl "http://localhost:8000/api/v1/gaps?exchange=SSE&start_date=2024-01-01&end_date=2024-12-31"

# 填补数据缺口任务
curl -X POST http://localhost:8000/api/v1/gaps/fill \
  -H "Content-Type: application/json" \
  -d '{"exchange": "SSE", "dry_run": false}'

# 数据质量报告
curl "http://localhost:8000/api/v1/gaps/report"
```

#### 交易日历接口
```bash
# 获取交易日历
curl "http://localhost:8000/api/v1/calendar/trading?exchange=SSE&start_date=2024-01-01&end_date=2024-12-31"

# 获取下一个交易日
curl "http://localhost:8000/api/v1/calendar/trading/next?exchange=SSE&date=2024-01-01"

# 获取上一个交易日
curl "http://localhost:8000/api/v1/calendar/trading/previous?exchange=SSE&date=2024-01-01"
```

### 数据缺口检测

系统能够自动检测数据缺口：

```bash
# 基础缺口检测
python3 main.py gap --exchanges SSE SZSE

# 指定时间范围和严重程度
python3 main.py gap --exchanges SSE --start-date 2024-01-01 --severity high

# 生成详细报告
python3 main.py gap --exchanges SZSE --output gap_report.json --detailed
```

### 备份系统

#### 自动备份配置
```json
{
  "backup_config": {
    "enabled": true,
    "source_db_path": "data/quotes.db",
    "backup_directory": "data/PVE-Bak/QuoteBak",
    "retention_days": 30,
    "notification_enabled": true,
    "filename_pattern": "quotes_backup_{timestamp}.db",
    "max_backup_files": 10
  }
}
```

#### 手动备份
```bash
# 运行备份任务
python3 main.py job --job-id database_backup
```

## 🏗️ 项目架构

```
Quote/
├── main.py                    # 主程序入口
├── data_manager.py            # 数据管理核心
├── config/
│   ├── config.json            # 主配置文件
│   └── config-template.json   # 配置模板
├── requirements.txt           # 依赖列表
├── data_sources/              # 数据源模块
│   ├── base_source.py         # 数据源基类
│   ├── baostock_source.py     # BaoStock数据源
│   ├── akshare_source.py      # AkShare数据源
│   ├── tushare_source.py      # Tushare数据源
│   ├── yfinance_source.py     # YFinance数据源
│   └── source_factory.py      # 数据源工厂
├── database/                  # 数据库模块
│   ├── models.py              # 数据模型
│   ├── operations.py          # 数据库操作
│   └── connection.py          # 数据库连接
├── api/                       # API服务
│   ├── app.py                 # FastAPI应用
│   ├── routes.py              # API路由
│   ├── models.py              # API数据模型
│   └── middleware.py          # 中间件
├── scheduler/                 # 任务调度
│   ├── scheduler.py           # 调度器核心
│   ├── tasks.py               # 任务定义
│   ├── job_config.py          # 任务配置管理
│   └── monitor.py             # 调度器监控
├── utils/                     # 工具模块
│   ├── date_utils.py          # 日期时间工具 ⭐
│   ├── task_manager/          # Telegram任务管理器 ⭐
│   │   ├── task_manager.py     # 主控制器
│   │   ├── handlers.py         # 消息处理器
│   │   ├── models.py           # 数据模型
│   │   ├── formatters.py      # 消息格式化
│   │   └── keyboards.py        # 键盘布局
│   ├── logging_manager.py     # 日志管理
│   ├── security_utils.py      # 安全工具
│   ├── cache.py               # 缓存管理
│   ├── validation.py          # 数据验证
│   └── config_manager.py      # 配置管理
├── data/                      # 数据目录
│   ├── quotes.db              # 主数据库
│   └── PVE-Bak/               # 备份目录
├── log/                       # 日志目录
└── tests/                     # 测试用例
```

### 核心模块说明

- **⭐ utils/date_utils.py**: 智能时间处理和时区管理
- **⭐ utils/task_manager/**: Telegram机器人任务管理系统
- **scheduler/**: 完全配置化的任务调度系统
- **api/**: 现代异步RESTful API服务
- **data_sources/**: 多数据源统一接口和工厂模式
- **database/**: 现代SQLAlchemy 2.0 ORM操作

## 🔧 高级配置

### 数据源配置

#### BaoStock 配置（主要数据源）
```json
{
  "data_sources_config": {
    "baostock": {
      "enabled": true,
      "exchanges_supported": ["a_stock"],
      "primary_source_of": ["a_stock"],
      "max_requests_per_minute": 60,
      "max_requests_per_hour": 3000,
      "max_requests_per_day": 60000,
      "retry_times": 5,
      "retry_interval": 5.0
    }
  }
}
```

#### 限流配置
```json
{
  "rate_limit_config": {
    "max_requests_per_minute": 60,
    "max_requests_per_hour": 1000,
    "max_requests_per_day": 10000,
    "retry_times": 3,
    "retry_interval": 1.0
  }
}
```

### 调度器配置

```json
{
  "scheduler_config": {
    "enabled": true,
    "timezone": "Asia/Shanghai",
    "max_instances": 1,
    "misfire_grace_time": 300,
    "coalesce": true
  }
}
```

### Telegram 通知和任务管理配置

```json
{
  "telegram_config": {
    "enabled": true,
    "api_id": "your_api_id",
    "api_hash": "your_api_hash",
    "bot_token": "your_bot_token",
    "chat_id": ["your_chat_id"],
    "session_name": "MsgBot",
    "connection_retries": 3,
    "retry_delay": 5,
    "auto_reconnect": true,
    "timeout": 30,
    "intervals": {
      "tg_msg_retry_interval": 3,
      "tg_msg_retry_times": 5,
      "tg_connect_timeout": 30,
      "tg_auto_reconnect": true,
      "tg_max_reconnect_attempts": 5,
      "tg_reconnect_delay": 10
    }
  }
}
```

**功能说明**：
- **任务管理**: 支持完整的任务控制功能
- **权限控制**: 基于chat_id的访问控制
- **会话管理**: 自动重连和错误恢复
- **消息重试**: 智能消息发送重试机制

## 📊 支持的交易所和数据类型

### 交易所支持
- **SSE** - 上海证券交易所
- **SZSE** - 深圳证券交易所
- **BSE** - 北京证券交易所
- **HKEX** - 香港交易所
- **NASDAQ** - 纳斯达克
- **NYSE** - 纽约证券交易所

### 数据类型
- **日线数据** - OHLCV数据（存储非复权原始价格）
- **复权数据** - 查询时动态计算（前复权 `adjust=qfq` / 后复权 `adjust=hfq`）
- **复权因子** - 各品种除权除息事件和累积因子
- **基本面数据** - 股票基本信息
- **交易日历** - 交易日/非交易日

### 品种类型
- **stock** - 个股（A股）
- **index** - 指数（上证综指、深证成指等）
- **etf** - ETF基金（品种已入库，历史数据需 AkShare 补齐）

### 数据质量
- **实时评分** - 每条数据都有质量评分
- **完整性检查** - 数据完整性验证
- **异常检测** - 自动检测异常数据
- **缺口识别** - 智能识别数据缺失

## 🛠️ 故障排除

### 常见问题

#### 1. 下载卡住或中断
```bash
# 检查下载状态
python3 main.py status

# 检查数据缺口
python3 main.py gap --exchanges SSE SZSE

# 继续下载（续传模式）
python3 main.py download --exchanges SSE SZSE --resume
```

#### 2. 数据源异常
```bash
# 检查数据源状态
curl http://localhost:8000/health

# 查看数据源日志
grep "DataSource" log/sys.log
```

#### 3. 调度器问题
```bash
# 检查调度器状态
python3 main.py job --job-id system_health_check

# 重启调度器
python3 main.py scheduler
```

#### 4. 数据库问题
```bash
# 检查数据库状态
python3 main.py status

# 手动备份数据库
python3 main.py job --job-id database_backup

# 数据库优化
python3 main.py job --job-id weekly_data_maintenance
```

### 日志管理

```bash
# 实时查看日志
tail -f log/sys.log

# 查看错误日志
grep "ERROR" log/sys.log

# 查看调度器日志
grep "Scheduler" log/sys.log
```

## 📚 更新日志

### v2.3.0 (2025-10-18) ⭐ **重大更新**
- ✨ **Telegram任务管理系统**
  - 完整的Telegram机器人界面
  - 智能时间显示（相对时间 + 绝对时间）
  - 任务实时控制（启用/禁用/暂停/恢复）
  - 配置热重载功能
  - 权限管理和访问控制

- 🎯 **时间处理优化**
  - 新增 `utils/date_utils.py` 智能时间格式化
  - 多时区支持（A股、港股、美股）
  - 智能时间显示策略
  - 交易日历深度集成

- 🚀 **调度器增强**
  - 完全配置化的任务调度系统
  - 8种内置定时任务
  - 运行时任务状态管理
  - 任务执行历史记录

- 🛠️ **架构优化**
  - 模块化设计改进
  - 异步性能提升
  - 错误处理机制完善
  - 代码质量提升（25K+ 行代码，77个文件）

### v2.2.0 (2025-10-14)
- ✨ **新增功能**
  - 数据库自动备份任务 (`database_backup`)
  - 完整的备份配置管理
  - Telegram 通知集成
  - 智能备份文件清理
  - 数据缺口检测功能
  - 交互式下载模式
  - 市场预设组合

- 🎯 **核心改进**
  - 统一的配置管理机制
  - 增强的调度器任务系统
  - 改进的错误处理和通知
  - 优化的数据库操作

### v2.1.0 (2024-10-11)
- ✨ **新增功能**
  - 指定日期范围下载
  - 智能交易日历选择策略
  - 灵活的续传逻辑优化

- 🎯 **核心改进**
  - 根据下载场景选择合适的交易日历获取方式
  - 修复续传逻辑中的批次计算错误
  - 优化交易日判断，优先使用交易日历表

### v2.0.0 (2024-10-10)
- 🎉 统一了 BaoStock 和 AkShare 的限流机制
- 🗑️ 移除了冗余的 `_download_exchange_continuous` 方法
- 📈 优化了数据库结构，提升查询性能
- 🛡️ 增强了错误恢复和自动重试机制
- 📊 添加了数据质量评分系统
- 🚀 支持一次性下载模式 (`download_chunk_days=0`)

### v1.0.0 (2024-09-01)
- 🎯 初始版本发布
- 📊 支持多数据源下载
- 🔧 实现断点续传功能
- 🌐 提供RESTful API服务

## 🤝 贡献指南

1. Fork 项目
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

### 开发环境设置

```bash
# 设置开发环境
pip install -r requirements.txt

# 运行测试
python3 -m pytest tests/

# 代码格式化
black .
isort .
```

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🙏 致谢

- [BaoStock](http://baostock.com) - 提供免费的中国股市历史数据
- [AkShare](https://github.com/akshare/akshare) - 开源金融数据接口
- [Tushare](https://tushare.pro) - 专业金融数据服务平台
- [YFinance](https://github.com/ranaroussi/yfinance) - 雅虎财经数据接口

## 📞 联系方式

- 项目主页：[GitHub Repository](https://github.com/your-username/Quote)
- 问题反馈：[Issues](https://github.com/your-username/Quote/issues)
- 功能建议：[Discussions](https://github.com/your-username/Quote/discussions)

---

**Quote System** - 让金融数据管理变得简单高效！ 🚀

## 🔗 相关文档

- [数据库备份实施文档](DATABASE_BACKUP_IMPLEMENTATION.md)
- [API 接口文档](docs/api/restful_api.md)
- [配置文件详解](docs/configuration/config_file.md)
- [故障排除指南](docs/troubleshooting/faq.md)