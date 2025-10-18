# Telegram 任务管理器文档

## 📖 概述

Quote System 提供了功能完整的 Telegram 机器人任务管理界面，允许用户通过 Telegram 实时监控和管理系统的定时任务。该管理器支持智能时间显示、任务控制、配置热重载等高级功能。

## 🚀 功能特性

### ✨ 核心功能
- **实时任务监控** - 查看所有定时任务的状态和下次执行时间
- **智能时间显示** - 根据时间差自动选择最佳显示格式
- **任务控制** - 启用/禁用/暂停/恢复任务
- **配置热重载** - 运行时动态更新任务配置无需重启
- **权限管理** - 基于用户ID的访问控制
- **实时通知** - 任务执行状态和系统告警通知

### 🤖 智能时间显示
系统根据时间差自动选择最合适的显示格式：

| 时间差 | 显示格式 | 示例 |
|--------|----------|------|
| < 1小时 | "X分钟后" | "15分钟后" |
| < 24小时 | "今天 HH:MM" | "今天 20:00" |
| < 7天 | "周一 HH:MM" | "周一 14:00" |
| ≥ 7天 | "MM-DD HH:MM" | "10-25 14:00" |

## 🔧 配置设置

### Telegram 配置
在 `config/config.json` 中配置 Telegram 机器人：

```json
{
  "telegram_config": {
    "enabled": true,
    "api_id": "your_api_id",
    "api_hash": "your_api_hash",
    "bot_token": "your_bot_token",
    "chat_id": ["471105519"],
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

### 权限配置
系统只允许配置文件中指定的 `chat_id` 访问任务管理功能。

## 📱 使用指南

### 启动任务管理器

```bash
# 启动完整系统（包含任务管理器）
python3 main.py full --host 0.0.0.0 --port 8000

# 或分别启动调度器和API服务
python3 main.py scheduler
python3 main.py api --host 0.0.0.0 --port 8000
```

### 可用命令

#### 基础命令
- `/start` - 显示主菜单和帮助信息
- `/help` - 显示帮助信息

#### 任务管理命令
- `/status` - 查看所有任务状态和下次执行时间
- `/detail <task_id>` - 查看指定任务的详细信息
- `/reload_config` - 热重载配置文件

#### 任务控制命令（开发中）
- `/enable <task_id>` - 启用指定任务
- `/disable <task_id>` - 禁用指定任务
- `/pause <task_id>` - 暂停指定任务
- `/resume <task_id>` - 恢复指定任务
- `/run <task_id>` - 立即执行指定任务

### 使用示例

```bash
# 在Telegram中发送命令
/status                    # 显示任务状态
/detail daily_data_update  # 查看每日数据更新任务详情
/reload_config            # 重载任务配置
```

## 📋 内置任务列表

系统预置了8个定时任务，每个任务都有详细的配置和执行计划：

### 1. 每日数据更新 (daily_data_update)
- **执行时间**: 每周一至周五 20:00
- **功能**: 自动更新当日股票数据
- **特点**: 支持交易日检查、市场收盘等待

### 2. 系统健康检查 (system_health_check)
- **执行时间**: 每小时执行一次
- **功能**: 检查数据源状态、数据库健康、系统资源
- **特点**: 自动告警、性能监控

### 3. 每周数据维护 (weekly_data_maintenance)
- **执行时间**: 每周日 02:00
- **功能**: 数据库优化、日志清理、备份验证

### 4. 月度数据完整性检查 (monthly_data_integrity_check)
- **执行时间**: 每月1日 03:00
- **功能**: 检查上月数据缺口并自动修复

### 5. 季度数据清理 (quarterly_cleanup)
- **执行时间**: 每季度最后一天 04:00
- **功能**: 清理过期数据和文件

### 6. 数据库备份 (database_backup)
- **执行时间**: 每周六 06:00
- **功能**: 自动备份数据库文件
- **特点**: 支持Telegram通知、自动清理过期备份

### 7. 缓存预热 (cache_warm_up)
- **执行时间**: 每日 08:00
- **功能**: 预加载热门数据到缓存

### 8. 交易日历更新 (trading_calendar_update)
- **执行时间**: 每月1日 01:00
- **功能**: 更新各交易所交易日历

## 📊 状态显示示例

### /status 命令响应示例
```
📊 任务状态概览

• 总任务数：8
• 运行中：8
• 已禁用：0

🟢 运行中的任务：
• 每日数据更新任务 (daily_data_update) - 周一 20:00
• 系统健康检查 (system_health_check) - 45分钟后
• 每周数据维护 (weekly_data_maintenance) - 周日 02:00
• 月度数据完整性检查 (monthly_data_integrity_check) - 下月1日 03:00
• 季度数据清理 (quarterly_cleanup) - 下季度最后一天 04:00
• 缓存预热 (cache_warm_up) - 每天 08:00
• 交易日历更新 (trading_calendar_update) - 下月1日 01:00
• 数据库备份任务 (database_backup) - 周六 06:00

*可用的任务控制命令：*
• /run <task_id> - 立即运行任务
• /enable <task_id> - 启用任务
• /disable <task_id> - 禁用任务
• /detail <task_id> - 查看任务详情
```

## 🛠️ 技术实现

### 架构组件
```
utils/task_manager/
├── task_manager.py      # 主控制器类
├── handlers.py         # 消息处理器
├── models.py           # 数据模型
├── formatters.py      # 消息格式化器
├── keyboards.py        # 键盘布局
└── __init__.py
```

### 核心类
- **TaskManagerBot**: 主控制器，管理Telegram连接和消息处理
- **TaskManagerHandlers**: 消息处理器，处理用户命令和交互
- **TaskStatusInfo**: 任务状态信息模型
- **TaskManagerFormatters**: 消息格式化器

### 时间处理机制
系统使用 `utils/date_utils.py` 中的智能时间格式化功能：

```python
# 智能时间格式化示例
from utils.date_utils import DateUtils

# 格式化下次执行时间
next_run_time = task.next_run_time
status = DateUtils.get_task_status_display(next_run_time, "running")
# 结果: "45分钟后" 或 "周一 20:00"
```

## 🔐 安全特性

### 权限控制
- 基于 `chat_id` 的访问控制
- 只有授权用户可以使用任务管理功能
- 防止未授权访问和恶意操作

### 错误处理
- 完善的异常处理机制
- 自动重连和错误恢复
- 详细的错误日志记录

### 会话管理
- 持久化会话连接
- 自动重连机制
- 连接状态监控

## 📝 开发指南

### 添加新命令
1. 在 `handlers.py` 中添加新的命令处理方法
2. 在 `__init__.py` 中注册新命令
3. 更新帮助文档和示例

### 扩展任务管理功能
1. 修改 `models.py` 添加新的数据模型
2. 在 `handlers.py` 中实现相应的处理逻辑
3. 更新 `formatters.py` 添加新的消息格式

### 自定义时间显示
可以在 `utils/date_utils.py` 中修改时间格式化逻辑：

```python
@staticmethod
def format_next_run_time(next_run_time, task_status="running") -> str:
    # 自定义时间显示逻辑
    pass
```

## 🐛 故障排除

### 常见问题

1. **机器人无响应**
   - 检查Telegram配置是否正确
   - 确认 `chat_id` 是否在授权列表中
   - 查看系统日志获取详细错误信息

2. **任务状态显示异常**
   - 检查调度器是否正常运行
   - 确认任务配置是否正确
   - 重启调度器服务

3. **时间显示不准确**
   - 检查系统时区设置
   - 确认 `date_utils.py` 中的时间处理逻辑
   - 验证APScheduler的时间配置

### 日志查看
```bash
# 查看任务管理器日志
grep "TaskManager" log/sys.log

# 查看调度器日志
grep "Scheduler" log/sys.log

# 查看Telegram连接日志
grep "tgbot" log/sys.log
```

## 📈 性能优化

### 缓存机制
- 任务状态信息缓存，减少数据库查询
- 消息格式化结果缓存，提高响应速度

### 异步处理
- 所有网络操作采用异步处理
- 消息发送使用异步队列

### 资源管理
- 定期清理过期会话
- 优化数据库连接池

---

## 📞 技术支持

如需技术支持或报告问题，请查看系统日志或联系项目维护者。

**Note**: Telegram任务管理器是Quote System v2.3.0的核心功能之一，提供了现代化的任务管理体验。