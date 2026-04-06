# 常见问题解答 (FAQ)

## 📖 概述

本文档收集了用户在使用 Quote System 过程中遇到的常见问题及其解决方案。如果您遇到的问题不在本文档中，请提交 Issue 或在 Discussions 中提问。

## 🚀 安装和配置问题

### Q1: 安装依赖时出现错误
**问题描述：**
```bash
pip install -r requirements.txt
# 错误：No matching distribution found for package-name
```

**解决方案：**
1. 检查 Python 版本（需要 Python 3.8+）
2. 更新 pip：`pip install --upgrade pip`
3. 使用国内镜像源：
   ```bash
   pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple/
   ```
4. 如果使用 conda：
   ```bash
   conda install --file requirements.txt
   ```

### Q2: 数据库连接失败
**问题描述：**
```
Database connection failed: FATAL: database "quotedb" does not exist
```

**解决方案：**
1. **SQLite（默认）**：
   - 确保数据目录存在：`mkdir -p data`
   - 检查文件权限

2. **PostgreSQL**：
   ```bash
   # 创建数据库
   createdb -U postgres quotedb

   # 修改配置文件
   vim config/config.json
   # 修改 database_config.url
   ```

3. **MySQL**：
   ```sql
   CREATE DATABASE quotedb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   ```

### Q3: 配置文件格式错误
**问题描述：**
```
Config file error: Expecting ',' delimiter: line 15 column 9
```

**解决方案：**
1. 使用 JSON 验证工具检查格式
2. 检查是否有遗漏的逗号
3. 确保字符串使用双引号
4. 备份配置文件后重新生成：
   ```bash
   cp config/config.json config/config.json.backup
   cp config/config.example.json config/config.json
   ```

## 📊 数据下载问题

### Q4: 下载速度很慢
**问题描述：**
下载 1 年数据需要几个小时

**可能原因和解决方案：**

1. **特定数据源限流设置过严（针对 AkShare/BaoStock 分别配置）**：
   ```json
   // config/config.json
   {
     "data_sources_config": {
       "baostock": {
         "max_requests_per_minute": 120,  // 原先太低可以调高
         "max_requests_per_hour": 3000
       }
     }
   }
   ```

2. **批次大小过小**：
   ```json
   {
     "data_config": {
       "batch_size": 100,  // 增加到100
       "download_chunk_days": 0  // 启用一次性下载
     }
   }
   ```

3. **网络问题**：
   - 检查网络连接
   - 使用 VPN 或代理
   - 更换网络环境

### Q5: 下载中断后无法续传
**问题描述：**
```
Resume mode: No existing progress found
```

**解决方案：**
1. 检查进度文件是否存在：
   ```bash
   ls -la data/download_progress.json
   ```

2. 查看错误日志：
   ```bash
   tail -100 log/sys.log | grep -i error
   ```

3. 手动创建进度文件：
   ```bash
   echo '{"processed_instruments": 0}' > data/download_progress.json
   ```

4. 使用续传命令：
   ```bash
   python main.py download --exchanges SSE SZSE --resume
   ```

### Q6: 部分股票下载失败
**问题描述：**
```
Failed to download 000001.SZ: HTTP 503 Service Unavailable
```

**解决方案：**
1. **自动重试**：系统会自动重试3次
2. **增加数据源重试次数**：
   ```json
   {
     "data_sources_config": {
       "baostock": {
         "retry_times": 5,
         "retry_interval": 3.0
       }
     }
   }
   ```
3. **手动重新下载**：
   ```bash
   python main.py download --exchanges SZSE --start-date 2024-01-01 --end-date 2024-01-31
   ```

### Q7: 下载的数据不完整
**问题描述：**
某些日期的数据缺失

**解决方案：**
1. 检查数据质量报告：
   ```bash
   python main.py status | grep -i quality
   ```

2. 使用补全功能：
   ```bash
   python main.py download --exchanges SSE SZSE --start-date 2024-01-01 --end-date 2024-12-31
   ```

3. 检查是否为交易日：
   ```python
   # 查看交易日历
   curl "http://localhost:8000/api/v1/calendar/trading?exchange=SSE&start_date=2024-01-01&end_date=2024-01-31"
   ```

## 🌐 API 服务问题

### Q8: API 服务无法启动
**问题描述：**
```
uvicorn.error: [Errno 48] Address already in use
```

**解决方案：**
1. **查找占用端口的进程**：
   ```bash
   lsof -i :8000
   ```

2. **终止进程**：
   ```bash
   kill -9 <PID>
   ```

3. **使用其他端口**：
   ```bash
   python main.py api --port 8001
   ```

### Q9: API 返回 500 错误
**问题描述：**
```json
{
  "success": false,
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "服务器内部错误"
  }
}
```

**解决方案：**
1. **查看详细错误日志**：
   ```bash
   tail -100 log/api.log
   ```

2. **检查数据库连接**：
   ```bash
   python main.py status
   ```

2. **检查数据源状态**：
   ```bash
   curl "http://localhost:8000/api/v1/system/status"
   ```

### Q10: API 请求被限流
**问题描述：**
```json
{
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "请求频率超限"
  }
}
```

**解决方案：**
1. **降低请求频率**
2. **增加内部限流防封挂起阈值，提高高并发容灾能力**：
   （具体可以调高对应数据源中的 `max_requests_per_minute` 和 `max_requests_per_day`）
3. **使用批量接口**：
   ```bash
   # 使用查询最新行情替代单个查询
   curl "http://localhost:8000/api/v1/quotes/latest?instrument_ids=600000.SSE&instrument_ids=000001.SZSE"
   ```

## ⏰ 调度器问题

### Q11: 定时任务没有执行
**问题描述：**
每日数据更新任务没有运行

**解决方案：**
1. **检查调度器状态**：
   ```bash
   python main.py status | grep scheduler
   ```

2. **查看任务配置**：
   ```bash
   grep -A 10 "daily_data_update" config/config.json
   ```

3. **手动执行任务**：
   ```bash
   python main.py job --job-id daily_data_update
   ```

4. **检查系统时区**：
   ```bash
   timedatectl status
   # 或
   date
   ```

### Q12: 任务执行失败
**问题描述：**
```
Job "daily_data_update" raised an exception
```

**解决方案：**
1. **查看详细错误**：
   ```bash
   grep -A 20 "daily_data_update.*error" log/scheduler.log
   ```

2. **检查任务依赖**：
   - 数据库连接
   - 数据源可用性
   - 磁盘空间

3. **重新调度任务**：
   ```bash
   python -c "
   from scheduler.scheduler import task_scheduler
   import asyncio
   asyncio.run(task_scheduler.remove_job('daily_data_update'))
   asyncio.run(task_scheduler.add_daily_data_update())
   "
   ```

## 🔧 性能问题

### Q13: 内存使用过高
**问题描述：**
系统内存使用超过 80%

**解决方案：**
1. **减少批次大小**：
   ```json
   {
     "data_config": {
       "batch_size": 20  // 减少到20
     }
   }
   ```

2. **启用数据压缩**：
   ```json
   {
     "database_config": {
       "compression": true
     }
   }
   ```

3. **清理缓存**：
   ```bash
   python main.py job --job-id cache_cleanup
   ```

4. **监控内存使用**：
   ```bash
   python -m memory_profiler main.py download --exchanges SSE
   ```

### Q14: 磁盘空间不足
**问题描述：**
```
OSError: [Errno 28] No space left on device
```

**解决方案：**
1. **清理旧日志**：
   ```bash
   find log/ -name "*.log" -mtime +7 -delete
   ```

2. **清理备份数据**：
   ```bash
   find data/backups/ -name "*.sql" -mtime +30 -delete
   ```

3. **配置数据保留策略**：
   ```json
   {
     "data_config": {
       "data_retention_days": 1825  // 5年
     }
   }
   ```

4. **启用自动清理**：
   ```bash
   python main.py job --job-id weekly_data_maintenance
   ```

## 🌐 网络问题

### Q15: 数据源连接超时
**问题描述：**
```
TimeoutError: Failed to connect to data source
```

**解决方案：**
1. **调整各接口的超时与冷却网络参数**：
   ```json
   {
     "data_sources_config": {
       "baostock": {
         "connection_timeout": 60,  // 增加到60秒
         "network_error_retry_interval": 20.0
       }
     }
   }
   ```

2. **配置代理**：
   ```json
   {
     "proxy_config": {
       "enabled": true,
       "url": "http://proxy-server:port"
     }
   }
   ```

3. **使用 VPN 或更换网络环境**

4. **切换数据源**：
   ```json
   {
     "data_sources": {
       "baostock_a_stock": {
         "enabled": false
       },
       "akshare_a_stock": {
         "enabled": true,
         "priority": 1
       }
     }
   }
   ```

### Q16: SSL 证书错误
**问题描述：**
```
SSL: CERTIFICATE_VERIFY_FAILED
```

**解决方案：**
1. **更新证书**：
   ```bash
   # macOS
   /Applications/Python\ 3.11/Install\ Certificates.command

   # Linux
   pip install --upgrade certifi
   ```

2. **禁用特定 SSL 验证（不推荐或在代码层级统一捕获）**：
   对于部分国外接口（如 Yahoo Finance）：可通过 `verify=False`，本项目网络层已在大部分源接入默认屏蔽安全套接层强制约束。

## 🔍 调试技巧

### 查看日志
```bash
# 实时查看所有日志
tail -f log/sys.log

# 查看特定类型日志
grep "ERROR" log/sys.log
grep "daily_data_update" log/scheduler.log

# 查看最近的日志
tail -n 200 log/api.log
```

### 检查系统状态
```bash
# 检查数据库状态
python main.py status

# 检查系统状态摘要
curl "http://localhost:8000/api/v1/system/status"

# 检查任务状态
python -c "
from scheduler.scheduler import task_scheduler
import asyncio
print(asyncio.run(task_scheduler.get_jobs_status()))
"
```

### 测试配置
```bash
# 验证配置文件
python -c "
import json
with open('config/config.json') as f:
    config = json.load(f)
    print('配置文件格式正确')
"

# 测试数据库连接
python -c "
from database.operations import DatabaseOperations
import asyncio
async def test():
    db = DatabaseOperations()
    await db.initialize()
    print('数据库连接成功')
asyncio.run(test())
"
```

## 📞 获取帮助

### 日志文件位置
- 系统日志：`log/sys.log`
- API 日志：`log/api.log`
- 调度器日志：`log/scheduler.log`
- 错误日志：`log/error.log`

### 常用命令
```bash
# 查看系统状态
python main.py status

# 重启服务
python main.py scheduler

# 手动执行任务
python main.py job --job-id <task_id>

# 查看帮助
python main.py --help
```

### 提交问题
提交 Issue 时请包含：
1. 系统环境（操作系统、Python 版本）
2. 完整的错误信息
3. 相关的配置文件（敏感信息请隐藏）
4. 问题复现步骤

### 社区支持
- GitHub Issues：[提交问题](https://github.com/your-username/Quote/issues)
- GitHub Discussions：[功能讨论](https://github.com/your-username/Quote/discussions)
- 文档：[在线文档](docs/README.md)

---

## 🔧 快速检查清单

遇到问题时，按以下步骤检查：

1. ✅ 检查日志文件中的错误信息
2. ✅ 验证配置文件格式
3. ✅ 确认网络连接正常
4. ✅ 检查磁盘空间和内存使用
5. ✅ 验证数据库连接
6. ✅ 检查任务调度器状态
7. ✅ 查看系统时间设置

如果以上步骤都无法解决问题，请提交详细的问题报告。