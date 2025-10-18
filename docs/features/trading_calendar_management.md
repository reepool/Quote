# 交易日历管理功能

## 📖 功能概述

交易日历管理是 Quote System 的重要组成部分，负责管理各交易所的交易日历信息。系统支持智能的交易日历获取、缓存和更新机制，确保数据下载和更新的准确性。

## 🎯 核心功能

### 1. 交易日历获取
- **数据源获取**：从各数据源获取最新交易日历
- **本地缓存**：高效缓存交易日历数据
- **多交易所支持**：支持不同交易所的独立交易日历

### 2. 智能更新策略
- **定期更新**：每月自动更新交易日历
- **按需更新**：全历史下载时强制更新
- **缓存使用**：部分下载时使用缓存

### 3. 交易日判断
- **精确判断**：基于官方交易日历
- **调休支持**：正确处理调休安排
- **异常处理**：优雅处理数据缺失情况

## 🏗️ 系统架构

### 数据流
```
数据源 (BaoStock) → TradingCalendarDB → 缓存 (Memory) → 业务逻辑
```

### 核心组件
1. **TradingCalendarDB**: 交易日历数据库模型
2. **DataSourceFactory**: 数据源管理和获取
3. **DatabaseOperations**: 数据库操作接口
4. **缓存机制**: 内存缓存优化

## 📊 数据库结构

### trading_calendar 表
```sql
CREATE TABLE trading_calendar (
    id SERIAL PRIMARY KEY,
    exchange VARCHAR(10) NOT NULL,      -- 交易所代码
    date DATE NOT NULL,                 -- 日期
    is_trading_day BOOLEAN NOT NULL,    -- 是否交易日
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(exchange, date)              -- 联合唯一约束
);
```

### 索引优化
```sql
CREATE INDEX idx_trading_calendar_exchange_date ON trading_calendar(exchange, date);
CREATE INDEX idx_trading_calendar_is_trading ON trading_calendar(is_trading_day);
```

## 🎮 核心类和方法

### 1. DatabaseOperations 类

#### is_trading_day()
```python
async def is_trading_day(self, exchange: str, date: date) -> bool:
    """检查指定日期是否为交易日"""
    try:
        with self.get_session() as session:
            result = session.query(TradingCalendarDB).filter(
                TradingCalendarDB.exchange == exchange,
                TradingCalendarDB.date == date
            ).first()
            return result.is_trading_day if result else False
    except Exception as e:
        db_logger.error(f"Failed to check trading day: {e}")
        return False
```

#### get_trading_days()
```python
async def get_trading_days(
    self,
    exchange: str,
    start_date: date,
    end_date: date,
    is_trading_day: Optional[bool] = None
) -> List[date]:
    """获取指定日期范围内的交易日列表"""
    try:
        with self.get_session() as session:
            query = session.query(TradingCalendarDB.date).distinct()
            query = query.filter(TradingCalendarDB.exchange == exchange)
            query = query.filter(TradingCalendarDB.date >= start_date)
            query = query.filter(TradingCalendarDB.date <= end_date)

            if is_trading_day is not None:
                query = query.filter(TradingCalendarDB.is_trading_day == is_trading_day)

            query = query.order_by(TradingCalendarDB.date)
            return [row.date for row in query.all()]
    except Exception as e:
        db_logger.error(f"Failed to get trading days: {e}")
        return []
```

### 2. DataSourceFactory 类

#### update_trading_calendar()
```python
async def update_trading_calendar(
    self,
    exchange: str,
    start_date: date,
    end_date: date
) -> int:
    """更新交易日历"""
    source = self.get_primary_source(exchange)
    if not source or not hasattr(source, 'get_trading_calendar'):
        return 0

    try:
        calendar_data = await source.get_trading_calendar(exchange, start_date, end_date)
        if calendar_data:
            return await self.db_ops.save_trading_calendar(calendar_data)
    except Exception as e:
        ds_logger.error(f"Failed to update trading calendar: {e}")

    return 0
```

#### get_trading_days()
```python
async def get_trading_days(
    self,
    exchange: str,
    start_date: date,
    end_date: date
) -> List[date]:
    """获取交易日列表（优先使用缓存）"""
    # 先尝试从缓存获取
    if exchange in self.trading_calendar_cache:
        cached_days = [
            day for day in self.trading_calendar_cache[exchange]
            if start_date <= day <= end_date
        ]
        if cached_days:
            return sorted(cached_days)

    # 从数据库获取
    trading_days = await self.db_ops.get_trading_days(exchange, start_date, end_date)

    # 更新缓存
    if exchange not in self.trading_calendar_cache:
        self.trading_calendar_cache[exchange] = {}

    for trading_day in trading_days:
        self.trading_calendar_cache[exchange][trading_day] = True

    return trading_days
```

## 🔄 更新策略详解

### 场景1：全历史数据下载
```python
# 强制从数据源更新
force_update_calendar = True
await self._update_trading_calendar(exchange, start_date, end_date)
```

**特点：**
- 每次都从数据源获取最新交易日历
- 确保数据的准确性
- 适合首次下载或完整更新

### 场景2：指定日期范围下载
```python
# 使用缓存数据
force_update_calendar = False
# 直接使用缓存的交易日历
trading_days = await self.source_factory.get_trading_days(exchange, start_date, end_date)
```

**特点：**
- 使用本地缓存的交易日历
- 提高下载效率
- 减少不必要的网络请求

### 场景3：每日数据更新
```python
# 使用缓存数据
trading_days = await self.source_factory.get_trading_days(exchange, start_date, end_date)
```

**特点：**
- 完全依赖缓存
- 定期任务更新缓存

### 场景4：定期交易日历更新
```python
# 调度器任务：每月1号更新
await self.source_factory.update_trading_calendar(exchange, start_date, end_date)
```

**配置：**
```json
{
  "trading_calendar_update": {
    "enabled": true,
    "trigger": {
      "type": "cron",
      "day": 1,        // 每月1号
      "hour": 1,       // 凌晨1点
      "minute": 0
    },
    "parameters": {
      "exchanges": ["SSE", "SZSE"],
      "update_future_months": 6
    }
  }
}
```

## 🎯 业务逻辑优化

### 1. 交易日判断优化

**原逻辑：**
```python
# 使用 DateUtils 计算
if DateUtils.is_trading_day(exchange, today):
    # 执行操作
```

**优化后：**
```python
# 优先使用交易日历表
try:
    if await data_manager.db_ops.is_trading_day(exchange, today):
        # 执行操作
except Exception as e:
    # Fallback 到 DateUtils
    if DateUtils.is_trading_day(exchange, today):
        # 执行操作
```

### 2. 缓存策略

```python
class DataSourceFactory:
    def __init__(self):
        self.trading_calendar_cache = {}  # 交易日历缓存

    async def get_trading_days(self, exchange, start_date, end_date):
        # 1. 检查内存缓存
        # 2. 检查数据库
        # 3. 更新缓存
        pass
```

### 3. 错误处理

```python
async def _update_trading_calendar(self, exchange: str, start_date: date, end_date: date):
    """更新交易日历（带错误处理）"""
    try:
        dm_logger.info(f"Updating trading calendar for {exchange}")
        updated_count = await self.source_factory.update_trading_calendar(
            exchange, start_date, end_date
        )
        dm_logger.info(f"Updated {updated_count} trading days for {exchange}")
    except Exception as e:
        dm_logger.warning(f"Failed to update trading calendar for {exchange}: {e}")
        # 继续执行，不中断下载流程
```

## 📊 性能优化

### 1. 缓存优化
- **内存缓存**：交易日历数据缓存在内存中
- **批量查询**：一次性查询多个日期
- **预加载**：预加载常用日期范围

### 2. 数据库优化
- **索引优化**：合理创建索引
- **查询优化**：避免全表扫描
- **连接池**：复用数据库连接

### 3. 网络优化
- **批量更新**：减少网络请求
- **压缩传输**：压缩数据传输
- **异步请求**：非阻塞网络请求

## 🎛️ 配置选项

### 配置文件示例
```json
{
  "trading_calendar_config": {
    "cache_enabled": true,
    "cache_ttl_days": 30,
    "update_frequency": "monthly",
    "fallback_to_calculation": true,
    "max_future_months": 6
  }
}
```

### 参数说明
- `cache_enabled`: 是否启用缓存
- `cache_ttl_days`: 缓存有效期（天）
- `update_frequency`: 更新频率
- `fallback_to_calculation`: 是否回退到计算方式
- `max_future_months`: 最大未来月份

## 🔍 监控和调试

### 1. 日志记录
```python
dm_logger.info(f"Updated {updated_count} trading days for {exchange}")
dm_logger.warning(f"Failed to update trading calendar: {e}")
dm_logger.debug(f"Using cached trading calendar for {exchange}")
```

### 2. 性能监控
```python
# 缓存命中率
cache_hit_rate = cache_hits / (cache_hits + cache_misses)

# 更新耗时
update_duration = end_time - start_time

# 数据质量
data_quality = validate_calendar_data(calendar_data)
```

### 3. 健康检查
```python
async def health_check_trading_calendar(self) -> Dict[str, Any]:
    """交易日历健康检查"""
    return {
        "last_update": await self.get_last_update_time(),
        "cache_size": len(self.trading_calendar_cache),
        "data_completeness": await self.check_data_completeness(),
        "upcoming_holidays": await self.get_upcoming_holidays()
    }
```

## 🚨 故障处理

### 常见问题
1. **数据源不可用**：使用缓存或回退到计算方式
2. **数据格式错误**：记录错误并跳过
3. **网络超时**：增加重试机制
4. **数据库连接失败**：使用本地缓存

### 解决方案
```python
async def safe_get_trading_days(self, exchange, start_date, end_date):
    """安全的交易日获取"""
    try:
        # 尝试从缓存获取
        return await self.get_trading_days(exchange, start_date, end_date)
    except Exception as e:
        logger.error(f"Failed to get trading days: {e}")
        # 回退到计算方式
        return DateUtils.get_trading_days_in_range(exchange, start_date, end_date)
```

## 📝 最佳实践

### 1. 初始化设置
```bash
# 首次使用时，先更新交易日历
python main.py job --job-id trading_calendar_update
```

### 2. 定期维护
```bash
# 检查交易日历完整性
python main.py status | grep trading_calendar
```

### 3. 数据备份
```bash
# 备份交易日历数据
pg_dump -h localhost -U postgres -d quotedb -t trading_calendar > backup_calendar.sql
```

## 🔄 版本更新

### v2.1.0 (2025-10-11)
- ✨ 智能交易日历选择策略
- 🔧 优化缓存机制
- 🐛 修复多交易所场景下的判断错误
- 📊 增强性能监控

### v2.0.0 (2024-10-10)
- 🎉 统一交易日历管理
- 📈 添加数据质量检查
- 🛡️ 增强错误处理机制

## 📞 技术支持

如果交易日历出现问题：
1. 检查日志文件中的交易日历相关错误
2. 运行交易日历更新任务
3. 验证数据库中的交易日历数据
4. 提交问题反馈