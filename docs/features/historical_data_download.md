# 历史数据下载功能

## 📖 功能概述

历史数据下载是 Quote System 的核心功能，支持多种下载模式和灵活的配置选项。系统提供了从全量下载到精确日期范围下载的完整解决方案。

## 🚀 主要特性

### 1. 多种下载模式
- **全量历史下载**：下载指定交易所的所有历史数据
- **指定日期范围下载**：精确控制下载的日期范围
- **按年份下载**：支持指定一个或多个年份
- **续传下载**：支持中断后的断点续传
- **按品种类型下载**：支持 `--types stock index etf` 指定只下载股票、指数或 ETF

### 2. 智能下载策略
- **精确下载模式**：基于股票上市日期计算下载范围
- **分块下载**：支持灵活的分块策略
- **质量评估**：实时数据质量评分
- **自动过滤**：避免无效请求

### 3. 容错和恢复
- **断点续传**：下载中断后自动恢复
- **错误重试**：智能重试机制
- **进度跟踪**：详细的下载进度记录

## 📋 支持的交易所

| 交易所代码 | 交易所名称 | 支持状态 |
|-----------|-----------|----------|
| SSE | 上海证券交易所 | ✅ 完全支持 |
| SZSE | 深圳证券交易所 | ✅ 完全支持 |
| BSE | 北京证券交易所 | ✅ 完全支持 |
| HKEX | 香港交易所 | 🚧 开发中 |
| NASDAQ | 纳斯达克 | 🚧 开发中 |
| NYSE | 纽约证券交易所 | 🚧 开发中 |

## 🎯 使用方法

### 基本命令格式
```bash
python main.py download [参数]
```

### 命令参数详解

#### 交易所选择
```bash
--exchanges SSE SZSE BSE  # 指定交易所列表
--preset a_shares         # 使用市场预设组合
```

#### 日期范围指定
```bash
# 指定完整日期范围
--start-date 2024-01-01 --end-date 2024-12-31

# 只指定开始日期（到今天）
--start-date 2024-06-01

# 只指定结束日期（从开始）
--end-date 2024-12-31

# 按年份下载
--years 2023 2024
```

#### 续传控制
```bash
--resume       # 启用续传（默认）
--no-resume    # 禁用续传，重新开始
```

#### 品种类型指定
```bash
--types stock          # 仅下载股票
--types index          # 仅下载指数
--types stock index    # 同时下载股票和指数
--types stock index etf  # 所有品种（ETF 需 AkShare 支持）
```

## 📊 下载场景和逻辑

### 场景1：全历史数据下载
```bash
python main.py download --exchanges SSE SZSE
python main.py download --preset a_shares
```

**特点：**
- 从数据源获取最新交易日历
- 下载从股票上市日期到昨天的所有数据
- 适合首次使用或完整更新

**流程：**
1. 更新交易日历（从数据源）
2. 获取股票列表和上市日期
3. 计算每个股票的下载范围
4. 批量下载数据
5. 质量评估和保存

### 场景2：指定日期范围下载
```bash
python main.py download --exchanges SZSE --start-date 2024-06-01 --end-date 2024-12-31
```

**特点：**
- 使用缓存的交易日历
- 只下载指定日期范围内的数据
- 适合数据补全和特定分析

**流程：**
1. 使用缓存的交易日历
2. 筛选指定日期范围内的交易日
3. 批量下载数据
4. 质量评估和保存

### 场景3：续传下载
```bash
python main.py download --exchanges SSE SZSE --resume
```

**特点：**
- 从上次中断的位置继续
- 使用缓存的交易日历
- 保持下载进度

**流程：**
1. 读取下载进度文件
2. 计算未完成的批次
3. 从中断处继续下载
4. 更新进度

## ⚙️ 核心类和方法

### DataManager 类
```python
class DataManager:
    async def download_all_historical_data(
        self,
        exchanges: List[str] = None,
        start_date: date = None,
        end_date: date = None,
        resume: bool = True,
        quality_threshold: float = 0.7,
        force_update_calendar: bool = True
    )
```

**参数说明：**
- `exchanges`: 交易所列表
- `start_date`: 开始日期（可选）
- `end_date`: 结束日期（可选）
- `resume`: 是否启用续传
- `quality_threshold`: 数据质量阈值
- `force_update_calendar`: 是否强制更新交易日历

### 核心方法

#### 1. `_download_exchange_precise()`
精确下载模式，基于上市日期计算下载范围。

```python
async def _download_exchange_precise(
    self,
    exchange: str,
    instruments: List[Dict],
    start_date: date,
    end_date: date,
    quality_threshold: float,
    resume: bool = False
)
```

#### 2. `_download_instrument_by_trading_days()`
按交易日下载单个股票数据。

```python
async def _download_instrument_by_trading_days(
    self,
    instrument: Dict,
    exchange: str,
    trading_days: List[date],
    quality_threshold: float
) -> List[Dict]
```

#### 3. `_update_trading_calendar()`
更新交易日历。

```python
async def _update_trading_calendar(
    self,
    exchange: str,
    start_date: date,
    end_date: date
)
```

## 📈 下载进度跟踪

### 进度文件结构
```json
{
  "batch_id": "20251011_154830",
  "total_instruments": 5159,
  "processed_instruments": 2909,
  "successful_downloads": 2909,
  "failed_downloads": 0,
  "total_quotes": 10000000,
  "trading_days_processed": 0,
  "total_trading_days": 0,
  "data_gaps_detected": 0,
  "quality_issues": 0,
  "start_time": "2025-10-11T15:48:30.123456+08:00",
  "current_exchange": "SZSE",
  "errors": []
}
```

### 字段说明
- `batch_id`: 下载批次ID
- `total_instruments`: 总股票数量
- `processed_instruments`: 已处理股票数量
- `successful_downloads`: 成功下载数量
- `failed_downloads`: 失败下载数量
- `total_quotes`: 总行情数据量
- `data_gaps_detected`: 检测到的数据缺口
- `quality_issues`: 质量问题数量
- `current_exchange`: 当前处理的交易所

## 🎯 业务逻辑详解

### 1. 交易日历获取策略

**全历史下载：**
- 从数据源获取最新交易日历
- 确保数据的准确性

**指定日期下载：**
- 使用本地缓存的交易日历
- 提高下载效率

**续传下载：**
- 使用本地缓存的交易日历
- 避免重复获取

### 2. 批次处理逻辑

```python
# 批次大小配置
batch_size = 50  # 每批处理50只股票

# 批次计算
for batch_idx, batch in enumerate(batches, 1):
    if batch_idx < start_batch:
        continue  # 跳过已完成的批次
    # 处理当前批次
```

### 3. 续传逻辑修复

**修复前的问题：**
- 使用累计的 `processed_instruments` 计算批次
- 导致跨交易所时批次计算错误

**修复后的逻辑：**
```python
# 按交易所单独计算已处理数量
if exchange == 'SZSE' and resume:
    sse_count = await self.db_ops.get_instruments_by_exchange('SSE')
    exchange_processed = max(0, self.progress.processed_instruments - len(sse_count))

processed_batches = (exchange_processed // batch_size)
start_batch = processed_batches + 1
```

## 🔧 配置选项

### config.json 配置
```json
{
  "data_config": {
    "download_chunk_days": 2000,  // 分块下载大小
    "batch_size": 50,              // 批处理大小
    "quality_threshold": 0.7,      // 质量阈值
    "resume_enabled": true,        // 是否启用续传
    "instrument_types": ["stock", "index"]  // 控制下载和每日更新的品种类型
  }
}
```

> **注意**：`instrument_types` 同时控制历史下载和每日自动更新的品种范围。当前 BaoStock 不支持 ETF 日线数据，如需下载 ETF 需待接入 AkShare 数据源后再将 `etf` 加入此列表。

### 一次性下载模式
设置 `download_chunk_days: 0` 可以启用一次性下载模式，一次性获取所有历史数据。

## 📊 性能监控

### 下载速度优化
- **批量请求**：减少网络请求次数
- **并发控制**：合理的并发数量
- **限流机制**：避免触发数据源限制

### 内存管理
- **分批处理**：避免内存溢出
- **及时释放**：处理完立即释放内存
- **缓存策略**：合理使用缓存

## 🚨 错误处理

### 常见错误类型
1. **网络错误**：数据源连接失败
2. **数据错误**：返回数据格式异常
3. **存储错误**：数据库写入失败
4. **权限错误**：数据源访问权限不足

### 错误恢复机制
- **自动重试**：配置重试次数和间隔
- **降级策略**：主数据源失败时切换备用源
- **错误记录**：详细的错误日志记录

## 📝 最佳实践

### 1. 首次使用
```bash
# 推荐先下载少量数据测试
python main.py download --exchanges SSE --start-date 2024-01-01 --end-date 2024-01-31

# 确认无误后再进行全量下载
python main.py download --exchanges SSE SZSE
```

### 2. 数据补全
```bash
# 补充特定日期范围的数据
python main.py download --exchanges SZSE --start-date 2024-06-01 --end-date 2024-06-30
```

### 3. 续传使用
```bash
# 下载中断后继续
python main.py download --exchanges SSE SZSE --resume

# 重新开始（忽略之前进度）
python main.py download --exchanges SSE SZSE --no-resume
```

## 🔄 版本更新日志

### v2.4.0 (2026-03-26)
- ✨ 新增指数和 ETF 品种支持（`--types` CLI 参数）
- 🛡️ 每日更新任务自动包含指数，并通过 `instrument_types` 配置过滤 ETF
- 🚀 数据库写入层重构为 Bulk Upsert，大幅提升写入性能
- 🐛 修复 BSE 交易所代码过滤、resume 模式下日历截断、时区报错等多个 Bug

### v2.1.0 (2025-10-11)
- ✨ 新增指定日期范围下载功能
- 🎯 智能交易日历选择策略
- 🐛 修复续传逻辑中的批次计算错误
- 🔧 优化下载性能和错误处理

### v2.0.0 (2024-10-10)
- 🎉 统一了数据源限流机制
- 📊 添加了数据质量评分系统
- 🚀 支持一次性下载模式
- 🛡️ 增强了错误恢复机制

## 📞 技术支持

如果在使用过程中遇到问题，请：
1. 查看日志文件 `log/sys.log`
2. 检查配置文件 `config/config.json`
3. 参考故障排除文档
4. 提交 Issue 反馈问题