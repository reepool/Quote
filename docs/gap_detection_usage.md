# GAP检测功能使用说明

## 功能概述

新增的GAP检测功能允许您检测数据库中缺失的行情数据，并提供详细的报告分析，帮助您识别需要补充的数据缺口。

## 命令语法

```bash
python main.py gap [选项]
```

## 参数说明

### 必选参数
无（所有参数都是可选的）

### 可选参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `--exchanges` | list | 交易所列表，可选值：SSE, SZSE, HKEX, NASDAQ, NYSE |
| `--start-date` | string | 开始日期，格式：YYYY-MM-DD |
| `--end-date` | string | 结束日期，格式：YYYY-MM-DD |
| `--severity` | string | 严重程度过滤，可选值：low, medium, high, critical |
| `--output` | string | 输出报告文件路径 |
| `--detailed` | flag | 显示详细的股票级别缺口信息 |

## 使用示例

### 1. 基本缺口检测
检测所有交易所的缺口（默认从1990年到今天）：
```bash
python main.py gap
```

### 2. 指定交易所和日期范围
检测深交所2024年的数据缺口：
```bash
python main.py gap --exchanges SZSE --start-date 2024-01-01 --end-date 2024-12-31
```

### 3. 按严重程度过滤
只检测严重程度为critical的缺口：
```bash
python main.py gap --exchanges SSE SZSE --severity critical
```

### 4. 详细报告模式（显示具体缺失日期）
显示每只股票的具体缺口信息和缺失日期：
```bash
python main.py gap --exchanges SZSE --start-date 2024-01-01 --detailed
```

### 5. 保存报告到文件
将详细报告保存为JSON文件：
```bash
python main.py gap --exchanges SZSE --start-date 2024-01-01 --detailed --output reports/gap_report_2024.json
```

### 6. 综合使用
检测指定日期范围内的critical级别缺口，并保存详细报告：
```bash
python main.py gap \
  --exchanges SSE SZSE \
  --start-date 2023-01-01 \
  --end-date 2024-12-31 \
  --severity critical \
  --detailed \
  --output reports/gap_analysis.json
```

## 报告格式

### 控制台输出
报告包含以下部分：
- 📋 检测信息：交易所、日期范围、检测时间
- 📊 缺口摘要：总缺口数、受影响股票数
- 🎯 严重程度分布：各严重程度的缺口数量和百分比
- 📈 交易所分布：各交易所的缺口数量和百分比
- 🔝 受影响最严重的股票：按严重程度排序的前10名
- 📋 详细股票缺口信息（使用--detailed时显示）
  - 具体缺失日期列表（≤10天显示全部，>10天显示前10天）
  - 缺口范围和严重程度信息

### JSON报告格式
```json
{
  "detection_info": {
    "exchanges": ["SZSE"],
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "detection_time": "2025-10-11T20:00:00.000000"
  },
  "summary": {
    "total_gaps": 150,
    "affected_stocks": 45,
    "severity_distribution": {
      "low": 80,
      "medium": 50,
      "high": 15,
      "critical": 5
    },
    "exchange_distribution": {
      "SZSE": 150
    }
  },
  "stock_details": {
    "000001.SZ": {
      "total_gaps": 3,
      "total_missing_days": 8,
      "gaps": [
        {
          "start_date": "2024-01-15",
          "end_date": "2024-01-17",
          "days": 3,
          "severity": "medium",
          "recommendation": "Schedule immediate fill"
        }
      ]
    }
  },
  "top_affected_stocks": [
    {
      "symbol": "000001.SZ",
      "severity_score": 28,
      "total_missing_days": 8,
      "gap_count": 3
    }
  ]
}
```

## 新功能：具体缺失日期显示

### 功能特性
- **智能显示**: 根据缺失天数自动调整显示策略
- **≤10天**: 显示所有缺失的具体日期
- **>10天**: 显示前10个缺失日期，并提示剩余数量
- **格式友好**: 日期格式为 YYYY/MM/DD，每行显示5个日期
- **排序显示**: 所有缺失日期按时间顺序排列

### 显示示例

#### 缺失天数较少（≤10天）
```
📋 详细股票缺口信息:

📈 600797.SSE (缺口数: 1, 缺失天数: 3)
   缺失日期: 2024/12/20, 2024/12/23, 2024/12/24
   缺口范围: 2024/12/20 到 2024/12/24 (3天, medium)
```

#### 缺失天数较多（>10天）
```
📋 详细股票缺口信息:

📈 001872.SZSE (缺口数: 2, 缺失天数: 15)
   缺失日期: 2024/11/01, 2024/11/04, 2024/11/05, 2024/11/06, 2024/11/07
             2024/11/08, 2024/11/11, 2024/11/12, 2024/11/13, 2024/11/14
   ... 还有 5 个缺失日期
   缺口范围: 2024/11/01 到 2024/11/08 (6天, high)
   缺口范围: 2024/11/15 到 2024/11/20 (4天, medium)
```

### 实际应用
这个功能让您能够：
- **精确定位**: 知道具体缺少哪些日期的数据
- **批量补充**: 根据缺失日期列表进行精确的数据补充
- **问题排查**: 快速识别数据缺失的模式和原因
- **质量控制**: 确保关键日期的数据完整性

## 严重程度说明

| 严重程度 | 缺口天数 | 说明 | 建议 |
|----------|----------|------|------|
| low | 1天 | 单日数据缺失 | 在下次更新时监控 |
| medium | 2-5天 | 短期数据缺失 | 立即安排补充 |
| high | 6-20天 | 中期数据缺失 | 优先补充数据 |
| critical | >20天 | 长期数据缺失 | 调查原因，可能退市或停牌 |

## 实际使用场景

### 1. 数据质量检查
定期检查数据库的完整性：
```bash
# 每周检查一次最近的数据
python main.py gap --start-date $(date -d '1 month ago' +%Y-%m-%d) --detailed
```

### 2. 下载后验证
数据下载完成后验证下载质量：
```bash
python main.py gap --exchanges SZSE --start-date 2024-01-01 --output reports/post_download_check.json
```

### 3. 问题排查
针对特定股票的问题排查：
```bash
python main.py gap --exchanges SZSE --start-date 2024-01-01 --detailed | grep "000001.SZ"
```

### 4. 数据维护计划
根据严重程度制定数据补充计划：
```bash
# 优先处理critical级别的缺口
python main.py gap --severity critical --output reports/critical_gaps.json
```

## 注意事项

1. **性能考虑**：全量检测可能需要较长时间，建议限制检测范围
2. **新上市股票**：上市不足3个月的股票会有正常的"历史缺口"，这是正常现象
3. **非交易日**：系统已正确排除非交易日（周末、节假日）
4. **报告文件**：输出目录会自动创建，确保有足够的磁盘空间

## 故障排除

### 常见问题

1. **检测时间过长**
   - 缩小检测日期范围
   - 指定特定的交易所

2. **报告为空**
   - 检查数据库中是否有数据
   - 确认日期范围是否合理

3. **内存使用过高**
   - 避免检测过大范围的日期
   - 使用严重程度过滤减少结果

4. **输出文件错误**
   - 检查输出路径是否有写权限
   - 确保目录存在或可以创建