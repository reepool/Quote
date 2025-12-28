# RESTful API 接口文档（与当前代码一致）

本文档以 `api/app.py` 与 `api/routes.py` 为准，描述当前可用接口、路径与参数要求。

## 基础信息

### 启动服务
```bash
python main.py api --host 0.0.0.0 --port 8000
```

### 基础 URL
```
http://localhost:8000/api/v1
```

注意：`/api/v1` 本身没有根路由，直接访问会返回 404。

### 在线文档
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- OpenAPI JSON: `http://localhost:8000/openapi.json`

### 返回格式说明
当前实现未统一为单一 envelope（如 `success/data/message`）。部分接口直接返回列表/对象，部分返回 `{"success": true, ...}` 风格的 JSON。请参考各接口示例。

---

## 健康检查与系统状态

### GET /health
应用根路径健康检查（不带 `/api/v1` 前缀）。

### GET /api/v1/health
返回系统状态（与 `SystemStatusResponse` 结构一致）。

### GET /api/v1/system/status
返回系统状态详情（与 `SystemStatusResponse` 结构一致）。

---

## 交易品种（Instruments）

### GET /api/v1/instruments
查询交易品种列表，支持过滤与分页。

常用参数：
- `exchange`、`type`、`industry`、`sector`、`market`、`status`
- `is_active`、`is_st`、`trading_status`
- `listed_after`、`listed_before`
- `limit`、`offset`、`sort_by`、`sort_order`

### GET /api/v1/instruments/{instrument_id}
按 `instrument_id` 查询单个品种信息。

### GET /api/v1/instruments/symbol/{symbol}
按 `symbol` 查询单个品种信息。

---

## 行情数据（Quotes）

### GET /api/v1/quotes/daily
查询日线行情数据。

必填参数：
- `start_date` (datetime)
- `end_date` (datetime)

二选一参数：
- `instrument_id` 或 `symbol`

可选参数：
- `return_format`: `pandas`（默认）、`json`、`csv`
- `tradestatus`、`is_complete`、`min_volume`、`min_quality_score`
- `include_quality`、`include_metadata`
- `limit`、`offset`

返回格式说明：
- `pandas`: 仍返回 JSON（由 pandas DataFrame 序列化而来）
- `json`: 返回 JSON
- `csv`: 返回 `text/csv` 的内容（可直接 `pd.read_csv` 读取）

示例：
```bash
curl "http://localhost:8000/api/v1/quotes/daily?instrument_id=000001.SZSE&start_date=2024-01-01T00:00:00&end_date=2024-01-31T00:00:00&return_format=json"
```

### GET /api/v1/quotes/latest
获取指定品种的最新一条行情（从最近 5 天数据中取最新）。

必填参数：
- `instrument_ids`（可重复参数）

示例：
```bash
curl "http://localhost:8000/api/v1/quotes/latest?instrument_ids=000001.SZSE&instrument_ids=600000.SSE"
```

---

## 数据管理（Data Management）

### POST /api/v1/data/update
启动每日数据更新后台任务。

请求体：`QuoteQueryRequest`（当前实现要求 `start_date`、`end_date`）

注意：当前实现读取 `start_date` 作为目标日期，并未从模型中支持 `exchanges` 字段。

示例：
```bash
curl -X POST "http://localhost:8000/api/v1/data/update" \
  -H "Content-Type: application/json" \
  -d '{"start_date":"2024-10-01T00:00:00","end_date":"2024-10-02T00:00:00"}'
```

### POST /api/v1/data/download/historical
启动历史数据下载后台任务。

请求体：`BatchDownloadRequest`

示例：
```bash
curl -X POST "http://localhost:8000/api/v1/data/download/historical" \
  -H "Content-Type: application/json" \
  -d '{
    "exchanges": ["SSE", "SZSE"],
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "precise_mode": true,
    "resume": true,
    "quality_threshold": 0.7
  }'
```

### GET /api/v1/data/download/progress
获取历史下载任务进度。

---

## 数据缺口（Data Gaps）

### GET /api/v1/gaps
查询缺口数据（不会修复，只返回缺口列表）。

**必填参数**：
- `start_date` (date, YYYY-MM-DD)
- `end_date` (date, YYYY-MM-DD)

**可选参数**：
- `exchange` (string): 交易所代码，如 `SSE`/`SZSE`
- `instrument_id` (string): 品种代码，如 `300708.SZSE`（内部会转换为数据库格式）
- `severity` (string): 严重度过滤，`low`/`medium`/`high`/`critical`
- `gap_type` (string): 缺口类型过滤（如 `missing_data`）

**示例**：
```bash
curl "http://localhost:8000/api/v1/gaps?exchange=SSE&start_date=2025-01-01&end_date=2025-01-31"
```

### POST /api/v1/gaps/fill
启动缺口修复后台任务（异步执行，接口立即返回）。

请求体：`DataGapFillRequest`

**参数说明（常用）**：
- `exchange` (string, 可选): 指定交易所，未指定则默认 A 股交易所
- `severity_filter` (list, 可选): 严重度过滤，不传则不过滤
- `instrument_ids` (list, 可选): 指定品种列表
- `gap_type_filter` (list, 可选): 缺口类型过滤
- `max_gap_days` (int, 可选): 最大缺口天数过滤
- `dry_run` (bool, 可选): 试运行模式，不写入数据库

**说明**：
- 未传 `start_date` 时，系统默认“上市日期至今”范围执行缺口检测与修复。
- 未传 `exchange` 时，默认修复 A 股交易所。

**示例**：
```bash
curl -X POST "http://localhost:8000/api/v1/gaps/fill" \
  -H "Content-Type: application/json" \
  -d '{
    "exchange": "SSE",
    "severity_filter": ["high", "critical"],
    "dry_run": false
  }'
```

### GET /api/v1/gaps/report
返回缺口质量报告（当前实现为简化占位返回，字段为空/默认值）。

---

## 交易日历（Calendar）

### GET /api/v1/calendar/trading
查询交易日历（返回每一天是否为交易日）。

**必填参数**：
- `exchange` (string): 交易所代码，如 `SSE`/`SZSE`
- `start_date` (date, YYYY-MM-DD)
- `end_date` (date, YYYY-MM-DD)

**可选参数**：
- `include_weekends` (bool): 是否包含周末，默认 false
- `session_type` (string): 交易时段类型（目前仅回填到响应中，不影响逻辑）

**返回说明**：
返回一个日期列表，每条包含该日期是否为交易日。

**示例**：
```bash
curl "http://localhost:8000/api/v1/calendar/trading?exchange=SSE&start_date=2025-01-01&end_date=2025-01-10"
```

### GET /api/v1/calendar/trading/next
获取指定日期之后的下一个交易日。

**必填参数**：
- `exchange` (string)
- `date` (date, YYYY-MM-DD)

**示例**：
```bash
curl "http://localhost:8000/api/v1/calendar/trading/next?exchange=SSE&date=2025-01-10"
```

### GET /api/v1/calendar/trading/previous
获取指定日期之前的上一个交易日。

**必填参数**：
- `exchange` (string)
- `date` (date, YYYY-MM-DD)

**示例**：
```bash
curl "http://localhost:8000/api/v1/calendar/trading/previous?exchange=SSE&date=2025-01-10"
```

---

## 统计与验证

### GET /api/v1/stats
返回数据库与数据质量统计摘要。

### POST /api/v1/data/validate
数据质量验证（当前实现为简化占位返回）。

请求体：`DataValidationRequest`

---

## 当前未实现（文档中曾出现）

以下接口在当前代码中不存在，已从本文档移除：
- `GET /api/v1/status`
- `GET /api/v1/quotes/{instrument_id}`
- `GET /api/v1/quotes/batch`
- `GET /api/v1/indicators/{instrument_id}`
