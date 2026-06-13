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

### GET /
应用根路径。

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

查询语义：
- `symbol` 会先解析为一个首选品种，再只返回该品种的行情；当股票和指数共用代码时，默认优先股票。例如 `symbol=000001` 返回 `000001.SZ` 平安银行，不混入 `000001.SH` 上证综合指数。
- 需要精确查询股票、指数、ETF 等指定品种时，请使用 `instrument_id`，例如 `000001.SH`。

可选参数：
- `return_format`: `pandas`（默认）、`json`、`csv`
- `adjust`: `qfq`（前复权，默认）、`hfq`（后复权）、`none`（不复权）
- `tradestatus`、`is_complete`、`min_volume`、`min_quality_score`
- `include_quality`、`include_metadata`
- `limit`、`offset`

复权说明：
- 系统存储的是**非复权原始数据**，复权由 `AdjustmentEngine` 根据复权因子表实时计算
- 仅**股票**类品种支持复权，指数/ETF 不存在除权概念，即使传入 `adjust=qfq` 也会返回原始数据
- 前复权以查询范围内最新交易日为基准（价格=原始价格），历史价格向下调整使得价格序列连续
- 后复权以上市首日为基准，价格按除权事件累乘放大，反映真实持股收益

示例：
```bash
# 查询前复权数据（默认）
curl "http://localhost:8000/api/v1/quotes/daily?symbol=600519&start_date=2025-01-01&end_date=2025-12-31&adjust=qfq"

# 查询不复权原始数据
curl "http://localhost:8000/api/v1/quotes/daily?symbol=600519&start_date=2025-01-01&end_date=2025-12-31&adjust=none"

# 查询后复权数据
curl "http://localhost:8000/api/v1/quotes/daily?symbol=600519&start_date=2025-01-01&end_date=2025-12-31&adjust=hfq"

# 查询指数数据（指数无复权概念，adjust 参数被忽略）
curl "http://localhost:8000/api/v1/quotes/daily?instrument_id=000001.SH&start_date=2024-01-01&end_date=2024-01-31&return_format=json"
```

### GET /api/v1/quotes/latest
获取指定品种的最新一条行情（从最近 5 天数据中取最新）。

查询语义：
- 返回每个请求品种在查询窗口内 `time` 最大的记录。
- `instrument_ids` 支持数据库后缀和标准交易所后缀；例如 `000001.SZSE` 会归一化查询为 `000001.SZ`，`600000.SSE` 会归一化查询为 `600000.SH`。

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
获取历史下载任务进度。返回当前的下载批次、处理品种数、成功率、预估剩余时间等进度详情（`DownloadProgressResponse`）。

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

## 研究域（Research）

研究域接口统一使用 `/api/v1/research` 前缀。已落到本地 `research.db` 的标准化快照或物化结果原则上都应提供 API 读路径；未开放模块会返回明确的 gated/disabled 原因。

### GET /api/v1/research/company/{instrument_id}/financial-indicators

读取本地财务摘要/关键指标快照。当前接口对应 `financial_summaries`，只读本地库，不在请求时访问外部财务源。

路径参数：
- `instrument_id`：数据库格式代码，如 `600000.SH`、`000001.SZ`

可选参数：
- `include_snapshot`：是否返回完整摘要 JSON，默认 `true`

当前边界：
- 该接口是财务摘要基线，不等价于完整三大报表。
- 如果目标交易所配置为 optional-empty，可能返回结构化空占位。

示例：
```bash
curl "http://localhost:8000/api/v1/research/company/600000.SH/financial-indicators"
curl "http://localhost:8000/api/v1/research/company/600000.SH/financial-indicators?include_snapshot=false"
```

### GET /api/v1/research/company/{instrument_id}/financial-statements

读取本地完整财务报表组合快照。当前接口仍保持兼容读模型，对应 `financial_statements_raw`、`financial_facts`、`financial_indicator_snapshots`；底层同步/存储已新增 source manifest、全数值事实长表、hot/cold tier 和多期 readiness，但该单公司接口默认仍返回当前 bundle 形态。

路径参数：
- `instrument_id`：数据库格式代码，如 `600000.SH`、`000001.SZ`

可选参数：
- `include_statements`：是否返回原始报表分项，默认 `true`

主要字段：
- `report_period`、`publish_date`、`fiscal_year`、`fiscal_quarter`
- `revenue`、`net_income`、`operating_cf`
- `total_assets`、`total_liabilities`、`equity`
- `source`、`source_mode`、`data_as_of`
- `facts`、`indicators`、`statements`

当前边界：
- 单期接口保持兼容形态；多期读取使用 `/financial-statements/history`，避免在单期响应里混入不同报告期结构。
- 官方结构化 `SSE/CNInfo/BSE` 源默认仍 disabled；在 live probe 和小样本 backfill 通过前，实际生产链路仍以 AkShare fallback 为主。
- 仓库级财务 readiness 已提供 REST endpoint，也仍可通过 `scripts/research_financial_statements_rollout_validation.py` 做离线/调度前验证。

示例：
```bash
curl "http://localhost:8000/api/v1/research/company/600000.SH/financial-statements"
curl "http://localhost:8000/api/v1/research/company/600000.SH/financial-statements?include_statements=false"
```

### GET /api/v1/research/company/{instrument_id}/financial-statements/history

读取公司多报告期财务报表历史。默认只读本地 `financials.db`，不在请求时触发远程补数。

可选参数：
- `period_window`：报告期窗口模式，当前支持 `latest`
- `rolling_quarters`：最近报告期数量，默认 `12`
- `report_periods`：逗号分隔的显式报告期列表
- `include_statements`：是否包含原始报表详情，默认 `false`
- `requested_canonical_facts`：逗号分隔的 canonical 字段，用于 L1/L3 分层读取
- `profile`、`mapping_version`：字段映射 profile 与版本
- `include_local_core`：是否附加 L1 本地核心字段诊断
- `include_industry_facts`：是否附加 L1.5 行业专项字段诊断
- `allow_remote_extension`：是否显式允许 L3 东财远程扩展，默认 `false`

示例：
```bash
curl "http://localhost:8000/api/v1/research/company/600000.SH/financial-statements/history?period_window=latest&rolling_quarters=12&include_statements=false"
curl "http://localhost:8000/api/v1/research/company/600000.SH/financial-statements/history?report_periods=2026-03-31,2025-12-31&include_local_core=true"
```

### GET /api/v1/research/company/{instrument_id}/valuation/history

读取本地估值历史。当前接口对应 `valuation_history`，只读本地库。

可选参数：
- `start_date`：开始日期
- `end_date`：结束日期
- `limit`：最大返回点数，默认 `120`
- `include_details`：是否返回估值细节，默认 `true`

当前边界：
- `pe_ratio / pb_ratio / ps_ratio` 仍作为兼容字段保留，其中 `pe_ratio` 优先映射 `pe_ttm`、缺失时回退 `pe_static`，`pb_ratio` 映射 `pb_mrq`，`ps_ratio` 优先映射 `ps_ttm`、缺失时回退 `ps_static`。
- 当前估值历史已拆分 `pe_static / pe_ttm / pe_forward / pb_mrq / ps_static / ps_ttm / ps_forward`；`include_details=true` 时返回每个指标的 numerator、denominator、报告期和可得日。
- `market_cap` 为总市值，`float_market_cap` 为流通市值；两者来自本地收盘价和已落库 `valuation_inputs`，读取接口不会同步请求 CNInfo/AkShare。
- `pe_forward / ps_forward` 在 analyst forecast 输入未启用或缺失时返回空值，并在 details 中给出 explicit unavailable 状态和原因。

示例：
```bash
curl "http://localhost:8000/api/v1/research/company/600000.SH/valuation/history?limit=120"
```

### GET /api/v1/research/company/{instrument_id}/valuation/relative

读取本地相对估值。当前默认使用 authoritative 申万二级同行分组，分组字段由 `valuation.relative.benchmark_field` 配置解析，默认 `sw_l2_code`。

当前字段包括：
- `metric_variants`：本次参与 benchmark 的估值口径，默认 `pe_ttm / pb_mrq / ps_ttm`，并保留兼容字段统计。
- `benchmark_summary`：逐指标返回 valid peer count、mean、median、p25、p75、percentile rank、相对中位数溢价/折价。
- `diagnostics.metric_exclusions`：逐指标列出因缺失、非数值、负值或零值被排除的同行样本。
- `subject_valuation`、`peers`：保留静态、TTM、forward/MRQ 指标字段，便于调用方明确选择口径。

当前边界：
- `pe_forward / ps_forward` 无 forecast 输入时不会参与有效同行统计，只通过空值和 diagnostics 暴露缺失原因。
- 当 `valuation.relative.require_authoritative=true` 时，不使用 reference-only 行业归属降级生成同行分组。

示例：
```bash
curl "http://localhost:8000/api/v1/research/company/600000.SH/valuation/relative"
```

### GET /api/v1/research/company/{instrument_id}/valuation/percentile

读取单个品种在过去 N 个季度窗口内的估值历史分位。接口现场基于本地 `valuation_history` 序列计算，不持久化全市场分位矩阵。

可选参数：
- `as_of_date`：估值日期，默认使用最新可得日
- `quarters`：历史窗口季度数，默认 `12`
- `metrics`：逗号分隔的估值口径，默认 `pe_ttm,pb_mrq,ps_ttm`
- `min_points`：最小有效样本数，默认 `60`
- `negative_policy`：负值估值处理策略，支持 `flag/include/exclude`，默认 `flag`
- `include_series`：是否返回样本序列，默认 `false`

当前边界：
- `negative_policy=flag` 会保留负值样本并返回负值/零值计数，调用方不能把负 PE 静默解释为普通低估。
- `negative_policy=exclude` 且当前值本身被排除时，对应指标返回 `current_value_excluded`，分位为 `null`。

示例：
```bash
curl "http://localhost:8000/api/v1/research/company/600519.SH/valuation/percentile?as_of_date=2026-05-29&quarters=12&metrics=pe_ttm,pb_mrq,ps_ttm&min_points=60&negative_policy=flag"
```

### GET /api/v1/research/company/{instrument_id}/valuation/dcf

按需计算专业 DCF。当前默认走 `ProfessionalDcfEngine`，普通非金融公司支持 `nonfinancial_fcff.v1`；稳定低杠杆且 FCFE 输入齐全的非金融公司支持 `nonfinancial_fcfe.v1`；公用事业/基础设施和 REIT/类 REIT 支持轻量 DDM/FCFE 分派模型；银行支持 `bank_residual_income.v1`，直接估算股权价值并输出 implied P/B；证券公司支持 `broker_excess_capital.v1`，使用归一化 ROE residual income + excess capital 直接估算股权价值；资源周期行业支持 `cyclical_fcff_midcycle.v1`，使用 mid-cycle operating margin 避免直接外推高点利润。保险、地产 NAV、控股公司、高研发/亏损/短上市等 profile 已注册 guardrail，未实现完整模型时返回结构化 unavailable/partial，不静默套用通用 FCFF。

可选参数：
- `growth_rate`、`discount_rate`、`terminal_growth`、`projection_years`：估值假设覆盖。
- `model_profile`：显式指定模型 profile；显式指定会绕过自动推荐，但仍返回 readiness/warning。
- `model_strategy`：`auto / industry / characteristic / compare`。
- `valuation_date`：估值日期；财务事实必须满足 `data_available_date <= valuation_date`。
- `scenario_set`：当前支持 `standard / downside_only`；其他值返回 `invalid_parameters`。
- `terminal_method`：当前支持 `gordon_growth / perpetual_growth`；exit multiple 仍属后续扩展。
- `cash_flow_model`：`fcff / fcfe`。显式 `fcfe` 在 `operating_cf / net_debt_change / capex` 等输入足够时实算 FCFE；输入不足时返回结构化 unavailable/partial，不会伪装为成功 FCFF。
- `include_forecast_rows`、`include_sensitivity`、`include_lineage`：控制输出明细；关闭时会在 warnings/diagnostics 中记录 suppression。
- `include_model_comparison`：强制返回模型对比诊断；比较结果包含行业候选和公司特性候选的 result object，未实现模型以 unavailable/partial 结果表达。
- `include_workbook`：请求投行级 xlsx workbook artifact metadata；当前由本地 `DcfWorkbookBuilder` 生成 stdlib OOXML 工作簿，返回 `workbook_available / workbook_artifact_id / download_path / generated_at / expires_at / sheets / warnings`。
- `workbook_style`：覆盖 workbook 输出样式标识。
- `research_mode`：允许研究模式 partial 输出；生产默认缺关键字段时 fail closed。

主要输出：
- `model_profile / recommended_model / model_suitability_candidates / score_gap / selection_policy`
- `selected_cash_flow_model / cash_flow_model_selection`
- `assumptions`：WACC、无风险利率、ERP、债务成本、FX、行业 beta、商品价格假设及 lineage。
- `forecast_rows / scenarios / sensitivity`
- 银行模型额外返回 `implied_pb / financial_model_diagnostics / ddm_cross_check`，且 `enterprise_value=null`。
- 证券模型额外返回 `implied_pb / normalized_roe / excess_capital / broker_model_diagnostics`，且 `enterprise_value=null`。
- 周期模型额外返回 `cyclical_model_diagnostics`，包含 reported/normalized operating margin、mid-cycle cap/floor 和周期输入可得性。
- `readiness / diagnostics / warnings / lineage`
- `workbook`：仅在 `include_workbook=true` 时返回。artifact 默认写入 `data/reports/dcf_workbooks`，不写入 `valuation_history`。
- `cache_info`：进程内 bounded run cache 命中与 key/hash 摘要，包含 `cache_hit / cache_key / input_hash / parameter_hash / cached_at / expires_at / entry_count / invalidation_policy`。缓存身份包含标的、财务 bundle hash、最新收盘价和参数覆盖，输入或参数变化会自动失效。

示例：
```bash
curl "http://localhost:8000/api/v1/research/company/600519.SH/valuation/dcf?valuation_date=2026-04-18&model_strategy=auto&include_forecast_rows=true&include_sensitivity=true"
curl "http://localhost:8000/api/v1/research/company/688001.SH/valuation/dcf?model_strategy=compare&include_model_comparison=true&research_mode=true"
```

### GET /api/v1/research/valuation/dcf/workbooks/{artifact_id}

下载 `include_workbook=true` 返回的 DCF xlsx artifact。接口只允许读取配置报告目录内、以 `dcf_` 开头的 artifact id，避免路径穿越。

示例：
```bash
curl -OJ "http://localhost:8000/api/v1/research/valuation/dcf/workbooks/dcf_600519_SH_20260605_abcd1234"
```

### GET /api/v1/research/company/{instrument_id}/valuation/dcf/readiness

读取单公司专业 DCF profile readiness。接口只读本地标的、财务 bundle 和本地假设，不在请求时补远程数据。

主要字段：
- `ready`
- `profiles[].model_profile`
- `profiles[].implementation_status`
- `profiles[].ready`
- `profiles[].missing_fields`
- `profiles[].blockers`
- `coverage_diagnostics`

示例：
```bash
curl "http://localhost:8000/api/v1/research/company/600519.SH/valuation/dcf/readiness"
```

### GET /api/v1/research/company/{instrument_id}/valuation/dcf/input-gaps

读取某个模型 profile 的输入缺口。默认 profile 为 `nonfinancial_fcff.v1`。

可选参数：
- `model_profile`：如 `nonfinancial_fcff.v1`、`bank_residual_income.v1`、`cyclical_fcff_midcycle.v1`。

输出会列出缺失字段、requiredness、候选主源、fallback 源、是否可刷新和风险说明。当前 refresh eligibility 只做显式诊断，不自动触发远程请求。

示例：
```bash
curl "http://localhost:8000/api/v1/research/company/600519.SH/valuation/dcf/input-gaps?model_profile=nonfinancial_fcff.v1"
```

### GET /api/v1/research/valuation/dcf/model-profiles

读取 DCF 模型 profile 注册表，包括 required/optional fields、company types 和 implementation status。

示例：
```bash
curl "http://localhost:8000/api/v1/research/valuation/dcf/model-profiles"
```

### GET /api/v1/research/valuation/dcf/assumptions

读取本地 DCF 假设和 source registry。当前包括 A 股/RMB 10 年期国债收益率、美股/USD 10 年期美债收益率、港股/HKD 10 年期港债或外汇基金票据收益率、ERP、债务成本、FX、行业 beta 和商品价格假设。

可选参数：
- `market`：默认 `SSE`
- `currency`：默认 `CNY`

示例：
```bash
curl "http://localhost:8000/api/v1/research/valuation/dcf/assumptions?market=SSE&currency=CNY"
```

### POST /api/v1/research/valuation/dcf/assumptions/refresh

显式刷新 DCF 假设的入口。当前实现为本地优先 source policy/diagnostics：不会在 DCF 计算函数内部隐式联网；未配置远程 adapter 的 source 会返回 `unsupported`。

可选参数：
- `source_profile`：默认 `manual_config`，也可传 `china_bond_10y / us_treasury_10y / hk_gov_bond_or_efn_10y / all`
- `timeout_seconds`：显式超时秒数
- `dry_run`：默认 `true`

示例：
```bash
curl -X POST "http://localhost:8000/api/v1/research/valuation/dcf/assumptions/refresh?source_profile=china_bond_10y&timeout_seconds=10&dry_run=true"
```

### GET /api/v1/research/valuation/readiness

读取估值域 readiness 与 rollout blockers。

当前字段包括：
- `module_enabled`
- `target_instrument_count`
- `valuation_history_total`
- `metric_coverage`
- `exchange_coverage`
- `relative_valuation`
- `financial_statements`
- `blockers`

下一阶段会补充：
- source/parser distribution
- 更细粒度 valuation rollout 操作建议

示例：
```bash
curl "http://localhost:8000/api/v1/research/valuation/readiness"
```

### GET /api/v1/research/financial-statements/readiness

读取财务报表仓库 readiness 与 rollout blockers。

当前字段包括：
- `expected_report_periods`
- `readiness.gaps.period_coverage`
- `readiness.gaps.core_facts`
- `readiness.gaps.source_files`
- `readiness.gaps.tier_coverage`
- `readiness.blockers`

示例：
```bash
curl "http://localhost:8000/api/v1/research/financial-statements/readiness"
```

### GET /api/v1/research/company/{instrument_id}/beta

按需实时计算 benchmark-aware Beta。该接口只读取本地行情、本地复权因子和本地申万行业指数数据；不会预计算入库，也不会在读取时访问外部数据源。

可选参数：
- `benchmark_family`：基准族，默认 `market_default`。可选值：
  - `market_default`：按股票交易所/板块规则选择默认基准
  - `market_broad`：返回配置的宽基基准，当前包括沪深300、中证500、中证1000
  - `board`：返回板块/交易所基准，例如上证综指、深证成指、创业板指、科创50、北证50
  - `industry_sw_l2`：使用 authoritative 申万二级行业指数
  - `custom`：使用 `benchmark_instrument_id` 指定的自定义基准
  - `all`：一次返回 `market_default`、`board`、`market_broad`、`industry_sw_l2` 的去重基准结果
- `benchmark_instrument_id`：显式基准标的 ID，例如 `000300.SH`。传入后按该基准计算，不再自动替换为默认基准。
- `window_days`：Beta 窗口天数。未传入时返回配置默认窗口 `60 / 120 / 252`；传入时只计算指定 n 天窗口。
- `as_of_date`：计算截止日期，格式 `YYYY-MM-DD`。未传入时使用最新本地行情日期。
- `include_details`：是否返回 `diagnostics`，默认 `true`。

当前计算口径：
- 股票收益率默认使用本地 `qfq` 前复权 close。
- 指数基准默认使用不复权 price-index close。
- 申万二级行业 Beta 只使用 authoritative `industry_memberships` 映射出的行业指数，不使用 reference-only 行业字段。
- 若基准行情缺失、样本不足、基准收益方差为 0 或行业映射缺失，结果返回 `status=unavailable` 和结构化 `missing_reason`。

常用示例：
```bash
# 1. 默认 market_default 基准，返回默认 60/120/252 三个窗口
curl "http://localhost:8000/api/v1/research/company/600000.SH/beta"

# 2. 指定 180 天窗口，仍使用 market_default 基准
curl "http://localhost:8000/api/v1/research/company/600000.SH/beta?window_days=180"

# 3. 指定沪深300为自定义基准，并指定计算截止日期
curl "http://localhost:8000/api/v1/research/company/600000.SH/beta?benchmark_family=custom&benchmark_instrument_id=000300.SH&window_days=252&as_of_date=2026-05-29"

# 4. 返回所有配置的宽基 Beta，默认窗口 60/120/252 都会计算
curl "http://localhost:8000/api/v1/research/company/600000.SH/beta?benchmark_family=market_broad"

# 5. 一次返回默认、板块、宽基和行业基准的去重结果
curl "http://localhost:8000/api/v1/research/company/300708.SZ/beta?benchmark_family=all&window_days=252"

# 6. 板块/交易所基准，例如创业板股票会映射到创业板指
curl "http://localhost:8000/api/v1/research/company/300750.SZ/beta?benchmark_family=board&window_days=120"

# 7. authoritative 申万二级行业 Beta
curl "http://localhost:8000/api/v1/research/company/600000.SH/beta?benchmark_family=industry_sw_l2&window_days=252"

# 8. 关闭诊断字段，减少响应体
curl "http://localhost:8000/api/v1/research/company/600000.SH/beta?window_days=60&include_details=false"
```

响应字段要点：
- `items[].beta`：Beta 值；不可用时为 `null`
- `items[].benchmark_family / benchmark_instrument_id / benchmark_name`：实际使用的基准
- `items[].alpha`：日频单因子回归截距，不应直接年化为稳定超额收益
- `items[].residual_volatility`：残差年化波动率
- `items[].tracking_error`：股票相对基准的主动波动
- `items[].standard_error_beta / t_stat_beta / p_value_beta`：Beta 标准误、t 统计量和显著性 p 值；当前 p 值为无新增依赖的正态近似双尾 p 值
- `items[].quality_flag`：`high / medium / low / unavailable`，基于 R²、p 值和样本数的简化质量标记
- `items[].interpretation_flags`：解释性标记，例如 `low_explanatory_power`、`beta_not_statistically_significant`、`active_risk_dominates`、`unstable_across_windows`
- `items[].window_days / observation_count / min_observation_count / window_start / window_end`：窗口与样本信息
- `items[].stock_adjustment / benchmark_adjustment`：收益率口径
- `items[].diagnostics`：本地行情行数、收益率行数、对齐观测数、基准选择规则、p 值方法和跨窗口 Beta 差异等计算诊断

### GET /api/v1/research/company/{instrument_id}/shareholders

读取本地股东摘要快照。当前 `shareholders` 已按 `paid_high_availability` gate 开放，接口只读取本地 `shareholder_snapshots`，不会在请求时访问外部数据源。

路径参数：
- `instrument_id`：数据库格式代码，如 `600115.SH`、`000001.SZ`

可选参数：
- `include_snapshot`：是否返回完整 snapshot 明细，默认 `true`；设为 `false` 时只返回轻量摘要字段

主要字段：
- `holder_count`、`holder_count_report_date`
- `top_holders_report_date`、`top_holders_count`、`top_holders_total_ratio`
- `control_owner_name`、`control_owner_ratio`
- `source`、`source_mode`、`data_as_of`
- `snapshot`：可选明细，包含 `top_holders`、`ownership_clues`、`scope_sources` 等

示例：
```bash
curl "http://localhost:8000/api/v1/research/company/600115.SH/shareholders"
curl "http://localhost:8000/api/v1/research/company/600115.SH/shareholders?include_snapshot=false"
```

### GET /api/v1/research/shareholders/readiness

读取股东域 rollout readiness 与 API gate 状态。当前用于确认周更后本地快照是否仍满足正式读取要求。

主要字段：
- `module_enabled`
- `delivery_mode`
- `snapshot_api_enabled`
- `target_instrument_count`
- `snapshot_total`
- `scope_counts`
- `exchange_coverage`
- `blockers`
- `ready_for_paid_high_availability_rollout`

示例：
```bash
curl "http://localhost:8000/api/v1/research/shareholders/readiness"
```

当前配置状态：
- `shareholders.enabled = true`
- `delivery_mode = paid_high_availability`
- `snapshot_api_requires_mode = paid_high_availability`
- `shareholder_shadow_sync` 仅作为 Telegram `/run shareholder_shadow_sync` 手工全量刷新入口，不再常驻周六定时运行
- `shareholder_reconciliation_sync` 每周六 `12:30` 做全量读取 + changed-only 复核，只补写变化、缺失或 required scope 不完整标的
- `shareholder_incremental_sync` 每日 `06:30` 做 CNInfo 公告驱动增量检查，有变化才重写本地股东快照；公告先到而 data20 结构化数据未更新时进入 5 个自然日 pending recheck，且同一批公告不会滚动延长

股东信息更新任务：

| 任务 | 触发方式 | 对 API 数据的影响 |
|---|---|---|
| `shareholder_incremental_sync` | 每日 `06:30` / `/run` | 按公告候选定向刷新，变化或缺口才更新 `shareholder_snapshots` |
| `shareholder_reconciliation_sync` | 周六 `12:30` / `/run` | 全量读取后做 changed-only 复核，补足静默变化或历史缺口 |
| `shareholder_shadow_sync` | 仅 `/run` | 手工全量刷新 `shareholder_snapshots` |

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
返回数据库与数据质量统计摘要（与 `DataStatsResponse` 结构一致），包括品种数、行情数、高质量记录、缺口总数及各级分布等。

### POST /api/v1/data/validate
启动数据质量验证并返回基本验证结果（`DataValidationResponse`）。

**请求体**：
```json
{
  "exchange": "SSE",
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "validation_type": "completeness",
  "strict_mode": false
}
```

---

## 当前未实现（文档中曾出现）

以下接口在当前代码中不存在，已从本文档移除：
- `GET /api/v1/status`
- `GET /api/v1/quotes/{instrument_id}`
- `GET /api/v1/quotes/batch`
- `GET /api/v1/indicators/{instrument_id}`
