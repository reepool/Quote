# 投研数据引擎 (Research Data Engine) — 需求文档

> **定位**：本系统不生成报告，只**提供数据和计算结果**。下游消费者（AI 报告生成器 / 人工分析师）通过 API 获取所需数据后自行组织输出。

---

## 1. 系统边界与设计原则

### 1.1 系统边界

```
┌──────────────────────────────────────────────────────┐
│               下游消费者 (报告生成 / AI)                │
│   - 公司简况报告                                      │
│   - 深度研究报告                                      │
│   - 技术分析报告                                      │
└──────────────────┬───────────────────────────────────┘
                   │ REST API 调用
┌──────────────────▼───────────────────────────────────┐
│           Research Data Engine (本系统)               │
│                                                      │
│  ┌─────────┐ ┌──────────┐ ┌────────────┐           │
│  │基础数据层│ │ 计算引擎层│ │  API 服务层 │           │
│  └────┬────┘ └─────┬────┘ └──────┬─────┘           │
│       │            │             │                   │
│  ┌────▼────────────▼─────────────▼─────┐            │
│  │            数据存储层 (DB/Cache)      │            │
│  └─────────────────────────────────────┘            │
└──────────────────────────────────────────────────────┘
                   │ 数据采集
┌──────────────────▼───────────────────────────────────┐
│              外部数据源                               │
│  AkShare / BaoStock / Tushare / YFinance / pytdx     │
│  东方财富 / 同花顺 / 国债收益率 / 统计局              │
└──────────────────────────────────────────────────────┘
```

### 1.2 设计原则

| 原则 | 说明 |
|------|------|
| **数据先行** | 先确保数据可获取、可存储，再做计算 |
| **计算与存储分离** | 原始数据入库，计算结果可缓存但不作为唯一真相 |
| **渐进式建设** | 按模块优先级分阶段交付，不求一步到位 |
| **与现有 Quote 系统共存** | 复用现有行情数据基础设施，新增模块独立隔离 |

### 1.3 与现有 Quote 系统的关系

| 现有能力 (直接复用) | 新增能力 (本次设计) |
|---|---|
| 日线行情数据 (OHLCV) | 财务报表数据 |
| 复权因子计算 | 估值计算引擎 |
| 交易日历 | 技术指标计算引擎 |
| 品种基本信息 (代码/名称/交易所/行业) | 公司详细资料 |
| 数据源工厂 (AkShare/BaoStock 等) | 研报/舆情数据采集 |
| API 框架 (FastAPI) | 分析师预期数据 |

---

## 2. 模块总览：从报告需求到数据引擎

> 以下从三类最终报告反向拆解出 **12 个计算/数据模块**。

### 报告需求 → 模块映射

| 最终报告 | 所需信息 | 对应引擎模块 |
|----------|----------|--------------|
| **公司简况** | 基本信息 | M01 公司档案 |
| | 主营业务 / 产品 / 客户 / 成本 | M02 业务画像 |
| | 行业定位 | M03 行业定位 |
| | 3 年财务指标 | M04 财务指标 |
| | 分析师覆盖 | M05 分析师预期 |
| | 增长驱动 | M06 增长驱动(文本元数据) |
| **深度报告** | 行业分析 | M03 行业定位 (扩展) |
| | 竞争分析 | M07 竞争对标 |
| | 供需分析 | M08 供需指标 |
| | 研报摘要 | M09 研报数据 |
| | 绝对估值 | M10 估值引擎 |
| | 相对估值 | M10 估值引擎 |
| | 核心竞争力 | M02 + M04 组合 |
| | 风险分析 | M11 风险指标 |
| | 舆情分析 | M12 舆情数据 |
| | 投资价值判断 | M10 + M11 组合 |
| **技术分析** | 趋势指标 | M13 技术指标引擎 |
| | 动量指标 | M13 技术指标引擎 |
| | 波动指标 | M13 技术指标引擎 |
| | 成交量指标 | M13 技术指标引擎 |
| | 形态识别 | M14 形态识别引擎 |
| | 多周期分析 | M13 + M14 组合 |

---

## 3. 各模块详细需求

---

### M01 — 公司档案模块

**计算目标**：提供公司基本面快照数据

#### 所需数据变量

| 变量 | 类型 | 说明 | 必选 |
|------|------|------|------|
| `company_name` | str | 公司全称 | ✅ |
| `short_name` | str | 简称 | ✅ |
| `symbol` / `instrument_id` | str | 交易代码 | ✅ |
| `exchange` | str | 上市交易所 | ✅ |
| `listed_date` | date | 上市日期 | ✅ |
| `registered_capital` | float | 注册资本 (万元) | |
| `legal_representative` | str | 法人代表 | |
| `registered_address` | str | 注册地址 | |
| `office_address` | str | 办公地址 | |
| `website` | str | 公司网站 | |
| `employees_count` | int | 员工人数 | |
| `business_scope` | text | 经营范围 | |
| `company_profile` | text | 公司简介 | ✅ |
| `industry_csrc` | str | 证监会行业分类 | ✅ |
| `industry_sw` | str | 申万行业分类 | ✅ |
| `total_share_capital` | float | 总股本 (万股) | ✅ |
| `circulating_capital` | float | 流通股本 (万股) | ✅ |
| `total_market_cap` | float | 总市值 (万元) | ✅ |
| `circulating_market_cap` | float | 流通市值 (万元) | ✅ |

#### 数据源映射

| 数据 | 首选源 | 备选源 | AkShare 接口 |
|------|--------|--------|-------------|
| 公司基本信息 | AkShare (东方财富) | Tushare | `stock_individual_info_em` |
| 行业分类 (申万) | AkShare | BaoStock | `stock_board_industry_cons_em` |
| 行业分类 (证监会) | AkShare | Tushare | `stock_info_sh_name_code` 等 |
| 股本信息 | AkShare | BaoStock | `stock_individual_info_em` |

#### API 端点

```
GET /research/company/{symbol}/profile
Response: CompanyProfile 对象
```

---

### M02 — 业务画像模块

**计算目标**：提供公司主营业务结构化数据

#### 所需数据变量

| 变量 | 类型 | 说明 |
|------|------|------|
| `main_business` | text | 主营业务描述 |
| `products` | list[ProductItem] | 主要产品列表 (名称, 营收占比, 毛利率) |
| `revenue_by_product` | list[RevenueBreakdown] | 按产品分类营收 |
| `revenue_by_region` | list[RevenueBreakdown] | 按地区分类营收 |
| `top_customers_concentration` | float | 前五大客户集中度 (%) |
| `top_suppliers_concentration` | float | 前五大供应商集中度 (%) |
| `raw_materials` | list[str] | 主要原材料 |
| `cost_structure` | dict | 成本构成 (原材料/人工/制造费用占比) |

#### 数据源映射

| 数据 | AkShare 接口 | 说明 |
|------|-------------|------|
| 主营构成 (按产品) | `stock_zygc_ym` | 主营构成-按产品 |
| 主营构成 (按地区) | `stock_zygc_ym` | 主营构成-按地区 |
| 股票个股信息 | `stock_individual_info_em` | 含主营业务描述 |

> [!NOTE]
> 客户/供应商集中度、原材料构成等深度数据在公开免费接口中较难获取，通常需要从年报 PDF 中提取。初期可标记为 `null`，后续通过 AI 年报解析补充。

#### API 端点

```
GET /research/company/{symbol}/business
Response: BusinessProfile 对象
```

---

### M03 — 行业定位模块

**计算目标**：确定公司在行业中的位置、提供行业概况

#### 所需计算

| 计算项 | 算法 | 所需变量 |
|--------|------|----------|
| 行业内市值排名 | 排序 | 同行业全部公司市值 |
| 行业内营收排名 | 排序 | 同行业全部公司年营收 |
| 行业内净利排名 | 排序 | 同行业全部公司年净利 |
| 行业集中度 (CR5/CR10) | 前N家营收占行业总营收 | 行业内全公司营收 |
| 行业 HHI 指数 | Σ(市场份额²) | 行业内全公司营收 |
| 行业平均 PE/PB | 加权平均 | 行业内全公司 PE/PB |

#### 所需数据

| 数据 | 源 | AkShare 接口 |
|------|-----|-------------|
| 行业成分股列表 | AkShare | `stock_board_industry_cons_em` |
| 行业板块行情 | AkShare | `stock_board_industry_hist_em` |
| 同行业公司财务数据 | 批量查询 M04 | 同 M04 |
| 同行业公司市值 | AkShare | `stock_zh_a_spot_em` (实时行情含市值) |

#### API 端点

```
GET /research/company/{symbol}/industry-position
GET /research/industry/{industry_code}/overview
GET /research/industry/{industry_code}/ranking?metric=revenue&year=2025
```

---

### M04 — 财务指标模块 ⭐ (核心)

**计算目标**：提供结构化财务报表数据和衍生财务指标

#### 原始财务报表数据 (需采集存储)

| 报表 | 关键变量 | 说明 |
|------|----------|------|
| **利润表** | 营业收入、营业成本、毛利润、销售费用、管理费用、研发费用、财务费用、营业利润、利润总额、净利润、扣非净利润、EPS | 季报 + 年报 |
| **资产负债表** | 总资产、总负债、净资产、货币资金、应收账款、存货、固定资产、短期借款、长期借款、股东权益 | 季报 + 年报 |
| **现金流量表** | 经营活动现金流净额、投资活动现金流净额、筹资活动现金流净额、自由现金流 | 季报 + 年报 |

#### 衍生指标计算

| 指标 | 计算公式 | 所需变量 |
|------|----------|----------|
| **毛利率** | (营收 - 营业成本) / 营收 × 100 | revenue, cost |
| **净利率** | 净利润 / 营收 × 100 | net_income, revenue |
| **ROE** | 净利润 / 平均净资产 × 100 | net_income, equity(avg) |
| **ROA** | 净利润 / 平均总资产 × 100 | net_income, total_assets(avg) |
| **ROIC** | NOPAT / 投入资本 | 需要多个变量组合 |
| **资产负债率** | 总负债 / 总资产 × 100 | total_liabilities, total_assets |
| **流动比率** | 流动资产 / 流动负债 | current_assets, current_liabilities |
| **速动比率** | (流动资产 - 存货) / 流动负债 | current_assets, inventory, current_liabilities |
| **营收增长率** | (本期营收 - 上期营收) / 上期营收 × 100 | revenue(t), revenue(t-1) |
| **净利增长率** | (本期净利 - 上期净利) / 上期净利 × 100 | net_income(t), net_income(t-1) |
| **自由现金流** | 经营现金流 - 资本支出 | operating_cf, capex |
| **应收周转天数** | 365 × 平均应收 / 营收 | receivables(avg), revenue |
| **存货周转天数** | 365 × 平均存货 / 营业成本 | inventory(avg), cost |
| **研发费用率** | 研发费用 / 营收 × 100 | r_d_expense, revenue |
| **股息率** | 每股分红 / 股价 × 100 | dividend_per_share, price |
| **每股净资产 (BPS)** | 净资产 / 总股本 | equity, total_shares |
| **每股经营现金流** | 经营现金流 / 总股本 | operating_cf, total_shares |

#### 数据源映射

| 数据 | 首选源 | AkShare 接口 | 频率 |
|------|--------|-------------|------|
| 利润表 | AkShare | `stock_financial_report_sina(symbol, "利润表")` | 季度 |
| 资产负债表 | AkShare | `stock_financial_report_sina(symbol, "资产负债表")` | 季度 |
| 现金流量表 | AkShare | `stock_financial_report_sina(symbol, "现金流量表")` | 季度 |
| 主要财务指标 | AkShare | `stock_financial_abstract` | 季度 |
| 杜邦分析 | AkShare | `stock_financial_analysis_indicator` | 季度 |
| 分红信息 | AkShare | `stock_history_dividend_detail` | 年度 |

> [!IMPORTANT]
> 财务数据是整个系统的核心基础，M07/M10/M11 等模块都依赖它。建议**最优先**开发。

#### API 端点

```
GET /research/company/{symbol}/financials?report_type=annual&years=3
GET /research/company/{symbol}/financials/income?period=2024Q4
GET /research/company/{symbol}/financials/balance?period=2024Q4
GET /research/company/{symbol}/financials/cashflow?period=2024Q4
GET /research/company/{symbol}/financial-indicators?years=3
```

---

### M05 — 分析师预期模块

**计算目标**：汇总卖方分析师对公司的覆盖和一致预期

#### 所需数据变量

| 变量 | 类型 | 说明 |
|------|------|------|
| `covering_institutions` | int | 覆盖机构数量 |
| `covering_analysts` | int | 覆盖分析师数量 |
| `latest_ratings` | list[RatingItem] | 最新评级列表 (机构/分析师/评级/目标价/日期) |
| `consensus_rating` | str | 一致评级 (买入/增持/中性/减持/卖出) |
| `consensus_target_price` | float | 一致目标价 |
| `consensus_eps` | dict | 一致预期 EPS (当年/次年/后年) |
| `consensus_revenue` | dict | 一致预期营收 |
| `consensus_net_income` | dict | 一致预期净利润 |
| `rating_changes_30d` | list | 近30天评级变动 |

#### 数据源映射

| 数据 | AkShare 接口 | 说明 |
|------|-------------|------|
| 机构评级汇总 | `stock_comment_detail_zlkp_jgcyd` | 机构参与度 |
| 个股研报列表 | `stock_research_report_em` | 近期研报 |
| 盈利预测 | `stock_profit_forecast_em` | 一致预期数据 |
| 评级统计 | `stock_rank_forecast_cninfo` | 评级分布 |

#### API 端点

```
GET /research/company/{symbol}/analyst-coverage
GET /research/company/{symbol}/consensus-forecast
```

---

### M06 — 增长驱动模块 (元数据)

**计算目标**：提供增长相关结构化元数据，供 AI 分析

#### 所需数据

| 变量 | 来源 | 说明 |
|------|------|------|
| `r_d_investment_3y` | M04 财务模块 | 近3年研发投入及增长 |
| `capex_3y` | M04 财务模块 | 近3年资本支出 |
| `new_products` | 公告数据 | 新产品/新业务公告 |
| `patents_count` | AkShare | 专利数量 (如可获取) |
| `industry_growth_rate` | M03 行业模块 | 所在行业增速 |
| `revenue_growth_trend` | M04 计算 | 营收增长趋势 |
| `margin_trend` | M04 计算 | 利润率变化趋势 |

> [!NOTE]
> 增长驱动的**定性判断**交给下游 AI 完成。本模块只提供**定量数据和结构化元数据**。

#### API 端点

```
GET /research/company/{symbol}/growth-drivers
```

---

### M07 — 竞争对标模块

**计算目标**：提供同行业公司的对比数据矩阵

#### 所需计算

| 计算项 | 说明 |
|--------|------|
| **Peer Group 识别** | 基于申万行业分类，取同三级行业公司 |
| **关键指标对标矩阵** | 市值/营收/净利/毛利率/ROE/PE/PB 等 N×M 矩阵 |
| **百分位排名** | 目标公司各指标在 peer group 中的分位数 |
| **行业中位数** | Peer group 各指标中位数 |

#### 所需数据

- M03 行业成分股列表
- M04 各公司财务指标 (批量)
- 现有 Quote 系统行情数据 (市值/PE/PB)

#### API 端点

```
GET /research/company/{symbol}/peer-comparison
GET /research/company/{symbol}/peer-comparison?metrics=revenue,roe,pe&year=2025
```

---

### M08 — 供需指标模块

**计算目标**：从财务数据推导供需状态

#### 所需计算

| 指标 | 计算方式 | 所需变量 |
|------|----------|----------|
| **存货周转率** | 营业成本 / 平均存货 | cost, inventory |
| **存货同比变动** | (存货_t - 存货_t-1) / 存货_t-1 | inventory(t), inventory(t-1) |
| **应收账款增速** | 同比增长率 | receivables(t), receivables(t-1) |
| **产能利用率 (代理)** | 固定资产周转率 = 营收/固定资产 | revenue, fixed_assets |
| **在建工程/固定资产比** | 在建工程 / 固定资产 | construction_in_progress, fixed_assets |
| **预收/营收比** | 合同负债(预收) / 营收 | contract_liabilities, revenue |

> [!NOTE]
> 真实的行业供需数据（如产能/产量/开工率）多来自行业协会付费报告，免费数据源无法覆盖。本模块用**财务报表代理指标**间接推导供需状态。

#### API 端点

```
GET /research/company/{symbol}/supply-demand-indicators
```

---

### M09 — 研报数据模块

**计算目标**：采集和结构化研报元数据

#### 所需数据变量

| 变量 | 类型 | 说明 |
|------|------|------|
| `report_title` | str | 研报标题 |
| `institution` | str | 发布机构 |
| `analyst` | str | 分析师 |
| `publish_date` | date | 发布日期 |
| `rating` | str | 评级 |
| `target_price` | float | 目标价 |
| `report_type` | str | 类型 (深度/点评/行业) |
| `summary` | text | 摘要 (如有) |

#### 数据源映射

| 数据 | AkShare 接口 |
|------|-------------|
| 个股研报列表 | `stock_research_report_em` |
| 行业研报列表 | `stock_board_industry_summary_em` |

#### API 端点

```
GET /research/company/{symbol}/research-reports?limit=20&days=90
GET /research/industry/{industry_code}/research-reports?limit=20
```

---

### M10 — 估值引擎 ⭐ (核心)

**计算目标**：提供绝对估值和相对估值的计算结果

#### A. 相对估值

| 估值指标 | 计算公式 | 所需变量 |
|----------|----------|----------|
| **PE (TTM)** | 总市值 / 近4季净利润 | market_cap, net_income(ttm) |
| **PE (Forward)** | 总市值 / 一致预期净利 | market_cap, consensus_net_income |
| **PB** | 总市值 / 净资产 | market_cap, equity |
| **PS (TTM)** | 总市值 / 近4季营收 | market_cap, revenue(ttm) |
| **EV/EBITDA** | (市值+净债务) / EBITDA | market_cap, debt, cash, ebitda |
| **PEG** | PE / 净利增速 | pe, net_income_growth |
| **历史分位数** | 当前值在N年历史中的百分位 | pe/pb/ps 历史序列 |
| **行业对比** | 当前值 vs 行业中位数/平均数 | M07 竞争对标数据 |

#### 所需历史序列数据

计算历史分位需要：**每日的 PE/PB/PS 等估值序列**

```
PE(t) = 总市值(t) / 净利润(TTM at t)
```

- `总市值(t)` = 收盘价(t) × 总股本(t) → 来自现有 Quote 系统
- `净利润(TTM at t)` = 截至 t 日的最近4个季度净利润之和 → 需要季度利润表

> [!IMPORTANT]
> 估值历史序列依赖**行情 × 财报**的交叉计算。建议在财务数据入库后，由离线任务批量计算并缓存每日估值指标。

#### B. 绝对估值 (DCF)

| 变量/假设 | 说明 | 数据来源 |
|-----------|------|----------|
| **FCF (自由现金流)** | 经营现金流 - 资本支出 | M04 现金流量表 |
| **g (增长率)** | 分阶段: 高增长期 + 稳定期 + 永续期 | 一致预期(M05) / 历史增长率(M04) |
| **WACC (加权平均资本成本)** | Ke × E/(D+E) + Kd×(1-T) × D/(D+E) | 见下表 |
| **TV (终值)** | FCF_n × (1+g_perp) / (WACC - g_perp) | WACC, g_perp |
| **预测期** | 通常 5-10 年 | 假设参数 |

#### WACC 子计算

| 子变量 | 计算 | 数据来源 |
|--------|------|----------|
| **Rf (无风险利率)** | 10年期国债收益率 | AkShare: `bond_zh_us_rate(start_date)` |
| **β (Beta)** | 个股收益率与市场收益率的回归系数 | 现有行情数据自行计算 (250日) |
| **ERP (股权风险溢价)** | 市场历史平均超额收益 | 固定假设 6-8% 或从数据计算 |
| **Ke (股权成本)** | Rf + β × ERP | 计算 |
| **Kd (债务成本)** | 利息支出 / 有息负债 | M04 财务数据 |
| **T (税率)** | 所得税 / 税前利润 | M04 财务数据 |
| **D/E (资本结构)** | 有息负债 / (有息负债+市值) | M04 + 行情 |

#### 数据源映射 (估值专用)

| 数据 | 源 | 接口 |
|------|-----|------|
| 国债收益率 | AkShare | `bond_zh_us_rate` |
| 实时市值 | AkShare / 现有 Quote | `stock_zh_a_spot_em` |

#### Beta 计算

```python
# 算法: 250 日滚动回归
beta = Cov(R_stock, R_market) / Var(R_market)
# R = ln(P_t / P_t-1)  日对数收益率
# market = 沪深300 / 恒生指数 / S&P500 (按市场选择)
```

- 所需数据: 个股日收益率序列 + 基准指数日收益率序列 → **现有 Quote 系统已有**

#### API 端点

```
GET /research/company/{symbol}/valuation/relative
    → PE/PB/PS/EV-EBITDA + 历史分位 + 行业对比

GET /research/company/{symbol}/valuation/dcf
    → DCF 模型结果 (含 WACC / FCF 预测 / 终值)
    → 支持参数覆盖: ?growth_rate=0.15&discount_rate=0.10&terminal_growth=0.03

GET /research/company/{symbol}/valuation/history
    → PE/PB/PS 历史时间序列 (用于 Band 图)
    → ?metric=pe&period=5y

GET /research/company/{symbol}/beta?benchmark=000300&window=250
```

---

### M11 — 风险指标模块

**计算目标**：量化各类风险指标

#### 财务风险指标

| 指标 | 算法 | 所需变量 |
|------|------|----------|
| **Altman Z-Score** | 1.2×X1 + 1.4×X2 + 3.3×X3 + 0.6×X4 + 1.0×X5 | 营运资金/总资产, 留存收益/总资产, EBIT/总资产, 市值/总负债, 营收/总资产 |
| **Piotroski F-Score** | 9项二元评分 | ROA, CFO, ΔROA, 应计, Δ杠杆, Δ流动性, Δ股权, Δ毛利率, Δ资产周转 |

#### 市场风险指标

| 指标 | 算法 | 所需变量 |
|------|------|----------|
| **波动率** | 年化标准差 (20/60/250日) | 日收益率序列 |
| **最大回撤** | max(1 - P_t/P_max) | 日收盘价序列 |
| **VaR (95%)** | 收益率的第5百分位 | 日收益率序列 |
| **Beta** | 见 M10 | 同 M10 |
| **夏普比率** | (R_avg - Rf) / σ | 收益率, 无风险利率, 波动率 |

#### 事件风险信号

| 信号 | 数据源 | AkShare 接口 |
|------|--------|-------------|
| 大股东减持 | AkShare | `stock_inner_trade_xq` |
| 股权质押比例 | AkShare | `stock_gpzy_profile_em` |
| 高管变动 | AkShare | `stock_ggcg_em` |
| 诉讼/处罚 | 需要公告解析 | - |

#### API 端点

```
GET /research/company/{symbol}/risk-indicators
GET /research/company/{symbol}/risk/financial-score  → Z-Score + F-Score
GET /research/company/{symbol}/risk/market  → 波动率/VaR/最大回撤/夏普
GET /research/company/{symbol}/risk/events  → 减持/质押/高管变动
```

---

### M12 — 舆情数据模块

**计算目标**：提供市场行为和舆情相关数据

#### 所需数据

| 数据类别 | 具体数据 | AkShare 接口 |
|----------|----------|-------------|
| **龙虎榜** | 上榜原因/买卖席位/金额 | `stock_lhb_detail_em` |
| **大宗交易** | 成交价/折溢价率/买卖方 | `stock_dzjy_mrmx` |
| **融资融券** | 融资余额/融券余额/变动 | `stock_margin_detail_szse` 等 |
| **北向资金** | 个股北向资金持股 | `stock_hsgt_individual_em` |
| **资金流向** | 主力/超大/大/中/小单 | `stock_individual_fund_flow` |
| **股东持仓** | 十大流通股东变动 | `stock_gdfx_free_holding_analyse_em` |
| **限售解禁** | 解禁日期/解禁股数/类型 | `stock_restricted_release_summary_em` |

#### API 端点

```
GET /research/company/{symbol}/sentiment/fund-flow?days=30
GET /research/company/{symbol}/sentiment/lhb?days=90
GET /research/company/{symbol}/sentiment/block-trade?days=30
GET /research/company/{symbol}/sentiment/margin?days=30
GET /research/company/{symbol}/sentiment/northbound?days=30
GET /research/company/{symbol}/sentiment/shareholders
GET /research/company/{symbol}/sentiment/unlock-schedule
```

---

### M13 — 技术指标引擎 ⭐ (核心)

**计算目标**：对行情数据计算全套技术指标，支持多周期

#### 支持的时间周期

| 周期 | 数据源 | 说明 |
|------|--------|------|
| 日线 | 现有 Quote 系统 `daily_quotes` | 已有 |
| 周线 | 日线聚合计算 | 需新增聚合逻辑 |
| 月线 | 日线聚合计算 | 需新增聚合逻辑 |

> [!NOTE]
> 现有 Quote 系统已有独立的 `weekly_quotes` 和 `monthly_quotes` 表，可直接复用。

#### 趋势类指标

| 指标 | 算法 | 参数 | 所需数据 |
|------|------|------|----------|
| **SMA** | 简单移动平均 | period (5/10/20/60/120/250) | close |
| **EMA** | 指数移动平均 | period | close |
| **WMA** | 加权移动平均 | period | close |
| **MACD** | EMA(12) - EMA(26), Signal=EMA(9,DIF) | fast=12, slow=26, signal=9 | close |
| **DMA** | MA(短) - MA(长) | short=10, long=50 | close |
| **TRIX** | EMA(EMA(EMA(close,n))),n)的变化率 | period=12 | close |
| **布林带** | MA ± k×σ | period=20, k=2 | close |

#### 动量类指标

| 指标 | 算法 | 参数 | 所需数据 |
|------|------|------|----------|
| **RSI** | 100 - 100/(1+RS), RS=AvgGain/AvgLoss | period=14 | close |
| **KDJ** | K=SMA(RSV,9,3), D=SMA(K,3,3), J=3K-2D | k=9, d=3, j=3 | high, low, close |
| **CCI** | (TP - MA(TP)) / (0.015 × MD) | period=14 | high, low, close |
| **Williams %R** | (HH - C) / (HH - LL) × (-100) | period=14 | high, low, close |
| **ROC** | (C - C_n) / C_n × 100 | period=12 | close |
| **BIAS** | (C - MA) / MA × 100 | period=6/12/24 | close |

#### 波动类指标

| 指标 | 算法 | 参数 | 所需数据 |
|------|------|------|----------|
| **ATR** | EMA(TR, n), TR=max(H-L, \|H-C_prev\|, \|L-C_prev\|) | period=14 | high, low, close |
| **历史波动率** | 年化标准差 = std(日收益率) × √250 | window=20/60 | close |
| **布林带宽度** | (Upper - Lower) / Middle | period=20 | close |

#### 成交量类指标

| 指标 | 算法 | 参数 | 所需数据 |
|------|------|------|----------|
| **OBV** | 累积: if C>C_prev: +V else: -V | - | close, volume |
| **VWAP** | Σ(价格×成交量) / Σ(成交量) | period | close, volume |
| **量比** | 当日成交量 / 过去N日日均量 | days=5 | volume |
| **成交量MA** | 成交量的移动平均 | period=5/10/20 | volume |

#### 综合评分计算

```python
# 多指标综合评分示例
def calculate_technical_score(indicators: dict) -> dict:
    """
    返回:
    - trend_score: 趋势得分 (-100 ~ +100)
    - momentum_score: 动量得分 (-100 ~ +100)
    - volatility_score: 波动得分 (0 ~ 100)
    - volume_score: 量能得分 (-100 ~ +100)
    - composite_score: 综合得分 (-100 ~ +100)
    - signal: BUY / SELL / HOLD
    """
```

#### API 端点

```
GET /research/company/{symbol}/technical/indicators
    ?period=daily|weekly|monthly
    &indicators=macd,rsi,kdj,bollinger    # 可选指定
    &start_date=2025-01-01
    &end_date=2025-12-31

GET /research/company/{symbol}/technical/summary
    ?period=daily
    → 返回最新一期所有指标 + 综合评分 + 信号

GET /research/company/{symbol}/technical/score
    → 多周期综合技术评分
```

---

### M14 — 形态识别引擎

**计算目标**：识别 K 线形态和趋势形态

#### K 线形态 (单根/组合)

| 形态 | 识别规则 | 信号 |
|------|----------|------|
| 锤子线 | 下影线≥实体2倍, 上影线极短 | 看涨 |
| 倒锤子 | 上影线≥实体2倍, 下影线极短 | 看涨(需确认) |
| 十字星 | 开盘≈收盘, 有上下影线 | 变盘信号 |
| 吞没形态 | 大阳/大阴包含前一根 | 反转 |
| 启明星/黄昏星 | 三根K线组合 | 反转 |
| 三白兵/三乌鸦 | 三连阳/三连阴 | 持续 |

#### 趋势形态

| 形态 | 识别算法 | 信号 |
|------|----------|------|
| 头肩顶/底 | 峰值检测 + 颈线拟合 | 反转 |
| 双顶/双底 | 相近高点/低点检测 | 反转 |
| 三角收敛 | 高低点趋势线拟合 | 突破 |
| 箱体震荡 | 支撑/阻力区间检测 | 区间 |
| 上升/下降通道 | 平行趋势线拟合 | 持续 |

#### 支撑阻力位计算

| 方法 | 算法 |
|------|------|
| 枢轴点 (Pivot Point) | PP=(H+L+C)/3, S1=2PP-H, R1=2PP-L |
| 历史高低点 | 局部极值检测 (zigzag) |
| 成交密集区 | 成交量加权价格分布 (Volume Profile) |
| 均线支撑 | MA20/MA60/MA120/MA250 |

#### API 端点

```
GET /research/company/{symbol}/technical/patterns
    ?period=daily
    &lookback=60     # 回看天数
    → 返回识别到的形态列表 + 信号 + 可靠度

GET /research/company/{symbol}/technical/support-resistance
    ?period=daily
    → 返回支撑位/阻力位列表
```

---

## 4. 数据库设计 (新增表)

> 以下为在现有 Quote 系统基础上需要**新增**的数据表。

### 4.1 新增表概览

| 表名 | 模块 | 说明 | 更新频率 |
|------|------|------|----------|
| `company_profiles` | M01 | 公司档案 | 季度/事件触发 |
| `financial_statements` | M04 | 原始财务报表 (利润表/资产负债表/现金流) | 季度 |
| `financial_indicators` | M04 | 衍生财务指标 | 季度 (计算) |
| `analyst_forecasts` | M05 | 分析师预期与评级 | 日/周 |
| `research_reports` | M09 | 研报元数据 | 日 |
| `valuation_history` | M10 | 每日估值指标序列 (PE/PB/PS) | 日 (离线计算) |
| `sentiment_data` | M12 | 舆情数据 (资金流向/龙虎榜等) | 日 |
| `technical_indicators_cache` | M13 | 技术指标缓存 (可选) | 日 |

### 4.2 核心表结构 (示意)

```sql
-- 财务报表 (通用结构, 用 report_type 区分三表)
CREATE TABLE financial_statements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id VARCHAR(32) NOT NULL,
    report_type VARCHAR(16) NOT NULL,      -- income / balance / cashflow
    report_period VARCHAR(8) NOT NULL,     -- 2024Q4, 2024Q3 ...
    report_date DATE NOT NULL,             -- 报告期末日期
    publish_date DATE,                     -- 实际发布日期
    data JSON NOT NULL,                    -- 结构化报表数据 (JSON)
    source VARCHAR(32),
    created_at DATETIME,
    updated_at DATETIME,
    UNIQUE(instrument_id, report_type, report_period)
);

-- 估值历史 (每日计算一次)
CREATE TABLE valuation_history (
    date DATE NOT NULL,
    instrument_id VARCHAR(32) NOT NULL,
    pe_ttm FLOAT,
    pb FLOAT,
    ps_ttm FLOAT,
    ev_ebitda FLOAT,
    market_cap FLOAT,
    PRIMARY KEY(date, instrument_id)
);
```

---

## 5. 数据源可行性评估

### 5.1 可行性矩阵

| 模块 | 数据可获取性 | 免费源覆盖度 | 开发复杂度 | 优先级 |
|------|-------------|-------------|------------|--------|
| M01 公司档案 | ✅ 高 | ✅ 完全 | ⭐ 低 | P0 |
| M02 业务画像 | 🟡 中 | 🟡 部分 (深度数据需年报) | ⭐⭐ 中 | P1 |
| M03 行业定位 | ✅ 高 | ✅ 完全 | ⭐⭐ 中 | P0 |
| M04 财务指标 | ✅ 高 | ✅ 完全 | ⭐⭐ 中 | P0 |
| M05 分析师预期 | ✅ 高 | ✅ 完全 | ⭐ 低 | P0 |
| M06 增长驱动 | 🟡 中 | 🟡 部分 | ⭐ 低 | P2 |
| M07 竞争对标 | ✅ 高 | ✅ 完全 | ⭐⭐ 中 | P1 |
| M08 供需指标 | 🟡 中 | 🟡 代理指标 | ⭐⭐ 中 | P2 |
| M09 研报数据 | ✅ 高 | ✅ 完全 | ⭐ 低 | P1 |
| M10 估值引擎 | ✅ 高 | ✅ 完全 | ⭐⭐⭐ 高 | P0 |
| M11 风险指标 | ✅ 高 | ✅ 完全 | ⭐⭐ 中 | P1 |
| M12 舆情数据 | ✅ 高 | ✅ 完全 | ⭐⭐ 中 | P1 |
| M13 技术指标 | ✅ 高 | ✅ 完全 (复用行情) | ⭐⭐⭐ 高 | P0 |
| M14 形态识别 | ✅ 高 | ✅ 完全 (复用行情) | ⭐⭐⭐ 高 | P2 |

### 5.2 建议分期

| 阶段 | 模块 | 交付能力 | 预估工期 |
|------|------|----------|---------|
| **Phase 1** | M01 + M04 + M13 | 公司基本面快照 + 财务数据 + 技术指标 | 2-3 周 |
| **Phase 2** | M03 + M05 + M10 | 行业定位 + 分析师预期 + 估值引擎 | 2-3 周 |
| **Phase 3** | M07 + M09 + M11 + M12 | 竞争对标 + 研报 + 风险 + 舆情 | 2-3 周 |
| **Phase 4** | M02 + M06 + M08 + M14 | 深度业务画像 + 形态识别 | 2-3 周 |

---

## 6. 开放问题 (需要确认)

> [!IMPORTANT]
> 以下问题会影响具体实现方案，请 review 后反馈。

### Q1: 市场范围
当前系统支持 A 股 (SSE/SZSE/BSE) + 港股 (HKEX)。投研引擎是否也覆盖港股？还是先聚焦 A 股？

### Q2: 数据更新频率
- 财务数据：每季报发布后更新一次（约每季度1次）？
- 估值序列：每日收盘后计算？
- 技术指标：实时计算 (API 请求时) 还是预计算缓存？

### Q3: 计算策略
技术指标（M13）和估值历史（M10）涉及大量计算：
- **方案 A**：API 请求时实时计算（延迟高但无需存储）
- **方案 B**：每日离线预计算 + 缓存（延迟低但需要定时任务和存储空间）
- **方案 C**：混合模式（高频指标预计算，低频指标实时算）

### Q4: DCF 模型的交互度
DCF 模型涉及多个假设参数（增长率、折现率等），是否需要：
- 提供默认参数 + 允许用户覆盖？
- 提供多组参数的敏感性分析结果？

### Q5: 独立部署还是集成
本模块是作为 Quote 系统的子模块集成（共用数据库和 API 服务），还是独立部署（独立数据库 + 独立 API 端口）？

### Q6: Tushare Pro Token
部分财务数据 Tushare Pro 质量更好，是否已有 Tushare Pro token？或者完全依赖 AkShare？
