# 外汇汇率数据获取与维护系统需求说明书

> 更新日期：2026-06-27
> 适用项目：Quote System / Research Data Engine
> 文档定位：本文用于定义独立外汇数据模块的业务范围、数据源优先级、`fx.db` 存储架构、主数据治理、日更回补、API 服务、DCF/商品数据依赖和后续 OpenSpec 开发边界。
> 使用边界：本模块提供研究、估值和风险分析所需的外汇参考数据，不提供外汇交易信号，不连接真实交易账户。

---

## 1. 结论摘要

外汇数据不应放在商品期货或特殊商品数据层中。更合理的架构是新增独立 `data/fx.db`，将外汇作为和股票、期货、商品、财务、估值并列的数据域维护。

理由：

1. 外汇有自己的主数据体系：货币、货币对、在岸/离岸、即期/中间价/收盘价、交叉汇率、指数、远期点、掉期点、利率平价等。
2. 外汇被多个模块复用：商品美元价格换算、港股/海外资产估值、跨币种财务报表、宏观分析、风险敞口、组合汇率风险。
3. 外汇数据频率和治理方式不同于期货交易日历：核心是央行/交易中心/市场数据源的观测日、发布时间、时区、节假日和修订。
4. 长远看会扩展到更多国家和货币，独立数据库更利于容量、权限、备份、API 和调度管理。

第一阶段只实现人民币相关六组核心汇率：

| 需求名称 | 推荐规范化主序列 | 说明 |
|---|---|---|
| 在岸人民币/美元 | `USD/CNY` | 市场惯例为 1 USD = x CNY；可派生 `CNY/USD` |
| 离岸人民币/美元 | `USD/CNH` | 市场惯例为 1 USD = x CNH；可派生 `CNH/USD` |
| 在岸人民币/欧元 | `EUR/CNY` | 1 EUR = x CNY；可派生 `CNY/EUR` |
| 离岸人民币/欧元 | `EUR/CNH` | 1 EUR = x CNH；可由 `EUR/USD * USD/CNH` 派生，若无直接官方源 |
| 在岸人民币/日元 | `JPY/CNY` 或 `100JPY/CNY` | 官方中间价常以 100 日元兑人民币发布，需保留报价单位 |
| 离岸人民币/日元 | `JPY/CNH` 或 `100JPY/CNH` | 可由 `JPY/USD` 与 `USD/CNH` 派生，需记录派生规则 |

原则：

- 数据库存储应同时支持市场惯例报价和反向派生报价。
- 对人民币交叉汇率，优先使用官方直接报价；没有直接官方报价时，允许从已治理的基础货币对派生，但必须记录派生路径、日期、来源和误差风险。
- 外汇数据模块不替代商品数据层，只为商品、DCF、组合和宏观模块提供本地可审计汇率服务。

---

## 2. 美元指数是否放入 fx.db

客观观点：**美元指数适合放入 `fx.db`，但不应放在汇率表里，而应作为 `currency_index` 类型的外汇指标维护。**

原因：

- 美元指数不是两种货币之间的汇率，而是美元相对一篮子货币的指数。
- 它对商品价格、资源股、黄金、有色、原油、海外收入折算和宏观风险偏好都有解释力。
- 与外汇模块的主数据、来源、频率、时区、日更和 API 更接近。

建议：

- `FXI.DXY.ICE`：ICE U.S. Dollar Index，官方指数，优先级最高，但历史下载和授权可能受限。
- `FXI.DOLLAR_TRADE_WEIGHTED.FRED`：FRED/美联储美元贸易加权指数，可作为免费官方替代或补充。
- 数据表上使用 `instrument_type=currency_index`，不要混入 `currency_pair`。
- DXY 若来自 Yahoo、Stooq、Sina 等免费聚合源，只能标记为 fallback，不应标记 official。

---

## 3. 总体架构

### 3.1 数据域边界

新增独立外汇数据域：

```text
fx.db
  -> fx_currencies
  -> fx_instruments
  -> fx_series
  -> fx_observations
  -> fx_derivations
  -> fx_calendars
  -> fx_source_manifests
  -> fx_quality_issues
```

与其他模块关系：

| 模块 | 使用 FX 的方式 |
|---|---|
| 商品数据 | 读取 `fx.db` 中本地汇率，将 USD 商品转换为 CNY 或目标币种；商品层不维护 FX |
| DCF | 按公司报表币种、估值币种和商品输入币种读取本地 FX 序列 |
| 港股/海外资产 | 读取 HKD/CNY、USD/CNY、USD/HKD 等序列做估值换算 |
| 组合分析 | 读取 FX 序列计算币种敞口、换算收益和对冲影响 |
| 宏观分析 | 读取美元指数、有效汇率、央行中间价和离岸价差 |

### 3.2 标准流程

外汇数据层沿用项目已有治理风格：

```text
source venue -> currency/instrument master -> calendar/publication governance
             -> observations sync/backfill -> derivation/inversion/cross-rate
             -> quality/readiness -> API/DCF/commodity consumers
```

核心差异：

- 外汇没有交易所合约生命周期，但有货币、货币对、报价口径、时区、发布时间和来源生命周期。
- 中间价、即期价、收盘价、离岸价、指数、远期点都必须用不同 `rate_type` 或 `instrument_type` 区分。
- 不同来源同一货币对不能静默混写；必须保留 source profile。

---

## 4. 第一阶段范围

### 4.1 货币范围

第一阶段货币：

| 代码 | 名称 | 说明 |
|---|---|---|
| `CNY` | 在岸人民币 | 中国大陆在岸市场人民币 |
| `CNH` | 离岸人民币 | 离岸人民币，优先关注香港离岸市场 |
| `USD` | 美元 | 商品和国际资产主要计价货币 |
| `EUR` | 欧元 | 欧洲销售/采购、汇率风险和交叉汇率 |
| `JPY` | 日元 | 产业链、海外业务、宏观参考 |

### 4.2 第一阶段序列

| series_id | rate_type | 频率 | 主源策略 |
|---|---|---|---|
| `FX.USD_CNY.CFETS.MID.DAILY` | 中间价 | 日频 | 中国外汇交易中心/中国货币网 |
| `FX.EUR_CNY.CFETS.MID.DAILY` | 中间价 | 日频 | 中国外汇交易中心/中国货币网 |
| `FX.JPY_CNY.CFETS.MID.DAILY` | 中间价 | 日频 | 中国外汇交易中心/中国货币网，注意 100 JPY 报价单位 |
| `FX.USD_CNH.MARKET.SPOT.DAILY` | 离岸即期 | 日频 | 当前采用 AkShare/东方财富历史行情聚合源，标记 `aggregated_public` |
| `FX.EUR_CNH.MARKET.SPOT.DAILY` | 离岸即期 | 日频 | 当前采用 AkShare/东方财富历史行情聚合源，标记 `aggregated_public` |
| `FX.JPY_CNH.MARKET.SPOT.DAILY` | 离岸即期 | 日频 | 当前采用 AkShare/东方财富历史行情聚合源，源字段已按 100 JPY 报价 |
| `FX.EUR_CNH.DERIVED.DAILY` | 派生交叉 | 日频 | `EUR/USD * USD/CNH` 或同等治理路径 |
| `FX.JPY_CNH.DERIVED.DAILY` | 派生交叉 | 日频 | `JPY/USD * USD/CNH` 或同等治理路径，注意 100 JPY 单位 |
| `FXI.DXY.ICE.DAILY` | 美元指数 | 日频 | ICE 官方优先，若无法免费自动化则先 disabled |
| `FXI.USD_TRADE_WEIGHTED.FRED.DAILY` | 美元指数替代 | 日频 | FRED/美联储贸易加权美元指数 |

### 4.3 报价方向

外汇必须明确 `base_currency` 和 `quote_currency`：

```text
USD/CNY = 1 USD 兑换多少 CNY
EUR/CNY = 1 EUR 兑换多少 CNY
100JPY/CNY = 100 JPY 兑换多少 CNY
USD/CNH = 1 USD 兑换多少 CNH
EUR/CNH = 1 EUR 兑换多少 CNH
100JPY/CNH = 100 JPY 兑换多少 CNH
```

要求：

- 入库记录必须包含 `base_currency`、`quote_currency`、`quote_multiplier`。
- 反向汇率必须作为派生序列或 API 计算结果提供，不要混淆原始报价。
- JPY 对人民币的官方报价常以 100 JPY 为单位，必须用 `quote_multiplier=100` 或 `base_amount=100` 记录。

---

## 5. 数据源优先级

### 5.1 官方源优先

| 数据 | 主源 | 说明 |
|---|---|---|
| CNY 中间价 | 中国外汇交易中心/中国货币网、PBOC 公告链路 | 官方人民币汇率中间价，应作为在岸 CNY 主源 |
| EUR 参考汇率 | ECB Euro foreign exchange reference rates | 可用于 EUR/USD、EUR/CNY 等交叉校验，适合欧洲官方口径 |
| USD 汇率 | Federal Reserve / FRED H.10 | 免费官方或准官方源，可用于 USD 相关交叉校验 |
| DXY | ICE | 官方指数，但免费自动历史访问和使用许可需验证 |
| 美元贸易加权指数 | Federal Reserve / FRED | 免费官方替代指标，不等同于 ICE DXY |

### 5.2 备源与聚合源

| 来源 | 用途 | 质量标记 |
|---|---|---|
| AkShare | 人民币中间价、金十、东方财富等包装接口 | `aggregated_public` |
| Yahoo Finance | CNH 即期、DXY ETF/指数参考 | `aggregated_public` |
| Stooq | DXY 或部分外汇历史 | `aggregated_public` |
| Investing/Sina/Eastmoney | CNH 或交叉汇率备源 | `aggregated_public` |
| 手工导入 | 特殊历史修复、官方下载文件导入 | `manual_verified` 或 `manual_unverified` |

原则：

- 官方源能取到的，不用聚合源做主源。
- 离岸 CNH 缺少单一官方公开中间价，可能需要使用市场聚合源或交易平台数据；必须明确标记，不能伪装成 official。
- 交叉汇率能从官方基础汇率派生时，优先派生并保存 lineage。

---

## 6. 数据模型

### 6.1 `fx_currencies`

| 字段 | 说明 |
|---|---|
| `currency_code` | ISO 或扩展代码，例如 `USD`、`CNY`、`CNH` |
| `name` | 名称 |
| `country_or_region` | 国家或地区 |
| `currency_type` | `fiat`、`offshore_fiat`、`basket`、`index_component` |
| `central_bank` | 中央银行或管理机构 |
| `active` | 是否启用 |
| `metadata_json` | 特殊说明 |

### 6.2 `fx_instruments`

| 字段 | 说明 |
|---|---|
| `instrument_id` | 例如 `FX.USD_CNY`、`FXI.DXY` |
| `instrument_type` | `currency_pair`、`currency_index`、`effective_exchange_rate`、`forward_point`、`swap_point` |
| `base_currency` | 基准货币 |
| `quote_currency` | 报价货币 |
| `quote_multiplier` | 报价基准数量，JPY 可为 100 |
| `market_scope` | `onshore`、`offshore`、`global`、`derived` |
| `category` | `major`、`rmb`、`dollar_index`、`cross_rate` |
| `active` | 是否启用 |

### 6.3 `fx_series`

| 字段 | 说明 |
|---|---|
| `series_id` | 例如 `FX.USD_CNY.CFETS.MID.DAILY` |
| `instrument_id` | 关联 instrument |
| `source_profile` | 来源口径 |
| `rate_type` | `mid`、`spot`、`close`、`fixing`、`reference`、`derived_cross`、`index_close` |
| `frequency` | `daily`、`weekly`、`monthly` |
| `timezone` | 来源时区 |
| `publication_lag` | 发布滞后 |
| `start_date` / `end_date` | 生命周期 |
| `quality_policy` | 质量规则 |

### 6.4 `fx_observations`

| 字段 | 说明 |
|---|---|
| `series_id` | 序列 ID |
| `observation_date` | 观测日期 |
| `value` | 汇率或指数值 |
| `base_currency` / `quote_currency` | 货币对 |
| `quote_multiplier` | 报价基准数量 |
| `source_profile` | 来源 |
| `source_url` | 来源 URL 或 API endpoint |
| `quality_flag` | `official`、`derived`、`aggregated_public`、`manual_verified`、`partial` |
| `revision_id` | 修订版本 |
| `metadata_json` | 原始字段、发布时间、修订、采集方式 |

唯一键：

```text
(series_id, observation_date, source_profile)
```

### 6.5 `fx_derivations`

用于记录反向汇率和交叉汇率：

| 字段 | 说明 |
|---|---|
| `derived_series_id` | 派生序列 |
| `source_series_ids` | 来源序列列表 |
| `formula` | 公式，例如 `EUR_CNH = EUR_USD * USD_CNH` |
| `date_policy` | 日期匹配规则 |
| `max_source_lag_days` | 最大允许滞后 |
| `quality_policy` | 派生质量规则 |

---

## 7. 日历与发布治理

外汇数据使用发布日/观测日治理，不使用商品期货交易日历：

- 官方中间价按发布机构工作日发布，缺失应区分节假日、未发布、源异常。
- 离岸市场可能在部分中国假日仍有报价，但流动性和来源质量不同，需按来源日历治理。
- 派生交叉汇率要求同日数据；如果同日数据缺失，可按配置使用最近可得日，但必须记录 source lag。
- DCF 读取时只能使用估值日或估值日前已发布数据，禁止未来函数。

---

## 8. API 与任务设计

### 8.1 任务

| 任务 | 说明 |
|---|---|
| `fx_master_sync` | 同步货币、货币对、指数、series 字典 |
| `fx_calendar_governance` | 维护外汇发布日/观测日历 |
| `fx_rate_backfill` | 按 scope 回补历史汇率 |
| `fx_rate_sync` | 日更汇率 |
| `fx_derivation_sync` | 生成反向汇率、交叉汇率、目标币种派生 |
| `fx_quality_check` | 检查缺口、异常跳变、源差异 |

### 8.1.1 在岸人民币优先落库顺序

第一步只落库官方在岸人民币中间价，不同时引入 CNH、FRED、ECB 或 ICE：

| 阶段 | 任务 | 默认范围 | 写入表 | 说明 |
|---|---|---|---|---|
| 1 | `fx_master_sync` | 全 FX 字典 | `fx_currencies`、`fx_instruments`、`fx_series`、`fx_source_manifests`、`fx_derivations` | 先写主数据和 source profile，保证后续数据有外键和来源口径 |
| 2 | `fx_calendar_governance` | `source_profiles=["cfets_rmb_fixing"]` | `fx_calendars` | 生成 CFETS/SAFE 发布日治理记录，区分 `observed`、`missing_expected_observation`、`expected_non_publication` |
| 3 | `fx_rate_backfill` | `scope_id="rmb_onshore_fixing"` | `fx_observations`、`ingestion_runs` | 显式日期范围历史回补 `USD/CNY`、`EUR/CNY`、`JPY/CNY` |
| 4 | `fx_rate_sync` | `scope_id="rmb_onshore_fixing"` | `fx_observations`、`ingestion_runs` | 日更当前在岸中间价 |
| 5 | `fx_quality_check` | 已落库在岸序列 | `fx_quality_issues`、`fx_readiness_snapshots` | 检查缺口、异常跳变、陈旧数据和 readiness |

调度层应把 `fx_master_sync` 和 `fx_calendar_governance` 配置为 `fx_rate_backfill` / `fx_rate_sync` 的 `pre_success` 前置依赖：

- `fx_master_sync` 先刷新货币、货币对、series 和 source profile，保证回补目标存在。
- `fx_calendar_governance` 再按本次任务日期范围维护发布日 / 观测日历；历史回补继承 `start_date` / `end_date` / `dry_run`，日更至少继承 `dry_run`。
- 前置依赖失败时，汇率拉取任务不得启动；避免在主数据或发布日治理缺失时直接写观测值。

`rmb_onshore_fixing` scope 只包含：

```text
FX.USD_CNY.CFETS.MID.DAILY
FX.EUR_CNY.CFETS.MID.DAILY
FX.JPY_CNY.CFETS.MID.DAILY
```

在岸人民币数据源采用 `cfets_rmb_fixing` source profile：

- 最新日优先读取中国货币网 / CFETS 当前官方 JSON。
- 历史回补读取 SAFE 人民币汇率中间价公开页，经 `akshare.currency_boc_safe` 包装。
- `USD`、`EUR` 字段按 SAFE 百元报价除以 100 后入库为市场惯例 `1 USD/EUR = x CNY`。
- `JPY` 保留官方常用的 `100 JPY = x CNY` 报价单位，`quote_multiplier=100`。

日历治理和汇率获取应像股票/期货一样任务化，但外汇日历不是交易所交易日历：

- `fx_calendar_governance` 是发布日 / 观测日治理，不写汇率数值。
- `fx_rate_backfill` / `fx_rate_sync` 只写汇率观测值，不负责推断发布日规则；但调度任务必须先执行主数据治理和发布日治理前置依赖。
- `fx_rate_backfill` / `fx_rate_sync` 写入成功后，应再执行发布日治理复核，把已写入观测的日期从 `missing_expected_observation` 更新为 `observed`。
- 历史回补必须显式传入 `start_date` / `end_date`，避免无边界抓取。
- 默认调度参数先使用 `rmb_onshore_fixing`，等 CNH 和美元指数链路单独验收后，再显式切换到 `rmb_core`。
- 长耗时任务必须在服务层记录关键阶段日志：任务开始、scope/series 解析、数据源请求开始/完成、解析行数、写入进度、写入统计、ingestion run 开始/结束和异常阻断原因。

### 8.1.2 离岸人民币 CNH 准备顺序

离岸人民币先单独验收 `USD/CNH`、`EUR/CNH`、`JPY/CNH` 三条 market spot，不使用 `rmb_core` 大范围混跑。当前主数据源为 `cnh_market_aggregated_public`，历史回补底层接口为 AkShare `forex_hist_em`（东方财富外汇历史行情）；Yahoo/yfinance chart 只作为最新单点 fallback，不作为全量历史主源：

```text
FX.USD_CNH.MARKET.SPOT.DAILY -> USDCNH
FX.EUR_CNH.MARKET.SPOT.DAILY -> EURCNH
FX.JPY_CNH.MARKET.SPOT.DAILY -> JPYCNH
```

质量边界：

- 该源是免费聚合市场源，`quality_flag=aggregated_public`，不能标记为 official。
- AkShare/东方财富 `JPYCNH` 已按 `100 JPY = x CNH` 报价，写库时直接保存并保留 `quote_multiplier=100`。
- Yahoo/yfinance fallback 返回值按 `1 base currency` 报价；若使用 fallback，`JPYCNH=X` 必须乘以 100 后再保存。
- CNH 使用独立 `weekday_24x5` source calendar，不复用 CFETS/SAFE 在岸中间价日历。
- 全量 CNH 三条 direct spot 的共同起始日期以三条序列共同可用为准；当前接口样本显示 `USD/CNH` 可追溯到 2010-08-23，`EUR/CNH` 与 `JPY/CNH` 从 2025-02-24 起可用，因此三条同时回补的全量 dry-run 起始日暂定 `2025-02-24`。

实施顺序：

| 阶段 | 任务 | 参数 | 写入表 | 说明 |
|---|---|---|---|---|
| 1 | `fx_master_sync` | 无 | `fx_currencies`、`fx_instruments`、`fx_series`、`fx_source_manifests` | 确保 `rmb_offshore_spot` scope 和 CNH series 已进入主数据 |
| 2 | `fx_calendar_governance` | `source_profiles=["cnh_market_aggregated_public"]` | `fx_calendars` | 生成 CNH 市场源自己的发布/观测日历 |
| 3 | `fx_rate_backfill` | `scope_id="rmb_offshore_spot"` | `fx_observations`、`ingestion_runs` | 回补三条 CNH market spot |
| 4 | `fx_calendar_governance` | 同阶段 2，并继承回补日期范围 | `fx_calendars` | 回补后复核，把已写观测日更新为 `observed` |
| 5 | `fx_quality_check` | 已落库 CNH 序列 | `fx_quality_issues`、`fx_readiness_snapshots` | 检查缺口、跳变和陈旧数据 |

建议先执行 dry-run：

```text
/run fx_calendar_governance source_profiles=cnh_market_aggregated_public start_date=YYYY-MM-DD end_date=YYYY-MM-DD dry_run=true
/run fx_rate_backfill scope_id=rmb_offshore_spot start_date=YYYY-MM-DD end_date=YYYY-MM-DD dry_run=true
```

dry-run 通过后再将同样参数改为 `dry_run=false`。当前调度静态前置依赖仍默认治理 `cfets_rmb_fixing`，这是为了保护已经验收的在岸人民币默认任务；CNH 首次落库阶段必须显式运行 `cnh_market_aggregated_public` 的日历治理。

### 8.2 API

建议端点：

```text
GET /api/v1/research/fx/dictionary
GET /api/v1/research/fx/series
GET /api/v1/research/fx/rates?series_id=...&start_date=...&end_date=...
GET /api/v1/research/fx/convert?from=USD&to=CNY&date=2026-06-26&amount=1
GET /api/v1/research/fx/indices?index_id=FXI.DXY
GET /api/v1/research/fx/readiness
```

---

## 9. 对商品和 DCF 的服务方式

商品数据层不维护汇率，只保留原始币种和单位。需要换算时：

1. 商品模块提供 `commodity_series_id`、`observation_date`、`raw_currency`、`raw_unit`。
2. FX 模块根据目标币种和日期返回本地已治理汇率。
3. DCF 或派生任务记录 `fx_series_id`、`fx_date`、`fx_rate`、`conversion_policy`。
4. 如果 FX 缺失，返回 readiness gap，不临时远程抓取。

---

## 10. 未来扩展建议

`fx.db` 后续可扩展存放：

- 主要货币即期汇率：USD、EUR、JPY、GBP、HKD、AUD、CAD、CHF、SGD、KRW 等。
- 人民币中间价、即期价、离岸价、在岸离岸价差。
- 美元指数、贸易加权美元指数、人民币有效汇率指数。
- 名义有效汇率、实际有效汇率，优先 BIS、CFETS 等官方源。
- 远期点、掉期点、NDF，供跨境融资和汇率风险分析。
- 主要央行政策利率和短端利率，支持利率平价和汇率压力分析。
- 外汇储备、跨境资本流动、银行结售汇等宏观外汇数据。
- 期权隐含波动率和风险逆转，后续用于汇率风险建模。

这些数据不必第一阶段实现，但数据库设计应预留 `instrument_type`、`rate_type`、`source_profile` 和 `metadata_json`，避免未来重构。

---

## 11. 第一阶段验收标准

- `fx.db` 可独立初始化，不依赖 `futures.db`。
- 可同步并查询 `USD/CNY`、`EUR/CNY`、`JPY/CNY` 官方在岸中间价。
- 可同步或派生 `USD/CNH`、`EUR/CNH`、`JPY/CNH`，并清楚标记来源质量。
- 支持反向汇率派生，例如 `CNY/USD`。
- 支持美元指数或免费替代美元指数序列，且标记为 `currency_index`。
- 所有汇率写入保留 source profile、观测日、发布时间或采集时间、质量标记。
- DCF/商品模块只能读取本地 FX 数据，不得估值时远程抓取。
- 禁止使用未来汇率。
