# 特殊商品现货与海外基准数据获取需求说明书

> 更新日期：2026-06-26
> 适用项目：Quote System / Research Data Engine
> 文档定位：本文用于定义国内五大商品期货交易所之外的特殊商品数据层，包括动力煤现货/长协价、LME 铜/铝、Brent/WTI、化工现货指数等免费可得数据源、注册要求、数据架构、更新维护和 DCF 对接边界。
> 使用边界：本模块服务于周期行业 DCF、行业景气诊断和商品研究输入，不提供商品交易信号，不替代付费产业数据库。
> 关联文档：外汇汇率、美元指数、币种换算和 `fx.db` 独立数据域见 `docs/development/fx_market_data_requirements.md`。

---

## 1. 结论摘要

国内五大商品期货交易所行情已经能够覆盖大部分 P0 周期品种，但 DCF 中仍需要若干“非国内期货主力连续”的外部锚：

1. **Brent/WTI**：免费可得性最好，应作为第一批接入。优先 FRED/EIA 官方或准官方 API，单位统一为 `USD/barrel`，频率日频。
2. **LME 铜/铝**：官方 LME 可提供网页登录下载和数据服务，但自动 API、历史深度和许可边界需要谨慎。第一阶段建议同时接入两层：FRED/IMF 月度全球铜铝价格作为可稳定自动化的长周期基准；LME 官方页面/报表作为可选增强源，先做可行性验证再生产化。
3. **动力煤现货/长协价**：这是最难免费稳定自动化的部分。权威指数和长协价格多在资讯商、协会或政策公告体系中，免费源常有版权、反爬、口径不完整问题。第一版应拆成两类：可自动更新的公开现货/库存/指数辅助数据，以及人工或半自动维护的政策/长协事件表。
4. **化工现货指数**：AkShare 可取生意社/100ppi 大宗商品现货价格和基差，适合做 P1 免费增强源；但它不是交易所官方源，应明确标记为 `aggregated_public_web`，并记录商品规格、单位、字段口径和缺失日期。

本模块不应复用 `futures_price_bars` 表，因为这些数据不是标准期货合约日线，也不一定有交易日历、OHLC、成交量、持仓量。第一版建议在 `data/futures.db` 中新增独立 `commodity_*` 表，复用期货域已有 DB 通道、任务报告、API 分区和 DCF 对接经验，但通过表名前缀与期货行情严格隔离。未来如果宏观/产业数据层扩大，可以再迁移到独立 `commodity.db` 或研究域统一库。

由于 Brent/WTI、LME、FRED/IMF、World Bank 等海外数据通常以美元计价，而 A 股 DCF 的财务报表和估值输入通常以人民币计价，本模块必须保留商品价格的原始币种和原始单位。汇率数据采集、维护、换算和目标币种派生不属于商品数据层职责，应由独立外汇数据模块和 `fx.db` 提供；商品层只记录原始币种/单位，并在需要人民币口径时声明对 FX 模块的依赖。

### 1.1 是否沿用国内五大交易所架构

结论：**可以沿用同一套框架，但需要把“交易所”抽象为更通用的 `venue/source_venue`**。国内五大交易所是标准期货交易所；FRED、EIA、World Bank、100ppi、生意社、金十、LME 官网报表则是官方统计源、交易所官网、资讯网站或聚合数据源。它们不都具备交易日历和合约生命周期，但都可以统一进入以下流程：

```text
source_venue -> commodity category -> commodity instrument -> series
            -> venue calendar / publication calendar governance
            -> master-data governance
            -> observations sync/backfill
            -> diagnostics/API/DCF readiness
```

统一边界：

- `source_venue` 对应数据采集源或发布机构，例如 `FRED`、`EIA`、`WORLD_BANK`、`LME`、`100PPI`、`SINA_FOREIGN_FUTURES`。
- `category` 继续使用商品分类，例如 `energy`、`coal`、`nonferrous`、`chemical`、`inventory`、`policy_contract`。
- `commodity_id` 对应根商品或指标，例如 `OIL.WTI.SPOT`、`METAL.COPPER.IMF`、`CN.COAL.THERMAL_5500.SPOT`。
- `series_id` 对应具体来源、频率和口径，例如 `OIL.WTI.SPOT.FRED.DAILY`。
- `calendar_governance` 对国内期货是交易日治理；对 FRED/EIA/World Bank 是发布日/观测日治理；对政策长协是生效期治理。
- `master_governance` 对国内期货维护交易品种和合约；对特殊商品维护商品、序列、单位、币种、频率、来源和规格。
- `daily_sync/backfill` 对日频源按日期回补；对月频源按月份展开为观测期；对政策事件按生效期展开或在 DCF 读取时按估值日匹配。

不应照搬的部分：

- 不要求所有源都有交易日历。FRED/EIA/World Bank 需要的是 `publication_calendar` 或 `observation_calendar`。
- 不要求所有源都有 OHLCV。特殊商品数据核心是 `observation_date + value + unit + source_profile`。
- 不要求所有源都能自动发现新品种。FRED/EIA/World Bank 第一版以配置化 series registry 为主；100ppi/LME 可以逐步做页面发现。
- 不把 LME、FRED、EIA 伪装成国内交易所。系统内部可以统一使用 `venue` 选择器，但 API/报告中必须清楚显示来源类型。

---

## 2. 数据范围与优先级

### 2.1 P0 接入范围

| 数据 | 推荐主源 | 备源 | 频率 | 单位 | 可得性判断 |
|---|---|---|---|---|---|
| WTI Cushing spot | FRED `DCOILWTICO` 或 EIA Open Data | AkShare/Sina 外盘期货仅作参考 | 日频 | USD/barrel | 高 |
| Brent Europe spot | FRED `DCOILBRENTEU` 或 EIA Open Data | AkShare/Sina 外盘期货仅作参考 | 日频 | USD/barrel | 高 |
| 全球铜价 | FRED/IMF `PCOPPUSDM` | World Bank Pink Sheet | 月频 | USD/metric ton | 高 |
| 全球铝价 | FRED/IMF `PALUMUSDM` | World Bank Pink Sheet | 月频 | USD/metric ton | 高 |
| LME 铜/铝官方价 | LME 官方 Market Data 报表 | AkShare/Sina 外盘期货、FRED/IMF 月度 | 日频或月频 | USD/metric ton | 中，需账号/许可边界验证 |
| 动力煤公开辅助数据 | 生意社/100ppi、金十沿海六大电库存、公开政策公告 | 手工导入政策/长协事件 | 日频/事件 | CNY/ton 或事件价 | 中低 |
| 化工现货/基差 | 生意社/100ppi，经 AkShare 或直连页面 | 交易所期货主力连续 | 日频 | 多为 CNY/ton | 中 |

### 2.2 不纳入第一阶段自动化的内容

- 付费版 Mysteel、SMM、百川盈孚、卓创、CCTD/CCI 会员数据。
- 需要登录、验证码或复杂授权的网页历史数据批量抓取。
- 无法确认版权和再分发许可的长历史日频现货价格。
- 自动解释所有煤炭长协合同条款、港口规格差、税费、运费和热值折算。

---

## 3. 免费源与注册要求

### 3.1 需要注册或申请的源

| 来源 | 注册地址 | 是否必须 | 用途 | 说明 |
|---|---|---:|---|---|
| FRED API | `https://fredaccount.stlouisfed.org` 或 FRED API key 页面 | 是 | WTI、Brent、FRED/IMF 铜铝月度 | FRED 文档要求 API 请求使用 API key。注册后把 key 配到项目密钥配置，不写入代码和文档。 |
| EIA Open Data API | `https://www.eia.gov/opendata/register.php` | 推荐 | WTI、Brent、能源相关官方数据 | EIA API 使用需要申请 key；bulk download 可不需要 key，但 API 方式更适合增量更新。 |
| LME.com 账号 | `https://www.lme.com/` account/register | 可选 | 官方 LME 日价/月均价、库存、成交量等报表 | LME 页面说明下载部分报表需要登录或注册；自动化前必须确认许可、下载链接稳定性和使用条款。 |

密钥配置原则：

- FRED/EIA API key 应放在运行环境变量或本地私有配置中，不写入 git 跟踪文件。
- 推荐环境变量名：`FRED_API_KEY`、`EIA_API_KEY`。
- 运行日志、Telegram 报告、OpenSpec 和需求文档不得输出 key 明文。
- 本项目配置文件只记录 `api_key_env` 或私有配置路径；Provider 初始化时由配置层读取并脱敏记录状态。

LME.com 账号使用原则：

- LME 免费账号仅用于验证官方页面、报表下载和许可边界。
- 自动化实现前，先用浏览器会话验证是否能下载铜/铝日价或月均价报表，并记录是否需要登录、cookie、动态 token、验证码或下载许可。
- 如果 LME 要求登录态，应仿照 DCE/GFEX 的浏览器 adapter 思路，封装为 `LmeOfficialReportProvider`，但不得在代码、配置或日志中保存账号密码明文。
- 如果 LME 只允许人工登录下载，第一版应保留 `ManualLmeReportImportProvider`，由用户下载后导入，避免违反使用条款或产生不稳定自动抓取。

### 3.2 不需要注册但需审慎使用的源

| 来源 | 用途 | 风险 |
|---|---|---|
| World Bank Pink Sheet | 月度大宗商品价格和年度价格 | 频率较低，适合 DCF 中长期锚，不适合日更景气监控 |
| 生意社/100ppi 公开页面 | 国内大宗商品现货价格、基差 | 非官方交易所源，字段规格和单位复杂，页面结构可能变动 |
| Sina 外盘期货 | 外盘期货历史日线 | 聚合源，不是交易所官方源；适合作为 LME/ICE/CME 官方源不可用时的备源 |
| 金十数据中心 | LME 库存、沿海六大电库存等宏观/产业数据 | 聚合源，非官方一手源；适合辅助景气指标，不适合替代核心价格锚 |
| 东方财富大宗商品价格指数 | 宏观景气辅助 | 指数口径需核对，不作为 DCF 核心价格假设主源 |

---

## 4. AkShare 来源拆解与官方替代判断

当前本机 AkShare 可提供以下相关接口。使用时必须把真实来源写入 `source_profile`，不能只记录 `akshare`。

| AkShare 接口 | AkShare 标注来源 | 可否找官方/一手源 | 建议定位 |
|---|---|---|---|
| `futures_spot_price_daily` / `futures_spot_price` | 生意社/100ppi，`https://www.100ppi.com/sf/` | 不是交易所官方源；可尝试直连 100ppi 页面，减少 AkShare 依赖 | P1 国内现货/基差增强源 |
| `futures_foreign_hist` | 新浪财经外盘期货，`GlobalFuturesService.getGlobalFuturesDailyKLine` | 对 LME/ICE/CME 应优先找交易所或 FRED/EIA/World Bank；Sina 只能备源 | 海外期货行情备源 |
| `macro_euro_lme_stock` | 金十数据中心 LME 库存 | 官方替代为 LME warehouse/stock reports，但自动化需登录/许可验证 | LME 库存辅助源 |
| `macro_china_daily_energy` | 金十中国沿海六大电库存 | 更一手的电厂/煤炭库存数据通常不可免费稳定 API 化 | 动力煤供需辅助源 |
| `spot_goods` | 新浪商品现货指数 | 可尝试寻找对应指数发布源，但第一版不作为核心价格 | 辅助观察 |
| `macro_china_commodity_price_index` | 东方财富大宗商品价格指数 | 东方财富为公开聚合源，非官方交易所源 | 宏观景气辅助 |

替代原则：

- **油价**：不用 AkShare 做主源；优先 FRED/EIA。
- **铜铝长期基准**：不用 Sina 外盘做主源；优先 FRED/IMF 或 World Bank 月度。
- **LME 日价**：优先验证 LME 官方报表；未完成前以 FRED/IMF 月度作为 DCF 长周期锚，Sina/AkShare 只作备查。
- **国内现货和化工指数**：如果没有交易所官方源，AkShare/100ppi 可以作为免费源，但必须保留 `source_profile=100ppi_public_web` 和字段口径。
- **动力煤长协**：不要用 AkShare 猜长协价；长协应作为政策/事件表，由公告或人工确认维护。

---

## 5. 数据模型需求

### 5.1 主数据表

建议新增 `commodity_price_instruments`：

| 字段 | 说明 |
|---|---|
| `commodity_id` | 稳定 ID，例如 `OIL.WTI.SPOT`、`METAL.COPPER.IMF`、`CN.COAL.THERMAL_5500.SPOT` |
| `name` | 中文或英文名称 |
| `category` | `energy`、`nonferrous`、`coal`、`chemical` 等 |
| `region` | `global`、`US`、`Europe`、`China`、`Qinhuangdao` 等 |
| `commodity_type` | `spot`、`benchmark`、`index`、`inventory`、`policy_contract` |
| `currency` | `USD`、`CNY` 等 |
| `unit` | `barrel`、`metric_ton`、`ton`、`day` 等 |
| `frequency` | `daily`、`weekly`、`monthly`、`event` |
| `primary_source_profile` | 主源 profile |
| `fallback_source_profiles` | 备源 profile 列表 |
| `active` | 是否启用 |

### 5.2 序列表

建议新增 `commodity_price_series`：

| 字段 | 说明 |
|---|---|
| `series_id` | 稳定 ID，例如 `OIL.WTI.SPOT.FRED.DAILY` |
| `commodity_id` | 关联主数据 |
| `source_profile` | 数据源口径 |
| `quote_type` | `spot_price`、`settlement_price`、`monthly_average`、`inventory`、`policy_price` |
| `frequency` | 数据频率 |
| `timezone` | 时间区 |
| `release_lag_days` | 预计发布滞后 |
| `start_date` / `end_date` | 生命周期 |
| `quality_policy` | 质量规则 |

### 5.3 观测值表

建议新增 `commodity_price_observations`：

| 字段 | 说明 |
|---|---|
| `series_id` | 序列 ID |
| `observation_date` | 观测日期或月份末日期 |
| `value` | 标准化数值 |
| `currency` | 标准化币种 |
| `unit` | 标准化单位 |
| `raw_value` | 原始数值 |
| `raw_unit` | 原始单位 |
| `source_profile` | 实际来源 |
| `source_url` | 来源 URL 或 API endpoint |
| `quality_flag` | `official`、`aggregated_public`、`manual_verified`、`estimated`、`partial` |
| `revision_id` | 修订版本，可为空 |
| `metadata_json` | 原始字段、规格、热值、地区等 |

扩展字段建议：

| 字段 | 说明 |
|---|---|
| `raw_currency` | 原始币种，例如 `USD`、`CNY` |
| `raw_unit` | 原始单位，例如 `USD/barrel`、`USD/metric_ton`、`CNY/ton` |
| `normalized_currency` | 标准化币种 |
| `normalized_unit` | 标准化单位 |
| `target_currency_dependency` | 若 DCF 需要目标币种口径，记录依赖的外汇模块能力，例如 `fx.db:USD/CNY` |
| `unit_conversion_policy` | 仅记录商品单位换算规则；货币换算不在商品层执行 |

唯一键建议：

```text
(series_id, observation_date, source_profile)
```

### 5.4 政策/长协事件表

动力煤长协价不适合强行建成日 K 线。建议新增 `commodity_policy_events`：

| 字段 | 说明 |
|---|---|
| `event_id` | 稳定 ID |
| `commodity_id` | 关联商品 |
| `effective_start` / `effective_end` | 生效期 |
| `policy_type` | `long_term_contract`、`price_range`、`benchmark_adjustment` |
| `price_low` / `price_high` / `benchmark_price` | 政策价格区间或基准 |
| `currency` / `unit` | 单位 |
| `source_url` | 公告或公开来源 |
| `quality_flag` | `official_policy`、`manual_verified`、`public_report` |
| `notes` | 口径说明 |

---

## 6. Provider 与调度设计

### 6.1 Provider 抽象

新增统一抽象 `CommodityPriceProvider`：

```text
discover_series()
fetch_observations(series_id, start_date, end_date)
normalize_observations(raw_payload)
validate_observations(normalized_rows)
```

每个 provider 必须声明：

- `source_profile`
- `source_name`
- `requires_api_key`
- `registration_url`
- `supported_frequency`
- `supported_commodity_ids`
- `copyright_policy`
- `rate_limit_policy`

### 6.1.1 Venue 抽象与 Scope 选择

特殊商品数据层应复用期货系统已经验证过的 scope 设计，但字段命名应更通用：

| 字段 | 说明 |
|---|---|
| `scope_id` | 稳定配置 ID，例如 `fred_energy_oil`、`world_bank_metals`、`lme_nonferrous`、`cn_100ppi_chemical` |
| `venues` | 数据发布机构或采集源，支持 `["all"]`，例如 `["FRED"]`、`["EIA"]`、`["LME"]` |
| `categories` | 商品分类，支持 `["all"]` |
| `commodity_ids` | 可选根商品列表 |
| `series_ids` | 可选精确序列列表，优先级最高 |
| `frequencies` | `daily`、`weekly`、`monthly`、`event` |
| `domains` | `master_data`、`calendar_governance`、`observations`、`diagnostics`、`policy_events` |
| `source_policy` | 主源、备源、fallback、是否允许聚合源 |
| `calendar_policy` | 交易日、发布日、观测日或生效期治理策略 |

示例 scope：

| scope_id | venues | categories | 用途 |
|---|---|---|---|
| `fred_energy_oil` | `["FRED"]` | `["energy"]` | WTI、Brent 日频现货 |
| `eia_energy_oil` | `["EIA"]` | `["energy"]` | EIA 原油官方数据校验或备源 |
| `world_bank_metals` | `["WORLD_BANK"]` | `["nonferrous"]` | 铜、铝等月度长期基准 |
| `fred_imf_metals` | `["FRED"]` | `["nonferrous"]` | FRED/IMF 铜铝月度价格 |
| `lme_nonferrous` | `["LME"]` | `["nonferrous"]` | LME 铜铝官方价、库存或月均价 |
| `cn_100ppi_chemical` | `["100PPI"]` | `["chemical"]` | 国内化工现货和基差 |
| `cn_coal_policy` | `["NDRC", "MANUAL"]` | `["coal"]` | 动力煤长协和政策价格事件 |

### 6.1.2 Calendar / Publication Governance

特殊商品数据不强制使用交易日历，但必须有可解释的时间治理：

| 数据类型 | 治理方式 |
|---|---|
| FRED/EIA 日频 | 使用观测日和发布滞后；非发布日不视为异常 |
| World Bank/FRED/IMF 月频 | 使用月度观测期；可按月末日期落库，DCF 读取时按估值日前最新可得值 |
| 100ppi 日频现货 | 使用中国交易日或网页实际发布时间；缺失日记录为 source_missing，不用 weekday 猜测补齐 |
| LME 官方报表 | 使用 LME 官方发布日或报表日期；若登录下载失败，保留 blocker |
| 动力煤长协/政策 | 使用 `effective_start/effective_end` 生效期，不做日频价格补点 |

因此，任务名称可以继续叫 `commodity_price_backfill/sync`，但报告中要区分 `trading_day_governance`、`publication_calendar_governance`、`policy_effective_period_governance`。

第一批 provider：

| Provider | 用途 |
|---|---|
| `FredCommodityProvider` | FRED WTI、Brent、IMF 铜铝月度 |
| `EiaCommodityProvider` | EIA 能源官方数据，作为油价主源或 FRED 校验源 |
| `WorldBankPinkSheetProvider` | 月度/年度大宗商品长期价格 |
| `AkshareCommoditySpotProvider` | 100ppi 现货和基差 |
| `SinaForeignFuturesProvider` | 外盘期货备源 |
| `LmeOfficialReportProvider` | LME 官方报表试点，完成登录/下载验证后启用 |
| `ManualPolicyEventProvider` | 动力煤长协/政策事件表导入 |

### 6.2 调度任务

新增任务建议：

| 任务 | 频率 | 用途 |
|---|---|---|
| `commodity_price_master_sync` | 每周 | 同步主数据和 series 字典 |
| `commodity_price_daily_sync` | 每日 22:30 后 | 同步日频 WTI/Brent、100ppi 现货、库存辅助指标 |
| `commodity_price_monthly_sync` | 每月 World Bank/FRED 更新后 | 同步 FRED/IMF、World Bank 月度价格 |
| `commodity_policy_event_sync` | 手工触发为主 | 导入动力煤长协/政策事件 |
| `commodity_price_readiness_check` | 每日或每周 | 检查 DCF 所需 commodity input 缺口 |

调度要求：

- DCF 请求不得实时触发远程下载，只读取本地已同步数据。
- DCF 如需目标币种换算，应调用独立 FX 数据模块的本地治理结果；商品同步任务不得维护外汇数据。
- 同步任务必须写入 `ingestion_runs` 和 source metadata。
- 正常日更报告应简洁输出：成功/失败、更新序列数、插入/变更/不变行、缺口和 warning。
- 出现 API key 缺失、源不可用、单位不一致、长时间无更新时，报告必须进入详细模式。

---

## 7. DCF 对接需求

### 7.1 周期行业输入

DCF 侧读取商品数据时，应使用明确的 `series_id` 或 `commodity_id + preferred_series_policy`：

| DCF 输入 | 推荐数据 |
|---|---|
| 原油价格假设 | `OIL.BRENT.SPOT.FRED.DAILY`、`OIL.WTI.SPOT.FRED.DAILY` |
| 全球铜价 mid-cycle | `METAL.COPPER.IMF.MONTHLY` 或 LME 官方月均价 |
| 全球铝价 mid-cycle | `METAL.ALUMINUM.IMF.MONTHLY` 或 LME 官方月均价 |
| 国内化工价差 | 国内期货主力连续 + 100ppi 现货/基差 |
| 动力煤长协价 | `commodity_policy_events` + 期货/现货辅助 |

### 7.2 诊断指标

每个价格序列应支持：

- 最新值和最新可得日期。
- 3 年、5 年、10 年均值。
- 3 年、5 年、10 年分位。
- 当前值相对长期均值偏离。
- 月度/季度均值，供 DCF 年化假设使用。
- 数据质量、来源、缺口、是否聚合源或人工事件。
- 原始币种、原始单位，以及是否需要独立 FX 模块提供目标币种换算。

---

## 8. 验收标准

### 8.1 P0 验收

- FRED API key 配置完成后，可同步 WTI、Brent 日频历史和日更。
- FRED/IMF 铜、铝月度序列可同步，单位为 `USD/metric_ton`。
- World Bank Pink Sheet 可作为月度价格备源或交叉校验源。
- 100ppi 现货/基差可通过 AkShare 或直连页面获取，明确记录 `raw_unit` 和规格差异。
- 动力煤长协不伪装成日频行情；先支持政策/事件表。
- API 能查询商品字典、序列、观测值、诊断指标。
- DCF readiness 能识别缺少油价、铜铝、动力煤或现货增强数据的输入缺口。
- DCF readiness 能识别商品输入需要外汇模块提供目标币种换算的依赖缺口。

### 8.2 质量门槛

- 不同来源同一商品不得静默混写，必须保留 `source_profile`。
- 单位不一致时不得直接比较或计算分位，必须先标准化。
- 聚合源不得标记为 official。
- 现货规格、地区、热值、税费、计价方式不明时，`quality_flag` 不得高于 `partial`。
- API key 不得写入 git、文档或日志。
- 商品层不得维护或生成外汇汇率；不同币种或单位不得未经独立 FX/单位标准化结果直接进入 DCF 或价差计算。

---

## 9. 后续 OpenSpec 建议

建议拆成两个 change：

1. `add-special-commodity-market-data-layer`
   - 建库表、provider 抽象、FRED/EIA/World Bank/100ppi 接入、API 查询、调度任务。
2. `connect-special-commodity-data-to-cyclical-dcf`
   - 将 Brent/WTI、铜铝、动力煤政策事件、化工现货/基差纳入 DCF input bundle、readiness 和周期诊断。

第一阶段开发前需要用户提供：

- FRED API key。
- EIA API key，推荐但不是 Brent/WTI 第一版的绝对 blocker。
- LME.com 免费账号，只有在决定做 LME 官方报表自动化时需要。
