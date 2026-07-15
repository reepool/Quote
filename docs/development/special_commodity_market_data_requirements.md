# 特殊商品现货与海外基准数据获取需求说明书

> 更新日期：2026-07-10
> 适用项目：Quote System / Research Data Engine
> 文档定位：本文用于定义国内五大商品期货交易所之外的特殊商品数据层，包括动力煤现货/长协价、LME 铜/铝、Brent/WTI、化工现货指数等免费可得数据源、注册要求、数据架构、更新维护和 DCF 对接边界。
> 使用边界：本模块服务于周期行业 DCF、行业景气诊断和商品研究输入，不提供商品交易信号，不替代付费产业数据库。
> 关联文档：外汇汇率、美元指数、币种换算和 `fx.db` 独立数据域见 `docs/development/fx_market_data_requirements.md`。

---

## 1. 结论摘要

国内五大商品期货交易所行情已经能够覆盖大部分 P0 周期品种，但 DCF 中仍需要若干“非国内期货主力连续”的外部锚：

1. **Brent/WTI**：免费可得性最好，应作为第一批接入。优先 FRED/EIA 官方或准官方 API，单位统一为 `USD/barrel`，频率日频。
2. **LME 六种基本金属 3M 代理行情**：铜、铝、锌、铅、镍、锡统一通过 AkShare 外盘期货接口获取。新浪详情页将这些品种标为“CFD 差价合约并非期货”，其价格跟踪 LME 三个月连续合约；因此数据库类型必须标为 `foreign_futures_3m_proxy`。新浪 `futures_foreign_hist` 为主源，东方财富 `futures_global_hist_em` 为请求或结构失败时的备源；两者均不标记为 LME 官方 Closing Price。FRED/IMF 与 World Bank 月度铜铝价格继续作为更长周期的独立 DCF 基准，不与 LME 3M 代理日线伪装成同一序列。
3. **动力煤现货/长协价**：国家统计局自 2014 年 1 月起公开发布“流通领域重要生产资料市场价格变动情况”，其中“山西优混（5500 大卡）”可作为免费官方动力煤市场价格参考；它是经营企业批发和销售价格的旬度统计，不是港口现货指数、期货结算价或年度长协价。长协价格仍使用政策生效期事件治理，不能由该旬度价格推导。
4. **化工现货指数**：AkShare 可取生意社/100ppi 大宗商品现货价格和基差，适合做 P1 免费增强源；但它不是交易所官方源，应明确标记为 `aggregated_public_web`，并记录商品规格、单位、字段口径和缺失日期。

本模块不应复用 `futures_price_bars` 表，因为这些数据不是标准期货合约日线，也不一定有交易日历、OHLC、成交量、持仓量。第一版建议在 `data/futures.db` 中新增独立 `commodity_*` 表，复用期货域已有 DB 通道、任务报告、API 分区和 DCF 对接经验，但通过表名前缀与期货行情严格隔离。未来如果宏观/产业数据层扩大，可以再迁移到独立 `commodity.db` 或研究域统一库。

由于 Brent/WTI、LME、FRED/IMF、World Bank 等海外数据通常以美元计价，而 A 股 DCF 的财务报表和估值输入通常以人民币计价，本模块必须保留商品价格的原始币种和原始单位。汇率数据采集、维护、换算和目标币种派生不属于商品数据层职责，应由独立外汇数据模块和 `fx.db` 提供；商品层只记录原始币种/单位，并在需要人民币口径时调用本地 FX 服务或声明对 FX 模块的依赖缺口。商品同步任务不得临时远程抓取汇率，也不得把 FX 派生值写回商品价格表。

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
| WTI Cushing spot | EIA Open Data `RWTC` | FRED `DCOILWTICO` 逐日期补缺 | 日频 | USD/barrel | 高 |
| Brent Europe spot | EIA Open Data `RBRTE` | FRED `DCOILBRENTEU` 逐日期补缺 | 日频 | USD/barrel | 高 |
| 全球铜价 | FRED/IMF `PCOPPUSDM` | World Bank Pink Sheet | 月频 | USD/metric ton | 高 |
| 全球铝价 | FRED/IMF `PALUMUSDM` | World Bank Pink Sheet | 月频 | USD/metric ton | 高 |
| 全球化肥基准 | World Bank Pink Sheet（磷矿石、DAP、TSP、尿素、氯化钾） | 后续按品种评估 FRED/IMF | 月频 | USD/metric ton | 高，官方公开月度数据 |
| 全球农产品基准 | World Bank Pink Sheet（棕榈油、大豆、豆油、豆粕、玉米、美国HRW小麦、世界糖、Cotton A Index、TSR20橡胶） | 后续按品种评估 FRED/IMF | 月频 | USD/metric ton 或 USD/kg | 高，官方公开月度数据 |
| LME 铜/铝/锌/铅/镍/锡 3M 代理 | AkShare/Sina 外盘期货 | AkShare/东方财富全球期货 | 日频 | USD/metric ton | 高，CFD/聚合代理；新浪约自 2016-07-11，东财约自 2013-06-21 |
| 动力煤公开市场参考 | 国家统计局“山西优混（5500 大卡）”旬度价格 | 100ppi/库存指标仅作辅助；政策长协独立维护 | 旬度/事件 | CNY/ton 或事件价 | 高（统计局序列）/人工核验（长协） |
| 化工现货/基差 | 生意社/100ppi，经 AkShare 或直连页面 | 交易所期货主力连续 | 日频 | 多为 CNY/ton | 中 |

World Bank 化肥序列使用独立 `world_bank_fertilizers` scope，并保持为全球月度基准，不与国内100ppi尿素现货合并。2026-07-13 官方工作簿探测确认磷矿石、TSP、尿素和氯化钾自1960-01起可用，DAP自1967-01起可用，五项单位均为 `$/mt`，规范化为 `USD/metric_ton`。短窗口 dry-run 获取90条；全历史复验获取3,905条，主数据和月份治理均为成功。官方工作簿中磷矿石 `2023M11` 明确使用 `…` 无值标记，系统将其登记为带工作簿 URL 的来源治理例外，不插值、不伪造观测；其余月份无未解决缺口。正式写入 `run_id=131` 新增3,905条，幂等复验 `run_id=132` 为新增0、变更0、不变3,905，均无 warning/blocker；验证后已加入 `special_commodity_international_monthly_price_sync`。

World Bank 农产品使用独立 `world_bank_agriculture` scope。2026-07-13 官方工作簿探测确认棕榈油、大豆、豆油、豆粕、玉米、美国HRW小麦、世界糖和Cotton A Index连续覆盖1960-01至2026-06，各798个月；TSR20橡胶连续覆盖1999-01至2026-06，共330个月；生命周期内均无缺月。油籽、谷物按 `USD/metric_ton`，糖、棉花和橡胶按 `USD/kg` 保存。短窗口 dry-run `run_id=134` 覆盖2025-01至2026-06，9个序列共162条；全历史 dry-run `run_id=135` 共6,714条；正式写入 `run_id=137` 新增6,714条；幂等复验 `run_id=138` 为新增0、变更0、不变6,714。各阶段主数据与月份治理均成功且无 warning/blocker，验证后已加入 `special_commodity_international_monthly_price_sync`。

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

密钥配置原则：

- FRED/EIA API key 应放在运行环境变量或本地私有配置中，不写入 git 跟踪文件。
- 推荐环境变量名：`FRED_API_KEY`、`EIA_API_KEY`。
- 运行日志、Telegram 报告、OpenSpec 和需求文档不得输出 key 明文。
- 本项目配置文件只记录 `api_key_env` 或私有配置路径；Provider 初始化时由配置层读取并脱敏记录状态。

### 3.2 不需要注册但需审慎使用的源

| 来源 | 用途 | 风险 |
|---|---|---|
| World Bank Pink Sheet | 月度大宗商品价格和年度价格 | 频率较低，适合 DCF 中长期锚，不适合日更景气监控 |
| 生意社/100ppi 公开页面 | 国内大宗商品现货价格、基差 | 非官方交易所源，字段规格和单位复杂，页面结构可能变动 |
| Sina 外盘期货 | LME 3M 历史日线主源 | 聚合源，不是交易所官方源；必须保留真实来源、代码、OHLCV 和观测日期 |
| 东方财富全球期货 | LME 3M 请求失败备源 | 聚合源，历史更长但接口存在 IP/频控；必须通过统一 proxy patch 运行时访问 |
| 金十数据中心 | LME 库存、沿海六大电库存等宏观/产业数据 | 聚合源，非官方一手源；适合辅助景气指标，不适合替代核心价格锚 |
| 东方财富大宗商品价格指数 | 宏观景气辅助 | 指数口径需核对，不作为 DCF 核心价格假设主源 |

---

## 4. AkShare 来源拆解与官方替代判断

当前本机 AkShare 可提供以下相关接口。使用时必须把真实来源写入 `source_profile`，不能只记录 `akshare`。

| AkShare 接口 | AkShare 标注来源 | 可否找官方/一手源 | 建议定位 |
|---|---|---|---|
| `futures_spot_price_daily` / `futures_spot_price` | 生意社/100ppi，`https://www.100ppi.com/sf/` | 不是交易所官方源；可尝试直连 100ppi 页面，减少 AkShare 依赖 | P1 国内现货/基差增强源 |
| `futures_foreign_hist` | 新浪财经外盘期货，`GlobalFuturesService.getGlobalFuturesDailyKLine` | 本项目将其作为 LME 3M 日线主源，不能标记为交易所官方 Closing Price | LME 日线主源 |
| `futures_global_hist_em` | 东方财富全球期货 | 本项目将其作为新浪请求/结构失败备源；不使用其缺失或为零的成交量覆盖新浪数据 | LME 日线备源 |
| `macro_euro_lme_stock` | 金十数据中心 LME 库存 | 官方替代为 LME warehouse/stock reports，但自动化需登录/许可验证 | LME 库存辅助源 |
| `macro_china_daily_energy` | 金十中国沿海六大电库存 | 更一手的电厂/煤炭库存数据通常不可免费稳定 API 化 | 动力煤供需辅助源 |
| `spot_goods` | 新浪商品现货指数 | 可尝试寻找对应指数发布源，但第一版不作为核心价格 | 辅助观察 |
| `macro_china_commodity_price_index` | 东方财富大宗商品价格指数 | 东方财富为公开聚合源，非官方交易所源 | 宏观景气辅助 |

替代原则：

- **油价**：不用 AkShare 做主源；优先 FRED/EIA。
- **铜铝长期基准**：继续优先 FRED/IMF 或 World Bank 月度，作为独立长期周期序列。
- **LME 3M 日价**：新浪/AkShare 为主源，东方财富/AkShare 为备源；只作为聚合 LME 3M 市场行情，和 LME 官方 Closing Price、Cash Price 分开定义。
- **国内现货和化工指数**：如果没有交易所官方源，AkShare/100ppi 可以作为免费源，但必须保留 `source_profile=100ppi_public_web` 和字段口径。
- **动力煤市场价**：使用国家统计局官方旬度“山西优混（5500 大卡）”批发和销售市场价格，保留观测旬与发布日期，不展开成伪日频。
- **动力煤长协**：不要用 AkShare 或国家统计局市场价猜长协价；长协应作为政策/事件表，由公告或人工确认维护。
- **动力煤政策区间首期落地**：发改价格〔2022〕303号自2022-05-01起规定秦皇岛港下水煤（5500千卡）中长期交易价格合理区间为含税 `570-770 CNY/ton`，且进口煤不适用。该记录写入政策事件表的 `value_low/value_high`，不自行计算 `value_mid`，不伪装成实际长协成交价或日行情。后续晋陕蒙出矿区间沿用同一配置化治理契约逐项验证。

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
| `scope_id` | 稳定配置 ID，例如 `fred_energy_oil`、`world_bank_metals`、`world_bank_fertilizers`、`lme_nonferrous`、`cn_100ppi_chemical` |
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
| `eia_energy_oil` | `["EIA"]` | `["energy"]` | WTI、Brent canonical 日频现货；EIA 主源、FRED 逐日期备源 |
| `world_bank_metals` | `["WORLD_BANK"]` | `["nonferrous"]` | 铜、铝等月度长期基准 |
| `world_bank_fertilizers` | `["WORLD_BANK"]` | `["fertilizer"]` | 磷矿石、DAP、TSP、尿素和氯化钾全球月度基准 |
| `world_bank_agriculture` | `["WORLD_BANK"]` | `["agriculture"]` | 油籽、谷物、糖、棉花和TSR20橡胶全球月度基准 |
| `fred_imf_metals` | `["FRED"]` | `["nonferrous"]` | FRED/IMF 铜铝月度价格 |
| `lme_nonferrous` | `["LME"]` | `["nonferrous"]` | LME 铜、铝、锌、铅、镍、锡 3M 聚合日线 |
| `cn_100ppi_chemical` | `["100PPI"]` | `["chemical"]` | 国内化工现货和基差 |
| `cn_coal_policy` | `["NDRC", "MANUAL"]` | `["coal"]` | 动力煤长协和政策价格事件 |

### 6.1.2 Calendar / Publication Governance

特殊商品数据不强制使用交易日历，但必须有可解释的时间治理：

| 数据类型 | 治理方式 |
|---|---|
| FRED/EIA 日频 | 使用观测日和发布滞后；非发布日不视为异常 |
| World Bank/FRED/IMF 月频 | 使用月度观测期；可按月末日期落库，DCF 读取时按估值日前最新可得值 |
| 100ppi 日频现货 | 只使用网页/API 实际观测日；缺失日记录为 source gap，不用中国交易日或 weekday 猜测补齐 |
| LME 3M 聚合日线 | 仅使用新浪主源实际返回日期；主源请求/结构失败时使用东财返回日期；不根据 weekday 推断交易日或休市日 |
| 国家统计局动力煤旬度价 | 使用旬期末作为 `observation_date`，同时保存旬期起止日和官方发布日期；只认国家统计局正文证据，不按交易日展开 |
| 动力煤长协/政策 | 使用 `effective_start/effective_end` 生效期，不做日频价格补点 |

因此，任务名称可以继续叫 `commodity_price_backfill/sync`，但报告中要区分 `trading_day_governance`、`publication_calendar_governance`、`policy_effective_period_governance`。

日期治理必须基于来源证据：

- 不允许用周一至周五、国内交易日历或月初/月末枚举结果冒充来源日历。
- FRED/EIA/100ppi 仅把实际返回的观测日期写入治理日历；若来源同时返回发布日期、修订日期或 realtime vintage，应一并保存。
- World Bank 以 Pink Sheet 工作簿中的实际月份为月度观测期，并保留工作簿更新时间/版本证据。
- LME 当前采用来源观测交易日治理：只有选定主/备源实际返回的日期可进入日历和行情落库；缺日先标记 source gap，未来接入可靠官方闭市公告后再增强为完整开闭市日历。
- 政策价和长协价使用生效期，不应派生虚构的逐日行情。

### 6.1.3 Master Data Governance

每一个启用的 `series_id` 都必须有来源级主数据治理，静态配置只能作为治理候选，不能单独视为“已验证”：

| 来源 | 具体主数据证据 |
|---|---|
| FRED | `series` metadata：官方 series id、title、frequency、units、observation_start、last_updated |
| EIA | API v2 route/facet 和数据字段：series description、frequency、units、首个观测日 |
| World Bank | Pink Sheet 工作簿列名、单位行、首末有效月份、工作簿更新时间 |
| 100ppi | 配置映射 + 实际返回字段、symbol、日期和值；无法从来源确认的规格/税基保持 partial |
| LME | AkShare 可用品种表、`futures_foreign_detail` 返回的 CFD/非期货属性、交易所、报价单位、每手吨数、tick、3M 说明，另加主备代码、payload 列和首末观测日；未知代码形成 discovery warning |
| 政策/长协 | 官方公告标题、发布机构、发布日期、生效期、价格口径和来源 URL |

治理结果写入独立的 `commodity_master_governance`，至少记录：`series_id`、来源名称、频率、币种、单位、生命周期、证据 URL/hash、治理状态、质量标记和更新时间。配置重新加载不得覆盖已经持久化的来源证据。

### 6.1.4 Unified Governance Pipeline

行情回补、日更和月更必须复用同一条流程：

```text
scope 解析
  -> provider/governance adapter registry 解析
  -> 主数据治理
  -> 获取一次来源 payload
  -> 基于 payload/官方日历完成日期治理
  -> 主数据与日期 gate
  -> 仅写入治理日期范围内的观测值
  -> 诊断和报告
```

如果观测日期只能从价格 payload 中获得，允许网络层先获取一次 payload 并在同一任务内复用，但必须在任何行情落库前完成日期治理。任务层不得识别来源页面字段，也不得针对某一商品写 skip 或特殊分支；所有差异都收敛在 adapter 层。

第一批 provider：

| Provider | 用途 |
|---|---|
| `FredCommodityProvider` | FRED WTI、Brent、IMF 铜铝月度 |
| `EiaCommodityProvider` | EIA 能源官方数据，作为油价主源或 FRED 校验源 |
| `WorldBankPinkSheetProvider` | 月度/年度大宗商品长期价格 |
| `AkshareCommoditySpotProvider` | 100ppi 现货和基差 |
| `AkshareForeignFuturesProvider` | 配置化外盘期货主备链；LME 使用新浪主源、东财备源 |
| `ManualPolicyEventProvider` | 动力煤长协/政策事件表导入 |
| `NbsProductionMaterialsProvider` | 国家统计局流通领域重要生产资料旬度市场价格；官方检索发现文章并解析产品表格 |

`AkshareCommoditySpotProvider` 必须采用配置驱动：只有当 `commodity_price_series.metadata`
中明确配置 `akshare_function`、日期列、数值列、原始单位、地区/规格或来源 URL 时才允许抓取；
否则任务必须以 `missing_100ppi_series_mapping` 阻塞该序列。通过 AkShare 获得的 100ppi/生意社
数据仍应写入 `source_profile=100ppi_public_web`，质量标记为 `aggregated_public_web`，
不得因为使用 AkShare 包装而标记为官方源。

当前已验证的具体实现边界：

- EIA 使用 API v2 数据集路由和 facet 查询（`petroleum/pri/spt/data`），不使用会忽略日期边界的旧 `seriesid` 兼容端点；provider 必须分页并再次按任务起止日期过滤。
- World Bank 使用官方 `CMO-Historical-Data-Monthly.xlsx` Pink Sheet 月度工作簿，按 `Monthly Prices` 中的 `Copper`、`Aluminum` 列解析；`api.worldbank.org` 普通国家/指标接口不提供这组 Pink Sheet 商品序列。
- 100ppi 已配置化落地 PTA 现货参考 `CMD.CN.CHEMICAL.PTA.SPOT.100PPI.DAILY`；后续品种沿用同一 provider/governance adapter，以独立商品、序列和 scope 逐个验证。甲醇 `CMD.CN.CHEMICAL.METHANOL.SPOT.100PPI.DAILY` 的源端可用起点为 2014-06-17；乙二醇 `CMD.CN.CHEMICAL.ETHYLENE_GLYCOL.SPOT.100PPI.DAILY` 的源端可用起点为 2018-12-10；PVC `CMD.CN.CHEMICAL.PVC.SPOT.100PPI.DAILY` 的源端可用起点为 2013-01-04；聚丙烯 `CMD.CN.CHEMICAL.POLYPROPYLENE.SPOT.100PPI.DAILY` 的源端可用起点为 2014-02-28。乙二醇、PVC 和聚丙烯使用 DCE 已验证交易日历进行覆盖诊断。各序列均通过 AkShare `futures_spot_price_daily` 包装 100ppi 页面，任务日期映射为接口的 `start_day/end_day`；单位为 `CNY/ton`，质量标记保持 `aggregated_public_web`，地区、规格和含税口径按来源披露。
- 国家统计局动力煤序列 `CMD.CN.COAL.THERMAL.SHANXI_BLEND_5500.NBS.TEN_DAY` 使用独立 `NBS` venue、`nbs_production_material_market_price` source profile 和 `cn_nbs_thermal_coal` scope。provider 通过国家统计局站内官方检索发现新旧栏目文章，解析正文中的产品名称、单位和本期价格；主数据治理核验“山西优混/5500 大卡/吨”，日期治理仅写入来源实际发布的旬期。历史起点按国家统计局说明设为 2014-01-10，配置和报告必须明确该序列不是长协价。
- 国家统计局该栏目旧标题将旬期明确写为每月 `1-10`、`11-20`、`21-30` 日；31 天月份的下旬治理日期仍为 30 日，2 月下旬使用当月最后一日。现代“上旬/中旬/下旬”标题必须沿用同一口径，不能用自然月末 31 日制造假缺口。
- 历史标题同时存在 `1-10日` 与 `1日-10日`、半角连字符与中文破折号等变体，NBS adapter 必须在来源实现层统一解析；任务治理层不得按年份或单个旬期添加跳过逻辑。官方发布日程明确取消的春节等旬期应配置为带证据 URL 的来源日历例外。
- NBS 来源日期治理必须分别报告理论旬期、官方证据支持的停发例外、实际发现旬期和未解决缺口。已治理停发例外不触发任务降级；缺少官方证据的空缺仍保持 `warning`，不得按周末或经验推断。
- 低频或发布制来源在任务窗口内可能不存在任何应披露观测。provider 明确返回 `expected_periods=0` 且 `unresolved_dates=0` 时属于“已治理合法空窗口”，主数据治理应复用该序列最后一次已验证的来源证据，日期治理应返回成功且不生成伪观测日；只有存在应披露日期却缺少来源证据时才允许 warning/block。任何来源的临时刷新失败都不得用 `blocked/unverified` 覆盖 `commodity_master_governance` 中最后一次成功证据，失败详情保留在本次 `ingestion_runs` 报告中。
- 对存在可稳定分页目录的国家统计局来源，adapter 必须以官方栏目目录为主发现路径，仅在目录未覆盖应披露统计期时才调用政府搜索接口补缺；目录已完整覆盖时不得为了交叉检索重复访问搜索接口。adapter 必须识别 HTTP 200 中的 JavaScript/WZWS challenge；直连网络失败或命中 challenge 时，通过项目统一代理配置按需提取少量新出口，成功后本任务后续官网页面沿用代理路径。所有调用搜索接口的 NBS adapter 均须遵守同一访问策略：HTTP 200 不代表业务成功，`ok=false`、缺少结果字段或长区间双向检索均无候选必须触发来源异常；`429/503` 或“访问频繁/稍后/繁忙”等临时错误执行有界退避，当前默认最多3次、间隔5秒和10秒；`code=-101` 或“当前网络地址已被禁用”先进行最多3次有界代理出口轮换，仍被阻断才立即打开任务级熔断，本轮不再继续广搜、翻页或逐期补查。代理出口同样可能被封，不能用无界重试或持续更换出口掩盖来源缺口；任何代理错误日志不得包含 token、Cookie 或代理凭据。
- 2026-07-12 真实烟雾验证：2014 年首期旧标题页面解析为观测期 2014-01-01 至 2014-01-10、发布日期 2014-01-14、价格 625.6 CNY/ton；2026 年 6 月下旬新标题页面解析为观测期 2026-06-21 至 2026-06-30、价格 854.6 CNY/ton。临时库端到端 dry-run 的主数据治理、日期治理和 provider 均为 success，未写生产库。
- 中长程特殊商品 provider 调用必须按配置间隔输出 heartbeat 日志，至少包含来源、序列、任务日期范围和累计耗时；任务开始/结束日志不能替代运行中的阶段进度日志。默认间隔为60秒。
- 特殊商品采集、治理、质量诊断和 heartbeat 属于数据任务日志，必须通过项目统一 `DataSource` task-domain logger 写入 `log/task.log`；不得使用未路由的模块根 logger 写入 `log/sys.log`。`sys.log` 仅保留应用初始化、服务、网络连接和系统运行日志。
- 全量和长窗口 dry-run 必须输出并保留每条序列的实际首末观测日、年度行数、数值范围、非正值、重复日期、最大绝对涨跌样本、原始/规范化币种单位。对配置了 `expected_calendar_exchange` 的日频序列，还必须使用数据库中已治理交易日历计算覆盖率、缺失日期、最长连续缺口和年度覆盖，禁止用 weekday 生成预期日期。
- 2026-07-11 PTA 全历史 dry-run 实际覆盖2013-01-04至2026-07-10，共3,276条；与可用的 CZCE 已验证交易日历重叠区间覆盖率为99.8084%，缺少2017-01-20、2017-01-26、2018-09-12、2022-01-28、2025-01-27五个孤立日期。100ppi 官方2018-09-12页面明确显示“暂无数据”；AkShare单日和区间接口共用同一100ppi页面，不构成独立备源。公开期货行情或不同机构的华东现货报道不得自动填入该100ppi序列，缺口保留并在诊断中披露。
- LME 第一批覆盖六种 3M 金属：`CAD/AHD/ZSD/PBD/NID/SND` 对应新浪主源，`LCPT/LALT/LZNT/LLDT/LNKT/LTNT` 对应东财备源。主源请求、空全量 payload 或必需列缺失时切换备源；历史回补还应以同一范围内其他 LME 品种的来源观测日期并集识别孤立缺日，仅对受影响品种请求东财并逐日补缺。正常日更在新浪覆盖请求日期时不请求东财，避免双源全量下载和东财反爬压力。观测值使用 close/latest，完整 OHLCV、实际来源、补缺原因和请求尝试写入 metadata。
- 跨源审计不能把所有缺日都判为数据错误。交易所公告可证明的品种级停牌或市场中断应作为通用 `observation_exceptions` 治理记录排除；例如 LME 镍 2022 年 3 月停牌窗口。东财也没有的日期若无公告证据，保留为 unresolved source gap，不使用 weekday 猜测。
- 报告必须输出主源行数、备源补齐数、未解决缺口、已治理例外和 `close` 超出来源 `low/high` 的统计。CFD/LME 3M 代理的 close 与盘中 OHLC 可能属于不同会话口径，此类记录保留原值并标记诊断，不擅自修正高低价。
- 2026-07-10 真实短窗口验证：`lme_nonferrous` 六品种在 2026-07-06 至 2026-07-09 由新浪主源获取 24 条观测，六条主数据治理和来源交易日治理均成功；强制主源映射失效后，铜由东财 `LCPT` 成功接管，生命周期起点为 2013-06-21。临时库 write 验证写入六条观测、六条主数据证据和六条来源交易日记录，未使用 weekday 推断。
- 2026-07-11 全历史审计：新浪 2016-07-11 至 2026-07-10 六品种初始返回 15,216 条；同品种日期并集发现镍缺5日、锡缺7日。镍缺日落在 LME 公告证明的 2022 年3月停牌窗口，按治理例外排除；东财 `LTNT` 补齐锡7日，最终 15,223 条，实际只增加1次东财请求，未解决缺口为0。六品种全量东财审计显示其历史约自 2013-06-21、OHLC 内部一致性更高，但与新浪重叠期并非同一收盘口径：锡平均绝对价差约68美元/吨、最大3,525美元/吨，镍最大850美元/吨。因此仍保留新浪主源、东财逐日补缺备源，不因覆盖更长而切换主源或无治理拼接2013-2016历史。
- 2026-07-11 东财主源倒置试验：通过统一 `akshare_proxy_patch` 成功抓取六品种 2013-06-21 至 2026-07-10 共 19,849 条原始记录，接口速度可接受且来源 OHLC 没有出现 close 超出 low/high；但 `2023-04-07` 仅铜、铝出现记录，铜与前一日 OHLC/close 完全重复，铝也基本重复，而锌、铅、镍、锡及新浪六品种均无该日记录，属于休市日疑似陈旧/伪观测。东财主源治理因此产生4个 unresolved gap warning。结论是东财技术可用但日期和收盘口径不优于新浪，且历史只多约3年而非10年，生产主备顺序不调整。

### 6.2 调度任务

当前特殊商品任务及显示语义：

| 任务 ID | 显示名称 | 频率 | 用途 |
|---|---|---|---|
| `commodity_price_master_sync` | 特殊商品主数据与序列字典同步 | 每周 | 同步主数据和 series 字典 |
| `special_commodity_overseas_daily_price_sync` | 海外特殊商品日频价格同步 | 周二至周六 08:00 | 同步 LME 3M 代理日线和 EIA/FRED WTI、Brent |
| `special_commodity_domestic_spot_price_sync` | 国内特殊商品现货价格与基准同步 | 周一至周五 22:30 | 同步100ppi国内现货、NBS动力煤旬价和CCTDA周频BSPI |
| `special_commodity_industrial_indicator_sync` | 大宗商品非价格产业指标聚合同步 | 周一至周五 16:30 | 在一个任务内按 scope 独立调度产量、库存、运费、进口量和仓单等非价格指标 |
| `special_commodity_international_monthly_price_sync` | 国际特殊商品月频价格基准同步 | 每月10日、20日 08:40 | 同步 FRED/IMF、World Bank 月度价格基准 |
| `special_commodity_policy_governance_sync` | 特殊商品政策目录发现与事件治理 | 每月15日 09:10 | 发现NDRC新增/修订政策、保存证据和候选，并在同一任务末尾幂等对账正式事件 |
| `special_commodity_observation_backfill` | 特殊商品价格与产业指标历史观测回补 | 仅手工 | 对指定 scope 和区间执行治理前置、dry-run 或正式回补 |
| `commodity_price_readiness_check` | DCF 商品输入就绪检查 | 每日或每周 | 检查 DCF 所需 commodity input 缺口 |

政策治理已经收口为单一月度运维入口 `special_commodity_policy_governance_sync`：任务自动完成目录发现、文号与版本去重、正文/附件存证、候选解析，以及既有配置政策和已批准候选的正式事件幂等对账。新候选不得自动批准；报告提供一条批准并提升命令和一条拒绝命令。正式事件的新增、更新、不变计数必须互斥，已批准候选若已被同语义正式事件覆盖，应单独报告而不得重复计入“不变”。原独立 `special_commodity_policy_event_sync` 任务已删除，底层 validator/service 仅作为月度发现和人工审核链路的共享实现；政策区间不得展开为日行情。

当前工程使用独立的 `special_commodity_*` 任务域，不把特殊商品现货并入国内五大交易所的 `futures_market_data_sync`。任务 ID 按“地域 + 数据语义 + 频率”命名，直接区分海外日频价格、国内现货价格、国际月频价格、非价格产业指标、政策治理和历史观测回补；旧的歧义 ID 已一次性删除且不保留别名。所有价格和指标入口复用同一个内部治理执行器，不形成平行链路。海外日频任务 `special_commodity_overseas_daily_price_sync` 在周二至周六 08:00（Asia/Shanghai）运行，覆盖 `lme_nonferrous` 与 `eia_energy_oil`；国内现货与价格基准任务 `special_commodity_domestic_spot_price_sync` 在周一至周五 22:30 运行，覆盖已完成历史回补和治理验证的 `cn_100ppi_chemical`、`cn_100ppi_methanol`、`cn_100ppi_ethylene_glycol`、`cn_100ppi_pvc`、`cn_100ppi_polypropylene`、`cn_100ppi_styrene`、`cn_100ppi_urea`、`cn_100ppi_caustic_soda`、`cn_100ppi_soda_ash`、`cn_100ppi_glass`、`cn_100ppi_asphalt`、`cn_100ppi_lpg`、`cn_100ppi_natural_rubber`、`cn_100ppi_softwood_pulp`、`cn_nbs_thermal_coal` 与 `cn_coal_bspi`。

`special_commodity_industrial_indicator_sync` 是唯一的产业指标域聚合任务，不为每个产品或数据源创建新任务。调度配置通过 `scope_runs` 为每个 scope 独立声明启用状态、运行日和观测窗口；任务逐 scope 执行统一 provider/governance/persistence 流水线，单一 scope 失败不得阻止其他 scope，最终只发送一份聚合报告。当前 `cn_coal_cbcfi` 使用 `provider_latest` 窗口，`cn_coal_bohai_port_inventory` 使用21日滚动窗口，`cn_nbs_raw_coal_output` 使用月度四个月回看窗口并在每月15日至22日到期运行；三个 scope 均已完成历史/可用窗口验证、正式写入、幂等复验和共享调度验收。未来新增产量、库存、运费、进口量或仓单指标时，只增加 scope 和 adapter，不增加同目的调度任务。

2026-07-14，`cn_nbs_raw_coal_output` 全历史 dry-run `run_id=148` 获取48条官方累计产量观测，覆盖2022-02至2026-05，预期月份48、发现48、未解决缺口0。正式写入 `run_id=149` 新增48条观测、48条来源月份治理和1条主数据治理记录；回补入口幂等复验 `run_id=150` 为新增0、变更0、不变48。三个阶段均为主数据治理成功、日期治理成功且无 warning/blocker。应用重启后的首次到期共享调度 `run_id=163` 使用月度四个月回看窗口获取2期并全部保持不变，0 warning、0 blocker，scope 到期判断、窗口和聚合报告验收均已完成。

滚动窗口任务默认回看最近10个自然日，而来源窗口任务使用来源公开边界。各 scope 隔离来源、频率、报告和日期治理。国内现货、官方旬价和产业指标不进入期货连续合约任务，因为来源观测日、单值价格或指数和缺口治理不同于交易所交易日、合约生命周期和 OHLCV 治理。原始 FRED 序列继续独立保存用于来源审计；其他100ppi品种需完成逐品种治理和历史验证后再加入国内现货任务。World Bank/FRED-IMF 月频和政策事件继续使用独立频率。08:00缓存预热保持在08:20，避免同分钟竞争。其他特殊商品任务继续保持手工或未启用状态：

`fred_imf_metals`、`world_bank_metals`、`world_bank_fertilizers` 与 `world_bank_agriculture` 使用同一个 `special_commodity_international_monthly_price_sync`，每月10日、20日 08:40（Asia/Shanghai）运行并滚动回看最近6个月。双月更用于覆盖 World Bank 月初发布以及 IMF 数据经 FRED 转发时可能出现的额外延迟，回看窗口同时吸收历史修订；观测日期表示统计月份，不表示月初当日成交价。World Bank 金属、化肥与农产品是各自独立的 Pink Sheet 全球月度基准，不得覆盖、平均或伪装成 IMF/FRED 或国内100ppi现货。每个 scope 须先完成全历史 dry-run、正式落库和幂等验证，验证通过后才可加入月更任务。

月更报告中的 `changed` 必须表示规范价格语义发生变化，包括规范值/原始值、币种、单位、来源代码或质量标记变化。FRED `realtime_start/realtime_end`、请求日期、来源 URL、parser 版本或其他审计 payload 单独变化时，不得误报为价格修订；任务仍须更新并保留最新 metadata 与 `raw_payload_hash`，供来源追溯和 vintage 审计。

2026-07-11 World Bank 全历史 dry-run 验证成功：铜、铝各798个月，覆盖1960-01至2026-06，主数据与月份治理均成功，无 warning/blocker。与本地 FRED/IMF 1992-01至2026-05的413个重叠月份完全对齐，均为 `USD/metric_ton`；铜平均相对差0.116610%、月收益相关性0.999246，铝平均相对差0.116405%、月收益相关性0.999018。正式写入 `run_id=76` 新增1,596条观测、1,596条来源月份治理记录和2条主数据治理证据，零 warning/blocker。验证后 `world_bank_metals` 已与 `fred_imf_metals` 一同加入独立月更任务，但始终保持独立 `series_id` 和来源 lineage，不构造跨源 canonical 平均值。

原油 source-chain 必须遵循统一规则：同日优先 EIA；EIA 缺少而 FRED 存在时才使用 FRED；同日数值不一致时保留 EIA，并记录两源值、差异和实际来源；不得无来源地覆盖或平均。

FRED/EIA 官方 API 的主数据与观测值请求必须共用有界重试策略。SSL EOF、连接重置等瞬时网络错误在重试耗尽前不得直接把单一序列判定为主数据阻断；每次重试必须记录请求阶段、尝试次数和退避时间。

- `/run special_commodity_overseas_daily_price_sync scope_id=fred_energy_oil start=YYYY-MM-DD end=YYYY-MM-DD dry_run`
- `/run special_commodity_observation_backfill scope_id=fred_energy_oil start=YYYY-MM-DD end=YYYY-MM-DD dry_run`
- `/run special_commodity_observation_backfill scope_id=eia_energy_oil start=YYYY-MM-DD end=YYYY-MM-DD dry_run`
- `/run special_commodity_observation_backfill scope_id=world_bank_metals start=YYYY-MM-DD end=YYYY-MM-DD dry_run`
- `/run special_commodity_observation_backfill scope_id=lme_nonferrous start=YYYY-MM-DD end=YYYY-MM-DD dry_run`
- `/run special_commodity_observation_backfill scope_id=cn_100ppi_chemical start=YYYY-MM-DD end=YYYY-MM-DD dry_run`
- `/run special_commodity_observation_backfill scope_id=cn_nbs_thermal_coal start=YYYY-MM-DD end=YYYY-MM-DD dry_run`
- `/run special_commodity_policy_governance_sync adapter_id=ndrc start_date=YYYY-MM-DD end_date=YYYY-MM-DD dry_run`
- `/run special_commodity_calendar_governance scope_id=fred_energy_oil start=YYYY-MM-DD end=YYYY-MM-DD dry_run`

部署后 API 统一位于 `/api/v1` 前缀下：

- `GET /api/v1/research/commodities/dictionary`：商品及序列字典。
- `GET /api/v1/research/commodities/series?active_only=true`：启用序列。
- `GET /api/v1/research/commodities/observations?series_id=...&start_date=...&end_date=...`：指定序列观测值。
- `GET /api/v1/research/commodities/diagnostics?target_currency=CNY`：覆盖、无本地观测序列、治理质量和本地 FX 依赖诊断；该字段不伪装成按频率计算的 freshness 判断。
- `GET /api/v1/research/commodities/policy-events?commodity_id=...`：政策/长协事件及有效期。
- `GET /api/v1/research/commodities/policy-candidates?review_status=...`：政策解析候选及短审核码。
- `POST /api/v1/research/commodities/policy-candidates/review?...`：按短码、完整 ID 或文号审核，并可通过统一 validator 一步提升。
- `GET /api/v1/research/commodities/source-documents?source_profile=...`：官方来源文档版本和哈希证据，不返回正文。
- `GET /api/v1/research/commodities/indicators?...`：按分类、序列和日期读取非价格产业指标。
- `GET /api/v1/research/commodities/series-candidates?...`：读取未进入正式主数据的扩品候选。
- `POST /api/v1/research/commodities/series-catalog-sync?dry_run=true`：执行扩品发现，不自动上线。
- `POST /api/v1/research/commodities/policy-discovery?...`：执行官方政策目录发现，默认 dry-run。
- `POST /api/v1/research/commodities/price-sync?...`：受 API 工作负载保护的价格同步。
- `POST /api/v1/research/commodities/calendar-governance?...`：观测日/发布期治理。

调度要求：

- DCF 请求不得实时触发远程下载，只读取本地已同步数据。
- DCF 如需目标币种换算，应调用独立 FX 数据模块的本地治理结果；商品同步任务不得维护外汇数据。FX 模块已独立落地后，商品诊断和 DCF readiness 应优先读取 `fx.db` 本地转换结果；只有 FX 缺失、过期或质量不足时才报告 `requires_fx_conversion`。
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
| 全球铜价 mid-cycle | `CMD.METAL.COPPER.IMF.FRED.MONTHLY` 或 World Bank 月度；LME 3M 日线用于当前景气校验 |
| 全球铝价 mid-cycle | `CMD.METAL.ALUMINUM.IMF.FRED.MONTHLY` 或 World Bank 月度；LME 3M 日线用于当前景气校验 |
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
- 若整合源在 adapter 内已做单位换算，观测构造层必须通过配置化换算因子同时保留上游 `raw_value/raw_unit` 和标准化 `value/unit`；不得将 adapter 输出单位冒充上游原始单位。玻璃现货按 100ppi/AkShare 规则从 `CNY/m2` 以因子 `80` 换算为 `CNY/ton`。
- 动力煤长协不伪装成日频行情；先支持政策/事件表。
- API 能查询商品字典、序列、观测值、诊断指标。
- DCF readiness 能识别缺少油价、铜铝、动力煤或现货增强数据的输入缺口。
- DCF readiness 能识别商品输入需要外汇模块提供目标币种换算的依赖缺口；当本地 FX 模块已提供可用转换时，readiness 应记录 FX series、FX 日期、汇率、转换策略和 lineage，而不是继续报缺口。

### 8.2 质量门槛

- 不同来源同一商品不得静默混写，必须保留 `source_profile`。
- 单位不一致时不得直接比较或计算分位，必须先标准化。
- 聚合源不得标记为 official。
- 现货规格、地区、热值、税费、计价方式不明时，`quality_flag` 不得高于 `partial`。
- API key 不得写入 git、文档或日志。
- 商品层不得维护或生成外汇汇率；不同币种或单位不得未经独立 FX/单位标准化结果直接进入 DCF 或价差计算。商品 provider 只能写原始商品观测值，目标币种视图应由读取/诊断/DCF 输入准备阶段通过本地 FX 服务生成并保留 lineage。

---

## 9. OpenSpec 实施状态

本需求已由以下两个 change 完成：

1. `add-special-commodity-market-data-layer`
   - 建库表、provider 抽象、FRED/EIA/World Bank/100ppi 接入、API 查询、调度任务。
2. `connect-commodity-data-to-cyclical-dcf`
   - 将 Brent/WTI、铜铝、动力煤政策事件、化工现货/基差纳入 DCF input bundle、readiness 和周期诊断。

运行环境要求：

- FRED API key。
- EIA API key，用于当前 WTI/Brent 官方主源。
- LME.com 账号不再是当前聚合行情方案的运行依赖。

后续政策发现、实际长协证据、煤炭库存/港口价/运费及更多连续商品序列的增强需求，见 [特殊商品政策、产业证据与行情目录增强需求说明书](special_commodity_market_data_enhancement_requirements.md)。该增强拆分为 `enhance-commodity-policy-and-industrial-evidence` 与 `expand-special-commodity-series-catalog` 两个 OpenSpec change，避免将低频政策治理和 100ppi 行情扩品混为同一任务。

截至 2026-07-15，沿海煤炭运价已按统一工业指标契约增加上海航运交易所 CBCFI 综合指数 provider、主数据、来源日期治理和独立 scope。官方匿名页只提供本期和上期，两期均按请求区间接收；因此当前能力仍是从部署日起持续积累，不代表已完成 2011 年以来的历史回补。该 scope 已进入共享的 `special_commodity_industrial_indicator_sync` 聚合调度，不进入任何价格日更任务。

环渤海港口煤炭库存已复用同一工业指标契约实现 CCTDA TTCI 周报 adapter、主数据、来源周期/发布日期治理和 `cn_coal_bohai_port_inventory` scope。该序列仅保存中国煤炭运销协会周报披露的环渤海港口/四港合计库存事实值，单位为 `10k_ton`，观测日为周报统计期末，发布日期单独保存；不得解释为港口吞吐量、沿海电厂库存或商品价格，也不得把 TTCI 指数点当作 `CNY/ton`。公开 TTCI 目录自2024-08-02起可枚举，但早期周报并未披露该库存指标，当前同口径指标起点为首次实际披露日2025-02-08。adapter 优先解析“截止日+环渤海四港合计库存”的正式统计句，避免被正文中其他港口、电厂库存或调入调出值污染；对旧的港口统计句，仅当存在唯一符合配置数量级的候选时进行字段对齐并保留 reconciliation 标记。“报告存在但未披露正式库存指标”计入来源覆盖诊断，不因泛化提及港口库存而误报解析故障。该 scope 已完成全历史 dry-run、生产 write、重复 write 和共享调度幂等验收，当前由 `special_commodity_industrial_indicator_sync` 以21日滚动窗口维护。

港口价格按独立语义治理。CCTD 5500K/5000K/4500K 日频现货参考价因许可限制和公开历史连续性不足继续阻断；不能因港口库存或 BSPI 来源可用而放行。CCTDA 公开文章目录已落地独立 BSPI 周频价格 adapter、`association_public_price` 治理、`CMD.CN.COAL.PORT_PRICE.BSPI.CCTDA.WEEKLY` 和 `cn_coal_bspi` scope。该序列为 `CNY/ton`、`data_kind=market_price`，只保存价格事实、明确周报周期、发布日期和 URL，不保存正文、不调用 robots 禁止的结构化 API，也不替代 CCTD 日价或实际长协价。全历史 dry-run `run_id=166` 形成45条观测且0解析失败/未解决日期；生产 `run_id=167` 新增45条，重复 `run_id=168` 全部不变；重启后聚合 `run_id=169` 成功加载全部16个国内价格 scope，0 warning、0 blocker。BSPI 已由 `special_commodity_domestic_spot_price_sync` 正式维护。

同日已按相同工业指标契约实现国家统计局规模以上工业原煤累计产量 provider、主数据和 `cn_nbs_raw_coal_output` scope。该序列保存国家统计局官方年初至统计月累计绝对量，单位为 `10k_ton`；1—2月合并口径不会被拆成单月值，后续月份也不自动通过累计差分生成“官方月度产量”。生产 dry-run 曾因 adapter 只接受 `/sj/zxfb/` 而漏掉发布在 `/sj/zxfbhjd/` 的2026年4月文章，同时辅助搜索接口对本机 IP 返回禁用，形成47/48期 warning。adapter 现已将两个国家统计局官方发布栏目纳入同一来源契约，并改为先扫描官方目录、只对未解决统计期调用辅助搜索；目录完整覆盖48期时搜索请求为零。搜索临时限流按统一配置有界退避，`-101` IP禁用则立即熔断本轮搜索，避免继续消耗被封出口。修复后的2022-02-28至2026-05-31隔离全历史 dry-run取得48/48期，主数据治理和来源日期治理均成功，零 warning/blocker；生产 write 写入48期，重复 write 为48期 `unchanged`。对2017年以来的向前探测确认，2017—2021年旧版工业增加值文章不稳定包含同精度原煤绝对量，因此当前生产历史起点为2022-02-28；更早能源生产材料需作为独立来源完成精度和口径治理后才能扩展。首次实际到期共享调度 `run_id=163` 已验证月度四个月回看窗口、到期判断和聚合报告，获取2期且全部不变，无 warning/blocker。
