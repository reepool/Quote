# 商品期货行情数据获取和更新维护系统需求说明书

> 更新日期：2026-06-13
> 适用项目：Quote System / Research Data Engine
> 文档定位：本说明书用于定义商品期货行情与周期景气数据层的业务需求、数据架构、更新维护、对外服务、验收标准和后续 OpenSpec 拆解边界。本文聚焦商品信息获取、存储、更新维护和对外提供；DCF 估值模型公式、行业估值模板和投资结论不在本会话范围内。
> 当前 OpenSpec：`add-futures-market-data-module`、`prioritize-official-futures-sources`
> 使用边界：本系统为量化研究和估值输入提供可审计数据，不提供商品交易信号，不替代人工投资判断。

---

## 1. 结论摘要

### 1.1 需求合理性判断

当前需求方向合理，而且与本项目现有状态匹配：

1. 现有专业 DCF 配置已经包含 `cyclical_fcff_midcycle.v1`，并预留了 `commodity_price_assumption`、`cycle_index_level`、`midcycle_operating_margin` 等输入字段；商品价格与周期景气数据层正好补齐这条路径。
2. 项目已有研究域的通用形态：`research/providers/*` 数据源封装、`research/storage.py` 持久化、`ingestion_runs` 和 `raw_payload_audit` 审计、`config/10_research.json` 研究域配置、`config/05_scheduler.json` 调度配置、`api/routes.py` 对外服务、readiness 接口和输入缺口诊断。
3. 周期资源行业 DCF 的核心问题不是“拿到最新价格”，而是用足够长、可追溯、口径明确的历史序列判断 mid-cycle price、景气分位、价差分位和均值回归风险。

需要收敛的地方：

1. 第一阶段不应追求全商品、全现货、全库存、全开工率覆盖。免费或低成本来源中，最可控的是国内期货日线、主力连续或指数连续、少量海外基准和可计算价差。
2. 现货价格、产业库存、开工率、公司产销量和单位成本很重要，但口径复杂、付费比例高、自动化稳定性弱，应作为 P1/P2 增强层，不作为第一版 DCF 数据层上线 blocker。
3. 商品行情系统应是研究域数据资产，不应混入股票日行情主更新任务，也不应直接写入现有 `daily_quotes` 股票行情表。它需要独立的商品标的主数据、价格时序、连续合约构造、价差指标、周期诊断和 readiness。

### 1.2 第一阶段目标

第一阶段建设一个可上线、可审计、可回补、可供 DCF 调用的商品期货行情主数据系统：

- 覆盖国内主要商品期货品种的日线历史数据和最新可得价格。
- 支持主力连续、指数连续或近月合约等研究口径，并明确构造方法。
- 计算 3 年、5 年、10 年均值、分位数、偏离度、波动率、周期高低位等诊断指标。
- 支持产品-原料价差的版本化定义与派生计算。
- 建立行业/公司到商品暴露的映射表，供周期行业 DCF 选择输入。
- 对外提供商品价格、周期诊断、价差、行业映射和 readiness 查询接口。
- 与现有 DCF readiness、input gaps 和 `cyclical_model_diagnostics` 对接，但不在商品系统内计算最终估值。

---

## 2. 与现有项目架构的关系

### 2.1 现有可复用能力

商品期货数据层应沿用项目已有研究域模式：

| 现有能力 | 复用方式 |
|---|---|
| `research/providers/*` | 新增商品行情 provider，封装 AKShare、交易所、海外公开源，不在业务逻辑中直接调用第三方 API |
| `research/storage.py` | 新增商品数据表、upsert、查询、readiness、审计接口 |
| `ingestion_runs` | 每次商品同步、回补、价差重算、诊断重算都记录 run id、source、mode、status、metadata |
| `raw_payload_audit` | 保存第三方源原始响应摘要、hash、字段映射和异常样例，便于复盘 |
| `config/10_research.json` | 新增 `commodity_market_data` 模块配置、来源优先级、品种清单、派生指标版本 |
| `config/05_scheduler.json` | 新增商品日更、周度回补、月度完整性检查任务 |
| `api/routes.py` / `api/models.py` | 当前实现新增 `/research/futures/...` 查询、readiness 和诊断接口；业务语义仍为商品期货数据层 |
| `ResearchStorageManager` readiness 风格 | 商品域也输出 coverage、missing、source fallback、quality flags 和 blockers |
| DCF bounded cache | DCF 按请求读取商品诊断结果，仍保持估值结果 bounded cache，不长期全市场落库 |

### 2.2 不应复用或不应混用的部分

以下边界必须明确：

- 不把商品期货序列塞进股票 `daily_quotes` 主行情表，避免股票复权、交易所、标的生命周期和质量规则混淆。
- 不让 `daily_data_update` 自动承担商品大宗数据同步；商品域应有独立 job，避免拖慢 A 股日更。
- 不在 DCF 请求中隐式触发远程商品行情下载。DCF 只能读取本地已同步数据或返回 input gap，防止估值 API 出现不可控延迟和来源漂移。
- 不把现货、库存、开工率作为第一阶段硬依赖。缺失时输出 warning 和较低诊断置信度，而不是阻塞全部周期模型。
- 不把商品价格诊断解释为买卖建议或商品择时信号。

---

## 3. 业务目标和非目标

### 3.1 业务目标

商品期货行情数据层应支持以下研究问题：

1. 当前核心商品价格处于历史周期的什么分位？
2. 当前价格、价差是否明显高于或低于 mid-cycle 水平？
3. 当前利润率是否可能受周期顶部或底部扭曲？
4. 不同周期行业应参考哪些收入端商品、成本端商品和价差指标？
5. DCF 输入中的商品假设来自哪个来源、哪个口径、哪个日期、哪个计算版本？
6. 对某家公司，周期模型缺少哪些商品输入，是否可以降级为研究模式？

### 3.2 非目标

第一阶段不追求：

- 付费产业数据库全覆盖。
- 全市场所有商品现货价格自动化。
- 高频或分钟级商品行情。
- 商品期货交易策略、CTA 信号、仓位建议。
- 自动解析所有上市公司产销量、单位成本和长协比例。
- DCF 模型本身的完整行业公式实现。
- 对外提供实时交易级行情服务。

---

## 4. 数据范围和优先级

### 4.1 数据层优先级

| 数据层 | 必要性 | 阶段 | 用途 |
|---|---:|---:|---|
| 商品期货日线历史 | 必需 | P0 | mid-cycle、分位数、波动率、历史高低位 |
| 商品最新可得价格 | 必需 | P0 | 估值日诊断、景气位置、敏感性起点 |
| 主力连续/指数连续/近月连续 | 必需 | P0 | 长周期比较，降低换月干扰 |
| 产品-原料价差 | 需要 | P0/P1 | 中游加工利润、吨毛利周期 |
| 海外核心基准 | 需要 | P0/P1 | 全球定价商品，例如铜、油、黄金、铁矿石 |
| 现货价格/产业指数 | 增强 | P1 | 更接近企业销售或采购口径 |
| 库存、仓单、开工率、产量 | 增强 | P2 | 供需周期验证 |
| 公司产销量、单位成本、长协比例 | 增强 | P2 | 量价成本模型和公司级 DCF |

### 4.2 第一阶段 P0 覆盖清单

P0 优先选择流动性较好、可通过国内交易所或 AKShare 低成本获取、对 A 股周期行业解释力强的品种。

| 行业 | P0 品种 |
|---|---|
| 煤炭 | ZC 动力煤、JM 焦煤、J 焦炭 |
| 钢铁黑色 | I 铁矿石、RB 螺纹钢、HC 热卷、SS 不锈钢、SF 硅铁、SM 锰硅 |
| 有色金属 | CU 铜、AL 铝、AO 氧化铝、ZN 锌、PB 铅、NI 镍、SN 锡、AU 黄金、AG 白银 |
| 油气能源 | SC 原油、FU 燃料油、LU 低硫燃料油、BU 沥青、PG LPG、Brent、WTI |
| 化工 | TA PTA、PX 对二甲苯、EG 乙二醇、MA 甲醇、V PVC、L LLDPE、PP 聚丙烯、EB 苯乙烯、SA 纯碱、SH 烧碱、UR 尿素 |
| 橡胶 | RU 天然橡胶、NR 20 号胶、BR 丁二烯橡胶 |
| 新能源材料 | LC 碳酸锂、SI 工业硅、PS 多晶硅 |
| 建材造纸 | FG 玻璃、SP 纸浆 |

### 4.3 P1/P2 增强清单

| 类别 | 示例 | 说明 |
|---|---|---|
| 海外金属 | LME 铜、铝、锌、铅、镍、锡 | 全球定价商品的国际锚，注意授权和延迟 |
| 能源基准 | Henry Hub、TTF、JKM、Dubai/Oman | 油气和化工成本端增强 |
| 国内现货 | 秦皇岛 5500 动力煤、SMM 有色、Mysteel 钢材、百川化工、生意社价格 | 口径复杂，需保留地区、规格、税费和单位 |
| 库存仓单 | 交易所仓单、LME 库存、港口库存、社会库存 | 第一阶段只建议交易所仓单或官方库存作为可控试点 |
| 开工率产量 | 高炉开工、电炉开工、化工开工率、产能利用率 | 多来自资讯商，应配置为可选源 |
| 公司经营数据 | 年报产销量、单位成本、长协价格比例 | 后续与公告解析、财务事实层协同 |

---

## 5. 数据来源原则

### 5.1 来源优先级

| 来源类型 | 优先级 | 使用原则 |
|---|---:|---|
| 交易所官方日行情 | P0 | 权威源，优先用于原始合约日线；当前同步路由先请求官方源，再按持仓量/成交量本地构造主力连续 |
| AKShare | P0 fallback | 仅作为官方源禁用、不支持、空返回或失败后的兜底补源；必须保留接口名、版本、mode、字段映射和 fallback 记录 |
| 海外官方公开源 | P1 | 适合 Brent、WTI、Henry Hub 等公开时间序列；注意单位和时区 |
| Yahoo/FRED/EIA 等公开源 | P1 | 可作海外基准研究源，生产使用要标记授权和延迟限制 |
| 产业资讯商 | P1/P2 | Mysteel、SMM、百川、生意社等；需区分免费/付费、授权、口径和稳定性 |
| 人工配置或手工导入 | P2 | 作为临时补充，必须有 analyst/manual 标记和生效日期 |

### 5.2 供应商封装要求

新增 provider 时必须满足：

- 通过统一 provider 类封装，不在 DCF、API 或 sync 逻辑中直接散落 AKShare 或网页调用。
- 每个 provider 声明 `source_name`、`source_mode`、`source_profile`、`supported_markets`、`supported_granularities`。
- 对空返回、字段缺失、单位异常、重复日期、日期倒序、网络失败分别给出明确错误或 warning。
- 网络集成测试与单元测试分离，单元测试使用 fixture，不依赖实时行情。

### 5.3 官方主源落地状态

本轮 OpenSpec `prioritize-official-futures-sources` 将官方交易所源从“预留来源”推进为可执行 provider。当前实现不进行真实历史回填落库，重点是落实主源路由、字段标准化、主力序列构造和 fallback 可审计性。

| 交易所 | 主源接口 | 当前能力 | 兜底 |
|---|---|---|---|
| SHFE | `https://www.shfe.com.cn/data/tradedata/future/dailydata/kxYYYYMMDD.dat` | 解析官方合约日行情，按品种筛选，按持仓量/成交量构造主力连续 | `akshare_futures` |
| INE | `https://www.ine.cn/data/tradedata/future/dailydata/kxYYYYMMDD.dat` | 与 SHFE 同族 payload 解析，支持原油、低硫燃料油、20 号胶等 | `akshare_futures` |
| DCE | `http://www.dce.com.cn/dcereport/publicweb/dailystat/dayQuotes` | 解析官方日行情 JSON，规范 OHLC、结算价、成交量、持仓量、成交额 | `akshare_futures` |
| CZCE | `http://www.czce.com.cn/cn/DFSStaticFiles/Future/YYYY/YYYYMMDD/FutureDataDaily.txt` | 解析官方日行情文本，规范合约、价格、成交量和持仓量 | `akshare_futures` |
| GFEX | `http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadList` | 解析官方日行情 JSON，支持碳酸锂、工业硅、多晶硅等 | `akshare_futures` |

统一口径：

- 存储目标仍为 `data/futures.db` 的 `futures_price_bars`，不混入股票行情表。
- 价格字段统一为 `open/high/low/close/settlement`，数量字段统一为 `volume/open_interest/amount`，币种和单位继承 `futures_series` 主数据。
- 官方源构造的主力连续写入 `source_profile=exchange_official`、`source_interface=official_*`、`construction_method=official_open_interest_main`，并在 `metadata.underlying_contract` 保留当日选中的真实合约。
- AkShare 只在官方源 disabled、unsupported、empty 或 failed 时调用；同步结果和 ingestion metadata 输出 `official_status`、`fallback_status` 和 `source_selection` 计数。
- readiness 输出 `official_bar_count`、`fallback_bar_count`、`source_profiles`，并对仅有兜底覆盖的序列标记 `fallback_only_coverage`。

剩余未做：

- 尚未执行全量历史回填，也未验证每个交易所在十年以上历史区间的接口可得性和限流边界。
- 尚未实现海外官方基准、现货、库存、仓单、开工率等 P1/P2 数据源。
- 官方源的成交额单位仍按交易所原始口径保留并标记 `amount_unit_exchange_reported`，后续如需跨交易所金额比较，应增加显式单位换算规则。

---

## 6. 商品主数据设计

### 6.1 商品标的主数据

需要建立独立商品标的注册表。当前工程落地使用 `futures_instruments`，以便和专用 `futures.db`、`/research/futures/*` API 命名一致；下文 `commodity_*` 表名均表示业务语义，实际实现以 `futures_*` 为准。

核心字段：

| 字段 | 说明 |
|---|---|
| `commodity_id` | 系统内部商品标的 ID，例如 `CNF.CU.SHFE`、`CNF.I.DCE`、`INT.BRENT.ICE` |
| `symbol` | 交易所或来源代码，例如 `CU`、`I`、`SC` |
| `name` | 中文名称 |
| `english_name` | 英文名称，可选 |
| `category` | 能源、黑色、有色、化工、农产品、新能源材料等 |
| `exchange` | SHFE、INE、DCE、CZCE、GFEX、LME、CME、ICE、SGX、EIA、FRED 等 |
| `market` | CN、GLOBAL、US、EU、HK 等 |
| `currency` | CNY、USD、EUR 等 |
| `unit` | 元/吨、美元/桶、美元/吨、元/克等 |
| `price_multiplier` | 单位标准化倍率 |
| `quote_type` | futures_contract、continuous_main、continuous_index、spot、industry_index |
| `active` | 是否启用同步 |
| `priority` | P0/P1/P2 |
| `source_profiles` | 可用来源列表 |
| `lineage_notes` | 口径说明 |

### 6.2 合约和连续序列

商品期货必须区分真实合约和连续序列：

| 对象 | 说明 |
|---|---|
| `contract` | 实际可交易合约，例如 `CU2601` |
| `main_continuous` | 主力连续，适合研究但存在换月规则 |
| `index_continuous` | 指数连续或加权连续，适合长期周期比较 |
| `nearby_continuous` | 近月连续，适合期限结构和短周期诊断 |

连续序列必须记录：

- 构造方法：source_native、open_interest_roll、volume_roll、calendar_roll、vendor_index。
- 是否回调平滑：none、ratio_adjusted、difference_adjusted。
- 换月规则和阈值。
- 每个交易日对应的底层真实合约。
- 构造版本和参数 hash。

第一阶段可以优先接受 AKShare 原生主力连续或指数连续，但必须把 `construction_method=source_native`、`source_interface`、`source_version` 写入 lineage，不能把它误标为交易所官方原始合约。

---

## 7. 存储架构需求

### 7.1 数据库归属

建议商品域放在研究数据体系内，优先使用独立 SQLite 文件或研究域可迁移表组：

- 推荐并已纳入本轮实现：`data/futures.db`，便于与 `research.db`、`valuation.db` 分离，降低大时序数据对既有查询的影响。
- 可接受：先落在 research storage 管理下，但表组必须独立，便于后续迁移到 DuckDB/PostgreSQL。

无论选择哪种物理文件，代码层应通过 `ResearchStorageManager` 或独立 `CommodityStorageManager` 统一访问，不允许业务层直接散写 SQL。

### 7.2 核心表

建议第一阶段最少包含以下表：

| 表 | 作用 |
|---|---|
| `futures_instruments` | 商品期货标的主数据 |
| `futures_series` | 主力连续、指数连续、近月连续等研究序列元数据 |
| `futures_price_bars` | 日线价格序列，含真实合约和连续序列 |
| `futures_continuous_mapping` | 连续序列每日对应底层合约和换月信息；阶段二完善 |
| `futures_spread_definitions` | 价差公式定义、权重、单位、版本 |
| `futures_spread_values` | 派生价差时序 |
| `futures_cycle_diagnostics` | 分位数、均值、偏离、波动率、景气状态 |
| `futures_exposure_mappings` | 行业/公司到收入端、成本端、价差的映射 |
| `futures_source_manifests` | 来源接口、字段映射、版本和拉取参数 |
| `futures_readiness_snapshots` | 覆盖率、缺口、异常、fallback 和质量摘要 |

### 7.3 价格序列字段

`futures_price_bars` 至少需要：

| 字段 | 说明 |
|---|---|
| `commodity_id` | 商品标的 ID |
| `series_id` | 序列 ID，例如 `CNF.CU.SHFE.main` |
| `trade_date` | 交易日期 |
| `open` / `high` / `low` / `close` | OHLC |
| `settlement` | 结算价，可为空但应优先保留 |
| `volume` | 成交量 |
| `open_interest` | 持仓量 |
| `amount` | 成交额，可选 |
| `currency` | 币种 |
| `unit` | 价格单位 |
| `source` | 来源 |
| `source_mode` | direct、proxy_patch、manual_import 等 |
| `source_interface` | 具体接口名 |
| `quote_type` | contract、main_continuous、index_continuous、nearby_continuous、spot |
| `underlying_contract` | 连续序列当日对应真实合约，可为空 |
| `adjustment_method` | none、ratio_adjusted、difference_adjusted 等 |
| `quality_flag` | ok、partial、stale、missing_fields、unit_unverified 等 |
| `raw_payload_hash` | 原始响应 hash |
| `ingestion_run_id` | 同步 run |
| `created_at` / `updated_at` | 审计时间 |

主键建议：

```text
(series_id, trade_date, source, source_mode)
```

如后续要支持多来源并存，应通过 `source_priority` 或 read path 参数选择默认源，不覆盖其他来源。

### 7.4 周期诊断字段

`futures_cycle_diagnostics` 至少需要：

| 字段 | 说明 |
|---|---|
| `series_id` | 商品序列 |
| `as_of_date` | 诊断日期 |
| `lookback_years` | 3、5、10、full |
| `latest_price` | 最新价格 |
| `mean_price` | 窗口均值 |
| `median_price` | 窗口中位数 |
| `percentile` | 当前价格分位数 |
| `z_score` | 标准化偏离 |
| `drawdown_from_high` | 相对窗口高点回撤 |
| `distance_from_low` | 相对窗口低点位置 |
| `rolling_volatility` | 滚动波动率 |
| `mean_deviation_pct` | 相对均值偏离 |
| `cycle_state` | high、normal、low、extreme_high、extreme_low、insufficient_history |
| `history_coverage_ratio` | 窗口覆盖率 |
| `min_required_observations` | 最低观测数 |
| `calc_method` / `calc_version` | 计算方法和版本 |
| `input_hash` / `parameter_hash` | 输入和参数 hash |

---

## 8. 指标与计算需求

### 8.1 基础价格指标

每个 P0 商品连续序列至少计算：

- 最新价格。
- 3 年、5 年、10 年均值。
- 3 年、5 年、10 年中位数。
- 3 年、5 年、10 年分位数。
- 相对 3 年、5 年、10 年均值偏离。
- 窗口高点、低点、当前相对位置。
- 60 日、120 日、252 日滚动波动率。
- 最大回撤和当前回撤。
- 数据覆盖率、缺失日期数量、最近更新时间。

### 8.2 价差指标

价差定义必须版本化，不能只写在代码里。`futures_spread_definitions` 应支持：

- `spread_id`
- `name`
- `formula_version`
- `legs`
- `weight`
- `direction`
- `unit_conversion`
- `currency_conversion`
- `valid_from` / `valid_to`
- `lineage_notes`

第一阶段建议覆盖：

| 价差 | 用途 |
|---|---|
| 螺纹钢 - 铁矿石 - 焦炭 | 高炉钢利润 proxy |
| 热卷 - 铁矿石 - 焦炭 | 板材利润 proxy |
| PTA - PX | PTA 加工利润 |
| PVC - 电石 | 氯碱/PVC 利润 proxy |
| PP - 丙烯或石脑油 | 聚烯烃利润 proxy |
| 玻璃 - 纯碱 - 燃料 | 玻璃利润 proxy |
| 尿素 - 煤或天然气 | 氮肥利润 proxy |
| 铝 - 氧化铝 - 电力 proxy | 电解铝利润 proxy，第一版电力成本可手工参数化 |

价差结果同样计算分位数、均值偏离和景气状态。

### 8.3 mid-cycle price 输出

商品系统可以输出 mid-cycle 候选值，但不应武断决定 DCF 终值假设。建议输出：

- `midcycle_price_candidate`
- `method`: 5y_mean、10y_mean、trimmed_mean、inflation_adjusted、manual_override
- `confidence`
- `supporting_metrics`
- `warnings`

DCF 模型应根据行业模板和 analyst override 决定是否采用。

### 8.4 数据质量规则

必须检查：

- 日期是否重复。
- OHLC 是否满足 `low <= open/close <= high`。
- 成交量、持仓量是否为非负。
- 结算价和收盘价异常偏离。
- 单日跳变是否超过阈值。
- 连续序列换月日是否有明显断点。
- 历史窗口观测数是否足够。
- 单位、币种、交易所是否与商品主数据一致。

质量异常不得静默忽略。轻微异常写 warning，严重异常应进入 readiness blocker 或 fallback。

---

## 9. 行业和公司暴露映射

### 9.1 映射目标

周期行业不能简单“一个行业绑定一个商品”。系统应支持行业和公司两级映射：

- 行业级默认映射：按申万行业、交易所行业、业务类型给出推荐商品组合。
- 公司级覆盖映射：对多产品、资源禀赋差异、进口原料、长协比例高的公司覆盖默认映射。

### 9.2 映射字段

`futures_exposure_mappings` 至少包含：

| 字段 | 说明 |
|---|---|
| `mapping_id` | 映射 ID |
| `scope_type` | industry、instrument、portfolio |
| `scope_id` | 行业代码或 instrument_id |
| `product_name` | 产品或业务板块 |
| `revenue_series_id` | 收入端商品序列 |
| `cost_series_ids` | 成本端商品序列列表 |
| `spread_ids` | 价差指标 |
| `direction` | positive、negative、mixed |
| `transmission_strength` | high、medium、low 或数值 |
| `lag_days` | 价格传导滞后 |
| `region` | 地区 |
| `currency` | 币种 |
| `priority` | P0/P1/P2 |
| `confidence` | high、medium、low |
| `source` | mapping_config、analyst_override、company_report |
| `valid_from` / `valid_to` | 生效区间 |
| `notes` | 口径说明 |

### 9.3 首批映射示例

| 公司类型 | 收入端 | 成本端 | 关键诊断 |
|---|---|---|---|
| 煤炭企业 | ZC、JM、动力煤现货候选 | 运费、税费后续增强 | 煤价分位、长协价缺失 warning |
| 铜矿企业 | CU、LME 铜候选 | 能源、人工后续增强 | 铜价分位、美元/CNY warning |
| 电解铝 | AL | AO、电力 proxy、预焙阳极后续增强 | 吨铝价差 |
| 钢铁 | RB、HC | I、JM、J、废钢后续增强 | 高炉钢价差、电炉钢价差 |
| 炼化 | 成品油/化工品 | SC、Brent、WTI、LPG | 裂解价差、化工价差 |
| PTA | TA | PX | PTA-PX 价差 |
| 纯碱/玻璃 | SA、FG | 煤、天然气、纯碱 | 玻璃-纯碱价差 |
| 锂盐 | LC | 锂辉石后续增强 | 锂价分位、加工利润缺口 |

---

## 10. 更新维护和调度

### 10.1 同步任务

建议新增独立调度任务：

| 任务 | 默认状态 | 频率 | 说明 |
|---|---|---|---|
| `futures_market_data_sync` | enabled | 交易日晚间 | 同步 P0 商品期货最新日线和连续序列，当前实现任务名 |
| `futures_market_data_backfill` | disabled/manual | 手工或周末 | 历史回补，按品种和日期范围执行，默认禁用 |
| `futures_spread_recompute` | enabled | 日更后 | 重算价差和价差诊断 |
| `futures_cycle_diagnostics_refresh` | enabled | 日更后 | 重算分位数、均值、周期状态 |
| `futures_market_data_readiness` | 可由 API/维护任务生成 | 每日或每周 | 生成覆盖率和缺口摘要 |
| `futures_source_version_check` | 待后续拆分 | 每日中午 | 检查 AKShare 等依赖版本，复用现有依赖检查机制 |

商品日更建议安排在 A 股日更和行业日更之后，避免资源竞争。例如 21:30-23:00 区间，`max_instances=1`。

### 10.2 回补策略

历史回补必须显式指定：

- 品种范围。
- 数据源和 mode。
- 起止日期。
- 是否覆盖已有数据。
- 是否只补缺口。
- 最大运行时间。
- 每源限流。
- 失败重试次数。

默认策略：

- 已有相同 `series_id + trade_date + source + source_mode` 且 hash 未变时跳过。
- hash 变化时保留审计记录并更新当前行，同时在 run metadata 记录变化数量。
- 不删除其他来源的历史记录。
- 大规模全量重建默认禁用，需要手工触发。

### 10.3 readiness 和失败处理

readiness 至少输出：

- 启用品种数。
- 已覆盖 P0 品种数。
- 每个品种最近交易日。
- 每个品种 3/5/10 年覆盖率。
- 最近同步是否成功。
- 缺失日期数量。
- source fallback 使用比例。
- stale 品种列表。
- 单位或币种未验证列表。
- 连续序列构造缺口。
- 价差无法计算原因。

blocker 示例：

- P0 商品连续序列为空。
- 最近价格超过配置天数未更新。
- 10 年窗口覆盖率低于阈值且无降级策略。
- 价差核心 leg 缺失。
- 来源字段结构变化导致解析失败。

warning 示例：

- 只有 3 年数据，无法计算 10 年分位。
- 使用 AKShare 原生连续序列，换月规则不可完全审计。
- 海外基准使用第三方延迟源。
- 现货数据缺失，使用期货 proxy。

---

## 11. 对外服务需求

### 11.1 API 原则

商品域 API 应沿用现有研究域 REST 风格：

- 查询接口只读，不隐式触发远程同步。
- 支持 `source`、`source_mode`、`series_type`、`start_date`、`end_date`、`include_lineage` 参数。
- 所有响应包含 `as_of_date`、`data_available_cutoff`、`quality_flags` 或 `warnings`。
- readiness 和 input gaps 单独提供，便于前端、运维和 DCF 调用方判断可用性。

### 11.2 建议接口

| 接口 | 用途 |
|---|---|
| `GET /research/futures/instruments` | 查询启用商品期货主数据和序列 |
| `GET /research/futures/{series_id}/prices` | 查询商品期货价格序列 |
| `GET /research/futures/{series_id}/cycle-diagnostics` | 查询分位数、均值、周期状态 |
| `GET /research/futures/spreads` | 查询价差定义 |
| `GET /research/futures/spreads/{spread_id}/values` | 查询价差时序和诊断 |
| `GET /research/company/{instrument_id}/futures-exposure` | 查询单家公司 DCF 相关商品期货映射和可用性 |
| `GET /research/futures/readiness` | 商品期货域 readiness |
| 待后续：`GET /research/futures/input-gaps` | 按品种、行业或公司查询缺口 |

### 11.3 DCF 对接

DCF 调用商品数据时应遵循：

- `cyclical_fcff_midcycle.v1` 读取本地 `futures_cycle_diagnostics`、`futures_spread_values` 和 `futures_exposure_mappings`。
- 如果商品数据缺失，DCF 返回 `cyclical_cycle_inputs_missing` 或更具体 input gap，不在估值请求中远程下载。
- DCF 输出的 `cyclical_model_diagnostics` 应包含：
  - 核心商品 series id。
  - 商品最新价格。
  - 3/5/10 年分位。
  - midcycle price 候选。
  - 价差分位。
  - 映射方向和置信度。
  - 商品数据来源和可得日。
  - 缺失或降级原因。

---

## 12. 配置需求

### 12.1 `config/10_research.json`

建议新增模块：

```json
{
  "modules": {
    "commodity_market_data": {
      "enabled": true,
      "storage": {
        "database": "data/futures.db"
      },
      "sources": {
        "akshare": {
          "enabled": true,
          "mode_priority": ["direct", "proxy_patch"],
          "timeout_seconds": 20,
          "rate_limit_per_minute": 30
        },
        "exchange_official": {
          "enabled": false,
          "timeout_seconds": 20
        },
        "manual_import": {
          "enabled": true
        }
      },
      "coverage": {
        "p0_required": true,
        "min_3y_coverage_ratio": 0.90,
        "min_5y_coverage_ratio": 0.85,
        "min_10y_coverage_ratio": 0.70,
        "max_stale_trading_days": 3
      },
      "diagnostics": {
        "lookback_years": [3, 5, 10],
        "volatility_windows": [60, 120, 252],
        "cycle_state_thresholds": {
          "extreme_low": 0.10,
          "low": 0.30,
          "high": 0.70,
          "extreme_high": 0.90
        }
      },
      "registry": {
        "include_default_p0_universe": true,
        "note": "Use the built-in domestic P0 futures universe; explicit instruments are treated as overrides."
      }
    }
  }
}
```

### 12.2 `config/05_scheduler.json`

新增任务应遵循现有任务风格：

- `enabled`
- `description`
- `report`
- `pre_run_notify`
- `trigger`
- `max_instances`
- `misfire_grace_time`
- `coalesce`
- `parameters.max_runtime_seconds`

历史回补任务默认 disabled 或 manual-only。

---

## 13. 测试和验收标准

### 13.1 单元测试

必须覆盖：

- 商品代码和交易所规范化。
- AKShare/交易所 payload 字段映射。
- 空数据、缺字段、重复日期、异常 OHLC。
- 连续序列 lineage 写入。
- 价差公式和单位换算。
- 分位数、均值、波动率和周期状态计算。
- readiness blocker 和 warning 判断。

### 13.2 集成测试

集成测试应标记为外部数据源测试，默认不阻塞普通单元测试。覆盖：

- 1-3 个国内期货 P0 品种最近数据拉取。
- 1 个主力连续序列。
- 1 个海外基准。
- 一次增量同步。
- 一次缺口回补。

当前实现已提供开发验证脚本：

```bash
/home/python/miniconda3/envs/Quote/bin/python scripts/dev_validation/validate_futures_market_data_smoke.py \
  --series-ids CNF.CU.SHFE.main \
  --start-date 2026-06-01 \
  --end-date 2026-06-13 \
  --db-path /tmp/quote_futures_market_data_smoke.db \
  --timeout-seconds 5 \
  --write-enabled
```

脚本默认写 `/tmp/quote_futures_market_data_smoke.db`，不会污染生产 `data/futures.db`。如果网络或 DNS 不可用，应返回非零退出码并输出 provider 失败原因，而不是长时间挂起。

### 13.3 验收标准

第一阶段完成时至少满足：

1. P0 清单中不少于 80% 品种完成本地历史序列同步。
2. 每个已启用品种有主数据、价格序列、来源 lineage 和最近同步状态。
3. 已启用品种可查询 3 年、5 年诊断；历史足够的品种可查询 10 年诊断。
4. 至少 5 个核心价差可重算并输出分位数。
5. 商品域 readiness 可解释缺口，不静默返回空结果。
6. DCF 周期模型可读取商品诊断摘要，缺失时返回 input gap。
7. 文档、配置、调度和 API 文档同步更新。

---

## 14. 风险和边界

1. 期货价格不等于企业真实销售或采购价格，尤其是煤炭长协、进口矿、化工现货和区域建材。
2. 主力连续存在换月跳变，vendor 原生连续序列可能无法完全复现换月规则。
3. 海外数据存在时区、交易日、币种、单位和授权问题。
4. 现货和产业指标口径复杂，免费源稳定性弱，不能作为第一版硬承诺。
5. 商品价格高分位不等于公司盈利必然高分位，成本、产量、税费、长协比例和汇率都会影响利润。
6. DCF 的 base case 应使用 mid-cycle 假设，不应直接外推最新商品价格。
7. 数据系统只提供事实、诊断和 lineage，不输出买卖建议。

---

## 15. 推荐实施顺序

### 阶段一：商品期货主数据和价格序列

- 新增商品域配置。
- 新增商品标的主数据。
- 实现 AKShare provider 和 fixture 测试。
- 建立 `futures_price_bars`、`futures_source_manifests`、`futures_readiness_snapshots`。
- 实现 P0 品种日线和连续序列同步。
- 暴露基础价格 API 和 readiness API。

### 阶段二：周期诊断和价差

- 实现 3/5/10 年分位数、均值、波动率、周期状态。
- 建立价差定义表和核心价差重算任务。
- 暴露 cycle diagnostics 和 spread API。
- 对接 DCF `cyclical_model_diagnostics`。

### 阶段三：行业/公司映射

- 建立行业级商品暴露映射。
- 支持公司级 override。
- 接入申万行业标准层。
- 暴露公司商品暴露和 input gaps API。

### 阶段四：现货、库存、开工率和海外增强

- 增加现货 provider 或手工导入模板。
- 增加交易所仓单、LME 库存、EIA 库存等相对可控来源。
- 增加开工率、库存和产量字段。
- 提高 mid-cycle price 和 mid-cycle margin 的诊断置信度。

---

## 16. 后续 OpenSpec 拆解建议

建议拆成三个连续 change，而不是一次性大包：

1. `add-commodity-market-data-storage-and-sync`
   - 商品主数据、P0 provider、价格表、日更/回补、基础 readiness。
2. `add-commodity-cycle-diagnostics-and-spreads`
   - 分位数、均值、波动率、价差定义、价差时序、诊断 API。
3. `connect-commodity-data-to-cyclical-dcf`
   - 行业/公司映射、DCF input gaps、`cyclical_model_diagnostics` 对接、API 文档和验收测试。

这样可以先形成可用数据资产，再逐步增强估值接入，避免第一版过度设计或被不可控现货源阻塞。
