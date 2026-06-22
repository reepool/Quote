# 商品期货行情数据获取和更新维护系统需求说明书

> 更新日期：2026-06-17
> 适用项目：Quote System / Research Data Engine
> 文档定位：本说明书用于定义商品期货行情与周期景气数据层的业务需求、数据架构、更新维护、对外服务、验收标准和后续 OpenSpec 拆解边界。本文聚焦商品信息获取、存储、更新维护和对外提供；DCF 估值模型公式、行业估值模板和投资结论不在本会话范围内。
> 当前 OpenSpec：`add-futures-market-data-module`、`prioritize-official-futures-sources`、`add-futures-master-calendar-dictionary-api`、`add-futures-trading-day-governance`、`backfill-official-futures-trading-calendar`、`add-futures-scope-config`
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
| `config/11_futures.json` | 期货域独立配置文件，承载来源、交易所、品类、下载 scope、主数据、交易日历、回补和诊断参数；`config/10_research.json` 仅保留研究域总开关或兼容桥接 |
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

### 4.4 下载范围和分阶段上线

期货数据域必须支持像股票行情一样按配置拆分下载和上线，但期货的范围选择应同时支持交易所、商品分类、商品根品种和研究序列四层。目标是允许先完成某个交易所或某类品种的交易日历、主数据、日线行情和日更任务，再逐步扩展到其他交易所和品类，而不是要求一次完成所有历史下载。

标准下载范围对象称为 `download_scope`：

| 字段 | 说明 |
|---|---|
| `scope_id` | 稳定配置 ID，例如 `gfex_all`、`shfe_nonferrous_precious` |
| `enabled` | 是否启用该 scope |
| `exchanges` | 交易所列表，支持 `["all"]` 表示所有已启用交易所 |
| `categories` | 商品分类列表，支持 `["all"]` 表示所选交易所下全部分类 |
| `instrument_ids` | 可选显式商品根品种列表；为空时由交易所和分类解析 |
| `series_types` | 研究序列类型，例如 `main_continuous`、`index_continuous`；默认 `main_continuous` |
| `domains` | 要执行的数据域：`master_data`、`trading_calendar`、`daily_bars`、`spreads`、`diagnostics` |
| `source_policy` | 官方主源、备源和 fallback 策略 |
| `calendar_quality_gate` | 允许写行情前的最低交易日历质量 |
| `request_policy` | 超时、重试、限速、退避、代理策略 |
| `schedule` | 是否接入日更、调度时间、最大运行时间 |

`all` 通配规则：

- `exchanges=["all"]` 解析为当前期货配置中启用的全部交易所，不包含未来预留但未启用的交易所。
- `categories=["all"]` 解析为所选交易所中启用 instrument 覆盖到的全部分类。
- `exchanges=["all"]` 与 `categories=["all"]` 可以同时使用，表示当前启用期货 universe 的全部品种。
- 如果同时给出 `instrument_ids`，则 `instrument_ids` 是进一步收敛条件，不应扩大 `exchanges/categories` 的范围。
- 解析后的 scope 必须在 run metadata 中落成具体 `exchanges/categories/instrument_ids/series_ids`，禁止只记录 `all`。
- 无法解析到任何品种时必须返回 blocker，例如 `empty_futures_download_scope`。

首批建议 scope：

| scope_id | exchanges | categories | 用途 |
|---|---|---|---|
| `gfex_all` | `["GFEX"]` | `["all"]` | 广期所已配置品种，当前包括新能源材料和贵金属，历史短，适合先完成端到端验证 |
| `ine_all` | `["INE"]` | `["all"]` | 原油、低硫燃料油、20 号胶，验证上期系独立交易所日历 |
| `shfe_nonferrous_precious` | `["SHFE"]` | `["nonferrous", "precious_metal"]` | 有色和贵金属，DCF 需求高，但历史长、需谨慎限速 |
| `shfe_energy_rubber_pulp` | `["SHFE"]` | `["energy", "rubber", "pulp_paper"]` | 上期所其他 P0 品类 |
| `dce_ferrous_chemical_energy` | `["DCE"]` | `["ferrous", "chemical", "energy"]` | 大商所黑色、化工、能源，依赖 Chrome/nodriver |
| `czce_chemical_building_coal` | `["CZCE"]` | `["chemical", "building_material", "coal"]` | 郑商所 P0 品类 |
| `domestic_all` | `["all"]` | `["all"]` | 全部国内期货 universe，仅在分交易所验证完成后启用 |

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
| DCE | `http://www.dce.com.cn/dcereport/publicweb/dailystat/dayQuotes` | 通过真实 Chrome/Xvfb + nodriver 建立官网 JS 会话，再用页面内 `fetch` 调用官方 JSON；解析 OHLC、结算价、成交量、持仓量、成交额 | `akshare_futures` |
| CZCE | `http://www.czce.com.cn/cn/DFSStaticFiles/Future/YYYY/YYYYMMDD/FutureDataDaily.txt` | 解析官方日行情文本，规范合约、价格、成交量和持仓量 | `akshare_futures` |
| GFEX | `http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadList` | 解析官方日行情 JSON，支持碳酸锂、工业硅、多晶硅、铂、钯等 | `akshare_futures` |

统一口径：

- 存储目标仍为 `data/futures.db` 的 `futures_price_bars`，不混入股票行情表。
- 价格字段统一为 `open/high/low/close/settlement`，数量字段统一为 `volume/open_interest/amount`，币种和单位继承 `futures_series` 主数据。
- 官方源构造的主力连续写入 `source_profile=exchange_official`、`source_interface=official_*`、`construction_method=official_open_interest_main`，并在 `metadata.underlying_contract` 保留当日选中的真实合约。
- DCE 官方接口有瑞数动态 token 防护，普通 `requests`、静态 Cookie、Playwright/headless Chromium 均不能作为生产主路径；工程实现隔离在 `DceOfficialBrowserClient`，要求真实有头 Chrome，服务器环境使用 Xvfb。浏览器启动后会尝试 `maxTradeDate` 作为 warm-up 探针，但该探针失败不直接阻断，最终以实际请求的 `dayQuotes` 或 `contractInfo` 结果为准。若页面内 `fetch` 返回 `HTTP -1 / TypeError: Failed to fetch`，说明当前 Chrome 会话未正确穿过官网 JS 环境，client 必须关闭当前会话并重启后再重试，不能复用坏 session 扫完整个日期区间。异步调度/Telegram 任务环境中不能直接调用该同步 browser client，必须通过 `OfficialFuturesMarketDataProvider` 的 DCE 专用单线程 executor 进入，确保 Chrome 会话、nodriver loop 与请求线程一致，避免 `This event loop is already running` 或跨线程关闭问题。DCE `dayQuotes.tradeType="0"` 表示期货、`"1"` 表示期权；非交易日会返回 `contractId=null` 的“总计”行，必须过滤后再判定交易日。
- AkShare 只在官方源 disabled、unsupported、empty 或 failed 时调用；同步结果和 ingestion metadata 输出 `official_status`、`fallback_status` 和 `source_selection` 计数。
- readiness 输出 `official_bar_count`、`fallback_bar_count`、`source_profiles`，并对仅有兜底覆盖的序列标记 `fallback_only_coverage`。

剩余未做：

- 尚未执行全量历史回填，也未验证每个交易所在十年以上历史区间的接口可得性和限流边界。
- 尚未实现海外官方基准、现货、库存、仓单、开工率等 P1/P2 数据源。
- 官方源的成交额单位仍按交易所原始口径保留并标记 `amount_unit_exchange_reported`，后续如需跨交易所金额比较，应增加显式单位换算规则。

---

## 6. 商品主数据设计

### 6.0 设计结论

研究和 DCF 默认读取的核心序列应是“每个商品品种的一条或多条连续日 K 线”，其中第一优先级是 `main_continuous` 主力连续。但工程底层不能只保存连续合约结果，必须同时维护商品根品种、真实合约、研究序列和交易日历四类基础对象：

| 层级 | 示例 | 作用 |
|---|---|---|
| 商品根品种 `instrument_id` | `CNF.CU.SHFE` | 代表一个交易所上市商品品种，是分类、单位、交易所、来源策略和 DCF 映射的主键 |
| 真实合约 `contract_id` | `CU2407`、`CU2408` | 交易所官方日行情的原始对象，用于审计、补数、换月和期限结构 |
| 研究序列 `series_id` | `CNF.CU.SHFE.main` | DCF 和周期诊断主要读取对象，可包含主力连续、指数连续、近月连续等 |
| 交易日历 | `SHFE:2026-06-12` | 判断应同步日期、缺口、最近交易日、夜盘归属和日更触发时点 |

因此，“研究主要用主力连续日 K”是对上层使用者的简化口径，不是存储和数据治理层的完整模型。

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

`instrument_id` 命名建议：

| 范围 | 命名样例 | 说明 |
|---|---|---|
| 国内期货 | `CNF.CU.SHFE`、`CNF.I.DCE` | `CNF` 表示 China futures，第二段为品种代码，第三段为交易所 |
| 海外期货 | `GLF.CL.CME`、`GLF.B.CO.ICE` | `GLF` 表示 global futures，第二段为标准品种或来源代码，第三段为交易所 |
| 官方或准官方指数 | `IDX.BRENT.EIA`、`IDX.HH.FRED` | 用于非交易所连续合约但可作为商品基准的公开时间序列 |
| 现货或产业价格 | `SPOT.CU.SMM`、`SPOT.QHD5500.CCTD` | 后续增强层，必须记录地区、规格、税费、来源和授权状态 |

分类字段应使用受控枚举，避免自由文本漂移：

| 标准分类 | 示例 |
|---|---|
| `ferrous` | 铁矿石、螺纹钢、热卷、不锈钢、硅铁、锰硅 |
| `nonferrous` | 铜、铝、氧化铝、锌、铅、镍、锡 |
| `precious_metal` | 黄金、白银 |
| `energy` | 原油、燃料油、低硫燃料油、沥青、LPG、动力煤 |
| `chemical` | PTA、PX、乙二醇、甲醇、PVC、PP、纯碱、尿素 |
| `agriculture` | 豆粕、豆油、棕榈油、玉米、白糖、棉花等后续可扩展品种 |
| `rubber` | 天然橡胶、20 号胶、丁二烯橡胶 |
| `new_energy_material` | 碳酸锂、工业硅、多晶硅 |
| `building_material` | 玻璃等 |
| `pulp_paper` | 纸浆等 |

### 6.1.1 主数据发现治理

商品期货主数据不能长期只依赖手工维护的静态 P0 种子。交易所新增品种时，官方日行情、合约清单或公告可能先出现新的 `variety` 代码；系统必须自动发现并进入主数据治理流程。

主数据发现治理应分为三层：

| 层级 | 说明 | 生产写入策略 |
|---|---|---|
| 自动发现 | 官方日行情或合约清单出现本地未识别 `variety`，例如 GFEX `PT`、`PD` | 写入候选发现表，不直接进入正式主数据 |
| 自动补全 | 通过交易所品种规则页、合约规则页、公告、官方来源 URL 和必要的辅助源补全名称、分类、单位、合约乘数、最小变动价位等 | 按置信度标记 `discovered_unverified`、`discovered_verified_partial`、`discovered_verified` |
| 确认入库 | 高置信候选可按配置自动 promotion；低置信或冲突候选通过 Telegram 通知人工确认 | 通过正式 upsert 写入 `futures_instruments` 和默认 `main_continuous` 序列 |

统一抽象：

- 建立 `FuturesMasterDiscoveryAdapter` 标准接口；上层 service 不写死某个交易所页面结构。
- 各交易所分别实现 adapter，但治理编排必须共用同一接口。第一阶段落地 DCE 与 GFEX；SHFE、INE、CZCE 后续在调试各自官方源时逐个补充，不允许新增交易所专用旁路任务。
- 候选发现记录必须保存 `exchange`、`variety_symbol`、`candidate_instrument_id`、`first_seen_trade_date`、`last_seen_trade_date`、`observed_contracts`、`evidence_url`、`confidence_score`、`quality_flag`、`review_status`。
- 发现未知品种时不能静默忽略；报告和 readiness 必须标记 `needs_master_review`。

GFEX 落地要求：

- 使用 GFEX 官方日行情发现未知 `variety` 和样本合约。
- 使用 GFEX 官方品种规则页或公告补全主数据字段；如果官方页面暂不可稳定解析，则至少持久化候选和 evidence，等待人工确认。
- `PT/PD` 暴露的问题应作为第一批回归样例：系统应能从未知品种发现走到候选记录、补全、确认/promotion，而不是靠人工修改代码列表。

当前实现状态：

- 已新增 `futures_master_discoveries` 候选表、`FuturesMasterDiscoveryCandidate`、`FuturesMasterDiscoveryAdapter` 和 `FuturesMasterDiscoveryGovernanceService`。
- 已实现通用配置型 discovery adapter：所有已配置交易所都可基于官方日行情发现 unknown variety；使用 `config/11_futures.json.master_data_discovery.adapters.<EXCHANGE>.known_products`、默认 P0 主数据种子或交易所特定内置补充元数据完成 enrichment，字段不完整时进入 pending review。
- 已支持 `/futures_master_discovery_governance ...` 与 `/run futures_master_discovery_governance ...` 手工任务；默认 dry-run，显式 `write` 才写库。
- 已接入 `futures_master_governance`：未知品种不再只停留在 warning，而会形成 discovery 候选；默认不阻断已知品种合约治理。
- 已接入 readiness：存在 pending/低置信 discovery 时输出 `needs_master_review:<exchange>:<symbol>`。

### 6.2 合约和连续序列

商品期货必须区分真实合约和连续序列：

| 对象 | 说明 |
|---|---|
| `contract` | 实际可交易合约，例如 `CU2601` |
| `main_continuous` | 主力连续，适合研究但存在换月规则 |
| `index_continuous` | 指数连续或加权连续，适合长期周期比较 |
| `nearby_continuous` | 近月连续，适合期限结构和短周期诊断 |

官方交易所日行情通常返回真实合约日 K，而不是连续合约。连续合约是本系统或第三方数据源构造出来的研究序列。后续实现应遵循以下数据流：

```text
官方真实合约日行情
-> futures_contracts / futures_contract_price_bars
-> futures_continuous_mapping
-> futures_price_bars 中的 main/index/nearby 研究序列
-> cycle diagnostics / spreads / DCF
```

连续序列必须记录：

- 构造方法：source_native、open_interest_roll、volume_roll、calendar_roll、vendor_index。
- 是否回调平滑：none、ratio_adjusted、difference_adjusted。
- 换月规则和阈值。
- 每个交易日对应的底层真实合约。
- 构造版本和参数 hash。

第一阶段可以优先接受 AKShare 原生主力连续或指数连续，但必须把 `construction_method=source_native`、`source_interface`、`source_version` 写入 lineage，不能把它误标为交易所官方原始合约。

### 6.3 期货交易日历和交易时段

商品期货不是 7*24 连续交易。国内商品期货存在日盘和夜盘，夜盘交易会归属到交易所定义的下一个交易日；不同交易所、不同品种的夜盘时段也可能不同。系统必须基于交易日 `trade_date` 管理日 K，而不能简单按自然日判断。

第一阶段至少维护交易所级交易日历：

| 字段 | 说明 |
|---|---|
| `calendar_id` | 例如 `SHFE:2026-06-12` |
| `exchange` | SHFE、INE、DCE、CZCE、GFEX、CME、ICE 等 |
| `trade_date` | 交易所定义交易日 |
| `is_trading_day` | 是否交易日 |
| `session_type` | day、night、day_and_night、closed |
| `has_night_session` | 是否有夜盘 |
| `source` | exchange_official、akshare_fallback、manual 等 |
| `metadata` | 节假日、临时休市、交易时段、来源说明 |

第二阶段再细化到品种级 session：

- `futures_instrument_sessions`：记录品种是否有夜盘、日盘/夜盘开始结束时间、交易时区。
- 海外品种需要记录 `timezone`、`local_trade_date`、`system_trade_date` 和节假日来源。
- readiness 和同步任务必须使用交易日历判断“应有数据但缺失”和“非交易日合法为空”。

### 6.4 交易日治理

交易日治理是商品期货数据层的前置治理能力，重要性等同于主数据治理。它不是简单生成一个周一到周五的日历表，而是要回答“某个交易所、某个品种、某个交易日是否应当有数据，以及应当请求哪些官方接口日期”的问题。

交易日治理的职责边界：

| 职责 | 要求 |
|---|---|
| 交易所级交易日历 | 至少维护 SHFE、INE、DCE、CZCE、GFEX 的交易日、非交易日、节假日、临时休市和来源质量 |
| 品种级交易时段 | 维护日盘、夜盘、无夜盘、特殊夜盘品种和交易日归属规则 |
| 官方公告解析 | 优先解析交易所休市安排、临时调整、夜盘暂停或恢复公告；无法结构化时进入人工复核队列 |
| 数据下载日程 | 历史回补、日更、dry-run 均必须先由交易日治理生成目标交易日集合，不能按自然日盲拉 |
| 缺口判断 | readiness、gap scan 和 stale 判断必须区分“应有数据但缺失”和“非交易日合法为空” |
| 来源质量 | 每个日历行必须记录 `source_profile`、`quality_flag`、`evidence_url` 或 `metadata`，避免 estimated 日历伪装成官方日历 |

交易日治理分层：

| 层级 | 示例 | 默认用途 |
|---|---|---|
| `exchange_calendar` | `SHFE:2026-06-12` | 下载日程、最近应更新交易日、交易所全品种休市判断 |
| `instrument_session` | `CNF.CU.SHFE` 夜盘 21:00-01:00 | 夜盘归属、品种级特殊交易时段、未来分钟或夜盘数据扩展 |
| `instrument_calendar_override` | 某新品种上市前无数据、某品种暂停夜盘 | 上市/退市/暂停交易、品种级例外 |
| `manual_review_queue` | 官方公告无法可靠解析 | 人工确认、证据留存、禁止静默猜测 |

来源优先级：

1. 交易所官方结构化日历或公告。
2. 交易所官方网页公告，经解析器抽取并保留公告 URL、标题、发布时间和解析版本。
3. 监管或交易所统一休市通知。
4. 人工复核录入。
5. 本地 weekday seed 仅作为开发、离线测试或临时降级候选，必须标记 `quality_flag=estimated`；它不构成准确交易日历覆盖，不能作为 2010 起历史回填、生产日更或 dry-run 验收依据。

前置门禁：

- 日更任务在计算目标日期前必须刷新或验证对应交易所日历。
- 历史回补任务必须从交易日治理服务获取交易日集合，并在 run metadata 中记录日历来源、质量和目标交易日数量。
- 全品种 dry-run 必须按交易所分组，用交易日集合驱动请求日期；单个交易日失败应记录到交易所/品种/日期维度，而不是把整批自然日失败混在一起。
- 如果目标日期的交易日历质量低于配置阈值，例如仍为 `estimated`，生产写入应默认阻断或降级为 dry-run，除非显式人工允许。
- 启用交易日治理的行情生产写入不得直接使用 `estimated` 或 `estimated_unverified` 日历；如果目标区间存在低质量日历，行情任务必须先按交易所和连续日期段自动触发官方日历回填，重新展开目标交易日并再次执行质量门禁。只有官方日历回填失败、仍无法达到 `backfilled_verified` 或人工验证质量时，生产写入才应阻断。dry-run 可以继续执行，但必须报告实际最低日历质量和风险。
- 如果官方公告和本地已存日历冲突，系统应保留冲突诊断并要求人工复核，不能自动覆盖生产日历。

对全量下载的影响：

- 全量历史回补仍以起止自然日期作为外部输入，但实际请求必须转换为交易日列表。
- 对缺少结构化官方历史日历的长区间，主路径应直接按 `exchange + natural_date` 调用交易所官方日行情接口验证：有可解析合约行则写为交易日；只有在该交易所已进入“空 payload 可判休市”的可靠覆盖区间后，官方确认无报表/空报表才可写为非交易日。早于可靠覆盖起点的空 payload 只能说明该接口可能无历史行情覆盖，必须保留为 unresolved/manual-review，不能误写成休市；网络失败、格式异常或无法分类的日期也不得 weekday 猜测。
- 官方接口失败必须结构化分类：`network_unreachable`、`dns_failure`、`timeout`、`tls_failure`、`official_not_found_or_no_report`、`possible_anti_bot_or_ip_risk_control`、`unexpected_html_payload` 等。若同一交易所 URL 从其他 IP 可访问、但本机返回网络不可达、403/429、WAF/验证码/瑞数挑战或长期超时，应标记为疑似本机 IP 风控或网络策略问题，暂停生产全量回填，只允许小范围诊断探测。
- 官方历史日历回填默认范围为 `2010-01-01` 至当前可由官方日行情验证的日期；未来日期只有在官方公告或结构化日历可准确确认时才入库，否则保持未知，不写 estimated。
- DCE 日历验证必须使用与行情相同的官方浏览器辅助路径：先访问 `http://www.dce.com.cn/dce/channel/list/168.html` 建立瑞数运行环境，再通过页面内 `fetch('/dcereport/publicweb/dailystat/dayQuotes')` 请求 `tradeType="0"` 的期货日行情。有效合约行数大于 0 判定交易日；只有 `contractId=null` 的“总计”行时判定非交易日；HTTP/JS/浏览器启动失败均判为 unresolved，不能回退为 weekday 猜测。
- 对海外品种，必须使用对应交易所时区和假期，不允许把国内日历套用到 CME、ICE、LME、SGX 等交易所。
- 后续如接入跨市场价差，应保留各腿本地交易日和统一对齐日，避免把一个市场休市导致的缺口误算为价差变化。

### 6.5 长期扩展原则

框架必须为未来引入国内和海外其他品种预留空间：

- 国内扩展：农产品、航运、金融期货、期权标的或交易所指数可以复用 `instrument_id`、`contract_id`、`series_id` 三层模型。
- 海外扩展：CME、ICE、LME、SGX 等需要支持不同币种、单位、时区、节假日和合约代码规则。
- 非期货基准：EIA、FRED、World Bank、交易所库存、官方仓单等可作为 `benchmark_series` 或 `indicator_series`，但不能混同为真实期货合约。
- 现货和产业数据：后续接入时必须记录规格、地区、含税/不含税、交割地、数据授权和可得日期。

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
| `futures_instrument_categories` | 商品分类字典和标准分类枚举 |
| `futures_contracts` | 真实合约主数据，包括合约月份、上市/到期、交易单位、最小变动价位等 |
| `futures_trading_calendar` | 交易所或品种级交易日历，用于缺口判断、日更边界和最近交易日 |
| `futures_instrument_sessions` | 品种交易时段和夜盘规则；第一阶段可预留或弱实现 |
| `futures_series` | 主力连续、指数连续、近月连续等研究序列元数据 |
| `futures_contract_price_bars` | 官方真实合约日 K，保留交易所原始合约层数据 |
| `futures_price_bars` | 日线价格序列，含真实合约和连续序列 |
| `futures_continuous_mapping` | 连续序列每日对应底层合约和换月信息；阶段二完善 |
| `futures_spread_definitions` | 价差公式定义、权重、单位、版本 |
| `futures_spread_values` | 派生价差时序 |
| `futures_cycle_diagnostics` | 分位数、均值、偏离、波动率、景气状态 |
| `futures_exposure_mappings` | 行业/公司到收入端、成本端、价差的映射 |
| `futures_source_manifests` | 来源接口、字段映射、版本和拉取参数 |
| `futures_readiness_snapshots` | 覆盖率、缺口、异常、fallback 和质量摘要 |

### 7.3 真实合约字段

`futures_contracts` 至少需要：

| 字段 | 说明 |
|---|---|
| `contract_id` | 系统合约 ID，例如 `CNF.CU.SHFE.CU2407` |
| `instrument_id` | 商品根品种 ID |
| `exchange` | 交易所 |
| `exchange_contract_code` | 交易所合约代码，例如 `CU2407` |
| `contract_month` | 合约月份，例如 `2024-07` |
| `listed_date` | 合约上市日期，可为空但应尽量补齐 |
| `last_trade_date` | 最后交易日 |
| `delivery_month` | 交割月份 |
| `contract_multiplier` | 合约乘数 |
| `tick_size` | 最小变动价位 |
| `currency` | 币种 |
| `unit` | 报价单位 |
| `active` | 是否仍可交易或仍需维护 |
| `source` | 合约主数据来源 |
| `metadata` | 交割品级、交割地、交易时段等扩展信息 |

`futures_contract_price_bars` 至少需要：

| 字段 | 说明 |
|---|---|
| `contract_id` | 真实合约 ID |
| `instrument_id` | 商品根品种 ID |
| `trade_date` | 交易日 |
| `open/high/low/close/settlement` | 官方合约日 K |
| `volume/open_interest/amount` | 成交量、持仓量、成交额 |
| `source_profile` | `exchange_official`、`akshare_fallback` 等 |
| `source_interface` | 官方接口或备源接口 |
| `parser_version` | 解析版本 |
| `raw_payload_hash` | 原始行 hash |
| `quality_flag` | ok、partial、invalid 等 |

### 7.4 连续序列价格字段

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

### 7.5 交易日历字段

`futures_trading_calendar` 至少需要：

| 字段 | 说明 |
|---|---|
| `exchange` | 交易所 |
| `trade_date` | 交易日 |
| `is_trading_day` | 是否交易日 |
| `timezone` | 交易所时区 |
| `session_type` | day、night、day_and_night、closed |
| `source_profile` | 官方、备源或人工 |
| `quality_flag` | official、official_parsed、backfilled_verified、manual_verified、estimated、estimated_unverified、conflict、missing |
| `parser_version` | 日历解析或官方日行情探测版本 |
| `evidence_url` | 官方公告、官方日行情接口或其他证据 URL |
| `notice_id` / `manual_override_id` | 公告或人工复核引用 |
| `metadata` | 节假日、临时休市、来源接口、官方响应 hash、row_count、失败原因等 |

唯一键建议为 `(exchange, trade_date)`。如果后续启用品种级 session，则新增 `(instrument_id, trade_date)` 层，不破坏交易所级日历。

### 7.6 周期诊断字段

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
| `futures_official_calendar_backfill` | enabled/manual_only | 手工触发 | 按交易所官方日行情可靠覆盖起点验证交易日/非交易日，只写 `futures_trading_calendar`，不写行情价格；早于官方日行情可靠覆盖起点的空 payload 记为 unresolved，不写为休市；未知未来不使用 weekday 猜测；配置默认 `dry_run=true`、`max_days=10`，真实落库必须显式 `write` |
| `futures_trading_day_governance` | enabled | 日更前、回补前 | 维护交易所/品种交易日历、休市公告、交易时段和目标交易日集合，是商品数据同步前置任务 |
| `futures_master_governance` | enabled/manual_only | 手工触发；后续可作为日更前置 | 维护商品根品种、研究序列和真实合约主数据；按指定交易所使用官方日行情逐交易日发现合约代码，依赖已验证交易日历，默认 `dry_run=true`、`max_days=10`，真实落库必须显式 `write` |
| `futures_market_data_sync` | enabled | 交易日晚间 | 同步 P0 商品期货最新日线和连续序列，当前实现任务名 |
| `futures_market_data_backfill` | disabled/manual | 手工或周末 | 历史回补，按品种和日期范围执行，默认禁用；生产写入默认要求交易日治理和主数据治理前置 |
| `futures_spread_recompute` | enabled | 日更后 | 重算价差和价差诊断 |
| `futures_cycle_diagnostics_refresh` | enabled | 日更后 | 重算分位数、均值、周期状态 |
| `futures_market_data_readiness` | 可由 API/维护任务生成 | 每日或每周 | 生成覆盖率和缺口摘要 |
| `futures_source_version_check` | 待后续拆分 | 每日中午 | 检查 AKShare 等依赖版本，复用现有依赖检查机制 |

商品日更建议安排在 A 股日更和行业日更之后，避免资源竞争。例如 21:30-23:00 区间，`max_instances=1`。正式 dry-run、历史行情回补和生产日更的顺序应为：先执行 `futures_official_calendar_backfill` 完成交易所官方日历落库，再执行 `futures_master_governance` 完成根品种、研究序列和真实合约主数据治理，然后进入全品种 dry-run，最后才执行行情历史回补或日更。其中 `futures_market_data_sync`、`futures_market_data_backfill` 和全品种 dry-run 必须依赖 `futures_trading_day_governance` 的成功结果；生产写入还应打开 `requires_master_data_governance`，使主数据治理成为行情写入前置。如果交易日治理失败、主数据治理失败或日历质量低于配置阈值，生产写入任务应阻断，dry-run 可以继续但必须显式标记风险。

主数据治理还必须维护根品种生命周期。对历史遗留、拆分、退市或长期不再挂牌的品种，不应在行情同步层按具体代码写特殊 skip，而应在 `futures_instruments.metadata.lifecycle` 中记录统一生命周期窗口，例如 `status`、`valid_from`、`valid_to`、`source`、`reason` 和必要的 `lineage`。行情同步只消费该生命周期：目标日期早于 `valid_from` 或晚于 `valid_to` 时，对应连续序列返回 `lifecycle_skip`，不请求官方源或备源。`active` 不直接等同于“当前仍在交易”；为了支持历史回补，历史遗留品种可以继续保留 `active=true` 参与历史研究 universe，但必须通过 lifecycle 阻止下线日之后的数据下载。

GFEX 单交易所上线时，调度配置应只打开 GFEX scope，不应使用 `domestic_all`。推荐在 `config/05_scheduler.json` 中把 `futures_market_data_sync.parameters` 调整为：

```json
{
  "scope_ids": ["gfex_all"],
  "mode": "direct",
  "dry_run": false,
  "requires_trading_day_governance": true,
  "requires_master_data_governance": true,
  "master_governance_max_days": 10,
  "max_runtime_seconds": 7200
}
```

该配置的日更执行顺序为：先由 `futures_trading_day_governance` 根据已落库的 GFEX 官方交易日历生成目标交易日；若没有目标交易日，例如休市日，则后续行情同步自然跳过；若存在目标交易日，则 `futures_master_governance` 仅针对这些目标交易日刷新/发现 GFEX 合约主数据；最后 `futures_market_data_sync` 以同一目标交易日集合更新日线和连续序列。历史回补同样应开启 `requires_master_data_governance=true`，显式传入 `start_date/end_date`，并将 `master_governance_max_days` 设为足够覆盖本次回补窗口或置空，避免只治理窗口前若干天。

手工触发方式：

```text
/futures_calendar_backfill exchange=GFEX start=2022-12-22 end=2022-12-31 dry_run max_days=10
/futures_calendar_backfill exchange=GFEX start=2022-12-22 end=2022-12-31 write max_days=10
/futures_calendar_backfill scope=gfex_all start=2022-12-22 end=2022-12-31 dry_run max_days=10
/futures_master_governance exchange=GFEX start=2022-12-22 end=2022-12-31 dry_run max_days=10
/run futures_master_governance exchange=DCE start=2000-06-01 end=2026-06-20 dry_run
```

- `dry_run` 是默认安全行为，只探测和报告，不落库。
- `write` 才会把官方验证后的交易日/休市日写入 `data/futures.db`。
- `max_days` 用于小批量验证，防止一次手工命令误触发大范围请求。

### 10.1.1 官方交易日历可靠起点

截至 2026-06-15 的官方接口实测结论：

| 交易所 | 当前可靠起点 | 官方接口状态 | 说明 |
|---|---:|---|---|
| DCE | 2000-06-01 | 可解析，需 Chrome/nodriver | 2000-01 和 2000-03 样本为空，2000-06-01 起连续样本可解析 |
| SHFE | 2002-01-07 | 可解析，但本机 IP 可能被限流 | 2000-2001 官方文件不可用；2026-06-15 本机在 2017 段出现连接超时，应暂停大批量请求或换网络继续 |
| INE | 2018-03-26 | 可解析，但与 SHFE 同属上期系域名，可能同受网络限流影响 | 2018-03-26 为原油期货上市首日，之前样本为空 |
| CZCE | 2015-10-15 | 当前静态文件接口可解析 | 当前接口无法证明 2015-10-15 之前的官方日历，需要另找旧版官方归档源 |
| GFEX | 2022-12-22 | 可解析，需模拟官网 AJAX 请求头；存在频控/风控 | 官网页面 `hqsj_tjsj.shtml` 的前端 JS 暴露 `POST /u/interfacesWebTiDayQuotes/loadList`。实测 2022-12-19 至 2022-12-21 为空，2022-12-22 起工业硅日行情可解析；连续快速请求后可能返回 567/HTML challenge，因此全量落库必须低频、可断点，必要时走手动代理通路 |

补充验证结论：

- GFEX 官方日行情接口必须带 `Referer: http://www.gfex.com.cn/gfex/rihq/hqsj_tjsj.shtml`、`Origin: http://www.gfex.com.cn`、`X-Requested-With: XMLHttpRequest`，否则容易返回 HTML challenge。
- GFEX 567/HTML challenge 的重试预算必须独立于普通网络错误重试预算。若同一日期先出现连接重置、再出现 567 challenge，不得因为普通 `retry_attempts` 已耗尽而提前切换 AkShare 备源；只有对应 challenge retry 预算耗尽后才允许判定官方源不可用。
- 2026-06-18/19 GFEX 频控实测：`0.05s`、`0.5s` 间隔约第 11 次请求开始出现 567；`0.75s` 间隔 20 次请求出现少量 567；`0.9s` 和 `1.0s` 间隔 20 次小样本曾未出现 567，但 2022-12-22 至 2026-06-18 全段 dry-run 在 `0.9s` 下仍有 130 个 unresolved，后续 200 次复现样本确认失败响应为 `text/html` 的 567 challenge。2026-06-18 全段 dry-run 在 `0.9s`、每 45 次暂停 30 秒、567 退避 20 秒下成功完成，耗时约 35.2 分钟，其中批次暂停占 840 秒；随后优化到每 90 次暂停 20 秒，全段 dry-run 成功，`challenges=11`、批次暂停占 280 秒；继续优化到每 90 次暂停 10 秒、567 退避 20 秒，全段 dry-run 仍成功，`trading_days=843`、`closed_days=432`、`unresolved=0`、`challenges=7`、`challenge_backoff_seconds=140`、`batch_pause_seconds=140`，耗时约 23.8 分钟。进一步优化到每 180 次暂停 10 秒、567 退避 10 秒，2022-12-22 至 2026-06-19 全段 dry-run 仍成功，`trading_days=843`、`closed_days=433`、`unresolved=0`、`challenges=11`、`challenge_backoff_seconds=110`、`batch_pause_seconds=70`。2026-06-19 使用同一参数正式 `write` 落库成功，写入 `1276` 行，`trading_days=843`、`closed_days=433`、`unresolved=0`、`challenges=12`、`challenge_backoff_seconds=120`、`batch_pause_seconds=70`，库内 `quality_flag=backfilled_verified` 且 `source_profile=exchange_official_daily_probe`。当前 GFEX 固化配置为 `request_interval_seconds_by_exchange.GFEX=0.9`、`challenge_retry_attempts_by_exchange.GFEX=3`、`challenge_backoff_seconds_by_exchange.GFEX=10`、`batch_pause_every_requests_by_exchange.GFEX=180`、`batch_pause_seconds_by_exchange.GFEX=10`。交易日历任务还必须在首轮扫描结束后对 unresolved 日期执行任务级补传，当前配置为 `retry_unresolved_passes=1`、`retry_unresolved_pause_seconds=60`、`progress_log_every=100`。若 full dry-run 仍留下少量 567 洞，再上调到 `1.2s` 或切换手动代理进行 gap-fill。
- 交易日历回填必须保留关键日志观察点：任务/交易所开始和结束、每 `progress_log_every` 次请求进度、单日 unresolved 原因、GFEX 567 challenge、请求级退避、批次暂停、任务级补传开始/结束和补回数量。Telegram 报告应展示 `retry_passes`、`retry_resolved` 和失败样本，避免 blocked 但无法定位原因。
- GFEX 同一页面还暴露 `POST /u/interfacesWebTpTradingCalendar/loadList`，返回的是交易/合约事件日历，不能单独证明全量交易日和休市日，但可作为交易日治理的辅助证据。
- SHFE/INE 当前小样本直连、`akshare_proxy_patch.install_patch()` 和手动授权代理均可返回官方 JSON。GFEX 当前直连和手动授权代理均可返回官方 JSON；`akshare_proxy_patch.install_patch()` hook 模式在当前 requests 路径存在 `impersonate` 参数兼容问题，且部分 GFEX 请求仍可能返回 HTML challenge。若本机 IP 再次被限流，优先用 `scripts/dev_validation/probe_futures_proxy_patch_access.py` 或 `scripts/dev_validation/probe_gfex_rate_limit_threshold.py` 复核代理可用性，再决定是否将手动代理通路纳入批量落库进程。
- AkShare 本地期货模块使用 `akshare.futures.cons.get_calendar()` 的通用交易日历，当前覆盖 `19901219` 至 `20261231`，来源为新浪/本地包内 `calendar.json`。它不是交易所级官方日历，不区分 SHFE、INE、DCE、CZCE、GFEX，只能作为备查或低质量兜底，不能替代官方交易日治理。

生产落库必须按交易所分批执行并复核，不允许一次性以 `2000-01-01` 跑全交易所。GFEX 已完成全段 dry-run 验证，可通过手工任务显式 `write` 落库：

```text
/run futures_official_calendar_backfill exchange=GFEX start=2022-12-22 end=2026-06-19 write
```

主数据治理应在交易日历落库并复核后执行。当前 `futures_master_governance` 已抽象为“单交易所 + 官方日行情合约发现”流程：按目标交易所读取已验证交易日历，按 `exchange + trade_date` 请求官方日行情，使用本地/已 promotion 根品种主数据映射真实合约，并写入 `futures_contracts`。GFEX、DCE 复用同一治理流程；后续 SHFE/INE/CZCE 接入时应优先复用该接口，只补交易所 parser 或 discovery/enrichment adapter，不应另建旁路任务。由于官方日行情不能直接提供上市日、最后交易日、交易单位、最小变动价位等完整合约规格，合约质量标记为 `official_daily_discovered_partial`，并在 metadata 中保留 `first_observed_trade_date`、`last_observed_trade_date` 和缺失字段说明。主数据发现层已新增标准化 `FuturesProductSpec` enrichment 接口；DCE 通过既有 Chrome/nodriver 会话访问官方 `/dcereport/publicweb/tradepara/contractInfo`，可提取品种名称、合约交易单位和最小变动价位；同时会从 `config/11_futures.json.master_data_discovery.adapters.DCE.listed_products_page` 指向的大商所首页自动发现“上市品种/合约规则”页面 URL，并可用 `product_rule_pages` 显式覆盖或补充，复用同一浏览器会话解析交易品种、交易代码、报价单位、交易单位和最小变动价位。`contractInfo` 中的 `unit` 是合约交易单位，不是报价单位，不能直接写入 `candidate_unit`；若 DCE 上市品种页面提供“报价单位”，该页面字段可作为 quote unit 的官方证据，否则报价单位和分类通过 `known_products` 的显式治理规则元数据补齐。主数据治理启动时还会用同一 adapter 尝试刷新已存在的 P0 根品种：只有字段确实变化，或拿到官方 product spec 证据时才更新 `futures_instruments/futures_series`，单纯复读内置 seed 不触发无意义写入；报告必须披露 `initial_instruments/final_instruments/refreshed_instruments` 与对应 series 计数。若官方规格 enrichment 源不可用，例如 DCE 浏览器会话失败，任务不能只写日志，必须在 Telegram/report warnings 中披露 `official_product_spec_enrichment_unavailable`、交易所、目标 symbols 和错误文本。对 DCE 这类依赖浏览器会话的官方源，单日请求失败不能立即形成永久缺口，任务应在全段扫描结束后按 `master_data.contract_discovery_retry` 进行任务级补跑，最终报告必须披露 `task_retry_passes`、`task_retry_resolved`、`failed_trade_dates`。

未知品种 discovery 已覆盖所有已配置交易所。当前静态 P0 根品种种子会作为内置 enrichment 元数据，交易所新增品种若未进入 P0 种子，也会在报告中以 `unmapped_<exchange>_varieties` warning 暴露，并生成 discovery 候选；通用 adapter 会优先从官方日行情 raw payload 中提取可用的品种名称证据，再合并交易所规格 enrichment 和 `known_products`。若名称、分类、币种、报价单位等关键字段完整，则可高置信 promotion；若只能拿到名称、合约乘数、tick 等部分字段，则保留 `discovered_verified_partial/pending`，不能写入正式 `futures_instruments`。GFEX 的 `PT`、`PD` 已作为内置补充元数据保留，用于回归验证“正式根品种种子落后时仍能发现并生成候选”的完整链路。DCE 当前已接入官方 `contractInfo` 规格 enrichment，并为 `A/B/BB/BZ/C/CS/FB/JD/LG/LH/M/P/RR/Y` 补充分类与报价单位元数据；全历史 dry-run 发现 2000-2003 年存在旧根品种 `S`（官方日行情名称“大豆”），其是 DCE 早期大豆/黄大豆 legacy soybean 合约代码，后续拆分为 `A` 黄大豆 1 号与 `B` 黄大豆 2 号两条体系。治理上 `CNF.S.DCE` 必须作为独立历史根品种入库，不映射为 `A` 或 `B`，并在 `metadata_json.master_discovery_evidence.product_lineage` 中记录 `successor_family=["CNF.A.DCE","CNF.B.DCE"]`、`primary_chronological_successor="CNF.A.DCE"`、`oilseed_import_soybean_successor="CNF.B.DCE"`。这些候选在官方规格与配置规则都可用时可自动 promotion。`futures_master_governance write` 在同一任务内完成 auto-promotion 后，会复用本轮已抓取的官方日行情 rows 重处理 newly promoted varieties，因此无需额外再跑一遍才能写入这些新品种的真实合约。SHFE、INE、CZCE 后续应按同一 `FuturesProductSpec` 标准接口接入各自官方合约规格或交易参数源。

新增未知品种的长期目标是“尽量自动入库、少人工干预”。所有交易所的 discovery adapter 必须实现统一的产品主数据 enrichment 接口，输出标准化 `name/category/currency/unit` 以及可选的 `contract_multiplier/tick_size/lineage` 证据。字段来源优先级为：交易所官方产品/合约规则接口、官方日行情 raw payload、交易所公告或规则页面解析、已审核本地规则补充、聚合源低质量兜底；不得用不透明猜测直接写正式主数据。日更或回补遇到 unknown variety 时，应先调用 enrichment adapter，若 `name/category/currency/unit` 均达到可信质量，则自动写入 `futures_master_discoveries` 并 promotion 到 `futures_instruments`、`futures_series`，随后复用本轮官方日行情 rows 写入对应真实合约；若仍缺关键字段，则写入 pending review 并在报告中明确列出缺失字段、来源、影响的合约和是否跳过行情写入。第一阶段必须先把 DCE 和 GFEX 做成完整 adapter：DCE 使用官方日行情、`/dcereport/publicweb/tradepara/contractInfo`、首页自动发现的大商所“上市品种/合约规则”页面和本地治理规则补充处理分类/单位及 legacy lineage；GFEX 使用官方日行情识别品种名称，并从 `config/11_futures.json.master_data_discovery.adapters.GFEX.listed_products_page` 指向的广期所上市品种入口页自动发现官方品种页面 URL，`product_rule_pages` 仅作为显式覆盖或补充，然后解析交易品种、交易代码、报价单位、交易单位和最小变动价位；项目内部 `category` 仍由已审核治理规则补齐，并在 field-level evidence 中标明为 governed metadata。任务发现新品种后只写 `futures.db`，不会自动回写 `config/11_futures.json`；Telegram 报告必须明确提示应维护的文件、JSON path（例如 `master_data_discovery.adapters.DCE.known_products.BZ`）和建议 entry，便于人工复核后把长期治理规则沉淀到配置。SHFE、INE、CZCE 后续接入时必须遵守同一规则：先接官方产品/合约规则页面或接口 adapter，缺失的项目内部分类由治理规则补齐，报告必须给出配置维护建议，不允许绕过统一 discovery/promotion 流程。

```text
/run futures_master_governance exchange=GFEX start=2022-12-22 end=2026-06-19 dry_run
/run futures_master_governance exchange=GFEX start=2022-12-22 end=2026-06-19 write
```

SHFE/INE 等长历史或易限流交易所可继续使用分段开发脚本辅助落库：

```bash
PYTHONPATH=/home/python/Quote LD_LIBRARY_PATH=/home/python/miniconda3/envs/Quote/lib \
/home/python/miniconda3/envs/Quote/bin/python scripts/dev_validation/backfill_futures_official_calendar.py \
  --exchanges SHFE \
  --end-date 2026-06-15 \
  --chunk-years 1 \
  --official-timeout-seconds 12 \
  --official-retry-attempts 2 \
  --official-retry-backoff-seconds 0.2 \
  --write \
  --replace-exchange-calendar \
  --output-path /tmp/quote_futures_calendar_shfe_write.json
```

不传 `--start-date` 时，脚本使用 `config/11_futures.json` 中的 `exchange_start_dates`；如需从断点续跑，可显式传入 `--start-date`。

### 10.1.2 API/数据库资源隔离要求

商品期货交易日历落库、主数据治理、历史回补和日更任务属于数据生产链路，其优先级高于外部查询访问。生产环境中发现外部 API 查询突发，尤其是 `/api/v1/quotes/daily` 这类高成本行情查询，可能占满 SQLite 异步连接池，导致估值输入同步、期货 dry-run 报告和调度任务收尾被延迟或中断。因此期货数据上线前，系统必须具备通用的 API/DB 资源隔离能力，而不是只依赖调低期货任务请求频率或只保护单一接口。

运行原则：

- 后台数据任务必须使用独立的 task async DB pool；外部 API 查询使用独立的 API async DB pool，不得占用任务侧连接池。
- API 查询在保护阈值内应排队等待，而不是优先直接拒绝。外部调用方主要可能是 AI 客户端，短时等待可接受，直接断开可能导致调用链不再重试。
- 排队必须有上限和超时，防止极端请求量造成协程和内存堆积。
- 任务报告发送必须有超时保护，Telegram 或网络异常不得导致已经完成的数据任务无法释放运行状态。
- 资源隔离是全局运行保障能力，适用于期货、估值输入、股票行情日更和其他维护任务；默认应通过 `/` 根路径保护规则覆盖所有 API，期货历史回补和交易日历落库不得在无资源保护的情况下与高并发外部查询同时运行。

建议默认策略：

| 项目 | 建议值 | 说明 |
|---|---:|---|
| 任务 async pool 容量 | 2 条 | `task_async_pool.pool_size=2`、`max_overflow=0`，调度和数据任务专用 |
| API async pool 容量 | 约 8 条 | 例如 `api_async_pool.pool_size=2`、`max_overflow=6`，外部访问专用 |
| 全 API 活跃查询并发 | 6 个左右 | API 访问独立分池后可适度放宽，默认通过 `/` 规则覆盖所有接口 |
| 全 API 等待队列 | 80-120 个 | 超出后才拒绝，避免无限堆积 |
| 高成本路径级限流 | 例如 `/api/v1/quotes/daily` 每分钟 30 次 | 保留对 AI 高频批量查询的单独节流能力 |
| 队列等待超时 | 120 秒左右 | 让外部 AI 客户端等待，但避免长期占用请求上下文 |
| 任务报告发送超时 | 45 秒左右 | 报告失败只影响通知，不改变数据任务结果 |

验收标准：

- 在外部 API 并发压力下，调度任务仍使用 task async pool 获取 DB 连接并完成任务状态写入。
- 超过活跃查询并发时，请求优先进入等待队列；只有队列满或等待超时才返回 busy 响应。
- 日志必须能区分 rate limit、队列等待、队列超时、队列满和报告发送超时。
- 期货交易日历 dry-run 或 write 任务的成功/失败不得被 Telegram 报告发送异常覆盖。

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

- 起止日期先交给交易日治理服务展开为 `exchange + trade_date` 目标集合，再按交易所和品种请求数据。
- 周末、节假日和交易所公告休市日不应发起常规官方日行情请求，除非该日期被标记为交易日或人工复核允许。
- 目标交易日集合、日历来源质量、公告证据和任何人工覆盖必须写入 ingestion run metadata。
- 已有相同 `series_id + trade_date + source + source_mode` 且 hash 未变时跳过。
- hash 变化时保留审计记录并更新当前行，同时在 run metadata 记录变化数量。
- 不删除其他来源的历史记录。
- 大规模全量重建默认禁用，需要手工触发。

### 10.3 readiness 和失败处理

readiness 至少输出：

- 启用品种数。
- 已覆盖 P0 品种数。
- 每个品种最近交易日。
- 每个交易所交易日治理质量、最新官方日历日期和 estimated/manual/official 覆盖比例。
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
- 目标交易所缺少可用交易日历，且当前任务要求生产写入。
- 官方休市公告解析冲突未复核，导致目标日期无法判定是否应有数据。

warning 示例：

- 只有 3 年数据，无法计算 10 年分位。
- 使用 AKShare 原生连续序列，换月规则不可完全审计。
- 海外基准使用第三方延迟源。
- 现货数据缺失，使用期货 proxy。
- 交易日历仍为 weekday seed 或 manual 临时覆盖，尚未由官方公告验证。

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
| `GET /research/futures/dictionary` | 查询商品期货数据字典，包括分类、交易所、品种、研究序列、字段口径和来源状态 |
| `GET /research/futures/instruments` | 查询启用商品期货主数据和序列 |
| `GET /research/futures/instruments/{instrument_id}` | 查询单个商品根品种的主数据、单位、分类、来源、可用序列和同步状态 |
| `GET /research/futures/contracts` | 按 `instrument_id`、`exchange`、`contract_month` 查询真实合约清单 |
| `GET /research/futures/contracts/{contract_id}/prices` | 查询真实合约日 K，用于审计、换月复核和期限结构研究 |
| `GET /research/futures/series` | 查询研究序列清单，支持 `instrument_id`、`series_type`、`source_profile` 过滤 |
| `GET /research/futures/prices` | 按 `instrument_id + series_type` 查询默认研究日 K，默认返回 `main_continuous` |
| `GET /research/futures/{series_id}/prices` | 查询商品期货价格序列 |
| `GET /research/futures/{series_id}/mapping` | 查询连续序列每日对应真实合约、换月日期和构造方法 |
| `GET /research/futures/calendar` | 查询交易日历，支持 `exchange`、`instrument_id`、`start_date`、`end_date` |
| `POST /research/futures/calendar/official-backfill` | 执行官方交易日历回填；支持 `exchange`、`start_date`、`end_date`、`dry_run`、`max_days`，只写日历不写行情 |
| `POST /research/futures/master-governance` | 执行商品期货主数据治理；支持 `exchange/scope`、`start_date`、`end_date`、`dry_run`、`max_days`，用于交易所分批上线前置检查 |
| `GET /research/futures/source-manifests` | 查询数据源、接口、主备优先级、字段口径和覆盖状态 |
| `GET /research/futures/{series_id}/cycle-diagnostics` | 查询分位数、均值、周期状态 |
| `GET /research/futures/spreads` | 查询价差定义 |
| `GET /research/futures/spreads/{spread_id}/values` | 查询价差时序和诊断 |
| `GET /research/company/{instrument_id}/futures-exposure` | 查询单家公司 DCF 相关商品期货映射和可用性 |
| `GET /research/futures/readiness` | 商品期货域 readiness |
| 待后续：`GET /research/futures/input-gaps` | 按品种、行业或公司查询缺口 |

API 默认读取策略：

- 对 DCF 和大部分研究场景，`GET /research/futures/prices?instrument_id=CNF.CU.SHFE` 默认返回 `main_continuous` 日 K。
- 如果调用方需要审计或期限结构，应显式调用 `contracts` 或 `contracts/{contract_id}/prices`。
- 如果同时存在官方构造和 AkShare 原生连续序列，默认使用 `source_profile` 优先级最高且 readiness 合格的序列；调用方可显式传 `source_profile`。
- 所有价格响应应返回 `series_id`、`instrument_id`、`series_type`、`source_profile`、`construction_method`、`unit`、`currency` 和 `warnings`，避免调用方误解口径。

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

### 12.1 独立期货配置文件

期货配置应从 `config/10_research.json` 中拆出，形成独立文件 `config/11_futures.json`。理由：

- 期货与股票同为市场数据一级域，配置体量会随交易所、品类、合约规则、日历治理、代理策略和海外市场扩展持续增加。
- 期货配置需要同时描述主数据、交易日历、真实合约、连续序列、数据源、回补 scope、日更 scope、价差和诊断参数，长期放在 `10_research.json` 会降低可读性。
- 独立配置文件便于按交易所/品类逐步上线，也便于后续把海外期货、现货、库存和产业指标拆分成独立 source profile。

迁移原则：

- `config/11_futures.json` 作为期货域单一事实配置源。
- `config/10_research.json` 只保留 `commodity_market_data.enabled`、存储路径兼容桥接或短期迁移字段；最终不再维护大段期货细节。
- 配置加载层应把 `11_futures.json` 合并进 `ResearchConfig.modules["commodity_market_data"]`，保持现有调用方兼容。
- 如果 `10_research.json` 和 `11_futures.json` 同时声明同一字段，生产应优先 `11_futures.json`，并输出迁移 warning。

建议结构：

```json
{
  "futures_config": {
    "enabled": true,
    "storage": {
      "database": "data/futures.db"
    },
    "universe": {
      "enabled_exchanges": ["SHFE", "INE", "DCE", "CZCE", "GFEX"],
      "enabled_categories": ["all"],
      "include_default_p0_universe": true,
      "future_extension_markets": ["CME", "ICE", "LME", "SGX", "EIA", "FRED"]
    },
    "download_scopes": {
      "gfex_all": {
        "enabled": true,
        "exchanges": ["GFEX"],
        "categories": ["all"],
        "series_types": ["main_continuous"],
        "domains": ["master_data", "trading_calendar", "daily_bars", "diagnostics"],
        "source_policy": "official_first_with_akshare_fallback",
        "calendar_quality_gate": "backfilled_verified",
        "request_policy": {
          "timeout_seconds": 20,
          "retry_attempts": 3,
          "retry_backoff_seconds": 30,
          "request_interval_seconds": 3,
          "manual_proxy_fallback": true
        },
        "schedule": {
          "daily_sync_enabled": false,
          "manual_backfill_only": true
        }
      },
      "domestic_all": {
        "enabled": false,
        "exchanges": ["all"],
        "categories": ["all"],
        "series_types": ["main_continuous"],
        "domains": ["master_data", "trading_calendar", "daily_bars", "spreads", "diagnostics"],
        "source_policy": "official_first_with_akshare_fallback"
      }
    },
    "sources": {
      "preferred_order": ["exchange_official", "akshare_futures"],
      "exchange_official": {
        "enabled": true,
        "enabled_exchanges": ["SHFE", "INE", "DCE", "CZCE", "GFEX"],
        "daily_interface": "official_daily_contract_bars",
        "timeout_seconds": 20,
        "retry_attempts": 2,
        "request_interval_seconds": 0.5,
        "fallback_to": "akshare_futures",
        "dce_browser": {
          "enabled": true,
          "bootstrap_page": "http://www.dce.com.cn/dce/channel/list/168.html",
          "env_browser_executable_path": "QUOTE_DCE_CHROME_PATH",
          "virtual_display": "auto",
          "settle_seconds": 9
        }
      },
      "akshare_futures": {
        "enabled": true,
        "mode_priority": ["direct", "proxy_patch"],
        "daily_interface": "futures_zh_daily_sina",
        "timeout_seconds": 30,
        "role": "free_aggregator_fallback"
      }
    },
    "master_data": {
      "maintain_contracts": true,
      "maintain_exchange_calendar": true,
      "default_research_series_type": "main_continuous",
      "category_dictionary": "managed_enum"
    },
    "trading_day_governance": {
      "enabled": true,
      "enabled_exchanges": ["all"],
      "official_calendar_backfill": {
        "source_profile": "exchange_official_daily_probe",
        "quality_flag": "backfilled_verified",
        "future_policy": "official_notice_only",
        "exchange_start_dates": {
          "SHFE": "2002-01-07",
          "INE": "2018-03-26",
          "DCE": "2000-06-01",
          "CZCE": "2015-10-15",
          "GFEX": "2022-12-22"
        }
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
      },
      "trading_days_per_year": 252
    }
  }
}
```

### 12.2 配置校验要求

配置加载和运行前校验必须覆盖：

- `download_scopes.*.exchanges` 只能使用启用交易所或 `all`。
- `download_scopes.*.categories` 只能使用已注册分类或 `all`。
- `all` 必须在运行前展开为具体目标，并写入 run metadata。
- `domains` 至少包含一个合法数据域。
- `daily_bars` 写入必须依赖 `trading_calendar` 质量门禁。
- 启用 `DCE` 官方源时必须能找到 Chrome 或明确以 dry-run/unavailable 诊断返回。
- 启用 `manual_proxy_fallback` 时不得在日志或报告中输出 token、proxy password、cookie。

### 12.3 `config/05_scheduler.json`

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
- 商品分类枚举和 `instrument_id` / `contract_id` / `series_id` 生成规则。
- 交易日历对非交易日、节假日、夜盘归属和最近交易日的判断。
- 真实合约日 K 到主力连续序列的映射和换月边界。
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

1. P0 清单中每个启用品种都有稳定 `instrument_id`、标准分类、交易所、单位、币种和默认 `main_continuous` 序列。
2. 官方源支持的国内品种可以保存真实合约日 K，并能构造或刷新主力连续序列。
3. 交易日历可以区分交易日、非交易日和最近应更新交易日，readiness 不把合法非交易日误判为缺口。
4. P0 清单中不少于 80% 品种完成本地历史序列同步。
5. 每个已启用品种有主数据、价格序列、来源 lineage 和最近同步状态。
6. 已启用品种可查询 3 年、5 年诊断；历史足够的品种可查询 10 年诊断。
7. 至少 5 个核心价差可重算并输出分位数。
8. 商品域 readiness 可解释缺口，不静默返回空结果。
9. DCF 周期模型可读取商品诊断摘要，缺失时返回 input gap。
10. 文档、配置、调度和 API 文档同步更新。

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

### 阶段零：独立期货配置和下载 scope

- 新增 `config/11_futures.json`，把期货配置从 `config/10_research.json` 拆出。
- 建立 `download_scopes`，支持交易所、分类、品种和序列四层范围选择。
- 支持 `exchanges=["all"]` 和 `categories=["all"]` 的通配配置，并在运行前展开为具体目标。
- 实现统一 `FuturesUniverseSelector`，供主数据治理、交易日历回补、行情 dry-run、历史回补、日更和 readiness 复用。
- 改造 CLI/API/scheduler，使期货任务可以按 scope 逐步上线，而不是一次性全交易所全品类运行。

### 阶段一：商品期货主数据、交易日历和数据字典

- 完善 `instrument_id`、`contract_id`、`series_id` 三层模型。
- 建立商品分类字典和国内/海外扩展命名规则。
- 新增主数据发现治理，自动发现官方源中的 unknown variety；通用配置型 adapter 覆盖所有已配置交易所，交易所特定字段通过 `known_products`、内置补充元数据或标准化 `FuturesProductSpec` enrichment 扩展。DCE/GFEX 已接入官方产品页或合约规格 enrichment 路径；DCE 还通过官方浏览器辅助 `contractInfo` 接口补充品种名称、合约交易单位和 tick，并通过显式规则元数据补齐已发现缺失根品种的报价单位与分类。主数据治理会在发现 unknown variety 前先尝试刷新已存在根品种，避免历史 P0 种子长期停留在英文名或空 metadata 状态；任何 enrichment 源失败必须进入任务报告 warnings。
- 新增交易所级交易日历和最近交易日判断。
- 新增真实合约主数据和合约日 K 存储结构。
- 暴露数据字典、合约、日历、source manifest 和默认主力连续查询 API。

### 阶段二：商品期货价格序列

- 新增商品域配置。
- 新增商品标的主数据。
- 实现 AKShare provider 和 fixture 测试。
- 建立 `futures_price_bars`、`futures_source_manifests`、`futures_readiness_snapshots`。
- 实现 P0 品种日线和连续序列同步。
- 暴露基础价格 API 和 readiness API。

### 阶段三：周期诊断和价差

- 实现 3/5/10 年分位数、均值、波动率、周期状态。
- 建立价差定义表和核心价差重算任务。
- 暴露 cycle diagnostics 和 spread API。
- 对接 DCF `cyclical_model_diagnostics`。

### 阶段四：行业/公司映射

- 建立行业级商品暴露映射。
- 支持公司级 override。
- 接入申万行业标准层。
- 暴露公司商品暴露和 input gaps API。

### 阶段五：现货、库存、开工率和海外增强

- 增加现货 provider 或手工导入模板。
- 增加交易所仓单、LME 库存、EIA 库存等相对可控来源。
- 增加开工率、库存和产量字段。
- 提高 mid-cycle price 和 mid-cycle margin 的诊断置信度。

---

## 16. 后续 OpenSpec 拆解建议

建议拆成连续 change，而不是一次性大包：

1. `add-futures-scope-config`
   - 独立 `config/11_futures.json`、`download_scopes`、`all` 通配语义、`FuturesUniverseSelector`、CLI/API/scheduler scope 参数和迁移兼容。
2. `add-futures-master-calendar-dictionary-api`
   - 商品根品种、真实合约、研究序列、分类字典、交易日历、数据字典 API、合约日 K API。
3. `add-commodity-market-data-storage-and-sync`
   - 商品主数据、P0 provider、价格表、日更/回补、基础 readiness。
4. `add-commodity-cycle-diagnostics-and-spreads`
   - 分位数、均值、波动率、价差定义、价差时序、诊断 API。
5. `connect-commodity-data-to-cyclical-dcf`
   - 行业/公司映射、DCF input gaps、`cyclical_model_diagnostics` 对接、API 文档和验收测试。

这样可以先形成可用数据资产，再逐步增强估值接入，避免第一版过度设计或被不可控现货源阻塞。
