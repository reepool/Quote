# 投研数据引擎（Research Data Engine）需求文档 v2

> 更新日期：2026-05-01
> 更新依据：基于 Quote System 当前代码、配置、数据库现状和本地基准测试。
> 系统定位：本系统只输出结构化数据、时间序列、事件流和确定性计算结果；不直接生成研究报告。下游消费者包括 AI 报告生成器、研究员工作台、筛选器和量化特征消费方。
> 配套执行文档：`docs/development/research_data_engine_execution.md`
> 配套工程变更包：`openspec/changes/stabilize-shenwan-official-mapping-cache/`
> 配套 source policy 变更包：`openspec/changes/harden-research-source-priority-policy/`
> 配套行业 readiness 变更包：`openspec/changes/add-industry-standard-readiness-api/`
> 配套股东 readiness 变更包：`openspec/changes/add-shareholder-readiness-api/`
> 配套 BSE 可空策略变更包：`openspec/changes/add-bse-empty-exchange-policy/`
> 配套申万组件集缓存变更包：`openspec/changes/cache-shenwan-component-sets/`
> 配套 official-code backlog 变更包：`openspec/changes/assetize-unmapped-official-code-backlog/`
> 配套 readiness backlog 可视化变更包：`openspec/changes/surface-unmapped-backlog-in-industry-readiness/`
> 配套 backlog override-review 信号变更包：`openspec/changes/add-override-readiness-signals-for-official-backlog/`
> 配套 backlog manual-override 建议变更包：`openspec/changes/add-manual-override-suggestions-for-official-backlog/`
> 配套申万官方分类主源变更包：`openspec/changes/promote-swsresearch-shenwan-classification-primary/`
> 配套申万指数分析历史回补变更包：`openspec/changes/add-akshare-sws-index-analysis-history/`
> 配套财务/XBRL 与相对估值加固变更包：`openspec/changes/harden-financial-xbrl-and-relative-valuation/`

---

## 0. 结论摘要

### 0.1 总体评估

- **方向合理**：你的核心理解是对的，这个系统本质上不是“再做一个行情下载器”，而是在现有 Quote System 之上，扩展出一个面向 AI 和研究员的 **本地优先研究数据服务层**。
- **原始需求文档的优点**：覆盖面广，模块拆分完整，已经把公司画像、财务、估值、技术分析、风险、舆情等主要研究对象列出来了。
- **原始需求文档的主要问题**：
  - 高估了免费外部数据源的稳定性和覆盖度，尤其是 **港股、分析师预期、研报、舆情、事件类数据**。
  - 混淆了“必须本地持久化的数据”和“可以实时计算/短期缓存的数据”。
  - 默认很多现有能力已经存在；实际工程推进中，这些能力都需要分阶段补齐。当前已落地研究域表、完整财务域、估值基线、技术指标域，以及 `shareholder_snapshots / analyst_forecasts / research_reports / sentiment_events / risk_snapshots` 基线；其中 `shareholders` 已完成全市场本地快照导入、周更调度和 `paid_high_availability` API 开放，`research metadata` 外部源模块仍需继续验证与调优。
  - 对 A 股和港股采用了近乎同等的数据可得性假设，这在现实中不成立。

### 0.2 本文档的核心结论

- **财务数据必须本地化存储**，并且要同时保留：
  - 原始报表快照
  - 规范化财务事实表
  - 派生财务指标表
  - 官方结构化披露文件 manifest、原始文件 hash、解析诊断和数据可得日
- **财务域必须区分当前已实现基线与下一阶段目标**：
  - 当前代码已具备 `financial_summaries`、`financial_statements_raw`、`financial_facts`、`financial_indicator_snapshots`、`valuation_history` 的 schema / sync / read API 基线
  - 当前财务主线已补齐 source manifest、全数值事实长表、hot/cold tier、多期回填/增量 catch-up checkpoint、覆盖缺口检测和仓库级 readiness 验证；官方结构化源仍因 URL 未验证而默认 disabled
  - 当前估值历史已补齐 PE/PB/PS 静态、TTM、forward/MRQ 口径拆分，相对估值已支持显式 metric variants、干净同行统计和排除诊断，scheduler/API readiness gate 已接入；剩余生产化关键在官方结构化源 live probe、小样本 backfill 与全量 readiness 复核
- **财务域不允许把上游 URL、报告期起点、限流、重试、并发、parser version、事实字段 alias、估值假设写死在业务逻辑里**；这些变量必须进入 `config/10_research.json`、scheduler 参数或后续独立 financial config，并在 `ingestion_runs` / source manifest 中留下运行时参数快照。
- **研究域数据源优先级应固定为“稳定免费 -> 稳定付费 -> 免费不稳定补充”**：
  - `BaoStock` 适合作为 A 股基础主数据、交易日历、复权因子、部分财务指标等稳定免费主源
  - `pytdx / TDX` 适合作为 A 股行情、板块、公司信息文本、除权除息和底层财务补充源
  - `efinance / 东方财富链路` 与 `AkShare` 在部分域具备更丰富覆盖，但都应视为存在反爬与结构波动风险的上游
  - 对缺乏可靠免费主源的域，应优先采用 `AkShare/efinance + proxy_patch` 这类稳定付费路径，再把直连免费源作为补充 fallback
- **`BSE` 必须保留在研究目标 universe 中**，但对暂不具备稳定上游覆盖的域，可按模块配置返回空占位结果，而不是让同步失败或读取硬 `404`
- **shareholders 已进入正式本地 snapshot/API 阶段**：
  - 当前配置为 `enabled=true / delivery_mode=paid_high_availability`
  - 导入主链采用 `AkShare:proxy_patch`，`cninfo:direct` 强制合并实控人线索，`akshare:direct / efinance:direct` 保留为 fallback
  - 周更任务 `shareholder_shadow_sync` 已启用，周六 `10:00` 对 `SSE / SZSE / BSE` 做全量刷新
  - rollout 复核命令：`python scripts/research_shareholder_rollout_validation.py --exchanges SSE,SZSE,BSE --skip-sync --fail-on-not-ready`
- **严格的申万行业体系应切换为申万官方分类文件主源**：
  - 分类主源使用 `swsresearch_classification_direct` 直连下载 `StockClassifyUse_stock.xls` 与 `SwClassCode_2021.xls`，直接维护官方分类 taxonomy、股票分类历史和 latest membership
  - 官方六位行业分类代码作为行业库主标识；`801xxx.SI / 85xxxx.SI` 申万行业指数代码只作为指数/行情/估值数据 alias，不与分类主键混用
  - 现有基于 `AkShare / Legulegu` 叶子成分股集合的 current membership 主线退居 fallback/诊断；当官方直连不可用时，`AkShare stock_industry_clf_hist_sw()` 可作为股票分类历史辅源，但应归一化进同一官方分类模型
  - strict Shenwan 行业表允许按迁移脚本整组清空并在新结构下重建，范围仅限行业标准层表，不影响其他 research 域
  - 日常刷新按每日任务执行，但通过 `ETag / Last-Modified / sha256 / 表内最大更新日期 / 覆盖率校验` 判断是否实际重写落库
  - 行业估值和指数表现另走 `industry_index_analysis_daily` 独立指标表：最新日更使用 `swsresearch_index_analysis_direct`，历史回补使用 AkShare-compatible `index_analysis_daily_sw` 上游语义；这些数据只按指数代码落表，不参与股票行业分类同步
  - **如果未能映射到申万一/二/三级官方分类口径，则这些字段只能作为公司基础信息中的参考字段，不能直接作为行业库、同行库或估值比较中枢使用**
- **技术指标不建议全量全历史预计算并长期存库**。优先采用：
  - 单标的实时计算
  - 最新快照缓存
  - 热门标的短期缓存
- **估值历史建议采用混合模式**：
  - `PE/PB/PS/market_cap` 等核心序列可以日更预计算
  - `DCF`、敏感性分析、同行对比等保留实时计算
- **部署策略建议 Phase 1 采用“服务集成、存储隔离”**：
  - 复用现有 Quote System 的代码仓、调度器、API、监控和 Telegram 运维能力
  - 研究域模块、表、路由和任务要独立命名与隔离
  - 为降低对现有生产库的影响，**推荐新增 `research.db` 或等价独立库文件**，而不是直接把全部研究表并入 `quotes.db`
  - 后续财务仓可从 `research.db` 拆分到 `data/financials.db`，但业务层必须通过 repository/storage 抽象访问，不允许在 `DataManager`、provider 或 API 层散落 SQLite 方言和文件路径假设
  - 若未来迁移到 DuckDB/PostgreSQL 等性能更优的数据库，应保持 `instrument_id + report_period + source lineage + availability metadata` 等逻辑模型不变，只替换存储适配层、迁移脚本和连接配置
- **市场范围建议分两期**：
  - Phase 1：A 股完整研究能力 + 港股价格/技术/基础信息能力
  - Phase 2：港股财务、估值、事件、分析师数据按源验证逐步补齐

---

## 1. 当前项目基线（基于现有代码与数据库事实）

本节不是目标设计，而是当前仓库已经存在的真实基础设施。后续设计必须建立在这些事实之上。

### 1.1 已有系统能力

| 项目 | 当前现状 |
|------|----------|
| 主数据库 | SQLite + WAL，路径 `data/quotes.db` |
| 数据库体量 | 约 `8.7 GiB` |
| 日线行情 | `daily_quotes` 约 `29,158,895` 行 |
| 交易标的 | `instruments` 约 `11,394` 条 |
| 交易日历 | `trading_calendar` 约 `47,702` 条 |
| 复权因子 | `adjustment_factors` 约 `88,090` 条 |
| 已启用市场 | `SSE`、`SZSE`、`BSE`、`HKEX` |
| 港股行情 | 已实际入库约 `9,299,564` 条日线 |
| 数据源路由 | PyTdx / BaoStock / AkShare / YFinance 已配置路由与降级链 |
| API | FastAPI，前缀 `/api/v1` |
| 调度 | APScheduler，配置化任务 + Telegram 通知 |
| 复权策略 | 原始价格入库，查询时动态复权 |

### 1.2 当前真实约束

| 约束 | 说明 |
|------|------|
| 研究域部分实现 | 当前已落地 `company_profiles`、`financial_summaries`、`shareholder_snapshots`、`industry_taxonomy`、`industry_official_classifications`、`industry_official_code_mappings`、`industry_memberships`、`financial_statements_raw`、`financial_facts`、`financial_indicator_snapshots`、`valuation_history`、`analyst_forecasts`、`research_reports`、`sentiment_events`、`risk_snapshots`；`shareholders` 已完成全量本地快照导入、周更调度、readiness 与 `paid_high_availability` API gate 开放 |
| 稳定性优先 | 当前研究域以“稳定免费 -> 稳定付费 -> 免费不稳定补充”为目标策略；基础研究域仍由 `BaoStock / PyTDX` 主导，而 strict Shenwan、股东摘要 whole-domain baseline、完整财报与部分 research metadata 域已进入 `AkShare proxy_patch` 主链优先的现实格局 |
| 北交所可空策略 | `BSE` 保留在 research universe 中；对暂不具备稳定上游覆盖的域，允许按模块配置返回空占位结果，不因无数据而阻断整体同步/读取 |
| 港股覆盖不对称 | 港股行情已接入，但研究类数据覆盖度明显弱于 A 股 |
| SQLite 是现实底座 | 当前系统已经证明 SQLite 可承载千万级行情；Phase 1 无需先迁库 |
| 文档与测试部分陈旧 | 现有部分文档和测试与代码不完全一致，后续应以代码和数据库为准 |

### 1.3 已识别的文档偏差

以下内容在原需求文档中不准确，已在本版修正：

- 原文称现有系统已有 `weekly_quotes`、`monthly_quotes` 表可直接复用。
  - **实际情况**：数据库中没有这两张表；仅在复权工具里预留了多周期接口。
- 原文默认多个研究类 AkShare 接口等同于“可稳定工程化使用”。
  - **实际情况**：免费接口可用于原型，但必须视为高波动上游，不能作为读取链路实时依赖。
- 原文将港股与 A 股在多个模块上视为同等可行。
  - **实际情况**：港股在公司资料、财务、分析师预期、舆情事件上的可得性明显更弱。

---

## 2. 项目定位与系统边界

### 2.1 目标定位

本项目的目标不是生成结论，而是提供：

- 统一代码体系下的 **研究数据中台**
- 面向 AI 与研究员的 **稳定读取接口**
- 面向量化/特征工程的 **确定性指标输出**
- 可追溯、可缓存、可调度、可回填的 **本地研究数据资产**

### 2.2 系统边界

```text
┌──────────────────────────────────────────────────────────┐
│                  下游消费者 / Consumer                   │
│  AI 报告生成器 / 研究员工作台 / 筛选器 / 量化特征调用方    │
└───────────────────────┬──────────────────────────────────┘
                        │
                        │ REST / Internal API
                        ▼
┌──────────────────────────────────────────────────────────┐
│               Research Data Engine（本系统）             │
│                                                          │
│  ┌──────────────┐  ┌────────────────┐  ┌──────────────┐  │
│  │ 数据采集层   │  │ 计算与衍生层   │  │ API 服务层   │  │
│  │ ingestion    │  │ analytics      │  │ read service │  │
│  └──────┬───────┘  └────────┬───────┘  └──────┬───────┘  │
│         │                    │                 │          │
│         └──────────────┬─────┴─────────────────┘          │
│                        ▼                                  │
│              本地存储层（DB / Cache / Audit）            │
└───────────────────────┬──────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────┐
│                    外部数据源 / Sources                  │
│   AkShare / BaoStock / YFinance / pytdx / 公告与网页源   │
└──────────────────────────────────────────────────────────┘
```

### 2.3 非目标（Phase 1 不做）

- 不做自动写报告、自动下结论、自动推荐投资结论
- 不做分钟级/逐笔级实时数据引擎
- 不做完整 PDF 年报解析流水线
- 不做通用回测框架
- 不做跨市场统一研究覆盖到美股

---

## 3. 设计原则

### 3.1 Local-First 原则

对下游读取频繁、对外部源稳定性敏感、需要可审计/可复现的数据，必须本地化。

### 3.2 原始真相与衍生结果分离

- 原始数据：外部接口返回的报表、事件、元数据
- 规范化数据：可直接用于查询和计算的结构化字段
- 衍生结果：估值、技术指标、风险评分、快照缓存

衍生结果可以重算，原始数据必须可追溯。

### 3.3 读链路不依赖外网

P0/P1 阶段的核心读取接口不应在请求时访问外部数据源。
外部源只能出现在：

- 定时同步任务
- 后台回填任务
- 管理员触发的补数任务

### 3.4 A 股优先，港股逐步扩展

市场目标是 A 股 + 港股，但不能假设两者在数据源层面对称。
工程策略必须是：

- A 股：主交付面
- 港股：先交付价格驱动模块，再验证基本面模块

### 3.5 复用现有 Quote 底座

必须复用现有：

- `instruments`
- `daily_quotes`
- `trading_calendar`
- `adjustment_factors`
- FastAPI / Scheduler / Telegram / 日志 / 监控

避免重复造轮子，也避免复制一套行情库到另一个服务里。

### 3.6 算法可扩展与版本化原则

`DCF`、技术指标、风险评分、同行对标都必须按“可替换算法模块”设计，而不是在 API 层写死公式。

工程约束如下：

- 所有计算模块必须显式区分：
  - `method`
  - `version`
  - `parameters`
- API 请求允许传入有限的参数覆盖，但系统必须保留默认参数集
- 计算结果需要记录：
  - `calc_method`
  - `calc_version`
  - `parameter_hash`
  - `data_as_of`
- 不把估值常数、指标窗口、阈值直接写入业务代码
- 新旧算法版本允许并存一段时间，以便回溯与 A/B 对比

建议在研究域中引入如下抽象：

- `ValuationEngine`
- `TechnicalIndicatorEngine`
- `RiskScoringEngine`
- `PeerSelectionStrategy`

### 3.7 参数化、配置化与项目风格一致性

后续所有研究域开发必须遵循当前项目已有的配置驱动风格，而不是为新需求临时堆分支逻辑。

具体要求：

- 数据源优先级、限流、重试、开关、市场范围全部配置化
- 指标窗口、估值假设、行业映射规则、缓存 TTL 全部配置化
- 统一复用现有：
  - `config_manager`
  - `BaseDataSource / RateLimitConfig`
  - Scheduler 配置体系
  - FastAPI 路由风格
  - 日志、告警、任务报告机制
- 禁止新增研究模块时复制一份独立的抓取/调度/日志框架
- 如果复用会明显扭曲现有抽象，则允许新增独立研究域基类，但接口风格必须与现有系统保持一致
- 对 `akshare_proxy_patch` 这类跨源基础设施，必须采用统一运行时入口集中安装与诊断，不允许在多个模块、脚本中重复注入、硬编码 token 或分散维护 patch 逻辑
- 统一 patch 运行时应提供：
  - 幂等安装
  - 状态查询
  - 配置集中读取
  - 标准验证脚本
  - 影响边界与重启说明文档

### 3.8 生产安全与兼容性优先

研究域建设必须默认“不破坏现有生产链路”。

因此需要遵守：

- 不修改现有行情表语义
- 不改变现有 API 行为和响应格式
- 研究域新增接口统一挂在 `/api/v1/research/*`
- 研究域任务独立配置、独立开关、独立告警
- 研究数据回填完成前，默认不对外开放依赖它的生产接口
- 新模块先影子运行，再灰度放量

---

## 4. 优化后的目标架构

### 4.1 架构分层

```text
L0  Quote Core（已存在）
    instruments / daily_quotes / trading_calendar / adjustment_factors

L1  Research Ingestion（新增）
    company/profile sync
    financial statement sync
    analyst/report/sentiment sync
    raw payload audit

L2  Research Storage（新增）
    company profiles
    financial raw + facts + indicators
    reports / forecasts / events
    valuation history
    technical latest snapshot cache

L3  Research Analytics（新增）
    peer comparison
    relative valuation
    DCF
    technical indicator engine
    pattern recognition
    risk scoring

L4  Research API（新增）
    /api/v1/research/company/*
    /api/v1/research/industry/*
    /api/v1/research/screener/*
```

### 4.2 与现有 Quote System 的关系

| 现有能力（直接复用） | 新增能力（本次建设） |
|----------------------|----------------------|
| 标的主数据 `instruments` | 公司档案、行业映射、股本快照 |
| 日线行情 `daily_quotes` | 财务报表、财务指标 |
| 复权因子与动态复权 | 估值历史、DCF、Beta、风险评分 |
| 交易日历 | 研究数据同步调度任务 |
| 数据源工厂与路由 | 研究数据采集器与数据治理 |
| API 框架与调度器 | 研究域 API 与研究域表 |

---

## 5. 数据域划分与存储策略

### 5.1 数据域划分

| 数据域 | 内容 | 是否本地持久化 | 原因 |
|--------|------|----------------|------|
| D0 市场基础数据 | 标的、日线、交易日历、复权因子 | 是 | 已是现有系统核心 |
| D1 公司/行业参考数据 | 公司档案、行业分类、股本、市值快照 | 是 | 高频读取，更新频率低，适合本地 |
| D2 财务基础数据 | 三大报表、摘要指标、分红 | 是 | 估值和研究的核心底座 |
| D3 研究元数据 | 分析师预期、研报元数据、舆情事件 | 是 | 读取频繁，外部源波动大 |
| D4 衍生研究数据 | 财务指标、Beta、风险指标、估值历史 | 是/部分是 | 高频展示或筛选需要本地化 |
| D5 技术分析结果 | 技术指标序列、最新技术快照、形态识别结果 | 混合 | 单标的实时算足够快，不应全量全历史入库 |
| D6 审计与回溯 | 原始 payload、同步任务日志、版本信息 | 建议是 | 便于追责、排错、重算 |

### 5.2 建议的持久化策略

#### 必须持久化

- `company_profiles`
- `industry_taxonomy`
- `industry_component_sets`
- `industry_memberships`
- `industry_official_classifications`
- `financial_statements_raw`
- `financial_facts`
- `financial_indicator_snapshots`
- `analyst_forecasts`
- `research_reports`
- `sentiment_events`
- `valuation_history`
- `ingestion_runs`

#### 建议缓存而不是全历史持久化

- `technical_indicator_latest`
- 热门标的技术指标时间序列缓存
- DCF 参数场景结果短期缓存
- 模式识别结果短期缓存

#### 不建议在 Phase 1 做全量全历史持久化

- 全市场、全历史、全指标的 `technical_indicators_cache`
- 全市场、全历史的复杂形态识别结果表

原因不是算不出来，而是 **性价比低**：对研究员和 AI 的典型请求形态而言，单标的即时计算更便宜、更灵活。

### 5.3 研究数据源评估与主备策略

这一部分回答两件事：

- 免费开放接口分别能承担什么角色
- 当免费直连不够稳定时，哪些域应前置付费代理链路

#### 5.3.1 结论先行

- **项目级数据源策略不再是“非 AkShare 优先”，而是“稳定性优先”**。
- **当存在稳定免费源时，应优先使用稳定免费源；当不存在可靠免费主源时，应优先使用稳定付费链路。**
- **BaoStock 不是完整财报源**，更适合做 A 股基础主数据、交易日历、复权因子、部分财务指标与校验源。
- **efinance 更像东方财富数据聚合层**，在公司概览、板块归属、股东户数、前十大股东、季度业绩摘要上很有价值，但工程上应视为网页聚合源。
- **pytdx 更偏底层行情与原始资料接口**，可提供公司信息目录/文本、板块文件、财务信息和除权除息；它可以进入 `company_profile` 与 `financial_summary` 的主备链，但**不能单独承担标准化股东/股权结构主源**。
- **“申万行业”与“股权结构”都不应寄希望于单一免费直连源一次性解决**，应采用本地归一化与多源拼接。
- **当前已本地验证可直接更新申万一/二/三级标准层的免费链路是 `AkShare`**：
  - 已核实接口：`sw_index_first_info / sw_index_second_info / sw_index_third_info / sw_index_third_cons`
  - `BaoStock / PyTDX / efinance` 当前都更适合作为行业参考层或映射线索层，而不是 strict Shenwan authoritative 主源

#### 5.3.2 能力矩阵

| 数据源 | 公司基本信息 | 股东/股权结构 | 行业/板块 | 财务数据 | 覆盖市场 | 工程评估 | 推荐角色 |
|--------|--------------|---------------|-----------|----------|----------|----------|----------|
| `BaoStock` | 有，`query_stock_basic` | 弱，无成体系的前十大股东/股权结构接口 | 有 `query_stock_industry`，但**不能默认等于申万行业** | 有财务与业绩相关接口，但更偏季度指标/摘要，**不等于完整三大报表** | A 股为主 | 接口相对结构化，当前项目已集成；但库内部有全局连接约束 | A 股基础主数据与财务指标补充源 |
| `efinance / 东方财富链路` | 有，`get_base_info` | 中，`get_top10_stock_holder_info`、`get_latest_holder_number` 可用，但仍不等于完整股权结构 | 有，`get_belong_board` 与行业字段，但**不是申万标准口径** | 有，`get_all_company_performance` 等摘要型财务/业绩数据；完整报表能力不宜直接假设 | A 股强，港美行情也有一定覆盖 | 字段面广、更新活跃，但本质是网页聚合链路；如需借助 `ak_proxy_patch` 稳定可用性，则与 AkShare 一样会引入额外成本 | 公司画像、股东摘要、行业板块、财务摘要主源 |
| `pytdx / TDX` | 有，公司信息目录与文本 | 弱，更多是文本与原始资料，不是标准化股权结构表 | 有板块文件与板块解析，但**不保证申万口径** | 有 `get_finance_info` 与历史专业财务抓取能力，但解析成本高、字段口径偏底层 | A 股强，扩展行情可覆盖部分港股/期货 | 速度快，但连接与协议层复杂，适合底层补数与校验 | 行情、除权除息、板块、原始资料补充源 |
| `YFinance` | 有 | 有部分 holders / analyst / financials | 有 sector / industry，但为 Yahoo 体系 | 有 income statement / balance sheet / cashflow / estimates | 港股/美股相对更有用 | 免费但非中国市场专用，且文档明确是研究/个人用途导向 | 港股与美股补充源，不作为 A 股研究主源 |
| `巨潮资讯 / HKEXnews / 交易所官网` | 有，且更权威 | 有，但多以公告/F10/披露页面存在 | 有行业与公司要览页面 | 有公告、财报、披露文件 | A 股/港股官方披露 | 权威但工程化成本高，适合低频补数和审计 | 审计源、回填源、关键字段兜底源 |

#### 5.3.3 对你关心字段的逐项判断

| 字段类型 | BaoStock | efinance | pytdx / TDX | 评估结论 |
|----------|----------|----------|-------------|----------|
| 公司基本信息 | 可以满足基础上市资料 | 可以满足，并且字段更丰富 | 可以拿到公司信息目录、正文文本与底层财务资料 | **可做多源备份**，若优先规避高反爬链路，推荐 `BaoStock -> pytdx -> efinance -> AkShare` |
| 股权结构信息 | 基本不够 | 可拿到前十大股东、股东户数等摘要 | 以原始资料为主，结构化弱 | **不能靠单一免费源一次性满足“完整股权结构”** |
| 申万行业信息 | 仅能提供 `query_stock_industry` 级别的参考分类，**不能视为 strict Shenwan authoritative** | 当前环境未完成本地接口核验；即使有行业/板块字段，也应先视为 reference-only | 当前可确认的是板块文件与板块解析能力，**不能视为 strict Shenwan authoritative** | **当前已验证可更新申万一/二/三级标准层的免费接口是 `AkShare`；其他免费源应视为映射线索层，不能直接驱动同行库和相对估值** |
| 财务数据信息 | 季度财务指标/摘要较强 | 业绩摘要、部分财务概览较强 | 有 `get_finance_info` 与历史专业财务抓取能力，但工程化成本较高 | **若目标是“财务摘要 + 关键指标”，`BaoStock + pytdx` 可作为基础链；若目标是完整三大报表，非 AkShare 免费源仍不足以完全替代** |

#### 5.3.4 推荐的主备方案

按数据域拆分后，推荐优先级如下：

- 当前工程执行口径：
  - 统一遵循“1. 稳定+免费，2. 稳定+收费，3. 不稳定+免费补充”
  - `BSE` 若暂无稳定上游覆盖，可按模块配置返回空占位；不要求所有域必须通过同一函数拿到北交所数据

- `instrument master / trading calendar / adjustment factors`
  - 主：现有 `pytdx + BaoStock`
  - 备：`AkShare`
- `company profile / base info`
  - 当前已实现主链：`BaoStock`
  - 当前已实现备链：`pytdx`
  - 继续保持稳定免费主链，不默认迁移到 `AkShare`
  - 免费补充 fallback：`efinance:direct / akshare:direct`
- `shareholder summary / holder number / top10 holders`
  - 当前系统级主源：`AkShare:proxy_patch`
  - 稳定付费补充：`efinance:proxy_patch`
  - 官方直连 fallback：`cninfo:direct`
  - 免费补充 fallback：`akshare:direct / efinance:direct`
  - 备：`巨潮资讯 / 交易所披露页解析`
  - `2026-04-19` 直连 live probe 已验证 `cninfo` 的 `股东人数及持股集中度`、`实际控制人持股变动`、`公司股本变动` 接口在当前环境下延迟约 `0.24s - 1.7s`，且 `10` 次 burst 未触发明显反爬；因此 `cninfo` 可视作股东域的官方低成本直连源
  - 但当前未验证 `cninfo` 对 `top10_holders` 提供同等级、稳定、可工程化的官方接口；即使同步层已支持缺失标的继续 fallback，**也不建议直接把整个 shareholders 域主源切为 `cninfo`**
  - 因此当前更合理的工程决策是：以 `AkShare:proxy_patch` 承担现阶段 whole-domain baseline 主源，`cninfo:direct / akshare:direct / efinance:direct` 作为 fallback；当前同步层已进一步升级为“缺失标的 + 缺失 required scope 继续 fallback 并按 scope merge”，同一标的会保留首个成功 snapshot 的 primary source，但允许后续 provider 补齐 `holder_count / top10_holders / reference_only_ownership_clues`
  - 当前已新增 `/api/v1/research/shareholders/readiness`，用于统一读取股东摘要覆盖率、source/source_mode 分布、按交易所覆盖率、模块 gate 与 `paid_high_availability` rollout blockers；后续 shareholders 是否开放正式读取，应优先以这条 readiness 接口为准
  - 当前已新增 `scripts/research_shareholder_rollout_validation.py`，用于将 shareholders shadow sync 与 readiness 复核串成可重复执行的工程命令；`--exchanges / --enable-module / --delivery-mode` 只做本次进程内覆盖，不改写配置文件
  - `2026-04-19` 代表性真实环境复验已确认：
    - `cninfo:direct` 可稳定返回 `holder_count + reference_only_ownership_clues`
    - `AkShare:proxy_patch` 可稳定返回 `holder_count + top10_holders + reference_only_ownership_clues`
    - shareholders raw payload 审计链路此前存在 `date` 序列化假失败，现已修复
    - 但这仍只是 `600519.SH / 000001.SZ` 级别的代表样本验证，不等于 whole-domain 已完成全市场 rollout
  - `2026-04-19` 扩大样本复验（`SSE 5 + SZSE 5`）进一步确认：
    - `cninfo:direct` 虽然能做到 `10 / 10` snapshot 覆盖，但 `holder_count` 仅覆盖 `6 / 10`，且 `top10_holders = 0 / 10`
    - `AkShare:proxy_patch` 在同一扩大样本上实现了 `holder_count / top10_holders / ownership_clues` 三个 required scope 的 `10 / 10`
    - 因此 shareholders 的 rollout 判定不能只看 snapshot 覆盖，必须引入 `required_scope` 维度；当前 readiness API 已按这个口径升级
  - `2026-04-19` 进一步扩大样本复验（`SSE 20 + SZSE 20`）后，结论收紧：
    - `direct` 链路（`cninfo:direct -> akshare:direct`）虽然已通过 scope-aware merge 将 `top10_holders` 提升到 `31 / 40`，但整体仍未满足 required scope
    - `proxy-first` 链路（`akshare:proxy_patch -> cninfo:direct -> akshare:direct`）在该轮大样本中 `top10_holders = 0 / 40`，whole-domain readiness 明确仍为 `not ready`
    - 对 `600000 / 000001 / 600519` 的单标的 probe 已确认 `AkShare` 原始接口与 provider 在 `direct / proxy_patch` 下都能正常产出 `top10_holders = 10`
    - 因此当前问题更像是批量运行时的稳定性/节流退化，而不是字段提取逻辑错误；whole-domain shareholders 仍不能进入 `paid_high_availability` rollout
    - 为下一轮排障，`AkShare` 股东 provider 已把分字段抓取异常写入 `raw_payload.fetch_errors`
  - `2026-04-20` 又对 `AkShare` 股东 provider 增加了可配置的 `top_holders` 节流与重试参数（默认 `request_interval=0.2 / retry_attempts=2 / retry_backoff=0.5`），并重新做了大样本复验：
    - `direct` 链路在这轮复验中进一步退化为 `top10_holders = 0 / 40`，且 `raw_payload_audit` 中出现 `38` 条 `akshare:direct` 的 `fetch_errors.top_holders = "No tables found"`
    - `proxy-first` 链路则从此前的 `top10_holders = 0 / 40` 恢复到 `33 / 40`，其中 `SSE = 20 / 20`、`SZSE = 13 / 20`
    - 这说明节流+重试对 `AkShare:proxy_patch` 主链是有效的，但 whole-domain readiness 仍然未达标
    - 因此当前结论更新为：`AkShare:proxy_patch` 仍是 whole-domain baseline 的唯一现实候选，`AkShare:direct` 不应作为 batch 主链；`shareholders` 继续通过模块 gate 控制是否正式开放
  - `2026-04-20` 又新增了 bounded same-source recovery pass，并对 live validation 做了补充复验：
    - shareholders 模块已新增 `same_source_recovery_candidates / batch_size / max_instruments` 三个配置项，ingestion metadata 也会记录 recovery run/attempt/resolved 计数
    - 针对上一轮 `SZSE` 剩余的 `7` 个 gap symbol 做 focused validation 后，`direct` 和 `proxy_patch` 两条链路都已达到 `7 / 7` required scope 完整覆盖；但 recovery 计数仍为 `0`，说明本轮成功不是靠 recovery 才达成
    - 随后重新执行 `SSE 20 + SZSE 20` 的 `proxy_patch` 大样本复验，结果已提升为 `40 / 40`，`holder_count / top10_holders / ownership_clues` 三个 required scope 也都是 `40 / 40`
    - 该样本下 readiness 返回 `ready_for_paid_high_availability_rollout = true`、`blockers = []`
    - 但这个结论仍然只是样本级 shadow validation，不应直接等同于全市场 rollout-ready
    - 当前项目决策已明确：不再把继续扩大样本或全市场验证作为继续开发的前置条件；后续保持 `AkShare:proxy_patch` 作为默认导入主链，正式 snapshot/API 由模块 gate 和业务决策控制
  - `2026-04-30` 全量导入与 API gate 收口：
    - `config/10_research.json` 已将 `shareholders.enabled=true`、`delivery_mode=paid_high_availability`、`snapshot_api_requires_mode=paid_high_availability`
    - `config/05_scheduler.json` 已启用 `shareholder_shadow_sync`，周六 `10:00` 对 `SSE / SZSE / BSE` 全量刷新，`limit_per_exchange=null`
    - 只读 readiness 复核：`snapshot_api_enabled=true`、`ready_for_paid_high_availability_rollout=true`、`blockers=[]`
    - 当前目标股票数 `5191`，`holder_count / top10_holders / reference_only_ownership_clues` 三个 required scope 均覆盖 `5191 / 5191`
    - 当前快照来源分布为 `akshare:proxy_patch = 5191`；`BSE` 已有 `300` 条快照，但 readiness target 仍按 `optional_empty_exchanges=["BSE"]` 口径不计入必达目标
    - 股东快照 API 已可通过 `GET /api/v1/research/company/{instrument_id}/shareholders` 本地读取，readiness 通过 `GET /api/v1/research/shareholders/readiness` 查看
- `industry classification`
  - 原始输入：`efinance board + BaoStock industry + pytdx block`
  - 系统输出：**本地归一化行业表**
  - 严格申万口径拟升级为官方分类主源：优先通过 `swsresearch_classification_direct` 下载申万官方 `StockClassifyUse_stock.xls` 与 `SwClassCode_2021.xls`，由官方六位行业分类代码直接维护一/二/三级 taxonomy、全量股票分类历史和 latest authoritative membership
  - `801xxx.SI / 85xxxx.SI` 申万行业指数代码不再作为行业库主键；它们作为指数行情、行业估值、成分股和 index-analysis 数据的 alias 字段保留，避免和官方分类代码混用
  - `AkShare stock_industry_clf_hist_sw()` 本质上是官方股票分类历史文件的包装，后续应退居官方直连不可用时的股票分类历史 fallback；`sw_index_first_info / second_info / third_info / third_cons` 退居指数 alias、诊断或 legacy fallback，不再作为分类 membership 主事实
  - 迁移时允许清空并重建 strict Shenwan 行业表：`industry_taxonomy / industry_memberships / industry_component_sets / industry_official_classifications / industry_official_code_mappings` 中与旧 index-code 主线相关的数据均可由新官方分类主线重新落库或归档
  - 新结构应补充 source-file manifest 与 full classification history，记录 URL、`ETag`、`Last-Modified`、sha256、行数、最大 `更新日期`、parser version 与 ingestion run id，支持每日刷新时稳定判断文件是否更新
  - 行业估值/指数分析已单独接入 `industry_index_analysis_daily`，字段包括收盘指数、成交量、涨跌幅、换手率、市盈率、市净率、均价、成交额占比、流通市值、平均流通市值、股息率；最新日更由 `swsresearch_index_analysis_direct` 负责，历史区间回补由 AkShare-compatible `index_analysis_daily_sw` 上游语义负责；该表按指数代码落独立指标，不参与股票分类判定
  - `2026-04-20` 已新增 `industry_component_sets`，用于缓存申万叶子行业成分股集合；provider 优先使用 `sw_index_third_cons`，再 fallback 到 `index_component_sw`
  - `2026-04-23` 已新增东财行业名补源：对当前 AkShare 组件集未覆盖的 12 只钢铁标的，东财 `f127` 均返回 `特钢Ⅱ`，已按本地 taxonomy 名称匹配写入 `801045.SI / 特钢Ⅱ`；因当前本地 taxonomy 中该节点是二级 leaf，`sw_l3_code` 保持为空
  - `2026-04-24` 已新增新浪结构化补源：从新浪财经 `相关资料` 页解析当前有效的申万相关行，优先采用 `85xxxx` 三级行业候选；若只得到 `801xxx` 二级候选，仍沿用本地 taxonomy 的 level-2 leaf-only 校验。东财继续作为轻量行业名 fallback，雪球暂保留为人工复核参考而不进入默认自动链路
  - `2026-04-24` 已优化定向 gap fill：在已有本地 taxonomy 缓存且只修具体 missing instrument ids 时，复用 `industry_taxonomy` 并跳过 audit-only official classification 拉取，避免单标的补缺被远端审计链路拖慢
  - `2026-04-24` 进一步验证申万官方分类文件对当前 active `SSE/SZSE/BSE` 股票 universe 全覆盖；官方主源落地后，应重新评估 `industry_standard` 的 `BSE optional-empty` 口径，目标是不再把 BSE 排除在 strict Shenwan readiness 之外
- `financial summary`
  - 当前已实现主链：`BaoStock`
  - 当前已实现备链：`pytdx`
  - 继续保持稳定免费主链，不默认迁移到 `AkShare`
  - 免费补充 fallback：`efinance:direct / akshare:direct`
- `full financial statements`
  - 目标主链：交易所、巨潮或其他官方结构化披露/XBRL/等价结构化文件 provider，按交易所、报告期、公告文件 manifest 发现和下载
  - 当前可运行基线：`AkShare:proxy_patch -> AkShare:direct`，只承诺最新报告期快照，不承诺完整历史仓和官方来源可审计性
  - `BaoStock / PyTDX`：保留为财务摘要、关键字段交叉校验或缺口诊断源，不作为完整三大报表主源
  - fallback 规则：官方源缺失、解析失败或字段不足时，允许配置化使用 AkShare fallback 补齐缺失期间或核心事实，但不得覆盖已确认的更高优先级官方事实；必须记录 fallback reason、source_mode 和 source-file lineage
  - 结论：**下一阶段完整财报目标不是继续扩大 AkShare 最新期快照，而是先做官方结构化源探测、manifest 和 parser，再把 AkShare 作为补充链路接入多期财务仓**

#### 5.3.5 对当前仓库的落地要求

当前项目已经有行情路由与降级链，因此研究域不应另起炉灶。
建议新增独立配置，例如：

```text
research_routing.company_profile
research_routing.shareholders
research_routing.industry
research_routing.financial_summary
research_routing.financial_statements
```

每个域单独配置：

- `primary`
- `secondary`
- `fallback`
- `timeout`
- `rate_limit`
- `enabled`

这样未来替换 `efinance`、补充 `AkShare`、或者接入官方披露解析时，不需要改业务代码。

---

## 6. 存储与计算可行性评估

### 6.1 本地存储容量评估

当前 `quotes.db` 已经在 SQLite 中稳定承载：

- `29,158,895` 条日线
- 约 `8.7 GiB`
- 平均约 `320 bytes/row`（含索引与元数据开销）

这说明本地数据库并不是瓶颈的第一来源。基于此，可以对研究数据做保守估算：

| 数据类型 | A 股估算 | A 股 + 港股估算 | 结论 |
|----------|----------|----------------|------|
| 财务原始报表 + 规范化事实 + 指标 | `1.5 ~ 4 GiB` | `3 ~ 8 GiB` | 可接受 |
| 估值历史（PE/PB/PS/market_cap，5~10年） | `1 ~ 3 GiB` | `2 ~ 5 GiB` | 可接受 |
| 技术指标最新快照缓存 | `< 200 MiB` | `< 400 MiB` | 很轻 |
| 技术指标全量全历史缓存 | `> 5 GiB` | `> 10 GiB` | Phase 1 不建议 |

> 说明：以上为工程估算，不是实测值。估算依据为当前 SQLite 实际承载规模、目标字段数量以及行级索引开销。

### 6.2 全量初始化耗时评估

如果把“完整三大报表”放在 Phase 1，就仍然要考虑 `AkShare` 或官方披露解析的批量抓取成本；
如果 Phase 1 先落“财务摘要 + 关键指标”，则可优先走 `efinance + BaoStock`，初始化成本会更低。

按 A 股约 `5,498` 个股票估算：

- 三大报表最少约 `3` 次/公司
- 若再加摘要指标、分析指标、分红信息，约 `5~6` 次/公司
- 总请求量约 `16,000 ~ 33,000` 次

在当前配置中，若使用 `AkShare` 作为完整报表兜底源，设定限流为 `120 req/min`，理论极限为：

- `2.2 ~ 4.6` 小时

考虑到免费源常见的：

- 重试
- 反爬
- 空响应
- 数据结构异常

实际工程耗时更保守地应按：

- **A 股首轮初始化：4 ~ 12 小时**
- **季度增量更新：可在夜间/周末任务内完成**

这个耗时仍然是可接受的，因为它是 **后台批处理成本**，不是前台交互成本。
但它也说明了一个事实：**“不用 AkShare”与“完整财报一次到位”很难同时成立**。

### 6.3 实时计算性能评估

基于当前本地行情库做了小型基准测试：

- 单标的 `000001.SZ`，读取 `8,550` 条日线并计算一组常见技术指标：
  - `SMA / MACD / RSI / KDJ / Bollinger / ATR / OBV`
  - **总耗时约 `42 ~ 47ms`**
- 10 个标的，合计 `45,945` 条日线，计算 `MACD / RSI / ATR`：
  - **总耗时约 `185 ~ 308ms`**
- 对 `8,550` 个交易日价格序列与 `40` 个季度点做本地估值时间轴映射模拟：
  - **纯计算约 `7 ~ 8ms`**

### 6.4 由此得出的结论

- **技术指标**：单标的、少量标的实时算完全可接受。
- **估值历史**：如果财务数据已经本地化，单标的实时算也可接受。
- **真正不适合实时访问外源的，是财务与研究元数据获取本身**。

因此：

- 财务、报告、预期、事件类数据必须本地化
- 技术分析应以实时计算为主
- 估值采用混合策略

---

## 7. 模块可行性与优先级（修正版）

### 7.1 模块矩阵

| 模块 | 内容 | A 股可行性 | 港股可行性 | 推荐存储策略 | 优先级 |
|------|------|------------|------------|--------------|--------|
| M01 | 公司档案 | 高 | 中 | 本地快照表 | P0 |
| M02 | 业务画像 | 中 | 低 | 本地快照 + 后续 PDF 补强 | P2 |
| M03 | 行业定位 | 高 | 中 | 本地映射 + 实时计算 | P1 |
| M04 | 财务报表与财务指标 | 高 | 中低 | 原始报表 + 事实表 + 指标表本地化 | P0 |
| M05 | 分析师预期 | 中 | 中低 | 本地快照表 | P1 |
| M06 | 增长驱动元数据 | 中 | 低 | 基于 M04/M09 的派生输出 | P2 |
| M07 | 竞争对标 | 高 | 中 | 实时计算 + 局部缓存 | P1 |
| M08 | 供需代理指标 | 中 | 低 | 基于财务表实时计算 | P2 |
| M09 | 研报元数据 | 中 | 中低 | 本地元数据表 | P1 |
| M10 | 估值引擎 | 高 | 中 | 核心估值历史本地化，DCF 实时 | P0 |
| M11 | 风险指标 | 高 | 中 | 市场风险实时算，事件风险本地表 | P1 |
| M12 | 舆情/行为数据 | 中 | 低 | 本地事件表 | P1/P2 |
| M13 | 技术指标引擎 | 高 | 高 | 实时计算 + 最新快照缓存 | P0 |
| M14 | 形态识别 | 中 | 中 | 实时计算 + 短期缓存 | P2 |

### 7.2 对原文可行性矩阵的修正

原文中以下判断过于乐观，本版已下调：

- `M05 分析师预期`：不能视为“免费源完全覆盖”
- `M09 研报数据`：更适合“元数据覆盖”，不应承诺稳定摘要全文
- `M12 舆情数据`：A 股可做，港股不能默认完全覆盖
- `M02 / M06 / M08`：如果不引入年报 PDF 解析，深度有限

---

## 8. 关键问题答复（对应原文 Q1-Q6）

### Q1：市场范围是否同时覆盖 A 股和港股？

**结论**：目标市场可以是 A 股 + 港股，但交付不能同步同深度。

**建议**：

- **Phase 1**
  - A 股：覆盖 P0/P1 研究能力
  - 港股：覆盖基础信息、行情、技术指标、部分估值
- **Phase 2**
  - 港股：补充财务、行业、事件、分析师等研究能力

**原因**：

- 当前仓库已经有港股行情底座，但研究源并不对称
- 如果一开始强行要求 A/H 同深度，会显著拖慢 P0 交付

### Q2：财务数据是否应本地存储？

**结论**：应该，而且是必须。

#### 推荐方案

- 本地存储 **原始财务报表快照**
- 本地存储 **规范化财务事实表**
- 本地存储 **派生财务指标**

#### 原因

1. **存储空间可接受**
   - 相比当前 `8.7 GiB` 行情库，财务数据增加的容量是可控的。
2. **初始化耗时可接受**
   - 首次批量同步虽慢，但属于后台任务，不影响前台读取。
3. **实时访问外部财务接口不可接受**
   - 单公司一次研究请求可能需要 5~10 次外部调用
   - 实际交互延迟会落在秒级，且极易受反爬和波动影响
4. **估值/同行/风险都依赖财务底座**
   - 如果财务不本地化，M07/M10/M11 都会变得不稳定

### Q3：技术指标和估值历史采用哪种计算策略？

**结论**：采用 **混合模式（Hybrid）**。

#### 技术指标（M13）

- **默认策略**：API 请求时实时计算
- **缓存策略**：保存“最新一期快照”和热门标的短期缓存
- **不建议**：全市场全历史全指标日更入库

#### 估值历史（M10）

- **核心序列**：`PE/PB/PS/market_cap` 建议日更预计算
- **实时部分**：`DCF`、参数覆盖、敏感性分析实时计算

#### 选择依据

- 单标的技术指标实时计算已经被本地基准证明是低成本
- 估值历史如果财务本地化，计算本身也不重
- 真正昂贵的是全市场全历史的全量缓存存储，而不是单次计算

### Q4：DCF 模型是否同时支持默认参数与敏感性分析？

**结论**：是，两者都应该支持。

#### 建议实现

- 提供系统默认参数：
  - 无风险利率
  - ERP
  - 稳定增长率
  - 预测期长度
- 允许请求覆盖：
  - `growth_rate`
  - `discount_rate`
  - `terminal_growth`
  - `beta_window`
- 返回多场景敏感性矩阵：
  - 悲观 / 基准 / 乐观
  - 例如 `g × WACC` 二维网格

### Q5：独立部署还是集成部署？

**结论**：Phase 1 推荐“服务集成、存储隔离”。

#### 方案对比

| 方案 | 优点 | 缺点 |
|------|------|------|
| 集成部署（同仓库/同 API 服务） | 复用现有行情库、调度、监控、权限、备份；避免重复同步行情；读链路最短 | 模块边界需要更严格，避免研究逻辑污染现有行情逻辑 |
| 独立部署（独立 DB/独立 API） | 研究域边界清晰，后续横向扩展方便 | 需要重复同步基础行情和标的主数据，增加一致性与运维成本 |
| 折中方案（同服务 + 独立研究库） | 接口与运维体系统一，同时降低对现有 `quotes.db` 的冲击 | 数据访问层需要支持跨库读取或逻辑拼装 |

#### 推荐落地

- **Phase 1**
  - 同仓库
  - 同 FastAPI 服务
  - 同调度器
  - **研究域建议独立 `research.db`**
  - 研究域使用独立模块、独立表前缀、独立路由前缀
- **Phase 2/3**
  - 如果研究 API 流量或分析任务明显增长，再考虑拆成独立读服务

### Q6：没有 Tushare Pro，是否仍可做？

**结论**：可以做，但必须降低“数据源绝对稳定”的预期。

#### 工程要求

- 以 **多源主备 + AkShare/官方披露兜底** 为原则，但所有研究同步任务都要有：
  - 重试
  - 限流
  - 空值容忍
  - 同步状态记录
  - 后台补数能力
  - 原始快照审计
- 不能把外部免费源当作查询时的直接依赖

### 8.1 本轮新增确认点（对应 2026-04-17 补充要求）

#### 1. 关于“多数据源备份 + 稳定性优先”

**结论**：方向要调整成“稳定性优先”，而不是“非 AkShare 优先”。

- 对 `公司基本信息 / 公司概览 / 行业板块 / 财务摘要`
  - 存在相对稳定的免费主源时，可以优先采用 `BaoStock + pytdx`
  - `efinance / AkShare` 作为更丰富字段或更高可用性的增强链使用
- 对 `股东摘要 / 前十大股东 / 完整三大报表 / 严格申万行业 / 完整股权结构`
  - 不能假设免费直连链路就能稳定覆盖
  - 若缺乏可靠免费主源，应优先把 `AkShare/efinance + proxy_patch` 放到主链
  - 免费直连源继续作为补充 fallback，而不是反过来

也就是说：

- **稳定免费优先**：成立
- **稳定付费次之**：在缺少可靠免费主源时应前置
- **免费不稳定补充**：只作为 fallback，不应承担主链职责

#### 2. 关于 DCF / 技术指标等算法未来还要继续打磨

**结论**：必须在第一版就预留算法替换面，否则后面会进入“改一个公式牵一片接口”的状态。

Phase 1 就应落实：

- 计算引擎接口化，而不是把公式塞进 API handler
- 计算方法、参数、版本、缓存策略显式建模
- API 响应附带 `calc_method / calc_version / parameter_hash`
- 数据库存储允许同一标的、同一天存在不同算法版本的结果或缓存

如果不这样设计，后续你想调 `DCF` 参数口径、换 `Beta` 计算窗口、替换技术指标实现，都会变成高风险改动。

#### 3. 关于项目风格、一致性、参数化和模块复用

**结论**：这条应提升为硬性约束，而不是“开发建议”。

需要明确写入实施原则：

- 研究域沿用当前仓库的配置驱动风格
- 新增数据源必须走统一工厂与配置，而不是业务代码里手写 if/else
- 指标参数、估值假设、行业映射、缓存 TTL、任务开关全部配置化
- 不复制现有调度、日志、告警、路由框架
- 只在“抽象明显不匹配”时新增独立基类，且接口风格保持一致

#### 4. 关于尽量不影响现有生产环境

**结论**：推荐“同仓库同服务，分库或分文件隔离”的折中方案。

本版最终建议为：

- 继续复用当前代码仓、FastAPI、Scheduler、Telegram 运维与监控
- 研究接口仍集成在现有服务下，通过 `/api/v1/research/*` 暴露
- 研究表默认落到 `research.db`（或等价独立库文件）
- 现有 `quotes.db` 保持读多写少，不让研究域大批量回填直接冲击原生产库

上线策略：

- 先影子同步
- 再内部验证
- 最后灰度开放只读接口

#### 5. 关于重新梳理需求后的错误、遗漏与补充

本轮重新梳理后，以下几点需要作为正式补充写入需求：

- **财务摘要** 与 **完整财务报表** 必须分开定义
- **股东摘要/前十大股东** 与 **完整股权结构** 必须分开定义
- **行业字段** 与 **申万行业体系** 必须分开定义
- 研究域必须有：
  - `source_summary`
  - `data_as_of`
  - `calc_version`
  - `parameter_hash`
  - `ingestion_status`
- 需要新增：
  - 影子运行方案
  - 灰度发布方案
  - 回滚方案
  - 数据源失效时的降级方案
  - 指标口径版本管理方案

以上内容如果不补，后面在实施阶段很容易反复返工。

---

## 9. 推荐的数据模型

### 9.1 表设计原则

- 对查询高频字段，使用结构化列，而不是只存 JSON
- 对上游原始返回，保留 `raw_json` 或 `payload_hash`
- 对所有同步任务记录来源、时间、版本和状态

### 9.2 建议新增表

| 表名 | 作用 |
|------|------|
| `company_profiles` | 公司档案快照 |
| `industry_taxonomy` | 行业体系字典与版本表 |
| `industry_component_sets` | 申万叶子行业成分股集合缓存 |
| `industry_official_classifications` | 官方六位行业码原始层与映射审计 |
| `industry_official_code_mappings` | 官方六位行业码到标准 taxonomy 的缓存资产与诊断层 |
| `industry_memberships` | 行业分类与成分归属 |
| `shareholder_snapshots` | 股东摘要快照 |
| `financial_statements_raw` | 原始三大报表快照 |
| `financial_facts` | 规范化财务事实表 |
| `financial_indicator_snapshots` | 派生财务指标 |
| `analyst_forecasts` | 分析师覆盖与一致预期 |
| `research_reports` | 研报元数据 |
| `sentiment_events` | 资金流、龙虎榜、减持、质押等事件 |
| `valuation_history` | 日度估值历史 |
| `technical_indicator_latest` | 最新技术指标快照 |
| `ingestion_runs` | 同步任务运行记录 |
| `raw_payload_audit` | 原始 payload 审计（可选但强烈建议） |

### 9.2A 行业表的推荐结构（严格申万标准）

行业部分需要明确区分两层：

- **参考层**
  - 来自 `BaoStock / pytdx / efinance / AkShare` 的原始行业或板块字段
  - 这类字段可以保留在 `company_profiles.industry_raw / sector_raw` 或原始 payload 中
  - 若未完成到申万标准体系的映射，只能作为“公司基础信息参考”，不能直接进入严格行业库
- **标准层**
  - 系统内部维护的、可用于行业查询、同行筛选和相对估值的 **申万一/二/三级标准行业库**

建议如下：

#### 1. 行业体系字典表

- `industry_taxonomy`
- 关键字段：
  - `taxonomy_system`
    - 示例：`sw`
  - `taxonomy_version`
    - 示例：`sw_2021`
  - `industry_code`
  - `industry_name`
  - `industry_level`
    - `1 / 2 / 3`
  - `parent_industry_code`
  - `is_active`
  - `valid_from`
  - `valid_to`
  - `source`
  - `source_mode`

说明：

- 这张表维护的是 **申万标准行业字典本身**
- 必须支持版本化，不能假设行业体系永远不变
- 必须能表达一级、二级、三级之间的父子关系

#### 1A. 行业成分集合缓存表

- `industry_component_sets`
- 关键字段：
  - `taxonomy_system`
    - 示例：`sw`
  - `taxonomy_version`
    - 示例：`sw_2021`
  - `industry_code`
    - 申万三级行业代码
  - `component_count`
  - `symbols_json`
  - `source`
  - `source_mode`
  - `built_at`

说明：

- 这张表维护的是 **全部申万三级行业当前成分股集合**
- 它不是临时缓存，而是 strict Shenwan current membership 的本地主数据资产
- 正式主线应先全量刷新这张表，再由本地集合反向生成 `industry_memberships`
- 当行业成分变化导致股票归属变更时，下一轮全量刷新会直接触发对应股票的重映射

#### 2. 官方行业原始分类表

- `industry_official_classifications`
- 关键字段：
  - `instrument_id`
  - `taxonomy_system`
  - `taxonomy_version`
  - `official_industry_code`
  - `official_start_date`
  - `official_update_time`
  - `mapped_industry_code`
  - `mapped_industry_name`
  - `mapped_industry_level`
  - `mapping_status`
    - `mapped / unmapped`
  - `mapping_confidence`
    - `high / medium / unmapped`
  - `source`
  - `source_mode`
  - `classification_json`

说明：

- 这张表承载的是“官方六位码 -> 当前标准层节点”的 latest raw classification 结果
- 它不是对外主要读取表，但它必须存在，因为：
  - `raw_payload_audit` 更适合去重审计，不适合表达官方分类最新态
  - authoritative 主链需要长期保留官方码、生效日期、映射状态和置信度
  - 后续若要把 company / valuation / industry change audit 做细，还需要依赖这张表

#### 2A. 官方行业码映射缓存表

- `industry_official_code_mappings`
- 关键字段：
  - `taxonomy_system`
  - `taxonomy_version`
  - `official_industry_code`
  - `best_taxonomy_industry_code`
  - `mapped_industry_code`
  - `mapping_status`
    - `mapped / unmapped`
  - `mapping_confidence`
    - `high / medium / unmapped`
  - `overlap_count`
  - `official_symbol_count`
  - `taxonomy_symbol_count`
  - `precision`
  - `recall`
  - `source`
  - `source_mode`
  - `built_at`
  - `mapping_json`

说明：

- 这张表是 `official six-digit code -> strict Shenwan taxonomy node` 的物化审计缓存层
- 它与 `industry_official_classifications` 的区别是：
  - `industry_official_classifications` 面向“股票在某时点拿到了什么官方行业码”
  - `industry_official_code_mappings` 面向“某个官方行业码当前如何映射到本地标准 taxonomy”
- 即便最终未通过阈值，这张表也应保留最佳候选节点和重叠指标，便于人工校验和后续规则补全
- `2026-04-20` 起，mapping payload 还会保留 `candidate_rankings` top-N 候选，供后续 `manual_override` 回修时参考
- `industry_membership_sync` 不应再依赖这张表生成当前股票归属；当前归属应优先来自申万叶子行业成分股集合反向生成
- 这张表仍可用于历史分类审计、异常观察和后续研究，不应作为相对估值同行分组的前置 blocker
- 工程上保留独立任务 `industry_official_mapping_refresh`，但它只服务 official history 审计；current `industry_membership_sync` 不再以它为前置
- 对高价值但仍 `unmapped` 的 official code，应允许通过配置化 `manual_overrides` 做审计化回修，并在 mapping payload 中显式标记 `mapping_source = manual_override`
- 这张表还应通过标准 research 读接口暴露给内部审计与运维链路，例如：
  - `/api/v1/research/industry/official-mappings`
  - `/api/v1/research/industry/official-mappings/{official_industry_code}`
  - `/api/v1/research/industry/official-mapping-backlog`
  - `/api/v1/research/industry/official-mapping-override-candidates`
  - `/api/v1/research/industry/official-mapping-override-review`
  - 用于查看 `unmapped` backlog、核对 `manual_override` 命中与映射候选
- `2026-04-20` 起，backlog 行还会给出 `review_priority / override_candidate_ready / override_candidate_reason` 等派生信号，用于判断哪些 unmapped official code 已值得优先进入人工回修
- `2026-04-20` 起，backlog 还会直接返回 `manual_override_suggestion`，并支持 `override_candidate_ready_only=true` 过滤，这样高信号行可以直接转成 `manual_overrides` 配置草稿
- `2026-04-20` 起，还应提供独立的 `official-mapping-override-candidates` 导出视图，将 ready rows 聚合成 `manual_overrides` 配置片段，供人工核验后写回配置
- `2026-04-21` 起，还应提供 `official-mapping-override-review` 审阅视图，将配置中的 `manual_overrides`、ready candidate 导出结果、以及 mapping cache 中实际生效的 `manual_override` 合并展示，用于判断哪些条目已生效、哪些待配置、哪些需要排查
- `official-mapping-override-review` 应支持 `attention_only=true` 和 `review_status=...` 定向过滤，方便只查看 `configured_not_applied / applied_not_configured / configured_applied_mismatch / configured_ready_candidate_mismatch` 等需要排查的状态
- `2026-04-21` 起，`industry/standard-readiness` 也应直接内嵌 `override_review` 摘要；但在三级成分股 current membership 主线确立后，`manual_override` 仅影响 official history 审计视图，不再计入 current membership readiness blocker
- `2026-04-21` 起，应提供仓库级 strict Shenwan rollout validation runner，标准执行顺序为 `industry_standard_sync -> industry/standard-readiness`；`industry_official_mapping_refresh` 默认跳过，仅在需要审计缓存刷新时显式启用
- rollout validation runner 的 CLI 生命周期应负责关闭 `DataManager` 资源，避免 AkShare/YFinance 等数据源的 aiohttp session 在验证结束后泄露
- `2026-04-21` runner 小样本验证已确认：`akshare:proxy_patch` 可成功完成 refresh 和 `SSE/SZSE` 代表样本 sync；映射缓存为 `433` 行，其中 `304` mapped、`129` unmapped；但 readiness 仍按全市场 `5191` 个目标标的计算，因此在 `--limit-per-exchange 2` 样本运行下返回 `not_ready` 是预期结果
- 同次验证发现 `480101` 是 official history 审计层的 high-impact unmapped code，样本标的为 `600000.SH`；由于候选 `857821.SI / 857831.SI` overlap 均为 `5`，该 code 不应按 best candidate 自动写入 `manual_overrides`
- `2026-04-24` 修正后续方向：`stock_industry_clf_hist_sw()` 的六位行业码不能稳定映射到旧 `sw_index_third_info` 指数代码体系，问题根源是“官方分类代码”和“申万行业指数代码”混用；新方案不再要求六位分类码映射到 `801xxx.SI / 85xxxx.SI` 才能判定归属，而是以官方六位分类码作为 strict Shenwan taxonomy 主键，指数代码仅作为 alias

#### 3. 行业成分归属表

- `industry_memberships`
- 关键字段：
  - `instrument_id`
  - `taxonomy_system`
  - `taxonomy_version`
  - `sw_l1_code`
  - `sw_l1_name`
  - `sw_l2_code`
  - `sw_l2_name`
  - `sw_l3_code`
  - `sw_l3_name`
  - `effective_date`
  - `expire_date`
  - `is_current`
  - `mapping_status`
    - `authoritative / mapped / reference_only`
  - `source`
  - `source_mode`
  - `data_as_of`

说明：

- 对股票来说，真正用于研究和估值的是这张“股票 -> 申万行业”的关系表
- 新方案中它应由申万官方股票分类历史的 latest row 生成当前归属，再通过官方分类代码表派生申万二级和一级
- 全量同步时，应按当前最新官方分类文件对每只股票做 current membership 重建；若某股票行业归属变化，当前行应被直接改写为新的 `sw_l1/sw_l2/sw_l3` 官方分类代码和名称
- `industry_official_classifications` 可迁移为 full classification history 或归档为旧审计表；不再需要“六位码 -> 8xx 指数 taxonomy”的映射层来生成 current membership
- 当官方分类文件验证后仍缺失个别股票时，先暴露 coverage gap，再允许配置化人工补源、结构化新浪补源和东财行业名补源作为异常诊断；这些补源不应覆盖已通过官方分类主源确认的归属
- 定向补缺任务可复用本地官方 `industry_taxonomy` 与 source-file manifest 缓存，且不要求刷新旧 `industry_component_sets`；index component set 只保留为指数/诊断资产
- 如果某股票当前拿到的只是免费源粗分类，而没有可靠映射到申万一/二/三级，则：
  - `mapping_status` 应为 `reference_only`
  - 该记录不应被同行筛选和相对估值直接使用

#### 3. 当前工程含义

当前已经落地的 industry 代码链路，**只能视为行业参考层 / 线索层**：

- 它已经具备：
  - 行业字段抓取
  - 行业 membership 存储
  - company overview 整合
- 但它 **还不等于严格申万标准行业库**

要满足最终需求，还需要在后续阶段补齐：

- 申万一/二/三级完整 taxonomy 版本维护
- 股票到申万标准层级的可靠映射
- 映射状态与变更审计
- 行业变动的定时更新与历史可追溯

### 9.3 财务表的推荐结构

不建议只做一个 `financial_statements` JSON 表后直接给 API 用。财务域应按“源文件 manifest -> 原始报表/原始 payload -> 全数值事实 -> 核心事实 -> 派生指标/估值”的层次设计。当前仓库已经在不破坏现有 API 的前提下补齐 `financial_source_files`、`financial_numeric_facts`、`financial_core_facts_hot/history`、`financial_numeric_facts_hot/history` 和 `financial_indicator_snapshots_hot/history`，并保留 `financial_statements_raw`、`financial_facts`、`financial_indicator_snapshots` 兼容表。

#### 1. 源文件 manifest / filing registry

当前表为 `financial_source_files`，后续也可在拆库后放入 `data/financials.db`：

- `id`
- `instrument_id`
- `symbol`
- `exchange`
- `report_period`
- `report_type`
- `filing_id`
- `source`
- `source_mode`
- `source_url`
- `archive_path`
- `content_hash`
- `content_type`
- `file_size`
- `disclosed_at`
- `downloaded_at`
- `parser_version`
- `parse_status`
- `diagnostics_json`
- `ingestion_run_id`

要求：

- 上游 URL、endpoint、timeout、retry、request interval、并发数、parser version 不得写死在 provider 内，必须来自配置或 provider 初始化参数
- source file hash 是判断修订报表、重复下载、增量 catch-up 的主依据之一
- 官方源不可用时，manifest 仍应记录缺失原因，避免 fallback 数据看起来像主源数据
- 当前 `FinancialStatementsShadowSyncService` 已在 backfill/catchup 中写入 manifest，并把本次 `report_periods`、`sync_mode`、checkpoint、tier maintenance 和 coverage gaps 写入 `ingestion_runs.metadata_json`

#### 2. 原始报表快照

当前基线表为 `financial_statements_raw`：

- `instrument_id`
- `statement_type`
- `report_period`
- `publish_date`
- `source`
- `source_mode`
- `statement_json`
- `ingestion_run_id`

后续增强：

- 增加或关联 `source_file_id`
- 增加 `report_type`、`data_available_date`、`parser_version`
- 对官方结构化文件保留原始归档路径，JSON 只作为解析后的快照视图

#### 3. 全数值事实长表

当前表为 `financial_numeric_facts`，并同步维护 `financial_numeric_facts_hot/history`。它用于保存官方 XBRL 或等价结构化文件中的所有数值事实，不只保存估值常用字段：

- `source_file_id`
- `instrument_id`
- `report_period`
- `fact_name`
- `namespace`
- `statement_family`
- `context_id`
- `unit`
- `decimals`
- `precision`
- `period_start`
- `period_end`
- `instant_date`
- `dimensions_json`
- `value`
- `raw_value`
- `parser_version`

要求：

- 不因当前 API 用不到而丢弃数值事实
- 不用表结构硬编码某一个 XBRL taxonomy 的字段全集
- 通过 `(instrument_id, report_period, fact_name, context_id, source_file_id)` 等组合唯一约束保证可重复写入

#### 4. 核心规范化事实表

当前基线表为 `financial_facts`，用于估值、筛选和公司概览的稳定字段：

- `instrument_id`
- `report_period`
- `report_type`
- `publish_date`
- `data_available_date`
- `revenue`
- `gross_profit`
- `net_income`
- `operating_cf`
- `total_assets`
- `total_liabilities`
- `equity`
- `inventory`
- `receivables`
- `fixed_assets`
- `shares_outstanding`
- `source`
- `source_mode`
- `source_file_id`
- `facts_json`

要求：

- 字段 alias 映射、单位换算、币种、报告口径必须版本化
- 估值只能使用 `data_available_date <= trade_date` 的事实，缺失可得日时必须返回 unavailable 或使用配置化保守规则
- fallback 数据只补缺，不覆盖高优先级事实

#### 5. 派生指标与估值指标

当前基线表为 `financial_indicator_snapshots`，后续继续承载：

- `gross_margin`
- `net_margin`
- `roe`
- `roa`
- `asset_liability_ratio`
- `revenue_yoy`
- `net_income_yoy`
- `fcf`
- `book_value_per_share`
- `operating_cf_to_net_income`

估值域不应继续只暴露模糊的 `pe_ratio / pb_ratio / ps_ratio` 口径。当前 `valuation_history` 已在保留兼容字段的同时新增以下明确口径：

- `pe_static`
- `pe_ttm`
- `pe_forward`
- `pb_mrq`
- `ps_static`
- `ps_ttm`
- `ps_forward`

每个估值指标都要记录：

- `numerator`
- `denominator`
- `denominator_fact_names`
- `source_report_periods`
- `data_available_dates`
- `calc_method`
- `calc_version`
- `parameter_hash`

#### 6. 财务事实热/冷分层

你的判断是合理的：大多数公司研究、估值、筛选和 AI 概览只需要最近两年加当年已披露报告期，通常不超过 `12` 个季度。财务域可以采用 **hot/cold tier** 设计，降低常用查询表体量，同时保留长历史研究能力。

建议语义：

- hot tier：最近 `N` 个报告期，默认 `N = 12` 个季度，并可额外保留 TTM、YoY、年报锚点所需的配置化 anchor period
- cold tier：超过 hot window 的历史报告期和全数值事实
- source manifest / filing registry：不按 hot/cold 分裂，保持全量、唯一、可审计
- API / service：默认查 hot tier；当请求显式指定更长 `start_period`、`lookback_quarters` 或 `include_history=true` 时，再 union cold tier

推荐物理形态：

```text
financial_core_facts_hot
financial_core_facts_history
financial_numeric_facts_hot
financial_numeric_facts_history
financial_indicator_snapshots_hot
financial_indicator_snapshots_history
```

也可以在未来数据库中映射为：

- PostgreSQL：按 `report_period` 或 `data_available_date` 分区，并提供 `financial_core_facts_hot` view
- DuckDB：hot 数据保留在常用 OLTP/SQLite 层，cold 数据放列式历史库
- SQLite：先用两张物理表 + repository union 查询，避免大表持续膨胀

关键规则：

- `N=12` 只是默认值，必须配置化，例如 `financial_statements.storage.hot_quarter_window`
- hot/cold 归档只改变物理位置，不改变逻辑主键、source lineage、API 语义和 readiness 口径
- 每次 daily catch-up 或 backfill 成功后，运行 tier maintenance：把超出 hot window 的行移动到 history，并保证 hot 表至少覆盖配置窗口
- 同一 `(instrument_id, report_period, report_type, source priority)` 不允许在 hot/history 中重复成为 active row；迁移过程应事务化或幂等
- TTM、YoY、环比、静态 PE 和 PB MRQ 需要的额外报告期应通过配置保留在 hot tier，不能为了严格 12 季度而破坏计算
- 长历史查询必须通过 repository 层合并 hot/history，业务代码不能直接拼两张表

这个方案的收益：

- 常用公司概览、最新财务、估值和筛选查询保持小表扫描或窄索引命中
- 历史财务研究、长周期因子、财务质量回测仍可查全量
- 为未来从 SQLite 迁移到 PostgreSQL/DuckDB 的冷热分区或列式历史仓预留一致语义

风险与约束：

- 需要维护 tier migration，不应在每次查询时动态搬迁
- 如果索引设计正确，单纯拆表未必比单表快很多；真正收益来自控制 hot 表大小、简化常用索引和减少缓存污染
- source manifest、raw archive 和 ingestion audit 不应被热/冷拆分打散，否则修订报表和追溯会变复杂

#### 7. 数据库迁移兼容性

财务域后续数据量和查询复杂度会高于普通快照表，因此要为 SQLite 之外的后端留迁移空间：

- 业务代码只依赖 storage/repository 方法，不直接拼 SQLite 方言 SQL
- upsert、分页、JSON 字段、时间字段和批量写入要集中在存储适配层处理
- 表设计优先使用可迁移类型：`TEXT / INTEGER / REAL / NUMERIC / TIMESTAMP / JSON text`
- 避免把 SQLite `rowid`、`PRAGMA`、`ATTACH` 语义作为业务逻辑前提
- 若拆分到 `data/financials.db` 或迁移到 PostgreSQL/DuckDB，API 响应模型、source lineage 和 readiness 语义保持不变
- hot/cold tier 在不同后端可以是两张表、分区表、物化视图或列式历史仓；业务层只感知 repository 的 `recent` / `history` 查询语义

### 9.4 技术指标表的推荐结构

Phase 1 只建议保留：

- `technical_indicator_latest`
  - `instrument_id`
  - `period`
  - `as_of_date`
  - `adjustment`
  - `applied_adjustment`
  - `calc_method`
  - `calc_version`
  - `parameter_hash`
  - `status`
  - `missing_reason`
  - `macd`
  - `macd_signal`
  - `rsi14`
  - `adx`
  - `plus_di`
  - `minus_di`
  - `stoch_k`
  - `stoch_d`
  - `cci`
  - `williams_r`
  - `boll_upper`
  - `boll_middle`
  - `boll_lower`
  - `atr14`
  - `volume_ratio`
  - `trend_score`
  - `signal`
  - `source`
  - `source_mode`
  - `data_as_of`
  - `summary_json`

不建议在 Phase 1 创建“全历史全指标大宽表”。

字段口径建议：

- Phase 1 统一采用 `stoch_k / stoch_d` 作为随机指标标准命名，不直接落地 `kdj_j`
- 若后续确实需要 `KDJ`，建议在算法层由 `stoch_k / stoch_d` 派生 `j = 3k - 2d`，避免底层表结构反复调整
- `technical_indicator_latest` 只承载“最新快照/缓存”职责，时间序列继续优先通过本地行情实时计算得到
- 技术快照必须保留 `calc_method / calc_version / parameter_hash`，以便未来算法调整后区分旧快照与新快照
- `/research/technical/readiness` 用于检查最新快照覆盖率、计算状态分布和 rollout blockers

### 9.5 外部技术分析方法论的吸收边界

在评估 `openclaw-tradingview-quant` 之后，结论如下：

- 它适合作为 **技术分析方法论与工作流参考仓库**
- 它 **不适合作为本项目的运行时依赖或核心计算引擎**

原因：

- 该仓库主要提供 `references/` 与 `workflows/` 级别的方法说明、模式库和 TradingView 数据结构解释
- 它没有提供可直接嵌入本项目的、稳定的本地技术指标计算运行时
- 其推荐的数据获取路径偏向 TradingView / RapidAPI 外部接口，不符合本项目“读链路不依赖外网、本地优先”的原则

因此建议采用“吸收能力，不引入依赖”的策略：

- **Phase 1 已吸收的方向**
  - 扩展本地技术指标覆盖面：
    - `MACD`
    - `RSI`
    - `Bollinger Bands`
    - `ATR`
    - `ADX / +DI / -DI`
    - `Stochastic K/D`
    - `CCI`
    - `Williams %R`
- **Phase 2 建议继续吸收的方向**
  - `pattern library` 中的经典形态识别规则，沉淀为本地 `technical/patterns` 规则引擎
  - `multi-timeframe analysis` 工作流，沉淀为 AI/研究员解释模板
  - `risk management` 工作流，沉淀为后续风险输出与仓位建议模块

明确边界：

- 不直接把 `openclaw-tradingview-quant` 接入生产运行时
- 不让 TradingView / RapidAPI 成为核心 research 读取链路的在线依赖
- 若未来引入外部技术分析数据，也必须作为 **后台同步源** 而非 **在线查询源**

---

## 10. API 设计（优化版）

### 10.1 核心读取接口

#### 公司与财务

```text
GET /api/v1/research/company/{instrument_id}/overview
GET /api/v1/research/company/{instrument_id}/profile
GET /api/v1/research/company/{instrument_id}/financial-statements
GET /api/v1/research/company/{instrument_id}/financial-indicators
GET /api/v1/research/company/{instrument_id}/dividends
```

#### 行业与同行

```text
GET /api/v1/research/company/{instrument_id}/industry
GET /api/v1/research/company/{instrument_id}/peers
GET /api/v1/research/industry/{industry_code}/overview
```

#### 估值

```text
GET /api/v1/research/company/{instrument_id}/valuation/relative
GET /api/v1/research/company/{instrument_id}/valuation/history
GET /api/v1/research/company/{instrument_id}/valuation/dcf
GET /api/v1/research/valuation/readiness
```

#### 技术分析

```text
GET /api/v1/research/company/{instrument_id}/technical/summary
GET /api/v1/research/company/{instrument_id}/technical/indicators
GET /api/v1/research/company/{instrument_id}/technical/patterns
GET /api/v1/research/technical/readiness
```

#### 研究元数据与风险

```text
GET /api/v1/research/company/{instrument_id}/analyst-coverage
GET /api/v1/research/company/{instrument_id}/research-reports
GET /api/v1/research/metadata/readiness
GET /api/v1/research/company/{instrument_id}/risk
GET /api/v1/research/company/{instrument_id}/events
```

### 10.2 Phase 1 必须满足的接口要求

- 读取路径不访问外部源
- 返回统一 `instrument_id`
- 返回 `data_as_of`
- 返回 `source_summary`
- 对计算型接口返回 `calc_method`、`calc_version`、`parameter_hash`
- 对缺失字段允许 `null`，但必须明确说明缺失原因

---

## 11. 调度任务设计（基于现有 APScheduler 扩展）

### 11.1 建议新增任务

| 任务 | 频率 | 作用 |
|------|------|------|
| `company_profile_sync` | 每日/每周 | 同步公司档案、行业映射、股本信息 |
| `industry_standard_sync` | 每日 | 通过 `swsresearch_classification_direct` 刷新申万官方分类文件；若文件未变化则短路成功，若变化则重建官方 taxonomy、分类历史和 latest membership |
| `industry_standard_gap_fill` | 每日/每周 | 官方主源落地后降级为异常诊断/补救任务；仅处理官方源验证后的残余缺口 |
| `industry_official_mapping_refresh` | 手动/归档 | 旧 official code -> index taxonomy 映射审计缓存；官方分类代码成为主键后不再作为 membership 同步前置 |
| `industry_index_analysis_sync` | 每日收盘后 | 同步申万行业指数最新日频估值、换手、涨跌幅、市值和股息率等指标，只写 `industry_index_analysis_daily` |
| `industry_index_analysis_backfill` | 手动/禁用定时 | 按日期区间回补申万行业指数分析历史数据；默认按“月份 + index_type”分块，必要时可用按日分块补缺 |
| `financial_statement_official_probe` | 手动/开发验证 | 探测 SSE/SZSE/CNInfo/BSE 官方结构化财报源，记录覆盖率、延迟、下载证据和字段稳定性，不写生产表或仅写临时验证库 |
| `financial_statement_backfill` | 手动/灰度批处理 | 按配置化 baseline、rolling quarters 和目标市场做多期财报回填，写 source manifest、全数值事实和核心事实，并按 hot/cold tier 维护最近报告期与历史报告期 |
| `financial_statement_catchup` | 每日晚间/季报季加密 | 根据 disclosure checkpoint 发现新增或修订披露，只处理变化的标的和报告期 |
| `financial_statement_reconciliation` | 每周，避开股东周更窗口 | 校验覆盖率、source hash、缺失报告期、缺失核心事实、解析失败项和 hot/cold tier consistency，执行有界补缺 |
| `financial_indicator_rebuild` | 财报同步后 | 基于核心事实重建规范化指标，记录 `calc_method / calc_version / parameter_hash` |
| `valuation_history_rebuild` | 每日收盘后 | 基于已可得财务事实重算核心估值历史；静态、TTM、forward 指标必须分口径输出 |
| `analyst_forecast_sync` | 每日/每周 | 更新一致预期 |
| `research_report_sync` | 每日 | 更新研报元数据 |
| `sentiment_event_sync` | 每日 | 更新资金流、龙虎榜、减持等事件 |
| `technical_snapshot_refresh` | 每日收盘后 | 刷新最新技术快照 |

官方申万分类主源的仓库级执行命令：

- source/schema/coverage 现场验证：`python scripts/validate_swsresearch_shenwan_classification_live.py --exchanges SSE,SZSE,BSE --use-db-manifest`
- 受控重建：`python scripts/research_industry_standard_rebuild_official.py --exchanges SSE,SZSE,BSE --drop-existing --force-refresh`
- 重建脚本会输出行业标准层相关表的 pre/post row counts、清理计数、sync 结果和 readiness；`--drop-source-files` 会清空 manifest 并强制非条件下载
- 申万行业指数分析已拆为最新日更和历史回补两条入口：`/industry_index_analysis_sync` 负责最新横截面，`scripts/research_ops/industry_index_analysis_backfill.py` 与 Telegram `/industry_index_analysis_backfill` 负责历史回补；两者只按指数代码写入 `industry_index_analysis_daily`，不参与股票行业分类判定
- 历史回补脚本支持 `--chunk-frequency day|month|quarter|year|none`。全量下载默认用 `month`；遇到单月分页 404、上游限流或局部缺口时，用 `day` 精确补缺。

### 11.2 调度实现原则

- 所有研究任务都应记录 `ingestion_runs`
- 失败不能影响现有行情任务
- 支持按市场、按模块手动补数
- 研究域任务与行情任务分开配置，避免耦合
- 研究域任务默认支持 `shadow_mode`
- 财务任务必须支持 `baseline_report_period`、`rolling_min_quarters`、`hot_quarter_window`、`hot_anchor_policy`、`max_concurrency`、`request_timeout_seconds`、`retry_attempts`、`request_interval_seconds`、`parser_version`、`fallback_policy` 等配置化参数
- 财务 catch-up 和 reconciliation 必须有 checkpoint，不能靠全市场全量重跑作为唯一维护方式
- 同步状态应区分：
  - `success`
  - `partial_success`
  - `degraded`
  - `failed`
- 对行业任务还应额外区分：
  - 是否仅拿到参考层字段
  - 是否已完成到申万一/二/三级标准层的 authoritative 映射

---

## 12. 分阶段实施计划（修正版）

### Phase 0：数据源接入验证与影子库准备

**目标**：在不影响现有生产环境的前提下，完成研究域最关键的前置验证。

**范围**：

- 引入 `research` 域配置
- 建立 `research.db`（或等价独立库文件）
- 接入 `efinance` 并完成最小能力验证
- 为公司概览、股东摘要、行业映射、财务摘要建立影子同步任务
- 建立研究域 `ingestion_runs`、`raw_payload_audit`
- 完成 100~300 只 A 股样本的字段完整性与稳定性评估

**交付结果**：

- 免费源能力矩阵的实测版
- 研究域影子库
- 源优先级与降级配置初稿

**预估工期**：`1 ~ 2 周`

### Phase 1：研究底座 + A 股财务核心 + 技术引擎

**目标**：尽快交付“可被 AI 和研究员真实消费”的第一版。

**范围**：

- `company_profiles`
- `industry_taxonomy` / `industry_memberships`
- `financial_statements_raw`
- `financial_facts`
- `financial_indicator_snapshots`
- `financial_source_files / financial_numeric_facts` 或等价官方结构化财报 lineage 层
- `technical_indicator_latest`
- 技术指标实时计算 API
- 基础研究路由 `/api/v1/research/company/*`

**交付结果**：

- 公司概览
- 行业参考层字段可读
- 近 3~10 年财务数据的目标不再靠最新期快照扩展，而是通过官方结构化披露优先的多期财务仓实现；第一批可从 `2024Q1` 起并满足至少 8 个季度 rolling floor
- 财务指标
- 技术指标摘要与时间序列
- 研究域只读接口灰度可用

**预估工期**：`3 ~ 5 周`

### Phase 2：行业、同行与估值

**范围**：

- 将 Phase 1 的行业参考层升级为 **严格申万一/二/三级标准层**
- `industry_taxonomy` 以申万官方六位分类代码为主键进行版本化全量维护
- `industry_classification_history` 保存官方股票分类历史全量行，并派生 latest membership
- `industry_memberships` authoritative 映射、行业变更后的重映射与缺口补齐
- `industry_index_analysis` 后续按申万行业指数代码保存官方指数分析指标，与股票分类同步解耦
- `valuation_history`
- 同行业对标
- 相对估值
- DCF + 参数覆盖 + 敏感性分析
- PE/PB/PS 静态、TTM、forward 指标口径拆分；forward 指标在 forecast 输入未启用或未覆盖时必须显式 unavailable

**交付结果**：

- 申万一/二/三级行业定位
- 仓库级标准执行顺序：`industry_standard_sync(官方分类文件刷新/重建) -> industry_standard_gap_fill(异常补缺) -> industry/standard-readiness`
- 估值时间轴
- 支持以 **申万一级 / 二级 / 三级行业** 作为同行比较中枢的矩阵；未显式指定时默认使用 **申万二级行业**，工程实现通过 `valuation.relative.benchmark_field` 配置解析，默认 `sw_l2_code`
- 当 `valuation.relative.require_authoritative=true` 时，相对估值只能使用 current authoritative 行业归属，不允许降级使用 reference-only 行业字段
- 相对估值按 `valuation.relative.metric_variants` 显式选择指标口径，默认 `pe_ttm / pb_mrq / ps_ttm`，并返回 valid peer count、mean、median、p25、p75、percentile rank、相对中位数溢价/折价和逐指标排除诊断
- valuation rollout readiness 应同时检查 `valuation_history` 覆盖、模块 gate 与 strict Shenwan current authoritative membership 前置条件
- financial readiness 应检查目标报告期覆盖、核心事实覆盖、source/parser 分布、fallback 占比、缺失可得日和解析失败项；valuation readiness 必须依赖 financial readiness，而不是只看 `valuation_history` 是否有行
- DCF 估值接口

**预估工期**：`2 ~ 4 周`

### Phase 3：研究元数据与风险

**范围**：

- `analyst_forecasts`
- `research_reports`
- `sentiment_events`
- 风险指标与事件风险输出

**交付结果**：

- 已完成 Phase 1 baseline：
  - 一致预期快照
  - 研报元数据
  - `notice / executive_share_change / pledge_ratio` 事件流
  - 本地派生 `risk_snapshots`
- 后续增强方向：
  - 数据源稳定性验证
  - 龙虎榜 / 资金流 / 更多事件类型
  - 风险评分参数与算法调优

**预估工期**：`3 ~ 5 周`

### Phase 4：深度业务画像、模式识别与港股研究扩展

**范围**：

- M02 / M06 / M08 / M14
- 年报解析接入（如需要）
- 港股基本面与研究域扩展

**预估工期**：`4 ~ 6 周`

---

## 13. 面向量化产品的扩展说明

你的原始诉求里提到“不只是研究输出，也包括量化产品输出系统”。
基于当前项目现状，推荐的做法是：

- Phase 1/2 先把研究域中的可复用特征沉淀出来
- 不急着做单独的量化平台

可直接复用为量化特征的内容包括：

- 财务因子
  - ROE、ROA、毛利率、净利率、营收增速、现金流质量
- 估值因子
  - PE/PB/PS 分位、EV/EBITDA、PEG
- 风险因子
  - 波动率、最大回撤、Beta、Altman Z-Score
- 技术因子
  - MACD、RSI、ATR、动量、均线偏离

后续若量化需求明确，可新增：

- `factor_snapshot_daily`
- `factor_exposure_history`
- `screening_rules`

但这不应阻塞当前研究数据引擎的建设。

---

## 14. 风险与前置条件

### 14.1 主要风险

| 风险 | 说明 | 应对 |
|------|------|------|
| 免费源波动 | AkShare 与网页源存在结构变更和反爬风险 | 本地持久化 + 审计 + 补数 |
| 多源口径不一致 | 同一字段在 BaoStock / efinance / pytdx 中定义可能不同 | 建立字段字典、口径版本与 source priority |
| 港股覆盖不足 | 港股研究数据源不稳定 | 分阶段交付，不承诺同深度 |
| 文档/测试陈旧 | 现有部分测试与代码脱节 | 后续同步修正文档和测试 |
| 单库继续膨胀 | 研究表加入后库体积继续增大 | 保持宽表克制，监控体积与查询延迟 |
| 业务定义不统一 | 同一指标不同口径 | 在指标表中记录口径与版本 |
| 新依赖未验证 | 当前运行环境未安装 `efinance` | 在 Phase 0 完成安装、样本验证与限流测试 |

### 14.2 前置条件

- 确认 A 股作为 Phase 1 主目标
- 接受港股在研究域上的延期交付
- 接受财务数据本地化
- 接受研究 API 在 P0/P1 以“稳定可用”优先，而不是一次性覆盖全部字段

---

## 15. Phase 1 验收标准

以下标准用于判断第一阶段是否真正可用：

### 15.1 数据层

- A 股公司档案完成本地化
- A 股财务三大报表完成本地化
- 关键财务指标可本地查询
- 所有 P0 接口读路径不依赖外网
- 研究域主备源可通过配置切换

### 15.2 API 层

- 单公司概览接口可返回完整基础信息
- 财务接口支持年度/季度查询
- 技术指标接口支持 `daily` 周期
- 所有接口返回 `data_as_of`
- 计算型接口返回 `calc_version`
- 数据型接口返回 `source_summary`
- 行业接口若未完成申万一/二/三级 authoritative 映射，必须显式标明该结果仅为参考层信息

### 15.3 性能层

- 单公司技术指标摘要 `p95 < 500ms`
- 单公司财务概览 `p95 < 1s`
- 单公司估值历史 `p95 < 1s`（Phase 2）

### 15.4 运维层

- 研究同步任务可被调度
- 失败任务可被追踪
- 任务结果可通过 Telegram 或日志确认
- 研究域异常不影响现有行情任务
- strict Shenwan 与相对估值 rollout readiness 可通过统一接口读取，而不是依赖手工查表
- valuation rollout readiness 可通过统一接口读取，并显式返回估值历史覆盖缺口、行业前置 blocker 与模块 gate 状态
- research metadata rollout readiness 可通过统一接口读取，并逐域返回 `analyst_forecasts / research_reports / sentiment_events` 的覆盖缺口、source/source_mode 分布和模块 gate 状态
- strict Shenwan readiness 以 current authoritative membership 覆盖为核心；official-code backlog 只作为历史分类审计摘要，不作为 rollout blocker
- shareholders rollout readiness 也应可通过统一接口读取，而不是依赖手工查表或单次样本同步

---

## 16. 最终建议

### 16.1 你当前最应该做的事

- 不要把这个项目理解成“多个 API 的套壳”
- 要把它理解成：
  - **已有行情数据底座**
  - **新增研究数据持久化层**
  - **新增确定性计算层**
  - **新增面向 AI / 研究员 / 量化的读取层**

### 16.2 最重要的工程决策

- **财务数据本地化**
- **研究域采用“稳定免费 -> 稳定付费 -> 免费不稳定补充”的多源主备策略**
- **技术指标按需计算**
- **估值采用混合模式**
- **A 股先行，港股二期**
- **Phase 1 同服务集成、同接口体系，但研究库独立**

### 16.3 本文档对应的最终实施口径

本系统不是报告生成系统，而是：

> 一个建立在 Quote System 之上的、面向 AI 与研究员的、本地优先、可追溯、可调度、可扩展的研究数据与研究计算引擎。
