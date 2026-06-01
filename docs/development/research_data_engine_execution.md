# 投研数据引擎实施主线与工程化推进文档

> 更新日期：2026-05-28
> 配套需求文档：[implementation_plan.md](/home/python/Quote/implementation_plan.md)
> 配套配置：[config/10_research.json](/home/python/Quote/config/10_research.json)
> 财务数据专项文档：[financial_data_system.md](/home/python/Quote/docs/financial_data_system.md)
> 当前推进焦点：在已完成财务获取、存储和公告驱动维护主线后，进入估值生产 rollout 准备：先通过 CNInfo/AkShare 同步 `shares_outstanding / float_shares` 到 `data/valuation.db.valuation_inputs`，再日频生成 `PE/PB/PS/market_cap/float_market_cap` 等核心估值序列；相对估值从 `valuation.db.valuation_history + research.db.industry_memberships` 动态计算，不持久化全量同行矩阵
> 当前估值生产 rollout 准备变更包：`openspec/changes/prepare-valuation-production-rollout/`
> 当前财务 source profile 变更包：`openspec/changes/promote-cninfo-data20-financial-source/`
> 当前财务 coverage gap 分类变更包：`openspec/changes/classify-financial-coverage-gaps/`
> 当前 source policy 变更包：`openspec/changes/harden-research-source-priority-policy/`
> 当前行业 rollout readiness 变更包：`openspec/changes/add-industry-standard-readiness-api/`
> 当前股东 rollout readiness 变更包：`openspec/changes/add-shareholder-readiness-api/`
> 当前 BSE 可空策略变更包：`openspec/changes/add-bse-empty-exchange-policy/`
> 当前申万组件集缓存变更包：`openspec/changes/cache-shenwan-component-sets/`
> 当前申万官方分类主源变更包：`openspec/changes/promote-swsresearch-shenwan-classification-primary/`
> 当前申万指数分析历史回补变更包：`openspec/changes/add-akshare-sws-index-analysis-history/`
> 已归档财务/XBRL 与相对估值加固变更包：`openspec/changes/archive/2026-05-06-harden-financial-xbrl-and-relative-valuation/`
> 已归档官方财务 endpoint/parser 变更包：`openspec/changes/archive/2026-05-06-discover-official-financial-xbrl-endpoints/`
> 已归档官方财务 SSE structured JSON 实现变更包：`openspec/changes/archive/2026-05-08-implement-sse-official-financial-json-source/`
> 已归档 SSE 财务批量回填准备变更包：`openspec/changes/archive/2026-05-08-prepare-sse-financial-batch-backfill-rollout/`
> 已归档 SSE 财务多报告期回填准备变更包：`openspec/changes/archive/2026-05-08-prepare-sse-financial-multiperiod-backfill/`
> 已归档 SSE 财务生产 backfill gate 变更包：`openspec/changes/archive/2026-05-08-prepare-sse-financial-production-backfill/`
> official-code backlog 相关变更包：`openspec/changes/assetize-unmapped-official-code-backlog/`（仅作为历史分类审计链路）
> 当前 readiness backlog 可视化变更包：`openspec/changes/surface-unmapped-backlog-in-industry-readiness/`
> 当前 backlog override-review 信号变更包：`openspec/changes/add-override-readiness-signals-for-official-backlog/`
> 当前 backlog manual-override 建议变更包：`openspec/changes/add-manual-override-suggestions-for-official-backlog/`
> 当前 override candidate 导出变更包：`openspec/changes/add-official-manual-override-export-api/`
> 当前 override review 变更包：`openspec/changes/add-official-override-review-api/`
> 当前 readiness override-review 摘要变更包：`openspec/changes/surface-official-override-review-in-industry-readiness/`
> 当前 override review filter 变更包：`openspec/changes/add-official-override-review-filters/`
> strict Shenwan current membership 变更包：`openspec/changes/use-shenwan-third-components-as-current-membership-source/`
> strict Shenwan rollout validation runner 变更包：`openspec/changes/add-industry-standard-rollout-validation-runner/`
> strict Shenwan gap detect + targeted repair 变更包：`openspec/changes/add-gap-detect-repair-task/`
> 已归档官方申万主源变更包：`openspec/changes/archive/2026-04-19-add-official-shenwan-membership-source/`
> 已归档行业加固变更包：`openspec/changes/archive/2026-04-18-harden-research-industry-standard-rollout/`
> 已归档基线变更包：`openspec/changes/archive/2026-04-18-implement-research-data-engine-phase1/`
> 配套验证记录：[research_shadow_sync_validation.md](/home/python/Quote/docs/development/research_shadow_sync_validation.md)
> 代理补丁运维说明：[proxy_patch_runtime.md](/home/python/Quote/docs/development/proxy_patch_runtime.md)

---

## 1. 文档定位

这份文档不替代 `implementation_plan.md`。

两者分工如下：

- `implementation_plan.md`
  - 负责回答“为什么做、做什么、边界是什么、总体架构如何设计”
- 本文档
  - 负责回答“先做什么、后做什么、当前做到哪一步、下一步按什么主线继续推进”
- `config/10_research.json`
  - 负责把当前阶段的研究域模块开关、源优先级、预算策略、技术分析参数固化为可执行默认值

本文件应作为后续推进投研数据引擎时的工程主线文档。任何新增开发，原则上都要先能映射到这里的主线任务，再进入编码。

---

## 2. 当前工程目标

当前主目标不是一次性做完完整的 Research Data Engine，而是先把 Phase 0 / Phase 1 主线交付稳定：

- 在不影响现有生产行情链路的前提下，建立独立的 research 域存储和同步机制
- 先交付可被 AI 和研究员直接消费的 A 股基础研究能力
- 将研究域默认 source policy 固化为“稳定免费 -> 稳定付费 -> 免费不稳定补充”
- 对所有研究域，统一遵循“1. 稳定+免费，2. 稳定+收费，3. 不稳定+免费补充”的 source priority
- 保持研究域读取链路本地优先，不在请求时依赖外部源
- 当前研究、财务和最新快照类写入任务在解析 `instruments.is_active` 股票池前，统一经过 `DataManager.ensure_instrument_master_fresh()` 主数据治理入口；该入口复用行情系统已有 `sync_instrument_master()`，不另建研究/财务专用股票字典逻辑
- 将 `BSE` 保留在 research universe 内；对暂不具备稳定上游覆盖的模块，允许按 `optional_empty_exchanges=["BSE"]` 返回空占位结果
- 为后续行业、股东、完整财务报表、估值、风险等模块预留统一扩展面

---

## 3. 开发主线流程

后续开发统一遵循下面这条主线，不再按零散需求点跳着实现。

### 3.1 公共底座主线

1. 明确需求边界与数据源策略
2. 明确模块开关、预算模式、主备链配置
3. 在对应独立库中建立表结构与审计表：轻量研究维表默认进入 `research.db`，财务事实进入 `financials.db`，高行数估值派生序列进入 `valuation.db`
4. 建立 provider 抽象、source policy、registry
5. 建立 shadow sync 服务和调度任务
6. 建立 read model / query service / API 路由
7. 补齐单元测试、调度测试、API 测试
8. 影子运行、样本验证、灰度开放

### 3.2 领域交付顺序

研究域后续按下面的顺序推进：

1. `company_profile`
2. `financial_summary`
3. `technical`
4. `industry`
5. `financial_statements_raw + financial_facts + financial_indicator_snapshots`
6. `shareholders`
7. `valuation`
8. `risk / research metadata`

排序依据：

- 先交付本地可读、对下游使用价值最高的能力
- 有稳定免费源时，优先使用稳定免费源
- 缺少可靠免费源时，优先使用稳定付费路径，再把免费不稳定直连作为补充 fallback
- 若存在稳定免费主源，优先使用稳定免费主源；只有在稳定免费主源不足时，才提升到稳定付费主链

### 3.3 每个领域的标准实施步骤

每个新领域都必须按相同顺序推进：

1. 明确标准化目标模型和最小字段集
2. 明确主源、备源、付费兜底和官方披露兜底
3. 设计 research 存储表和原始 payload 审计字段
4. 实现 provider snapshot 抽象
5. 实现 shadow sync 服务
6. 接入 scheduler 任务
7. 实现 read API
8. 补齐单元测试和路由测试
9. 在工程文档中更新状态

---

## 4. 当前工作进度总览

### 4.1 已完成的主线能力

| 工作流 | 当前状态 | 说明 |
|---|---|---|
| research 配置模型与加载 | 已完成 | `ResearchConfig`、`ResearchStorageConfig`、预算/路由配置已接入 |
| research 代理补丁运行时统一入口 | 已完成 | `akshare / yfinance` 的 `proxy_patch` 安装已统一收口到 `utils/proxy_patch_runtime.py`；验证脚本改为共享该入口，避免模块内重复注入、硬编码 token 和分散实现 |
| research 独立存储初始化 | 已完成 | `research.db` 初始化与 `quotes.db` 可选 attach 已实现 |
| 审计与运行记录 | 已完成 | `ingestion_runs`、`raw_payload_audit` 已落地 |
| `company_profile` provider + sync + read | 已完成 | `BaoStock`、`PyTDX` 主备链和读取接口已落地；`BSE` 在无稳定上游时允许返回空占位结果 |
| `financial_summary` provider + sync + read | 已完成 | `BaoStock`、`PyTDX` 主备链和读取接口已落地；`BSE` 在无稳定上游时允许返回空占位结果 |
| `shareholders` provider + sync + gated read | 已完成并开放本地 API | 已落地股东摘要表、shadow sync、scheduler、gated API 与 readiness；当前配置为 `enabled=true / paid_high_availability`，运行主链已调整为 `cninfo:direct` 官方结构化股东数据优先，`akshare:proxy_patch / akshare:direct / efinance:direct` 作为定向 fallback；同步逻辑已升级为“缺失标的 + 缺失 required scope 继续 fallback 并按 scope merge”；`BSE` 当前已落快照但 readiness 仍保留 optional-empty 保护 |
| `industry` provider + sync + read | 已完成基线实现 | 行业参考层已完成；`BSE` 已纳入 optional-empty 口径，读取可返回空占位，`industry_standard_sync` 也不再因 `BSE` 空结果单独降级 |
| `industry_standard` current membership + rollout readiness | 官方分类主源已落地，API 读路径已补齐 | 已以申万官方 `StockClassifyUse_stock.xls + SwClassCode_2021.xls` 作为 strict Shenwan 主源重建 taxonomy、classification history 和 latest membership；官方六位分类代码作为 taxonomy/membership 主标识；已补齐 taxonomy、stock membership、component-set API 读路径。若 `industry_component_sets` 物理缓存为空，component-set API 会从 authoritative `industry_memberships` 派生股票组合 |
| `industry_index_analysis` provider + sync + read | 已完成存储/同步/API、日更、历史回补与补缺入口 | 已接入申万 `day_week_month_report` 最新日频指数分析指标，并接入与 AkShare `index_analysis_daily_sw` 同上游语义的历史日期区间回补源；两条链路均独立写入 `industry_index_analysis_daily`，并提供按 `sw_index_code`、日期区间、latest、taxonomy alias benchmark 的 API 读取；`2026-04-27` 完成 `2010-01-01` 至 `2026-04-24` 全量回补复核，主表 `1,104,548` 行；该表不反推股票分类 |
| `shareholders` rollout readiness + validation runner | 已完成 | 已新增股东域 readiness 聚合读链路，可汇总 snapshot coverage、source/source_mode 分布、按交易所覆盖率、模块 gate 与 `paid_high_availability` rollout blockers；已新增仓库级 `scripts/research_shareholder_rollout_validation.py`，用于串联 shadow sync 与 readiness 复核 |
| `financial_statements` provider + sync + read | 官方 structured JSON 多 profile 生产前验证中；下一阶段转向本地核心层分层设计 | 已保留 `AkShare proxy_patch -> direct` fallback 主链，并新增官方结构化 filing provider、manifest-to-artifact 候选抽取、响应分类、XBRL/XML/ZIP/structured JSON parser dispatch、SSE commonQuery parser、CNInfo data20 parser、source manifest、带 `canonical_*` 标准字段元数据的全数值事实长表、core fact alias override、hot/cold tier、多期 backfill/catchup checkpoint、coverage gap detection 与仓库级 rollout validation runner；`SSE` 默认使用交易所 `commonQuery.do` 编码字段 JSON，也可显式使用巨潮 `data20/financialData` 行标签 JSON；`SZSE/BSE` 使用巨潮 `data20/financialData` 行标签 JSON；`2026-05-08` 已完成 SSE `300` 标的单报告期和 `100x2` 多期临时库 dry-run；`2026-05-09` 已完成 SZSE/BSE CNInfo data20 单标的与前 5 active 标的小样本临时库验证；`2026-05-12` 已确认真实 SZSE/BSE CNInfo data20 返回可落出 `revenue / net_income_parent / equity_total / total_assets / total_liabilities / operating_cf` 等统一长表字段，且不把总权益脏填为标准归母 `equity`。`2026-05-18` 源评估显示东财字段最宽但慢，新浪与同花顺字段数量更接近且可形成核心语义交集，CNInfo data20 是官方摘要而非完整三表。生产源仍 disabled；下一步是新增新浪/同花顺 core mapping audit、bank/nonbank profile、本地核心 facts 写入规则和东财按需扩展接口 |
| `analyst_forecasts` provider + sync + read | 已完成基线实现 | 已落地 `AkShare stock_profit_forecast_em` 标准化、影子同步、读取 API；仓库默认路由为 `proxy_patch -> direct`，模块默认保持 disabled；`BSE` 在无稳定上游时允许空占位结果 |
| `research_reports` provider + sync + read | 已完成基线实现 | 已落地 `AkShare stock_research_report_em` 元数据同步、读取 API；仓库默认路由为 `proxy_patch -> direct`，模块默认保持 disabled；`BSE` 在无稳定上游时允许空占位结果 |
| `sentiment_events` provider + sync + read | 已完成基线实现 | 已落地 `notice / executive_share_change / pledge_ratio` 事件标准化、影子同步、读取 API；仓库默认路由为 `proxy_patch -> direct`，模块默认保持 disabled；`BSE` 在无稳定上游时允许空占位结果 |
| `research metadata` rollout readiness read | 已完成 | 已新增 `analyst_forecasts / research_reports / sentiment_events` grouped readiness，可按域汇总 module gate、distinct instrument 覆盖、source/source_mode 分布、交易所覆盖率与 blockers |
| `risk` 派生重建 + read | 已完成 | 已落地本地 `risk_snapshots`、重建任务与读取 API，默认 enabled |
| `company overview` 聚合读取 | 已完成 | 可聚合 company profile 与 financial summary |
| `technical` 实时计算读接口 | 已完成 | 已支持 summary 与 indicators 时间序列 |
| `technical_indicator_latest` 持久化 | 已完成基线实现 | 已落地最新技术指标快照表、`technical_snapshot_refresh` 刷新服务与 scheduler 任务、cache coverage readiness API；现有 technical summary / indicators API 仍保持实时计算 |
| 研究域 scheduler 影子任务 | 已完成 | 已接入 `company_profile / industry / industry_standard / financial_summary / shareholders / financial_statements / valuation_history / technical_snapshot / analyst_forecast / research_report / sentiment_event / risk_snapshot` |
| research 单元测试主干 | 已完成 | storage / provider / sync / scheduler / route / technical 均有测试覆盖 |

### 4.2 已落地的数据库表

当前 research 域已落地以下表：

- `ingestion_runs`
- `raw_payload_audit`
- `company_profiles`
- `industry_taxonomy`
- `industry_component_sets`
- `industry_official_code_mappings`
- `industry_official_classifications`
- `industry_memberships`
- `industry_index_analysis_daily`
- `financial_summaries`
- `shareholder_snapshots`
- `financial_statements_raw`
- `financial_facts`
- `financial_indicator_snapshots`
- `analyst_forecasts`
- `research_reports`
- `sentiment_events`
- `risk_snapshots`
- `technical_indicator_latest`

注意：

- `company_profiles` 与 `financial_summaries` 目前是“部分字段标准化 + 完整快照 JSON 并存”的设计
- 这满足了 Phase 0 / Phase 1 的可读性要求，但还不是最终 fully normalized 形态
- `industry_index_analysis_daily` 同时承接最新日更与历史回补；`bargain_volume` 为成交量（亿股），百分比字段保持百分数值，不转换成小数比例

### 4.3 已落地的研究接口

当前已实现的 research API：

- `GET /api/v1/research/company/{instrument_id}/overview`
- `GET /api/v1/research/company/{instrument_id}/profile`
- `GET /api/v1/research/company/{instrument_id}/industry`
- `GET /api/v1/research/industry/taxonomy`
- `GET /api/v1/research/industry/component-sets`
- `GET /api/v1/research/industry/index-analysis`
- `GET /api/v1/research/industry/index-analysis/{sw_index_code}/latest`
- `GET /api/v1/research/industry/taxonomy/{industry_code}/index-analysis/latest`
- `GET /api/v1/research/industry/standard-readiness`
- `GET /api/v1/research/industry/official-mapping-backlog`
- `GET /api/v1/research/industry/official-mapping-override-candidates`
- `GET /api/v1/research/industry/official-mapping-override-review`
- `GET /api/v1/research/company/{instrument_id}/financial-indicators`
- `GET /api/v1/research/company/{instrument_id}/shareholders`
- `GET /api/v1/research/shareholders/readiness`
- `GET /api/v1/research/metadata/readiness`
- `GET /api/v1/research/company/{instrument_id}/financial-statements`
- `GET /api/v1/research/company/{instrument_id}/analyst-coverage`
- `GET /api/v1/research/company/{instrument_id}/research-reports`
- `GET /api/v1/research/company/{instrument_id}/events`
- `GET /api/v1/research/company/{instrument_id}/risk`
- `GET /api/v1/research/company/{instrument_id}/technical/summary`
- `GET /api/v1/research/company/{instrument_id}/technical/indicators`
- `GET /api/v1/research/technical/readiness`

API 补齐原则：

- 所有已经落到本地 research 存储的标准化表或物化快照，都必须有 API 读路径，或明确返回 gated/disabled 原因
- 新 research 域不能只完成 provider / storage / sync；进入完成态前必须补齐 read API、模型、路由测试和文档
- API 不应暴露上游源的临时字段名作为唯一语义；对字段含义或单位尚未确认的指标，应同时保留 `raw_payload` 或 source diagnostics

申万行业 API 当前状态：

- 股票当前申万一/二/三级归属：已通过 `GET /api/v1/research/company/{instrument_id}/industry` 查询 authoritative membership，返回官方六位分类代码、申万一二三级代码/名称、index alias、source、source_mode、effective/update time
- 申万行业 taxonomy：已通过 `GET /api/v1/research/industry/taxonomy` 按 `taxonomy_system/taxonomy_version/level/parent_code` 查询行业节点，包含 `sw_index_code` 与 alias 信息
- 申万叶子/三级行业股票组合：已通过 `GET /api/v1/research/industry/component-sets` 查询；优先读取本地 `industry_component_sets` 缓存，缓存为空时从 authoritative `industry_memberships` 按行业代码派生股票组合，支持按 `industry_code` 查询
- 申万行业指数分析：已通过 `GET /api/v1/research/industry/index-analysis` 和 `GET /api/v1/research/industry/index-analysis/{sw_index_code}/latest` 按 `sw_index_code + trade_date/latest` 查询 `industry_index_analysis_daily`；已通过 `GET /api/v1/research/industry/taxonomy/{industry_code}/index-analysis/latest` 按 taxonomy 显式 index alias 查询 benchmark

申万行业 API 后续收口：

- 申万行业指数分析不允许从官方六位分类代码推断指数代码，只能使用 taxonomy 中显式保存的 index alias；无 alias 时 API 返回 `missing_reason`
- 申万行业 readiness / coverage / gaps：保留现有 readiness，并补齐可按交易所、行业层级定位缺口的读路径

### 4.4 已落地的技术分析能力

当前技术分析链路采用本地行情实时计算，已支持：

- `SMA20 / SMA60`
- `EMA12 / EMA26`
- `MACD / MACD signal / MACD hist`
- `RSI14`
- `ADX / +DI / -DI`
- `Stochastic K / D`
- `CCI`
- `Williams %R`
- `Bollinger Bands`
- `ATR14`
- `volume_ratio`
- `trend_score / signal`

### 4.5 尚未落地或仅部分落地的能力

| 模块 | 当前状态 | 说明 |
|---|---|---|
| `industry` 严格申万标准层 | 官方分类主源与 API 已完成 | 已以申万官方 XLS 文件直接维护 taxonomy、classification history 和 latest membership，官方六位分类代码成为主键；已补齐 taxonomy、stock membership、component-set API 读取，其中 component-set 可从 authoritative membership 派生。后续只保留 gap/readiness 细分定位和 alias 查询体验优化 |
| `industry_index_analysis` | 已完成 provider/storage/sync/API、日更、历史回补与按日补缺入口 | 已确认官方最新日频字段并落 `industry_index_analysis_daily`；已提供 index-code/latest/date/taxonomy-alias benchmark API；直连 provider 继续负责最新横截面日更；AkShare-compatible 历史 provider 复用 `index_analysis_daily_sw` 的 SWS `index_analysis_report` 上游语义并自行控制 timeout/retry，负责历史日期区间回补并写同一张表；历史回补会返回按 index_type 的覆盖率与缺失指标统计，默认通过 ops/Telegram 手动触发，scheduler 配置项保持 disabled；CLI 支持按日分块补缺 |
| `shareholders` | 已完成并开放本地 API | 已落地 `shareholder_snapshots`、shadow sync、scheduler、gated snapshot API 与 readiness API；当前语义覆盖 `holder_count / top10_holders / reference_only_ownership_clues`，配置已切到 `enabled=true / paid_high_availability`；仓库默认路由为 `AkShare proxy_patch` 主链，`cninfo:direct` 强制合并实控人线索，`akshare:direct / efinance:direct` 为补充 fallback；全量导入后 readiness 已无 blockers，后续进入周更维护和异常补缺阶段 |
| `financial_statements` | 多期财务仓主线进行中 | 已落地原始报表表、事实表、指标快照表、shadow sync、读取 API、官方 source manifest、全数值事实长表、hot/cold tier、多期 backfill/catchup checkpoint、coverage gap detection 与 `scripts/research_financial_statements_rollout_validation.py`；`2026-05-01` 检查当前生产 `research.db` 中财务摘要、完整报表、事实、指标表均为 `0` 行，说明工程链路已升级但尚未完成真实全市场财务数据维护 |
| `valuation` | 已完成基线并完成指标口径加固，默认禁用；下一步准备独立 `valuation.db` rollout | 已落地 `valuation_history`、相对估值、DCF 抽象、scheduler 与 API；估值历史已拆分 static/TTM/forward/MRQ 指标，相对估值可按 metric variants 明确计算 valid peer count、分位数、percentile rank 和排除诊断；production rollout 后续不仅取决于全市场 current authoritative membership 覆盖率，还必须依赖真实多期财务事实、可得日、market-cap/share-count 输入和 `valuation.db` readiness gate |
| `analyst_forecasts` | 已完成基线实现，默认禁用 | 已落地标准化存储、shadow sync、scheduler 与 API；当前以 `AkShare stock_profit_forecast_em` 为主，默认保持 disabled 等待 source stability 与预算确认 |
| `research_reports` | 已完成基线实现，默认禁用 | 已落地研报元数据表、shadow sync、scheduler 与 API；仅承诺元数据覆盖，不承诺全文与长期稳定可用性 |
| `sentiment_events` | 已完成基线实现，默认禁用 | 已落地 `notice / executive_share_change / pledge_ratio` 事件基线、shadow sync、scheduler 与 API；后续可扩展龙虎榜和资金流事件 |
| `risk` | 已完成 | 已落地 `risk_snapshots`、重建任务与 API；当前为本地派生风险快照，读取时不访问外部源 |
| `technical_indicator_latest` 持久化 | 已完成基线实现 | 当前只落“最新快照/缓存”，不落全历史技术指标宽表；快照由本地行情与本地复权因子派生，读取 API 仍默认实时计算 |

财务域当前必须明确三个层次，避免后续开发误判：

- 已有代码层：`financial_summary`、`financial_statements`、`valuation_history` 均有 provider/sync/storage/API 基线；`financial_statements` 已升级为支持多报告期、source manifest、all numeric facts、hot/cold tier 和 repository readiness 的同步/存储链路。
- 当前数据层：财务生产数据已迁入并维护在 `data/financials.db`；估值域代码层已接入独立 `data/valuation.db`，用于保存 `valuation_inputs`、`valuation_history`、估值运行审计和 lineage，不能把 API 存在或表结构存在等同于估值已可用。
- 下一阶段目标层：`harden-financial-xbrl-and-relative-valuation` 已补齐 PE/PB/PS 指标语义、relative valuation 统计、scheduler 任务和 readiness API gate；当前 `implement-sse-official-financial-json-source`、`prepare-sse-financial-batch-backfill-rollout` 与 `prepare-sse-financial-multiperiod-backfill` 已推进到 SSE 官方 structured JSON parser、`300` 标的单报告期 dry-run、`100x2` 多报告期 dry-run 和 checkpoint 续跑验证；`promote-cninfo-data20-financial-source` 已确认 SSE/SZSE/BSE 均可走 CNInfo data20 结构化 JSON，其中 SSE 默认主源仍保持交易所 commonQuery；SSE 同样本对比显示 CNInfo 可用但不应替代 commonQuery，主要风险是 `equity` 归母权益/所有者权益合计口径差异。官方结构化源生产化仍取决于 source-profile-aware production backfill gate、字段稳定性、全量 readiness 证据和业务确认。
- 新的服务分层目标层：`2026-05-18` 之后，完整财务域不再把“单一源覆盖最全”作为唯一主线，而拆成三层：
  - L1 本地核心层：以新浪和同花顺在 bank/nonbank profile 下通过审计的严格语义交集为准，提供高频、本地、统一单位的财务事实服务；同花顺因速度和长表结构暂定为候选主更新源，新浪作为互备和中文语义校验源。
  - L2 官方摘要校验层：CNInfo data20 只承担官方摘要大项校验，字段远少于新浪/同花顺/东财，单位为 `CNY_10K`，不能被当作完整三表源。
  - L3 远程扩展层：东财字段最宽但较慢，默认不写入本地核心层，仅当 AI/研究请求需要 L1 不覆盖字段时按需调用，并转换成同一 canonical schema、单位和口径后返回。

#### 4.5.1 下一阶段财务字段映射原则

本地核心层的字段交集必须是 `confirmed semantic exact intersection`，不是字段名相似、数量级相似或单样本数值相近。

映射审计必须至少区分以下关系：

| 关系 | 是否进入 L1 本地核心交集 | 说明 |
|---|---|---|
| `exact_equivalent` | 是 | 字段含义、报表类型、口径、期间属性和单位均一致 |
| `equivalent_after_unit` | 是 | 仅单位不同，且可用明确 multiplier 转换 |
| `derived_equivalent` | 谨慎 | 必须有稳定公式、组件齐全并单独标记 derived |
| `broader_than` | 否 | 一个字段包含另一个字段，不能互相覆盖 |
| `narrower_than` | 否 | 一个字段只是另一个字段的子项 |
| `related_only` | 否 | 业务相关但不是同一事实 |
| `rejected` | 否 | 已确认不能映射，保留 rejection reason |
| `unknown_candidate` | 否 | 候选待验证，禁止进入生产核心层 |

字段映射验证顺序：

1. 按 `bank / nonbank` 分 profile 建立人工候选映射，不允许跨 profile 复用银行专项字段。
2. 做语义审查，确认报表类型、字段层级、归母/总额、合并口径、累计/单季、期末余额/期间发生额。
3. 做单位审查，记录 `source_unit / canonical_unit / unit_multiplier / value_type`。
4. 对多公司、多交易所、多年份、多报告期做数值误差校验。
5. 用会计恒等式辅助审计，例如 `资产 = 负债 + 所有者权益`、`所有者权益 = 归母权益 + 少数股东权益`、`净利润 = 归母净利润 + 少数股东损益`。
6. 未通过审计的字段只允许进入 `unknown/rejected/related` 目录，不得写入 L1 核心交集。

字段目录维护方式：

- 物理存储继续使用稳定长表事实模型，不随字段交集变动动态增删宽表列。
- 新增、削减或疑似语义变化的上游字段，应进入 `source_field_catalog` 和 mapping audit 队列。
- L1 核心交集通过 `canonical_fact_catalog + mapping_version + approved_for_core` 控制。
- API/read model 可按当前 approved mapping 投影宽表，但宽表投影不是存储事实源。

### 4.6 当前状态与需求文档的差异

这部分需要显式记录，避免误判进度：

- `implementation_plan.md` 中的 Phase 1 目标比当前实际代码覆盖更宽
- 当前真正完成的是：
  - research 底座
  - `company_profile`
  - `shareholders` 摘要基线
  - `industry` 参考层
  - `industry` 严格申万标准层的代码链路
  - `financial_summary`
  - `financial_statements`
  - `valuation` 基线实现
  - `analyst_forecasts / research_reports / sentiment_events` 基线实现
  - `risk` 基线实现
  - `technical` 读取能力
  - `technical_indicator_latest` 最新快照缓存基线
- 当前尚未完成的是：
  - SSE `structured_json` 已完成 `300` 标的单报告期临时库 dry-run，并完成 `100` 标的 x `2` 报告期多期 dry-run；SSE/SZSE/BSE 已确认 CNInfo data20 结构化 JSON 小样本可解析或可直接返回结构化 payload；SSE 同样本对比显示 `sse_commonquery` 比 `cninfo_data20` 更快、facts 更丰富，且 CNInfo 当前只能提供总权益口径，不能直接填标准归母 `equity`。下一步需要 profile-aware 生产 backfill CLI/readiness gate、生产库放量前复核和更长报告期窗口验证。BSE 官网托管 XBRL/XML/ZIP artifact 尚未找到，但这不阻塞 BSE 通过 CNInfo data20 source profile 推进结构化财务源
  - `research metadata / valuation` 等默认 disabled 或受 gate 控制模块的 production rollout 决策与更大范围运行复核
- 因此，`financial_statements.enabled=true` 只代表本地同步/读取链路可用，不代表官方结构化源已经生产可用或全市场历史财务仓已完成；`valuation` 虽已实现，但正式开放仍应以财务 readiness、`/api/v1/research/valuation/readiness`、模块 gate 和业务确认结果为准

补充说明：

- 截至 `2026-04-21`，当时仓库内 research 主线相关活跃 OpenSpec 变更均已进入 `complete` 状态；`2026-05-01` 之后新的财务主线以 `harden-financial-xbrl-and-relative-valuation` 为准，当前已开始实现配置、探测和路由加固任务
- 当前剩余工作分为两类：已完成模块的 `rollout / archive / 维护复核`，以及财务域下一轮正式需求切分与实现
- `yfinance` 的 `akshare_proxy_patch` 接入也已完成收口：
  - `validate_yfinance_proxy_patch.py`
  - `validate_yfinance_proxy_patch_1.py`
  - `validate_yfinance_source_live.py`
  - 三条验证链路已统一为共享 runtime，并用于分别验证直接 patch、兼容脚本和项目内生产抓取路径
- 当前已补齐仓库级 `valuation` rollout validation runner：
  - `python scripts/research_valuation_rollout_validation.py --exchanges SSE,SZSE,BSE --skip-sync`
  - `python scripts/research_valuation_rollout_validation.py --exchanges SSE,SZSE,BSE --sync-inputs --target-instrument-ids 600000.SH,001233.SZ,920009.BJ --allow-disabled-module`
  - 用于串联 `valuation_input_sync -> valuation_history_rebuild -> /api/v1/research/valuation/readiness` 等价读链路复核
  - runner 生命周期现已改为优先使用 `DataManager.initialize(include_data_sources=false, load_progress=false)`，只初始化 research 存储和数据库，不再为只读复核拉起整套行情源
  - `--skip-sync` 模式只做 readiness 快速读链路复核；正式 rollout 前仍必须用 bounded rebuild 检查 `data/valuation.db.valuation_inputs` 与 `valuation_history` 的实表结果
  - `2026-05-29` bounded 样本验证已覆盖 SSE/SZSE/BSE：`600000.SH / 001233.SZ / 920009.BJ` 输入同步 `3/3`，估值历史重建写入 `55` 行，其中 SSE `39`、SZSE `4`、BSE `12`
  - `2026-05-29` 样本股本复核：上述三只样本的落库股本与 CNInfo/AkShare live read-only 返回一致，源值按 `10k_share * 10000` 转为股；`001233.SZ` 与 `920009.BJ` 的流通股本加限售股本可回到总股本，`600000.SH` 当期无单列限售股本
  - `2026-05-29` 只读耗时采样：日更全市场快照覆盖 `5525` 个当前活跃 A 股标的约 `3s`，生产含 upsert 与重试按 `1-5min` 规划；全量历史回填按当前 `0.2s` 请求间隔和样本返回量估算约 `2-6h`，保守任务超时配置为 `48h`
  - `valuation_inputs` 全量历史预计 `30-70万` 行，约 `0.3-1.5GiB`；日更快照会按 `(instrument_id, as_of_date, source, source_mode, input_kind)` upsert，未变更股本不会产生无限日增行
  - `2026-05-30` 新增财务核心事实可得日回填工具 `research/financial_available_date_backfill.py`：`data_available_date` 代表披露可得日，不等同于 `report_period`；本地真实公告/披露证据优先，缺失时使用 A 股法定披露截止日做保守估算并写入 lineage。生产回填更新 `financial_core_facts_hot` `54316` 行，SSE/SZSE/BSE core facts 可得日覆盖恢复到 `100%`；该维护步骤已接入 `FinancialStatementsShadowSyncService` 成功收尾阶段，后续财务全量 backfill、catchup 和 reconciliation 成功后会自动补齐新增/更新 core facts 的可得日
  - `2026-05-30` 回填后估值 dry-run：SSE `20/20` 写入 `5040` 行，SZSE `20/20` 写入 `5040` 行，BSE `10/10` 写入 `2176` 行，`missing_financials=[]`
  - scheduler 已预留估值任务入口：`valuation_input_sync` 为周一至周五 `23:00` 已开启日更测试任务，实测约 `1-2min` 完成并安排在 A 股行情日更之后；`valuation_history_rebuild` 为周二至周六 `04:45` 已开启日更小窗口任务，默认检查最近 `7` 个交易日且 `write_policy=missing_only`；`valuation_history_weekly_reconcile` 为周六 `05:45` 已开启周度回补校验任务，默认检查最近 `60` 个交易日且只补缺失；`valuation_history_12q_rebuild` 为 `enabled=true / manual_only=true` 手动过去 `12` 个季度窗口任务，按本地最近 `12` 个季度财务事实最早可得日确定行情窗口，默认补缺失而非覆写；missing-only 路径会先按标的推导候选估值日期并检查完整主键，窗口已完整的标的直接跳过完整指标计算；`valuation_history_full_rebuild` 仅作为禁用的兼容旧别名，不再表达无限历史全量重建；上述重建任务允许在 `valuation.enabled=false` 时做受控重建，API 模块 gate 仍由 readiness 单独控制；`valuation_input_full_backfill` 为 `enabled=true / manual_only=true` 手动任务

### 4.7 当前项目级 Source Policy

当前研究域默认配置已经统一到下面这套规则：

- `budget.default_mode = availability_first`
- `budget.allow_paid_proxy = true`
- `free_chain = 稳定免费`
- `paid_chain = 稳定付费`
- `fallback_chain = 免费但不稳定的补充链路`

这意味着：

- 有稳定免费源时，仍优先走稳定免费源
- 没有可靠免费源时，允许稳定付费链路先于不稳定直连链路
- 对 `shareholders / analyst_forecasts / research_reports / sentiment_events` 这类 AkShare-heavy 域，仓库默认应优先 `proxy_patch`，再退回 `direct`
- 对 `financial_statements`，当前基线仍是 `AkShare proxy_patch -> direct`，但 source policy 已开始按服务层分开：L1 本地核心层使用新浪/同花顺严格语义交集，L2 CNInfo data20 做官方摘要校验，L3 东财按需远程扩展；`config/10_research.json` 已新增 `service_layers.local_core / official_summary_check / remote_extension` 配置槽，现阶段不改变生产默认顺序，字段进入 L1 仍必须通过 mapping version 和审计规则
- 对已按 `optional_empty_exchanges=["BSE"]` 配置的模块，`BSE` 无稳定上游时允许以空占位完成 sync/read，不把缺口升级成全模块失败

同时要明确一个工程边界：

- `config/10_research.json` 中有一部分 route slot 是按目标策略预留的
- 只有在对应 provider 已实现、source 已启用时，这些 slot 才会在运行时真正生效
- 因此文档中的 source policy 既是当前默认行为，也是后续 provider rollout 的约束，不等于每个域的所有候选都已全部实现

### 4.8 当前 rollout readiness 读链路

针对 strict Shenwan、相对估值、research metadata 以及 shareholders 的上线判定，当前已经新增：

- `/api/v1/research/industry/standard-readiness`
- `/api/v1/research/valuation/readiness`
- `/api/v1/research/metadata/readiness`
- `/api/v1/research/shareholders/readiness`
- `/api/v1/research/technical/readiness`

其中 `industry/standard-readiness` 已不再只返回 aggregate blocker：

- 它以 current authoritative `industry_memberships` 覆盖率作为 strict Shenwan / relative valuation rollout 的核心判定
- 它仍会内嵌 `unmapped_backlog` 与 `override_review` 摘要，但这些字段只服务 official history 审计，不再作为 current membership 或 relative valuation 的 blocker
- 可直接看到 top high-impact backlog rows，而不必先看 readiness、再单独调用 backlog API
- 可直接看到 `manual_override` 审阅链路是否仍有待处理项，而不必再额外调用 review API

行业 readiness 接口会直接读取 research 域真实状态，聚合：

- authoritative membership 覆盖数
- 当前 research 股票池目标数量与按交易所覆盖率
- `relative valuation` 是否满足 authoritative rollout 条件，以及明确 blocker 列表
- official mapping cache / latest official classification 的审计摘要

估值 readiness 接口会聚合：

- `valuation_history` 覆盖数与缺口
- `source / source_mode / calc_method / calc_version` 分布
- 当前 research 股票池按交易所覆盖率
- `module_enabled`
- strict Shenwan current authoritative membership 对相对估值的阻塞状态
- `valuation` rollout blocker 列表

research metadata readiness 接口会聚合：

- `analyst_forecasts / research_reports / sentiment_events` 三个外部源元数据域
- 逐域 `module_enabled`
- 逐域 distinct instrument 覆盖数、row count 与缺口
- `source / source_mode` 分布与域内附加分布
- 当前 research 股票池按交易所覆盖率
- grouped rollout blocker 列表

股东 readiness 接口会聚合：

- `shareholder_snapshots` 覆盖数与缺口
- `coverage_status / source / source_mode` 分布
- `required_scope / scope_counts / scope_coverage`
- 当前 research 股票池按交易所覆盖率
- `module_enabled / delivery_mode / snapshot_api_requires_mode`
- `paid_high_availability` rollout blocker 列表

technical readiness 接口会聚合：

- `technical_indicator_latest` 最新快照覆盖数与缺口
- `period / adjustment` 快照口径
- `source / source_mode / calc_method / calc_version` 分布
- `status / signal` 分布
- 当前 research 股票池按交易所覆盖率
- `technical` module gate、`latest_cache.enabled` gate 与 cache rollout blocker 列表

因此，后续是否允许把 `valuation`、`research metadata` 或 `technical cache` 从默认配置推向 rollout，不再依赖工程文档里的手工判断，而应优先以对应 readiness 读链路为准；`shareholders` 已在 `2026-04-30` 按 readiness 结果开放本地 API，后续继续用 readiness 做周更后的健康复核。

---

## 5. 当前主线的阶段划分

### 5.1 已完成阶段

#### Stage A：研究域底座

已完成内容：

- 独立 `research_config`
- 独立 `research.db`
- research storage bootstrap
- `ingestion_runs`
- `raw_payload_audit`

#### Stage B：基础公司画像

已完成内容：

- `company_profile` provider 抽象
- `BaoStock` company profile provider
- `PyTDX` company profile provider
- shadow sync
- read API

#### Stage C：财务摘要

已完成内容：

- `financial_summary` provider 抽象
- `BaoStock` financial summary provider
- `PyTDX` financial summary provider
- shadow sync
- read API

#### Stage D：行业参考层

已完成内容：

- `industry_taxonomy`
- `industry_memberships`
- `BaoStock` industry provider
- `PyTDX` industry fallback provider
- shadow sync
- read API
- `company overview` 行业字段整合

当前边界：

- 这一层只能视为“行业参考层 / 线索层”
- 若未可靠映射到申万一/二/三级标准口径，则该结果只能作为公司基础信息参考
- 它还不能直接作为严格行业库、同行筛选库或相对估值比较中枢
- 当前已本地验证可直接更新 strict Shenwan 一/二/三级的免费接口链路是 `AkShare`
- `BaoStock / PyTDX / efinance` 当前只应进入参考层或映射线索层，不能直接声明为 authoritative

#### Stage E：技术分析读引擎

已完成内容：

- 本地行情读取
- 动态复权接入
- technical summary API
- technical indicators API
- 指标体系扩展
- `technical_indicator_latest`
- `technical_snapshot_refresh`
- `/api/v1/research/technical/readiness`

当前边界：

- `technical_indicator_latest` 只承载最新快照/cache，不承载全市场全历史技术指标宽表
- 现有 `/technical/summary` 与 `/technical/indicators` 继续从本地行情实时计算，不要求 cache 命中
- 后续形态识别、KDJ-J 派生、多周期解释模板应作为 Phase 2 技术规则引擎继续推进

#### Stage F：完整财务域

已完成内容：

- `financial_statements_raw`
- `financial_facts`
- `financial_indicator_snapshots`
- `AkShare` financial statements provider
- shadow sync
- scheduler 任务
- `/api/v1/research/company/{instrument_id}/financial-statements`
- `/api/v1/research/company/{instrument_id}/financial-statements/history`

当前边界：

- 当前单期接口返回最新或指定报告期；历史接口支持按公司读取最近 rolling 报告期或显式报告期列表，默认仍只读本地 `financials.db`，不隐式触发远程补数
- 当前完整三大报表可运行基线仍以 `AkShare proxy_patch -> direct` 为主；官方结构化源、CNInfo data20 与 AkShare fallback 已进入同一长表事实模型，但尚未形成可生产推广的全市场本地财务仓
- `2026-05-18` 之后，本地常规财务服务目标调整为“新浪/同花顺严格交集核心层 + CNInfo 官方摘要校验 + 东财远程扩展”，不再把未审计的单一来源字段直接作为本地标准事实
- `BaoStock / PyTDX` 在完整报表域仍不具备同等级覆盖，因此暂不作为主源
- 截至 `2026-05-01` 当前生产 `research.db` 的财务核心表尚未完成实际数据落库；`2026-05-08` 已用临时库完成 SSE `300` 标的单报告期 dry-run 和 `100x2` 多报告期 dry-run，下一步必须先做生产 backfill gate、readiness 复核和小步生产库放量方案，再决定是否全量导入

下一阶段目标：

- 建立 L1 本地核心财务层：同花顺作为候选主更新源，新浪作为互备/语义校验源，只保存 bank/nonbank profile 下已审计通过的严格语义交集
- 建立字段映射审计工具和配置：输出 `canonical_fact / source_field / relationship / unit_multiplier / value_type / sample_pass_count / sample_fail_count / max_relative_diff / approved_for_core / mapping_version`
- 建立 L2 CNInfo 官方摘要校验：只用于核心大项、单位和口径交叉验证，不承担完整字段补全
- 建立 L3 东财按需扩展接口：本地核心层缺少 AI/研究请求字段时远程调用东财，转换为同一 canonical schema 后返回；默认不写入 L1 核心事实表
- 多期 backfill 默认覆盖 `2024Q1` 至最新披露期，并满足至少 8 个季度 rolling floor
- source manifest、归档路径、hash、parser diagnostics 和 `data_available_date` 必须进入存储模型
- 全数值事实长表与核心事实表分层；字段 alias、单位换算、parser version 全部配置化或版本化
- 财务事实和派生指标按 hot/cold tier 维护：默认 hot window 为最近 `12` 个季度，历史数据进入 history tier；`12` 是配置默认值，不得写死在查询逻辑中
- 读取 API 继续本地优先，不在请求时访问官方披露或第三方财务接口

#### Stage G：估值基线

已完成内容：

- `valuation_history`
- `valuation_input_sync`
- `relative valuation`
- `DCF` engine abstraction
- `valuation_history_rebuild`
- `/api/v1/research/company/{instrument_id}/valuation/history`
- `/api/v1/research/company/{instrument_id}/valuation/relative`
- `/api/v1/research/company/{instrument_id}/valuation/dcf`
- `/api/v1/research/valuation/readiness`
- 相对估值 peer group 由 `valuation.relative.benchmark_field` 解析，默认 `sw_l2_code`
- 若 `require_authoritative=true`，subject membership 不是 `authoritative` 时不会查询同行估值，直接返回结构化 unavailable
- valuation rollout readiness 会同时检查 `valuation_history` 覆盖、模块 gate、以及 strict Shenwan current authoritative membership blocker

当前边界：

- `valuation` 默认仍建议保持 disabled
- 估值日频派生序列的生产物理目标已在 storage 层指向 `data/valuation.db`，而不是继续扩大 `research.db`
- `valuation.db` 只保存估值输入、`valuation_history`、估值运行审计和必要 lineage，不复制财务大表、行业大表或行情全量数据
- `valuation_inputs` 必须显式记录 source、source_mode、input_kind、unit、as_of_date、data_as_of 与 diagnostics；金额单位的股本/资本事实不能作为 share-count 输入，除非存在明确 share unit 映射
- A 股股本输入主线使用 CNInfo/AkShare：全量回填走 `stock_share_change_cninfo(symbol, start_date, end_date)` 拉单标的股本变动历史；日更走 `stock_hold_change_cninfo(symbol="全部")` 拉全市场最新股本变动快照。CNInfo `总股本 / 已流通股份` 按万股转换为股后写入 `valuation_inputs`，`as_of_date` 记录变动日期，`data_as_of` 记录公告日期
- `valuation_history` 更新默认使用 `write_policy=missing_only`：日更、周更和过去 `12` 个季度窗口任务均先轻量推导候选估值日期，再按完整主键 `instrument_id + as_of_date + calc_method + calc_version + parameter_hash` 检查缺失；窗口已完整的标的直接跳过完整 PE/PB/PS 指标计算，存在缺口时只写缺失行；显式 `write_policy=overwrite` 用于股本历史、财务可得日、计算逻辑或参数修复后的受控覆写
- 过去 `12` 个季度窗口任务不是无限历史全量重建。当前财务核心事实只保留最近 `12` 个季度，因此 PE/PB/PS 历史派生以这组本地可得事实的最早 `data_available_date` 为边界；早于该边界的长历史估值需要另行扩展财务历史层后再设计
- `valuation_history.market_cap` 明确为总市值，优先由本地收盘价乘 `shares_outstanding` 得到；`float_market_cap` 由本地收盘价乘 `float_shares` 得到。两者是估值结果和审计 lineage 的派生冗余，保留在 `valuation.db` 便于复现、API 查询和后续备份/迁移
- `valuation_history.parameter_hash` 只表示“同一交易日估值结果如何计算”，不得包含任务窗口或调度参数；`lookback_days / window_mode / quote_limit_days / write_policy / progress_log_every / max_runtime_seconds` 等只进入 `ingestion_runs.metadata_json` 和任务报告。日更、周更、过去 `12` 个季度补足任务在单日计算规则相同的情况下必须使用同一个 canonical hash。读路径按当前 canonical hash 过滤，并在迁移期兼容当前配置的 legacy full-history hash；历史运行留下的旧窗口或旧参数行不得长期参与 API 最新值、readiness 覆盖率或 relative valuation 同行统计。清理或迁移旧 hash 前必须备份，推荐先运行 `scripts/research_valuation_storage_maintenance.py --db-path data/valuation.db` 做只读审计，再显式指定 `--delete-parameter-hash ... --confirm-delete` 或 `--rewrite-from-parameter-hash ... --rewrite-to-parameter-hash ... --confirm-rewrite`。
- 新股、刚挂牌标的或上市前财务历史不足标的，如果已经有本地 `valuation_inputs` 和交易行情，即使没有可用财务事实，也应写入 partial 估值历史行：`market_cap / float_market_cap` 正常计算，PE/PB/PS 指标置空并在 `details_json.metrics.*.missing_reason` 中记录 `financial_facts_not_available`。这类行用于保持市值序列连续，不代表财务估值口径已经可用。
- `details_json` 是日频高行数字段，必须保持 compact：保留输入 lineage、报告期、可得日、分母、指标状态和缺失原因，不保存完整 provider payload 或大体量 diagnostics。若需要完整审计，应放入源文件/raw payload 表或单独审计表，而不是在每个交易日重复写入。
- `relative valuation` 默认依赖 current authoritative `sw_l2` peer group
- 相对估值默认从 `valuation.db.valuation_history + research.db.industry_memberships` 即时计算或使用 bounded aggregate cache，不持久化全量 `subject_stock x peer_stock x trade_date x metric` 矩阵
- valuation rollout readiness 现在同时返回 `valuation_history` 覆盖、估值输入覆盖、metric coverage、财务 readiness、行业 readiness 与 storage 摘要
- 在全市场 current authoritative membership 或 valuation input 覆盖不足时，relative valuation rollout 仍应保持 gated，而不是降级使用 reference-only 行业字段或从成交额/换手率反推市值

#### Stage H：研究元数据与风险

已完成内容：

- `analyst_forecasts`
- `research_reports`
- `sentiment_events`
- `risk_snapshots`
- `analyst_forecast_sync`
- `research_report_sync`
- `sentiment_event_sync`
- `risk_snapshot_rebuild`
- `/api/v1/research/metadata/readiness`
- `/api/v1/research/company/{instrument_id}/analyst-coverage`
- `/api/v1/research/company/{instrument_id}/research-reports`
- `/api/v1/research/company/{instrument_id}/events`
- `/api/v1/research/company/{instrument_id}/risk`

当前边界：

- `analyst_forecasts / research_reports / sentiment_events` 当前都是 metadata-first 设计
- 外部 research metadata 源仍以 `AkShare` 为主，默认保持 disabled，避免对生产读取链路产生“已上线”的误导
- `sentiment_events` 第一轮只覆盖 `notice / executive_share_change / pledge_ratio`
- `risk` 当前采用本地派生模式，默认 enabled，但风险分数仍属于 Phase 1 baseline，需要后续结合行业、估值和更多事件流继续调优

### 5.2 当前下一主线

当前活跃主线已完成 `valuation` readiness 与 `technical_indicator_latest` 最新快照缓存基线，行业和股东进入维护复核；财务数据获取、存储和更新能力加固 change 已于 `2026-05-06` 归档，上一轮官方 endpoint 探测 change `discover-official-financial-xbrl-endpoints` 也已归档，SSE 批量单报告期准备 change `prepare-sse-financial-batch-backfill-rollout`、多报告期准备 change `prepare-sse-financial-multiperiod-backfill`、SSE structured JSON 实现 change `implement-sse-official-financial-json-source`、生产 backfill gate change `prepare-sse-financial-production-backfill` 均已于 `2026-05-08` 归档。当前 SSE 主线已经具备受 gate 控制的小步生产 backfill 入口，并已完成前 `10` 个 SSE 标的 x `2` 个报告期的生产写入 smoke；`2026-05-09` 新增主线是将 CNInfo data20 从 SZSE/BSE discovery evidence 提升为覆盖 `SSE/SZSE/BSE` 的 `cninfo_data20` source profile，统一管理 official source、parser、request policy、checkpoint 和 AkShare fallback。`2026-05-18` 后，财务主线已完成并归档 OpenSpec change `build-sina-ths-core-financial-layer`，按新浪/同花顺交集、字段映射审计、L1 本地核心读取和东财远程扩展接口完成代码落地；`2026-05-19` 新开 OpenSpec change `validate-sina-ths-local-core-live-audit`，进入真实样本 evidence 与生产启用 gate 阶段。

财务主线执行顺序：

1. 官方结构化源探测
   - 对 `SSE-commonQuery / SSE-CNInfo / SZSE-CNInfo / BSE-CNInfo` 分别做小样本 live probe，记录可下载 payload/artifact、字段稳定性、延迟、失败模式、source profile 和覆盖率
   - probe 脚本只做受控样本验证，不做全市场抓取
   - 当前 `scripts/dev_validation/probe_official_financial_filings.py` 已从 `config/10_research.json` 读取官方候选源和 `endpoint_candidates`，输出本地样本覆盖、URL 配置状态、请求 method、候选 key、证据来源、响应分类、artifact kind、parser candidate、readiness status/blocker、artifact 下载样本、响应大小和样本 hash；官方 endpoint URL 仍为空时会明确返回 `missing_endpoint_config`
   - `2026-05-06` 有界 live probe 结果已收紧：
     - `SSE 600000 / 2023Q4`：官方英文 XBRL 页面脚本暴露的 `https://query.sse.com.cn/commonQuery.do` 可通过 `COMMON_MAP_INCOMESTATEMENT_C / COMMON_MAP_BALANCESHEET_C / COMMON_MAP_CASHFLOW_C` 返回结构化 JSON，三个候选均被分类为 `structured_payload / structured_json / structured_financial_json.v1`，`readiness_status=structured_artifact_ready`
     - `CNInfo/SSE 600000 / 2025Q4`：`topSearch/query` 返回 `orgId=gssh0600000`，`data20/companyOverview/getCompanyInfo?scode=600000` 返回 `F002N=2`，`data20/financialData/getIncomeStatement / getBalanceSheets / getCashFlowStatement` 三个 endpoint 均返回 `application/json`，包含 `营业收入 / 总资产 / 经营活动产生的现金流量净额` 等行标签和年度/季度 bucket
     - `CNInfo/SZSE 000001 / 2025Q4` 与 `BSE 920833 / 2025Q4`：`new/information/topSearch/query` 可解析 `orgId`，`data20/companyOverview/getCompanyInfo?scode=...` 可解析 `F002N/sign`，`data20/financialData/getIncomeStatement / getBalanceSheets / getCashFlowStatement` 三个 endpoint 均返回 `structured_payload / structured_json`，并由 CNInfo parser 解析为 numeric facts/core facts
     - `BSE` 官网公告 metadata/PDF 路径仍只作为审计线索；当前没有确认 BSE 官网托管的 XBRL/XML/ZIP artifact，但 BSE 结构化财务源已可通过 CNInfo data20 推进
   - 因此官方结构化源仍不得视为 full-market production-ready；SSE 与 CNInfo data20 已从 endpoint 发现推进到 parser 接入和 isolated sync 验证阶段，后续必须按市场/source profile 分别跑 dry-run evidence、生产 gate 和 readiness
2. 配置与路由加固
   - 在 `config/10_research.json` 或后续独立 financial config 中配置 `baseline_report_period`、`rolling_min_quarters`、`hot_quarter_window`、`hot_anchor_policy`、`official_source_candidates`、`max_concurrency`、`request_timeout_seconds`、`retry_attempts`、`request_interval_seconds`、`parser_version`、`fallback_policy`
   - `financial_statements` 路由需要拆成三层：`local_core` 使用同花顺/新浪严格语义交集，`official_summary_check` 使用 CNInfo data20，`remote_extension` 使用东财按需补字段；各层 source order、timeout、缓存、mapping version 和字段审计阈值必须配置化。当前已新增配置槽，并已在现有 `GET /api/v1/research/company/{instrument_id}/financial-statements` 中以显式参数暴露 `service_layers` 诊断：默认响应保持兼容，只有传入 `include_local_core=true` 或 `allow_remote_extension=true` 等参数才返回 L1/L3 分层结果。生产配置仍保持 `local_core.enabled=false`、`remote_extension.enabled=false`，避免未审计字段直接进入生产读写路径
   - 旧的官方结构化源优先路径继续保留为 source profile/backfill/readiness 能力，但不得绕过 L1 字段交集审计直接写入本地核心服务层
   - 当前实现中 `sse / cninfo / bse` 已进入 `financial_statements.free_chain`，但官方财务子域默认 `enabled=false`；`ResearchSourcePolicyResolver` 会尊重 `sources.<source>.financial_statements.enabled=false`，因此在对应 source profile 未通过生产 backfill gate、字段稳定性和 readiness 前，实际生产解析结果仍保持 `AkShare proxy_patch -> AkShare direct`
3. 存储层扩展
   - 增加 source-file manifest，保存 official filing id / URL / archive path / hash / parser diagnostics / ingestion run
   - 增加全数值事实长表，保留 XBRL 或等价结构化文件中的所有 numeric facts
   - 扩展核心事实表，补齐 `report_type`、`data_available_date`、`source_file_id` 和 fallback provenance
   - 增加财务事实 hot/cold tier：hot tier 默认保留最近 `12` 个季度和必要 anchor periods，history tier 保存更早报告期；source manifest 和 raw archive 保持全量唯一，不按 hot/cold 拆散
   - 当前已落地 `financial_source_files`、`financial_numeric_facts`、`financial_core_facts_hot/history`、`financial_numeric_facts_hot/history`、`financial_indicator_snapshots_hot/history`，并保留现有 `financial_facts` 与 `financial_indicator_snapshots` 兼容表
   - 全数值事实长表的 `canonical_*` 列用于跨源字段统一，不改变原始 `fact_name` 和 `raw_fact_json`；股本类字段必须区分金额和股数，`S2010_0700 / 实收资本（或股本） / TOTAL_SHARE / 股本` 归一为 `share_capital_amount`、单位 `CNY`，`shares_outstanding` 只允许明确股数口径字段写入；未知字段必须保留 raw fact 和 unmapped 状态，不能为了填齐核心字段而静默改名或默认单位
   - L1 本地核心层不使用动态宽表迁移；物理存储继续是长表事实模型。字段交集、字段新增/削减和语义变更通过 `source_field_catalog / canonical_fact_catalog / mapping_version / approved_for_core` 管理，read model 再按需要投影为宽表
  - 当前 storage facade 为 `FinancialStatementStorageRepository`，调用方通过 manifest/numeric facts/core facts/tier maintenance helper 访问，不直接依赖物理表名；`2026-05-20` 已将财务域生产读写从 `data/research.db` 修正为通过 `financials_db_path` 路由到 `data/financials.db`，后续迁移到 DuckDB 或 PostgreSQL 时仍应替换 storage adapter，不改变 API 语义
  - `2026-05-18` OpenSpec change `build-sina-ths-core-financial-layer` 已进入实现：已完成 AkShare `ths_report` provider 接入、同花顺长表按报告期聚合、THS value-type 字段保留、第一版 Sina/THS core mapping catalog 与银行现金类拒绝映射测试
  - `2026-05-19` 继续完成该 change 的核心链路：新增离线样本 mapping audit 脚本、source-field mapping catalog 存储、mapping audit result 存储、L1 approved facts 的 mapping-version lineage、local-core read helper 和 Eastmoney remote extension service。聚焦测试覆盖 provider、mapping catalog、audit script、storage、local read、remote extension 和 fallback lineage，结果为 `16 passed`。离线 bounded audit 样本覆盖 `sina_report / ths_report / cninfo_data20 / eastmoney_report`，输出 `status=passed`、字段数分别为 `9 / 9 / 2 / 8`、`approved_mapping_count=9`、`approved_mapping_passed_count=8`、`missing_source_value_count=1`、`blocking_issue_count=0`。剩余风险是尚未做真实网络样本审计和全市场写入，未审计字段仍不得进入本地核心 facts
  - `2026-05-19` 已补齐 read API 接入：`/research/company/{instrument_id}/financial-statements` 新增可选 `report_period` 过滤和 `requested_canonical_facts/profile/mapping_version/include_local_core/allow_remote_extension` 参数；DataManager 会按配置返回 `service_layers.local_core` 的本地核心事实/缺字段诊断，或在远程扩展开关显式开启且配置允许时走 Eastmoney remote extension。当前默认调用不附加分层结果，不改变旧客户端响应；当 L1/L3 配置禁用时，API 返回 `disabled_by_config` 诊断而不是静默降级或联网补数。补充 focused tests 后，财务映射与 API 接入测试结果为 `21 passed`，OpenSpec strict validate 仍通过。
  - `2026-05-19` 已归档 `build-sina-ths-core-financial-layer`；归档前已把 delta spec 同步进主 `openspec/specs/research-data-engine/spec.md`，`openspec validate --specs --strict` 为 `3 passed, 0 failed`。当前新 change `validate-sina-ths-local-core-live-audit` 已创建并通过 `openspec validate validate-sina-ths-local-core-live-audit --strict`。
  - `2026-05-19` 已新增 `scripts/dev_validation/live_audit_sina_ths_local_core.py`：该 runner 只生成非破坏性 JSON evidence，默认不写生产库、不改 `local_core.enabled`；输入必须显式给出 `--target instrument_id:exchange:profile` 和 `--report-periods`，并可配置 sources、timeout、throttle、mapping version、output path。runner 通过 AkShare provider 获取 `sina_report / ths_report / eastmoney_report`，通过既有 official batch runner 获取 `cninfo_data20` 临时库 numeric facts，随后复用离线 mapping audit，输出 source timing/error、field count、semantic audit、accounting identity、promotion blockers 和 `promotable` 状态。当前已完成 fake-source 单元测试和 focused regression，结果 `11 passed`；真实联网样本尚未执行，仍是下一步 blocker。
  - `2026-05-19` 已执行受限 live smoke：`600000.SH:SSE:bank`、`600519.SH:SSE:nonbank`，报告期 `2024-12-31`，四源均为 `sina_report / ths_report / cninfo_data20 / eastmoney_report`，证据文件为 `/tmp/quote_sina_ths_local_core_live_audit_20260519_rerun.json`。四个源 fetch 全部成功，无 source failure；总耗时 `78.674s`。source timing 显示 `ths_report` 约 `1.37-1.46s`，`sina_report` 约 `2.82-4.17s`，`eastmoney_report` 约 `17.63-18.11s`，`cninfo_data20` 在两个样本上约 `2.23s` 与 `25.62s`。审计修复 `NaN` 误报后不再出现 canonical mismatch 或 accounting identity failure，但两个样本仍为 `needs_review`：银行样本缺已批准 `equity_parent / net_income_parent` exact mapping，非银行样本缺已批准 `equity_parent` exact mapping。结论是上游可获取且数值整体可比，但当前 mapping catalog 尚未覆盖 live Sina 标签变体，生产仍必须保持 `local_core.enabled=false`。
  - `2026-05-19` 随后用增强后的 `source_field_candidates` 证据升级 mapping catalog 到 `sina_ths_core_financial_facts.v2`，只批准三组已有四源数值交叉证据的标签变体：非银 `归属于母公司股东权益合计 -> parent_holder_equity_total`，银行 `归属于母公司的净利润 -> parent_holder_net_profit`，银行 `归属于母公司股东的权益 -> parent_holder_equity_total`；v1 保留用于历史审计。v2 复跑同一 live smoke，证据文件 `/tmp/quote_sina_ths_local_core_live_audit_v2_20260519.json`，结果 `status=passed`、`promotable_sample_count=2/2`、无 source failure、无 promotion blocker，总耗时 `54.925s`。当时配置中的 `service_layers.local_core.mapping_version` 已指向 v2，但 `local_core.enabled=false` 不变；生产启用仍需要更代表性的 bank/nonbank 样本和写入前 gate，而不是仅凭 2 个样本放开。
  - `2026-05-19` 已追加跨市场 bounded live audit：`000001.SZ:SZSE:bank`、`000333.SZ:SZSE:nonbank`、`300750.SZ:SZSE:nonbank`、`920833.BJ:BSE:nonbank`，报告期 `2024-12-31`，证据文件 `/tmp/quote_sina_ths_local_core_live_audit_v2_cross_market_tolerance_20260519.json`。结果 `status=passed`、`promotable_sample_count=4/4`、四源均无 fetch failure、无 promotion blocker，总耗时 `79.815s`。本轮同时明确 CNInfo data20 官方摘要层在 BSE 小市值样本上会出现几十元级舍入差异；审计已将 CNInfo canonical cross-check 容差单独设为 `absolute_tolerance=100`、`relative_tolerance=1e-5`，只用于官方摘要层跨源校验，不影响 Sina/THS 本地核心 exact mapping 审核。
  - `2026-05-19` 已将字段扩展方式从“少量核心字段手工补映射”调整为“全字段候选映射流水线”：mapping audit 会输出 `source_field_values` 全字段数值目录，并自动生成 `generated_mapping_candidates`，覆盖 `sina_report__ths_report / sina_report__eastmoney_report / ths_report__eastmoney_report` 三组候选关系；候选默认不批准，仅作为批量审核输入。单样本 `600519.SH / 2024-12-31`，只取 `sina_report / ths_report / eastmoney_report`，证据文件 `/tmp/quote_financial_full_field_candidates_600519_2024.json`：Sina `285` 字段、THS `262` 字段、Eastmoney `736` 字段；自动候选为 Sina-THS `152` 对、Sina-Eastmoney `163` 对、THS-Eastmoney `178` 对。后续扩全字段应优先批处理这些候选并落入版本化本地标准字段目录，而不是逐字段手工追加。
  - `2026-05-19` 已将候选 pair 进一步合并为 `local_standard_field_candidates` 字段簇：以 Sina-THS 交集为本地互备锚点，用 Eastmoney 匹配字段辅助命名和语义审核；每个簇保留建议本地字段 key、review status、数值证据、三源字段明细、`profile` 和 `unit_review`。`unit_review` 必须区分 `known_units_match / unit_conflict / requires_unit_review`；`profile` 必须作为 bank/nonbank 映射批准的隔离边界。复跑 `600519.SH / 2024-12-31`，证据文件 `/tmp/quote_financial_full_field_clusters_600519_2024.json`：候选簇 `152` 个，其中 `16` 个为 `known_canonical_candidate`，`136` 个为 `requires_semantic_review`，且 `152/152` 均有 Eastmoney 字段辅助解释。下一步应把字段簇导出/落库成可审核的本地标准字段目录草稿，再通过 mapping version 批准进入生产。
  - `2026-05-19` 已新增 `scripts/dev_validation/export_financial_mapping_review.py`，用于把字段簇导出为人工审核包；默认只导出拿不准的字段，避免把已知 canonical 候选和待审字段混在一起。基于 `/tmp/quote_financial_full_field_clusters_600519_2024_unit_review.json` 已导出 `/tmp/quote_financial_mapping_review_600519_2024_uncertain.csv`、`/tmp/quote_financial_mapping_review_600519_2024_uncertain.md`、`/tmp/quote_financial_mapping_review_600519_2024_uncertain.json`；当前待审行 `136` 条，均为 `requires_semantic_review` 且 `requires_unit_review`，代表这些字段尚未有本地标准字段单位定义，不允许自动批准。
  - `2026-05-19` 已新增 `scripts/dev_validation/import_financial_mapping_review.py`，用于校验人工审核后的 CSV 并导出 approved draft mapping JSON。审核 CSV 预留 `review_decision / approved_local_field / approved_semantic / approved_canonical_unit / approved_source_unit / unit_multiplier / relationship / approved_sina_field / approved_ths_metric / approved_eastmoney_field / review_notes`；只有 `approve_core` 且语义、单位、Sina/THS 字段和 relationship 完整的行会进入草案。该脚本只生成草案，不直接写生产库或修改 mapping catalog；拿不准、单位未明确或多源字段不唯一的行会留在人工审核/补证据状态。
  - `2026-05-19` 已补齐机器初审层，避免把所有候选字段直接转给人工：`local_standard_field_candidates` 现在包含 `machine_review.status/confidence/reasons/blockers/suggested_*`。规则为：三源同报表数值一致、Sina/THS/Eastmoney 字段唯一、单位可由标准别名或三表金额口径推断、无重复候选/每股收益同值混淆时，标为 `machine_approved_candidate` 并预填 `approve_core` 草案；一对多、多对一、Eastmoney 多字段同值、EPS 同值、缺第三源或单位冲突才进入人工包。基于 `600519.SH / 2024-12-31` 既有证据重新导出后，机器预审通过 `93/136`，人工包降为 `43` 行；导入校验可生成 `93` 条 approved draft mapping，且 `error_count=0`。
  - `2026-05-19` 已把人工包继续压缩为分组验证：`export_financial_mapping_review.py --output-group-csv` 会按 `per_share_basic_vs_diluted / multi_source_same_value_duplicate / same_value_label_variant / missing_third_source / unit_review` 等问题类型聚合，避免逐行审核。当前 `600519.SH / 2024-12-31` 的 `43` 行人工明细被汇总为 `5` 个验证组，见 `/tmp/quote_financial_mapping_review_600519_2024_human_groups.csv`；用户优先看分组 CSV，只有需要追溯证据时再看 row details。
  - `2026-05-19` 已根据用户反馈把人工审核文档改为中文 Markdown 分组说明，输出 `/tmp/quote_financial_mapping_review_600519_2024_human_review_cn.md`。用户已初步确认：G01 中应付账款同值时优先完整口径“应付票据及应付账款”，资产总计与负债权益总计虽然相等但不能互相覆盖；G02 中基础项/合计项多为同义表述；G05 中基本 EPS 与稀释 EPS 即使同值也不能交叉合并。G03/G04 已改为针对现金流 NOTE 附注字段和利润表不同层级项目同值给出具体解释。
  - `2026-05-19` 已把用户确认的 G01-G05 规则落入机器复核：应付账款/应付票据及应付账款、资产总计/负债权益总计、在建工程/合计项、其他应收应付/合计项、现金流主表/NOTE、利润表同值层级项、基本 EPS/稀释 EPS 均按语义建立 exact 或 reject 关系。重新导出的中文人工复核包 `/tmp/quote_financial_mapping_review_600519_2024_after_user_rules.md` 已降为 `0` 条人工项；全量预填导入校验 `/tmp/quote_financial_mapping_review_600519_2024_after_user_rules_all_prefilled_import.json` 为 `approved_count=124`、`rejected_count=24`、`ignored_count=4`、`error_count=0`。正式 mapping catalog 已升级到 `sina_ths_core_financial_facts.v3`，其中 EPS 保留 `CNY_per_share` 单位，配置 `service_layers.local_core.mapping_version` 已同步指向 v3；生产开关仍保持关闭。
  - `2026-05-19` 已修正 live promotion gate 的 required/optional 语义：mapping catalog 的 `approved_for_core=true` 是“允许转换并进入 L1”的字段目录，不是“每个 issuer-period 必须存在”的字段清单。`live_audit_sina_ths_local_core.py` 新增 `--required-canonical-facts`，默认沿用核心必达事实 `revenue / net_income_parent / total_assets / total_liabilities / equity_parent / operating_cf`。promotion blocker 只针对 required 字段：未批准映射输出 `required_local_core_mapping_unapproved`，已批准但样本缺值输出 `approved_local_core_fact_missing`；其他 v3 可选字段只在有值时参与 mapping/value/unit 审计，缺失不阻断。这是 v3 扩字段后避免误把行业/公司特有科目当成全局必填字段的关键保护。
  - `2026-05-19` 已执行一次覆盖更完整的 10 标的真实 v3 live audit，避免反复小批次测试。目标覆盖 `SSE/SZSE/BSE`、银行、普通非银、BSE、券商、保险；报告期 `2024-12-31`；四源 `sina_report / ths_report / cninfo_data20 / eastmoney_report` 全部 fetch 成功，无 source failure。证据文件 `/tmp/quote_sina_ths_local_core_live_audit_v3_10sample_20260519.json`，总耗时 `299.85s`；平均耗时为 THS `1.598s`、Sina `2.756s`、CNInfo data20 `2.391s`、Eastmoney `21.829s`。原始结果 `4/10` promotable：三家银行与 `600519.SH` 直接通过。四个普通非银/BSE blocker 来自错误批准的 OCI 子项映射：`外币财务报表折算差额` 与 `以后将重分类进损益的其他综合收益` 不能等同 THS `other_common_profit` 总项；已将这两类候选改为 machine rejected，并从 v3 正式目录移除。`600030.SH` 券商与 `601318.SH` 保险仍为真实剩余缺口，说明当前 `bank/nonbank` 二分 profile 不足以覆盖金融非银行报表，应后续新增或细化 `securities/insurance` profile，而不是把它们强行混入普通 nonbank。
  - `2026-05-19` 已按补充要求加入科创板样本 `688981.SH`：单样本真实 v3 live audit 证据 `/tmp/quote_sina_ths_local_core_live_audit_v3_star_688981_20260519.json`，结果 `status=passed`、`promotable_sample_count=1/1`、无 blocker，总耗时 `17.458s`。四源耗时/字段数为 THS `1.383s / 10`、Sina `2.238s / 286`、CNInfo data20 `1.811s / 20`、Eastmoney `10.805s / 737`。`live_audit_sina_ths_local_core.py` 新增 `--target-preset v3_coverage`，该预设将银行、普通非银、BSE、券商、保险与 `688981.SH:SSE:nonbank` 固化为同一覆盖集合，后续需要复核时可一次性运行该 preset。
  - `2026-05-19` 已开始拆分金融非银 profile：`v3_coverage` 预设中 `600030.SH` 改为 `securities`，`601318.SH` 改为 `insurance`；映射目录新增 profile 继承，`securities/insurance` 暂继承非银基础 exact mappings，同时补齐新浪金融非银权益标签 `归属于母公司的股东权益合计`、`负债及股东权益总计`、`所有者权益合计`。CNInfo data20 在 L1 live promotion 中调整为观测/诊断源：CNInfo 与 Sina/THS/Eastmoney 的 canonical 口径差异仍写入 evidence，但不再作为 Sina+THS 本地核心层启用 blocker；真正阻断 L1 的仍是 Sina/THS approved mapping value mismatch、会计恒等式失败、必达核心字段缺失，以及 Eastmoney 与本地 canonical 体系冲突。
  - `2026-05-19` 已新增统一财报 profile 解析模块 `research/financial_statement_profile.py`。财报 profile 是报表结构桶而不是行业全分类，目前为 `bank / securities / insurance / nonbank`；解析优先级为显式参数 > strict 申万行业 membership > company profile 原始行业/板块 > instrument metadata > 默认 `nonbank`。生产同步后续应先读取 `industry_memberships` 中的 `sw_l1_name / sw_l2_name / sw_l3_name / industry_code`：`银行` 对应 `bank`，`证券` 对应 `securities`，`保险` 对应 `insurance`，其余行业对应 `nonbank`。股票代码前缀只能辅助识别市场板块，例如 `688` 是科创板，不代表特殊财报 profile。
  - `2026-05-19` `live_audit_sina_ths_local_core.py` 已接入上述 resolver。显式 profile 写法 `--target 600030.SH:SSE:securities` 继续可用；若写作 `--target 600030.SH:SSE`，脚本会从本地 `industry_memberships/company_profiles` 自动解析 profile，并把 `profile_resolution` 写入 evidence。生产 backfill 接入前应沿用同一 resolver，避免同步链路和审计链路使用不同的公司类型判断。
  - `2026-05-20` 正式 backfill/sync 已开始使用同一 resolver 生成运行 evidence：`research_financial_statements_backfill.py` 会在 dry-run 和生产 write result 中输出 `financial_statement_profile_resolutions` 与 `financial_statement_profile_summary`；`FinancialStatementsShadowSyncService` 会在 exchange result 和 ingestion run metadata 中记录同样的 profile evidence。该改动只增加 profile 可观测性，不改变现有 official/CNInfo/AkShare 抓取和写入逻辑；后续 L1 本地核心层写入必须基于该 profile 选择字段映射目录。
  - `2026-05-20` L1 本地核心事实 lineage 已 profile-aware：fallback bundle 转全数值 facts 时，会按 instrument 的 `bank / securities / insurance / nonbank` profile 和 `local_core.mapping_version` 过滤映射目录；只有当前 profile 下 approved 的 Sina/THS 字段才写 `local_core_mapping`。若映射唯一确定 canonical fact，则 numeric fact 的 canonical 元数据也由 mapping catalog 补齐。读路径 `DataManager.get_research_financial_statements(... include_local_core=True)` 在未显式传 profile 时也会自动解析，并把 `profile_resolution` 放入 `service_layers.local_core`。
  - `2026-05-20` L1 mapping catalog 已接入同步链路的持久化闭环：`FinancialStatementsShadowSyncService` 每次 exchange run 会按配置的 `local_core.mapping_version` 与 `local_core.profiles` 幂等写入 `financial_source_field_mappings`，并把 `local_core_mapping_catalog` 写入 exchange result 和 ingestion metadata。读层仍以持久化 catalog + numeric fact lineage 双重约束筛选本地核心 facts，避免代码目录、写入 lineage 和读层目录版本不一致。
  - `2026-05-20` `v3_coverage` 真实 live audit 已一次性复跑完成：覆盖银行、普通非银、BSE、科创板、券商、保险共 `11` 个样本，报告期 `2024-12-31`，四源 `sina_report / ths_report / cninfo_data20 / eastmoney_report` 均成功，证据 `/tmp/quote_sina_ths_local_core_live_audit_v3_coverage_20260520.json`。结果为 `status=passed`、`promotable_sample_count=11/11`、`blocking_sample_count=0`、无 source failure，总耗时 `318.696s`。平均耗时/字段数：Sina `2.653s / 297.2`，THS `1.625s / 10.0`，CNInfo data20 `2.100s / 21.4`，Eastmoney `21.248s / 703.3`。该证据只证明 v3 promotion required canonical facts 在覆盖集合下可晋级，不代表所有行业所有扩展字段已完成。
  - `2026-05-20` AkShare 财报 provider 已对齐 L1 source order：仅当 `service_layers.local_core.enabled=true` 时，provider 使用 `local_core.source_order` 的 `ths_report -> sina_report`；默认关闭状态继续沿用全局 `statement_interface_order`，不改变既有 fallback 语义。
  - `2026-05-20` live audit 已补充报告期维度：summary 输出 `by_report_period / by_profile / source_metrics / period_field_variations`，用于区分行业差异和报告期差异。小样本真实多报告期复核覆盖 `000001.SZ:bank / 600519.SH:nonbank / 600030.SH:securities` 的 `2024-12-31 / 2024-09-30`，证据 `/tmp/quote_sina_ths_local_core_live_audit_v3_period_variation_20260520.json`，结果 `status=passed`、`promotable_sample_count=6/6`，两个报告期均无必达字段缺口；本次样本内各源字段数量未因年报/三季报变化而变化。后续扩大字段目录或启用 L1 时，仍必须把 period-level field variation 作为生产 gate 输入，不能只用一个年报样本判断字段稳定。
  - `2026-05-20` L1 本地核心层完成临时库端到端 dry run：临时打开 `local_core.enabled=true`，按 `ths_report -> sina_report` 顺序抓取 `600519.SH:nonbank / 600030.SH:securities / 000001.SZ:bank` 的 `2024-12-31`，写入 `/tmp/quote_l1_local_core_dryrun_20260520.db`，输出 `/tmp/quote_l1_local_core_dryrun_20260520.json`。sync `status=success`，写入 `3` 个 source manifest、`1372` 条 numeric facts；`get_local_core_facts()` 对三个标的的六个必达 canonical facts 均返回 `ready=true` 且无缺字段。临时库 raw statement 抽查显示写入 THS 长表字段，证明 L1 source order 已实际生效。
  - `2026-05-20` 本地核心字段映射目录已升级到 `sina_ths_core_financial_facts.v4`：把当前已审核的 Sina+THS 通用 L1 字段显式写入 `nonbank / bank / securities / insurance` 四个 profile，避免证券、保险、银行在同步和读层只依赖隐式继承。当前目录覆盖为：nonbank `130` approved，bank `131` approved + `1` rejected，securities `130` approved，insurance `130` approved；银行 rejected 项继续用于阻止“现金及存放中央银行款项”和通用 `total_cash` 被误当等价字段。v4 的边界是“当前已审核通用字段 profile 显式化”，不是所有行业专用可选字段的最终穷尽；后续若新增券商、保险、银行专用科目，应新建后续 mapping version，并保留语义、单位、数值和报告期差异证据。
  - `2026-05-20` v4 profile+报告期组合验证发现一个真实银行语义问题：`000001.SZ` 的新浪“发放贷款及垫款”为毛额，新浪“发放贷款及垫款净额”才与 THS `loans_payments_behalf` 和东财 `LOAN_ADVANCE` 数值一致；年报和三季报均复现。证据 `/tmp/quote_sina_ths_local_core_live_audit_v4_profile_period_20260520.json` 中 12 个样本有 10 个 promotable，2 个 blocker 均来自该字段。已升级到 `sina_ths_core_financial_facts.v5`：银行毛额映射 rejected，银行净额映射 approved exact，配置 `local_core.mapping_version` 指向 v5；复跑 `/tmp/quote_sina_ths_local_core_live_audit_v5_bank_fix_000001_20260520.json` 为 `status=passed`、`2/2` promotable。该例确认后续多报告期测试必须同时检查字段语义，而不是只看字段名相似。
  - `2026-05-20` 已完成 v5 同组完整复跑，证据 `/tmp/quote_sina_ths_local_core_live_audit_v5_profile_period_20260520.json`：`status=passed`、`12/12` promotable、无 required fact gaps。profile 维度为 bank `2/2`、nonbank `6/6`、securities `2/2`、insurance `2/2`；报告期维度为 `2024-12-31` `6/6`、`2024-09-30` `6/6`。source metrics 显示 THS 仍最快，平均约 `1.457s`；Sina 约 `2.540s`；CNInfo data20 约 `2.743s` 但字段很少；Eastmoney 最宽但平均约 `17.883s`。本轮仍记录 `18` 组 period field variations，说明不同报告期字段集合存在真实差异，扩展字段和行业专用字段必须按“有则审计、缺失不阻断”的 optional mapping 处理，不能升级成全局必填。
  - `2026-05-20` v5 已通过临时库端到端写入 dry run，证据 `/tmp/quote_l1_local_core_v5_profile_period_dryrun_20260520.json`，临时库 `/tmp/quote_l1_local_core_v5_profile_period_dryrun_20260520.db`。本轮临时打开 `local_core.enabled=true`，使用 `ths_report -> sina_report` 写入 6 个标的 x 2 个报告期，sync `status=success`，写入 `12` 个 source manifest、`5383` 条 numeric facts；`get_financial_local_core_facts()` 对全部 `12/12` 个 instrument-period 的六个必达字段返回 ready。库内 catalog 持久化结果为 v5：bank `133` rows / `131` approved，nonbank `130/130`，securities `130/130`，insurance `130/130`；raw statement 抽查确认写入 THS 长表字段。这说明 v5 不仅审计可过，写入 lineage、catalog 持久化和读层筛选也能闭环。
  - `2026-05-20` 新增可复用临时库验证脚本 `scripts/dev_validation/validate_sina_ths_local_core_dryrun.py`，把此前一次性的 v5 dry run 转成固定入口。脚本默认使用 `/tmp` 隔离 SQLite，临时启用 `service_layers.local_core.enabled=true`，按配置的 `source_order` 抓取 Sina/THS，写入 raw statement、source manifest、numeric facts 和 mapping catalog 后，再按 profile/mapping version 读回必达 canonical facts。脚本要求每个目标显式声明 `profile`，并使用不会触发 bank 字符串误判的 dry-run 行业代码；输出包含 `sync_result / profile_summary / local_core_reads / mapping_catalog_counts / source_manifest_counts`，用于后续行业专用字段和报告期字段差异扩展的证据沉淀。推荐命令：`/home/python/miniconda3/envs/Quote/bin/python scripts/dev_validation/validate_sina_ths_local_core_dryrun.py --report-periods 2024-12-31,2024-09-30 --output-path /tmp/quote_l1_local_core_dryrun.json`。
  - `2026-05-20` 新增全量导入准备的只读 manifest 脚本 `scripts/dev_validation/prepare_sina_ths_local_core_import_manifest.py`。它读取 active research target instruments，复用 `financial_statement_profile` resolver 从 `industry_memberships/company_profiles/instrument metadata` 解析 profile，输出 target file、profile confidence/source/reason、按 profile 的 mapping readiness、分片 batch plan 和下一步 dry-run 命令；`validate_sina_ths_local_core_dryrun.py` 同步支持 `--target-file`。小样本只读 smoke 命令为：`/home/python/miniconda3/envs/Quote/bin/python scripts/dev_validation/prepare_sina_ths_local_core_import_manifest.py --report-periods 2024-12-31,2024-09-30 --limit-per-exchange 2 --batch-size 2 --output-path /tmp/quote_l1_local_core_import_manifest_smoke.json --target-output-path /tmp/quote_l1_local_core_import_targets_smoke.txt`，结果 `status=ready`、`target_count=6`、profile 均为 `industry_membership/high`，v5 四个 profile 的必达字段 mapping readiness 均为 ready。该脚本支持 `--quiet`，用于全市场运行时只在 stdout 打印摘要，完整 manifest 写入文件。该步骤只生成全量导入计划，不抓取财报、不写生产库；全市场 dry run 应先用它生成 target file，再分片调用 L1 dry-run 脚本。
  - `2026-05-20` 已用该脚本完成全市场只读 manifest：`/tmp/quote_l1_local_core_import_manifest_full_20260520.json` 与 `/tmp/quote_l1_local_core_import_targets_full_20260520.txt`。摘要为 `status=ready`，`target_count=5490`，交易所分布 `SSE=2307 / SZSE=2883 / BSE=300`，profile 分布 `nonbank=5393 / securities=50 / bank=42 / insurance=5`；`default_profile_count=0`、`low_or_default_confidence_count=0`；v5 四个 profile 的 `revenue / net_income_parent / total_assets / total_liabilities / equity_parent / operating_cf` 均已批准且无 missing required mapping。按 `batch_size=20` 生成 `275` 个 batch。该结果只证明全市场目标清单、profile 解析和必达映射目录可进入下一步，不代表全市场财报已下载或可直接生产写入；下一步应先选择少量 batch 做 L1 临时库 dry run，再决定是否扩大到全量 dry run。
  - `2026-05-20` 已输出逐批 target 文件到 `/tmp/quote_l1_local_core_import_batches_20260520/`，共 `275` 个。随后完成三市场各一批 L1 临时库 dry run：SSE `batch_0016` 写入 `/tmp/quote_l1_local_core_dryrun_batch_0016_20260520.db`，`status=success`、`ready_read_count=40/40`、`total_numeric_facts_written=18173`、耗时 `49.44s`；SZSE `batch_0132` 写入 `/tmp/quote_l1_local_core_dryrun_batch_0132_20260520.db`，`status=success`、`ready_read_count=40/40`、`total_numeric_facts_written=18188`、耗时 `49.954s`；BSE `batch_0001` 写入 `/tmp/quote_l1_local_core_dryrun_batch_0001_20260520.db`，`status=needs_review`、`ready_read_count=32/40`、`total_numeric_facts_written=11854`、耗时 `36.134s`。BSE 缺口均在 `2024-09-30`，主要缺 `total_assets / total_liabilities / equity_parent`，少数同时缺 `revenue / net_income_parent / operating_cf`；原始表抽查表明部分 BSE 标的该期未写入三表或缺资产负债表，先归类为上游报告期/报表类型可得性缺口，不调整 v5 字段映射。
  - `2026-05-20` 已修复上述 BSE 分片暴露的 L1 回退粒度问题：当调用方显式指定 `report_periods` 时，AkShare 财报 provider 现在按报告期和三张表逐项合并 `ths_report -> sina_report`，不再因为 THS 返回利润表/现金流就停止并遗漏 Sina 资产负债表；同步层同步支持 mixed bundle 的逐报表来源映射，`balance_sheet` 可按 `sina_report` lineage，`profit_sheet/cash_flow_sheet` 可继续按 `ths_report` lineage 写入本地核心事实。同一 BSE `batch_0001` 复跑证据 `/tmp/quote_l1_local_core_dryrun_batch_0001_statement_lineage_fallback_20260520.json`：`ready_read_count=39/40`、`not_ready_read_count=1`、`total_numeric_facts_written=12283`、耗时 `45.288s`。唯一未 ready 为 `920020.BJ / 2024-09-30`，缺 `total_assets / total_liabilities / equity_parent`；跨源审计 `/tmp/quote_bse_source_gap_live_audit_920020_20240930.json` 证明 THS/Sina/Eastmoney 均未给出该期资产负债表候选字段，属于 issuer-period 级真实源缺口，后续只能走远程扩展、人工补录或等待源更新，不能通过放宽字段映射伪造完整性。
  - `2026-05-20` L1 dry-run gate 已支持显式 accepted source gap：`validate_sina_ths_local_core_dryrun.py --accepted-source-gap instrument_id:report_period:fact[,fact]`。该参数只用于已经由跨源审计确认的真实源缺口，脚本会继续输出 `not_ready_reads`，但同时拆分 `accepted_source_gap_reads` 与 `blocking_not_ready_reads`；只有 blocking 为 0 时状态才允许变为 `success_with_accepted_source_gaps`。对上述 BSE `920020.BJ / 2024-09-30` 加入 `--accepted-source-gap 920020.BJ:2024-09-30:total_assets,total_liabilities,equity_parent` 后，证据 `/tmp/quote_l1_local_core_dryrun_batch_0001_accepted_source_gap_20260520.json` 显示 `ready_read_count=39/40`、`accepted_source_gap_read_count=1`、`blocking_not_ready_read_count=0`、状态为 `success_with_accepted_source_gaps`。生产推进时，该状态可被视为“结构和映射通过，但存在显式记录的上游真实缺口”，不能等同于全字段完整。
  - `2026-05-20` 已执行一次且仅一次扩大范围 L1 dry-run，不再继续逐级扩大：选取 `batch_0001 / batch_0002 / batch_0058 / batch_0131 / batch_0200`，共 `100` 只股票、`2` 个报告期，合计 `200` 个 instrument-period。覆盖维度为 `BSE=40 / SSE=27 / SZSE=33`，profile 为 `nonbank=89 / bank=4 / securities=4 / insurance=3`，并包含科创板 `688` 样本和已知 BSE accepted source gap。证据 `/tmp/quote_l1_local_core_dryrun_expanded_once_20260520.json`，临时库 `/tmp/quote_l1_local_core_dryrun_expanded_once_20260520.db`；运行耗时 `226.375s`，写入 `199` 个 source manifest、`73936` 条 numeric facts，`ready_read_count=189/200`，`accepted_source_gap_read_count=1`，`blocking_not_ready_read_count=10`，最终状态 `needs_review`。10 个 blocking 缺口全部位于 `2024-09-30`，其中 `BSE=6`、`SSE 科创板=4`，均为 `nonbank`；主要缺资产负债表核心字段 `total_assets / total_liabilities / equity_parent`，`920050.BJ` 还缺利润与现金流核心字段且该期没有 source manifest。抽查 `financial_statements_raw` 显示多数 blocking 样本该期只落入利润表和现金流量表，没有资产负债表，因此当前归因为上游报告期/报表族可得性缺口。下一步不再扩大 dry-run，而是只对这 10 个 issuer-period 做跨源确认：确认三源都缺则登记 accepted source gap，若某源有字段则补回回退/映射逻辑。
  - `2026-05-20` 已对 `SSE 科创板=4` 单独确认，不直接沿用扩大 dry-run 结论。4 个样本为 `688807.SH / 688809.SH / 688816.SH / 688818.SH`；证据 `/tmp/quote_star_688_source_gap_live_audit_20240930.json` 显示，`2024-09-30` 下 THS 和 Eastmoney 有利润表、现金流量表，CNInfo data20 无字段，Sina 抓取失败，所有可用源均未提供资产负债表核心字段候选；而同一批临时库中这 4 只 `2024-12-31` 三表齐全。结合公开上市时间，这 4 只是 `2025-12` 至 `2026-02` 上市的新股，`2024-09-30` 属于上市前历史期，因此缺口分类为 `pre_listing_incomplete_structured_statement`。脚本 `--accepted-source-gap` 已支持第四段 classification，例如 `instrument_id:report_period:fact[,fact]:pre_listing_incomplete_structured_statement`，用于把这类已确认的上市前结构化报表不完整问题登记为 accepted gap；该分类不适用于已上市存量公司，也不允许绕过字段语义映射。
  - `2026-05-20` 已完成 `BSE=6` 个 blocking 样本的定向确认，证据 `/tmp/quote_bse_source_gap_live_audit_remaining_20240930.json`。`920027.BJ / 920028.BJ / 920045.BJ` 在四源下均没有 `2024-09-30` 资产负债表核心字段候选，可登记 `total_assets / total_liabilities / equity_parent` accepted source gap；`920050.BJ / 920056.BJ / 920060.BJ` 本次可从 Sina/Eastmoney 找到资产负债表候选，说明扩大 dry-run 中这三只的缺口是接口瞬时失败，不应接受为源缺口。定向 dry-run `/tmp/quote_l1_local_core_dryrun_bse_remaining_gap_recheck_20260520.json` 验证为 `status=success_with_accepted_source_gaps`、`ready_read_count=3/6`、`accepted_source_gap_read_count=3`、`blocking_not_ready_read_count=0`，耗时 `13.753s`。当前已确认的 accepted source gap 清单为 8 个 issuer-period：BSE `920020/920027/920028/920045` 的 `2024-09-30` 资产负债表三项，以及科创板新股 `688807/688809/688816/688818` 的上市前 `2024-09-30` 资产负债表三项。
  - 全量 dry-run 耗时估算：三市场代表批次 `20` 只股票、`2` 个报告期约 `45~50s`；扩大批次 `100` 只股票、`2` 个报告期耗时 `226.375s`。按全市场 manifest `5490` 只股票估算，当前两报告期的 L1 全量 dry-run 大约 `3.5~4.5` 小时；考虑 Sina/THS 瞬时失败、重试和磁盘写入波动，生产前应预留 `5` 小时级窗口。由于 AkShare 三表接口按股票返回多期 DataFrame，增加报告期不会严格线性增加网络下载时间，但会增加 bundle 构造、numeric facts 写入和 local-core 读回校验成本。
  - `2026-05-20` 已完成财务库物理目标修正后的旧库清理和新库小批 smoke。清理前分别创建 `/tmp/research_pre_financial_cleanup_20260520.db` 与 `/tmp/research_pre_financial_cleanup_20260520_sqlite_backup.db`；随后清空 `data/research.db` 中所有 `financial_*` 表数据，并清空 `ingestion_runs/raw_payload_audit` 中 `domain LIKE 'financial%'` 的记录，复核计数均为 `0`，`PRAGMA quick_check` 为 `ok`。小批 smoke 直接写 `data/financials.db`：`600519.SH:SSE:nonbank / 000001.SZ:SZSE:bank`，报告期 `2024-12-31`，证据 `/tmp/quote_financials_db_l1_smoke_20260520.json`；结果 `status=success`、`ready_read_count=2/2`、`total_source_manifests_written=2`、`total_numeric_facts_written=922`、`total_core_facts_written=2`、`financial_source_field_mappings=523`。复核确认 `financials.db` 承载新增财务数据，`research.db` 财务表保持 0 行。
  - 同日继续删除 `data/research.db` 中空的财务物理表，删表前备份 `/tmp/research_pre_financial_table_drop_20260520.db`；当前 `sqlite_master` 中不再存在 `financial_%` 表，`financial` 域 ingestion/raw payload 计数仍为 `0`，`PRAGMA quick_check` 为 `ok`。代码层同步加固：当 `financials_db_path != db_path` 时，`ResearchStorageManager.initialize()` 会在研究库初始化后移除研究库财务表，防止后续启动重新产生空表。L1 全量脚本已增加阶段日志，覆盖 start/storage initialized/sync started/sync completed/readback completed/evidence written。
  - 同日新增正式 Python 全量入口 `scripts/research_financial_l1_full_import.py`。它把此前临时 shell 操作收口为可复用 orchestrator：生成 manifest/targets/batch files，逐批调用 L1 local-core 写入 `data/financials.db`，维护 `progress.log`、`progress_state.json`、每批 evidence、summary 和 `--resume` 续跑语义，并内置当前已人工确认的 accepted source gap。后续 scheduler/Telegram 集成应调用该 Python 脚本或其 `run_full_import()`，不得把 shell 循环作为长期运维入口。
  - `2026-05-24` 财务任务化实现启动：新增 `financial_l1_full_import` 手工 `/run` 入口，直接复用 `run_full_import()`；新增 `FinancialDisclosureIncrementalSyncService`，复用 CNInfo announcement scanner 识别定期报告、更正、延期披露、停牌、退市风险和可能终止上市公告，候选维度为 `instrument_id + report_period`；新增 `financial_disclosure_event_state` 记录 pending recheck、`pending_delisting_risk`、accepted gap、blocking gap 和处理结果。增量/对账任务默认不自动启用，先通过 `/run` 做小范围验证。
  - 同日 live no-write 验证：`scripts/research_financial_disclosure_event_scan.py --exchanges SZSE,SSE --start-date 2026-04-01 --end-date 2026-05-24 --max-pages 1` 在联网放行后成功返回，SZSE/SSE 各扫描 30 条，SSE 命中 1 条 `关于收到股票终止上市决定的公告`，selection reason 为 `pending_delisting_risk`，但该公告未映射到财务报告期，因此仅作为审计证据，不放宽任何 instrument-period readiness gate。
  - 同日写入 smoke 覆盖 `002731.SZ / 688121.SH`、报告期 `2025-12-31 / 2026-03-31`：`002731.SZ` 扫描 `94` 条公告并选中 `4` 条停牌/退市风险提示，但均未映射到具体报告期，因此不生成候选、不接受缺口；`688121.SH` 扫描 `36` 条公告并选中 `5` 条，其中 `3` 条延期 2025 年年报公告可映射到 `2025-12-31`，状态写入 `financial_disclosure_event_state` 为 `periodic_report_delayed_or_suspended / pending_recheck`，缺失核心字段数为 `5`。存储层同步固定 pending recheck 截止时间，后续重复扫描不会滚动延长等待窗口。
  - `2026-05-26` 根据周度对账报告修正维护层 readiness 口径：本地核心 required facts 改为 `revenue / net_income_parent / equity_parent / total_assets / total_liabilities`，并支持 `required_core_facts_by_profile`；`outside_approved_local_core / mapping_catalog_empty` 归为 `mapping_policy_gap`，表示字段标准或映射准入不一致，不再调用 CNInfo/THS/Sina 补数；`missing_local_core_fact` 等才进入 source missing 路由。reconciliation 候选超过上限时按交易所、profile 和报告期均衡抽样，Telegram 报告拆分 accepted gaps、mapping policy gaps、source missing 和 blockers。
  - 同日复核 `financial_disclosure_reconciliation_sync` 第二次真实运行：`ingestion_run_id=405` 为 `changed=240 / blocking=260`，其中 `mapping_policy_gap=0`，说明旧 `net_income/equity` 准入问题已经消除。260 个 remaining blockers 中，256 个报告期结束日早于股票主数据上市日，集中在 `001237.SZ / 001365.SZ / 001393.SZ / 603407.SH / 603435.SH` 等 2025-2026 新股；剩余 4 个为 `002731.SZ`、`688121.SH` 的 `2025-12-31 / 2026-03-31`，CNInfo data20 三表 endpoint 返回但 `numeric_fact_count=0`，Sina/THS fallback 也未补足 required facts。维护层已前置上市/退市生命周期判断，把 `pre_listing_period` 与 `post_delisting_or_no_disclosure` 作为 accepted disclosure gap，不再进入补数源路由或 degraded blockers；同时 reconciliation 会复用已落库的 `accepted_disclosure_gap`，避免 `688121.SH / 2025-12-31` 这类已由公告解释的缺报在下一轮被重新打成 local blocker。`002731.SZ / 688121.SH` 的其他无精确公告解释期仍保持披露异常待补或 source missing，等待公告状态或后续结构化源更新。
  - 同日继续复核第三次 reconciliation：`ingestion_run_id=446` 为 `accepted=376 / changed=2 / blocking=122`。122 个 blockers 中，119 个为 BSE，且 `quotes.db.instruments` 里 BSE `listed_date` 全部为空；联网确认 `akshare.stock_info_bj_name_code()` 实际返回 `上市日期 / 所属行业 / 地区`，但 `data_sources/akshare_source.py` 过去把 BSE `listed_date` 硬编码为 `None`。已修复 BSE 主数据解析并执行 BSE 主数据刷新，当前 315 个 BSE 标的均有 `listed_date`，对原 119 个 BSE blockers 重新套生命周期规则后全部为 `pre_listing_period`。剩余 `002731.SZ / 2025-12-31, 2026-03-31` 与 `688121.SH / 2026-03-31` 在 `financials.db.cninfo_announcement_audit` 中已有“无法在法定期限内披露定期报告/停牌/可能被实施退市风险警示”公告，但标题未含具体报告期；维护层已新增受限复用规则，仅将公告日前 180 天内的近端报告期标记为披露异常 accepted/pending delisting，避免误放更早历史期。定向 dry-run 覆盖 `920178.BJ / 002731.SZ / 688121.SH` 的近端期次，结果 `status=success`、`accepted_gap_count=5`、`blocking_gap_count=0`。
  - `2026-05-22` 针对首次全量后剩余 blocking 缺口完成收口修正。Manifest 新增 `listed_date / delisted_date / excluded_report_periods / report_period_lifecycle_summary`，并在 full import 中自动把 `pre_listing_period` 与 `post_delisting_or_no_disclosure` 合并为 accepted source gap，避免上市前历史期和退市后未披露期阻断全量。AkShare fallback provider 改为字段级跨源补齐：在 target-period 模式下会继续读取后续源，只填补同一语义核心字段缺失项，不把同义备选字段重复写入同一 canonical 事实，并在 raw payload 中记录字段级来源。L1 local-core 写入新增两类受控派生归母净利润：`净利润 - 少数股东损益`，以及无少数股东损益/少数股东权益披露时 `净利润 -> 归母净利润`，均写入 `derived.net_income_parent` 并保留公式 lineage。定向真实 dry-run 覆盖原 `36` 条缺口涉及的 `20` 个标的 x `10` 个报告期：修正后 `ready_read_count=195/200`，剩余 `5` 条整期无结构化三表，其中 `600355.SH / 2026-03-31` 属退市后未披露生命周期缺口；`002731.SZ` 与 `688121.SH` 的 `2025-12-31 / 2026-03-31` 在本次 Sina/THS/Eastmoney 小范围探测中均未返回该期结构化三表，仍保留 review，不作为字段映射缺陷处理。
  - `2026-05-24` 补充财务披露异常分类要求：财务全量/补处理 manifest 在生命周期规则之外，还应接收 CNInfo 公告筛选结果，把 active 标的中由“定期报告无法按期披露、停牌、退市风险警示、可能终止上市”等公告解释的缺口标记为 `periodic_report_delayed_or_suspended`。该状态只表示当前结构化源缺失有披露事件依据，不能替代字段映射或跨源补齐；一旦后续公告或结构化源更新，仍应通过补处理导入补齐本地核心字段。
4. 官方 provider 与解析层
   - 当前 `ConfiguredOfficialFinancialFilingProvider` 已支持 direct endpoint 下载，也支持在无 `endpoint_url` 时先请求 `manifest_url`、抽取 XML/XBRL/ZIP/JSON/PDF 附件候选，再只下载结构化候选；本轮已扩展为可读取 `endpoint_candidates` 并按配置构造 method、headers、query/body、artifact base URL、promotion gate 和样本下载限制
   - 当前已新增 `FinancialXbrlNumericFactParser` 与 `FinancialStructuredFilingParserDispatcher`，支持 `xbrl_xml`、`xbrl_zip` 与 `structured_json` 路由，保留 namespace、context、unit、decimals/precision、period、dimensions、archive entry 和 parser diagnostics；`structured_html` 暂只记录 unsupported diagnostics，不假装解析成功
   - `FinancialSseStructuredJsonFactParser` 已接入 SSE `COMMON_MAP_INCOMESTATEMENT_C / COMMON_MAP_BALANCESHEET_C / COMMON_MAP_CASHFLOW_C`，把 `S2020_xxxx / S2010_xxxx / S2030_xxxx` 数值字段落为全数值 facts；`config/10_research.json` 通过 `fetch_all_endpoint_candidates=true` 表达“三张表来自三个 commonQuery candidate”，并通过 `structured_json_fact_parser` 与 `core_fact_alias_overrides` 配置 parser version 和核心事实 alias，避免把字段代码写死到同步服务
   - `FinancialCninfoData20StructuredJsonFactParser` 已接入 CNInfo `data20/financialData`，按中文行标签、`one/middle/three/year` 报告期 bucket 和 `CNY_10K -> CNY` 单位归一化解析；该 parser 服务 `SSE/SZSE/BSE + cninfo_data20`，与 SSE parser 共享同一个 dispatcher、source manifest、numeric facts、core facts 和 readiness 模型
   - 当前 core fact aliases 通过 `core_financial_facts.v1` 版本化映射提供，并在运行时合并配置化 override；fallback merge 只填补主源缺失字段，不覆盖官方主源已给出的事实
   - 官方 payload 只在写入 `parsed` source manifest 后才被 readiness 计为可用来源；manifest-only、PDF-only、blocked、parse-failed 或无核心事实的官方结果不会阻断 fallback，并会在 sync result 中记录 `official_fallback_reasons`
   - 新浪/同花顺 mapping 不能沿用简单 alias merge：必须新增 mapping audit，把候选字段对标记为 `exact_equivalent / equivalent_after_unit / derived_equivalent / broader_than / narrower_than / related_only / rejected / unknown_candidate`，只有 `approved_for_core=true` 的字段能进入 L1 本地核心层。审计结果必须同时输出 `canonical_field_matches`；当 promotion blocker 为 `approved_local_core_fact_missing` 时，live evidence 必须在 `source_field_candidates` 中保留各源实际字段名、报表类型、数值、canonical semantic 和单位，用于后续人工审核字段标签变体并升级 mapping version，不能作为自动合并依据。
5. 多期回填与增量维护
   - 默认从 `2024Q1` 到最新披露期，并满足至少 `8` 个季度 rolling floor；是否补 `2023A` 作为 TTM anchor 由配置决定
   - 当前 `FinancialStatementsShadowSyncService` 已支持 `report_periods` runtime override、`backfill/catchup` 模式、基于 source hash + core fact 存在性的 unchanged skip，并在 `ingestion_runs.metadata_json` 记录 checkpoint、resolved periods、运行配置、写入计数、tier maintenance 和 coverage gaps
   - 当前 `AkshareFinancialStatementsProvider` 已从单最新期扩展为按目标报告期返回多期 bundle；官方 provider 启用后会走 source manifest -> XBRL numeric facts -> core facts 的路径
   - 成功 backfill/catchup 后会执行 tier maintenance，将超出 hot window 的事实和指标幂等迁入 history
   - 生产写入 gate 必须同时检查 core readiness 和 long-form `numeric_fact_coverage`；核心字段 ready 但长表缺少 required canonical facts、存在单位冲突或没有 numeric coverage evidence 时，不得自动进入生产写入
   - weekly reconciliation 仍待后续 scheduler 阶段接入，职责是覆盖率、hash、缺失核心事实、解析失败补缺和 hot/history 一致性校验
6. 估值语义升级
   - 当前 `valuation_history` 已拆成 `pe_static / pe_ttm / pe_forward / pb_mrq / ps_static / ps_ttm / ps_forward`，并保留 `pe_ratio / pb_ratio / ps_ratio` 兼容字段
   - TTM 使用已披露的累计 YTD 财务事实构造，估值日只能使用 `data_available_date <= as_of_date` 的报告期，避免未来函数
   - 负净利润会生成负 PE 并可进入 PE 行业统计；分母为 `0` 的 PE 仍为 unavailable。PB/PS 继续要求正分母，避免负净资产或异常收入污染同行统计
   - 每个指标在 details 中记录 numerator、denominator、报告期和可得日；forward PE/PS 在 analyst forecast 未启用或覆盖不足时返回 explicit unavailable，不能复用 static/TTM denominator
7. readiness 与 rollout
   - 当前 storage/repository 已新增 `detect_financial_coverage_gaps` 与 `validate_financial_statement_readiness`，覆盖报告期、source file、核心事实、source/parser 分布、fallback 占比、parser failure、hot/cold tier coverage、stale hot rows 和 duplicate-tier conflicts
   - 当前已新增 `scripts/research_financial_statements_rollout_validation.py`，可串联小样本 sync 与仓库级 readiness；用 `--skip-sync` 可只读当前库状态，用 `--enable-official-source sse` 可仅在当前进程内临时启用 SSE 官方源和 endpoint candidates
   - 当前新增 isolated live validator：`python scripts/dev_validation/validate_sse_official_financial_json_live.py --instrument-id 600000.SH --exchange SSE --report-period 2023Q4`；也可通过 `--official-source cninfo --exchange SSE|SZSE|BSE` 验证 CNInfo data20。该命令使用 `/tmp` 临时 SQLite 库验证官方下载、parser、numeric facts 和 core facts，不写生产库
   - `2026-05-07` SSE isolated live validation 结果：
     - 单标的 `600000.SH / 2023Q4` 通过，下载 `3` 个官方 payload，写入 `3` 条 source manifest、`86` 条 numeric facts、`1` 条 core facts row，`revenue / net_income / total_assets / total_liabilities / equity / operating_cf` 均非空
     - 三标的 `600000.SH / 600519.SH / 600104.SH` 通过，耗时 `3.525s`，写入 `9` 个官方 payload、`309` 条 numeric facts、`3` 条 core facts row，核心字段覆盖率 `100%`
     - 五标的批次 `600000.SH / 600004.SH / 600009.SH / 600010.SH / 600011.SH` 通过，耗时 `5.915s`，写入 `15` 个官方 payload、`469` 条 numeric facts、`5` 条 core facts row
     - 五标的批次 `600015.SH / 600016.SH / 600018.SH / 600019.SH / 600028.SH` 通过，耗时 `7.898s`，写入 `15` 个官方 payload、`485` 条 numeric facts、`5` 条 core facts row
     - 同一进程一次性跑上述 `10` 个标的在 `180s` 外层 timeout 内未完成；全量准备必须改为小批量分片、checkpoint、超时重试和可续跑，不能把全市场放进单个长批次
     - 已新增 `scripts/dev_validation/validate_sse_official_financial_json_batches_live.py`，默认使用 `/tmp` 临时 SQLite 库，支持 `--batch-size / --batch-timeout-seconds / --request-timeout-seconds / --request-interval-seconds`，并记录每批失败标的；同样的 `10` 个标的按 `batch_size=5` 分两批后通过，总耗时 `12.005s`，失败标的 `0`，写入 `30` 条 source manifest、`954` 条 numeric facts 和 `30` 次 core fact 写入
     - batch runner 已支持 `--checkpoint-path`：`600000.SH / 600004.SH` 首次按 `batch_size=1` 写入 `6` 条 source manifest、`171` 条 numeric facts；第二次使用同一 checkpoint 时 `pending_instrument_count=0`，两个标的均被跳过，证明可续跑机制可用
     - `2026-05-08` 扩大到 `SSE` 前 `50` 个活跃标的、`2023Q4`、`batch_size=5`：`10/10` 批次通过，失败标的 `0`，总耗时 `61.319s`，吞吐约 `48.924` 标的/分钟，写入 `150` 条 source manifest、`4,862` 条 numeric facts；临时库复核为 `financial_core_facts_hot=50`，`revenue / net_income / total_assets / total_liabilities / equity / operating_cf` 六个核心字段均覆盖 `50/50`
     - `2026-05-08` 继续扩大到 `SSE` 前 `100` 个活跃标的、`2023Q4`、`batch_size=5`：`20/20` 批次通过，失败标的 `0`，总耗时 `121.849s`，吞吐约 `49.241` 标的/分钟，写入 `300` 条 source manifest、`9,696` 条 numeric facts；临时库复核为 `financial_core_facts_hot=100`，六个核心字段均覆盖 `100/100`；同 checkpoint 续跑时 `pending_instrument_count=0`、跳过 `100` 个标的
     - `2026-05-08` 完成 `SSE` 前 `300` 个活跃标的、`2023Q4`、`batch_size=5` dry-run：`60/60` 批次通过，失败标的 `0`，失败 instrument-period `0`，总耗时 `360.570s`，吞吐约 `49.921` 标的/分钟，写入 `900` 条 source manifest、`28,324` 条 numeric facts；临时库复核为 `financial_core_facts_hot=300`，六个核心字段均覆盖 `300/300`；同 checkpoint 续跑时 `pending_instrument_count=0`、跳过 `300` 个标的
     - `2026-05-08` 新增多报告期 dry-run：`600000.SH / 600004.SH` x `2023Q4 / 2024Q1` 通过，`4/4` instrument-period 成功，写入 `12` 条 source manifest、`333` 条 numeric facts，临时库 `financial_core_facts_hot=4` 且六个核心字段覆盖 `4/4`；同 checkpoint 续跑时 `pending_instrument_period_count=0`
     - 同日扩大到 `SSE` 前 `50` 个活跃标的 x `2023Q4 / 2024Q1`：`10/10` 批次通过，`100/100` instrument-period 成功，失败 `0`，总耗时 `117.297s`，吞吐约 `51.152` instrument-period/分钟，写入 `300` 条 source manifest、`9,453` 条 numeric facts、`300` 次 core fact 写入；临时库为 `financial_core_facts_hot=100` / `financial_facts=100`，六个核心字段均覆盖 `100/100`；同 checkpoint 续跑时 `pending_instrument_period_count=0`
     - 同日继续扩大到 `SSE` 前 `100` 个活跃标的 x `2023Q4 / 2024Q1`：`20/20` 批次通过，`200/200` instrument-period 成功，失败 `0`，总耗时 `232.305s`，吞吐约 `51.656` instrument-period/分钟，写入 `600` 条 source manifest、`18,858` 条 numeric facts、`600` 次 core fact 写入；临时库为 `financial_core_facts_hot=200` / `financial_facts=200`，六个核心字段均覆盖 `200/200`；同 checkpoint 续跑时 `pending_instrument_period_count=0`、跳过 `100` 个已完成标的
     - batch runner 默认输出已收敛为汇总和每批状态；完整嵌套 validator result 需显式传 `--include-batch-details`，避免 `50~300` 标的样本或 checkpoint 续跑时输出不可读
     - 当前新增生产导向 backfill 入口：`python scripts/research_financial_statements_backfill.py --exchange SSE --report-periods 2023Q4,2024Q1 ...`；默认 `write_enabled=false` 且写 `/tmp` 临时 SQLite，生产写入必须同时提供 `--write-enabled --storage-target production`，并通过 `--evidence-path` dry-run evidence gate 或显式 `--override-dry-run-gate` 留痕；gate 校验 `exchange / report_periods / source / source_mode / storage_target.kind / parser_version / request_policy / core facts / failed instrument-periods`。`2026-05-20` 已修正生产物理目标：财务生产读写使用 `data/financials.db`，运行结果的 `storage_target` 携带真实 `db_path`；`data/research.db` 中旧 `financial_*` 表仅为历史兼容/迁移来源，后续全量导入不得继续写入 `research.db`
     - `2026-05-08` 新入口 smoke：`600000.SH / 2023Q4` dry-run 通过，写入 `/tmp/quote_sse_prod_backfill_evidence_1_20260508.db` 并输出 `/tmp/quote_sse_prod_backfill_evidence_1_20260508.json` evidence，`1/1` instrument-period 成功，耗时 `1.442s`，写入 `3` 条 source manifest、`86` 条 numeric facts、`3` 次 core fact 写入；缺少 dry-run evidence 时的 `--write-enabled --storage-target production` 被 gate 拒绝并返回 `status=blocked / dry_run_evidence_missing`，未进入生产写入流程
     - 同日使用上述 evidence 完成 `600000.SH / 2023Q4` 生产写入 smoke：gate accepted，生产运行耗时 `1.371s`，`pre_readiness=not_ready` 转为 `post_readiness=ready`，写入 `3` 条 `sse/direct/parsed` source manifest、`86` 条 numeric facts，并更新核心事实；生产库复核为 `financial_source_files=4`、`financial_numeric_facts=86`、`financial_core_facts_hot=2`、`financial_facts=2`，其中 `600000.SH / 2023Q4` 的 `revenue / net_income / total_assets / total_liabilities / equity / operating_cf` 均非空；生产写入前备份为 `/tmp/research_pre_sse_prod_smoke_20260508.db`
     - 同日继续完成 `600000.SH / 600004.SH` x `2023Q4 / 2024Q1` 生产写入 smoke：gate accepted，生产运行耗时 `6.443s`，`4/4` instrument-period 全部 ready，生产库复核为 `financial_source_files=13`、`financial_numeric_facts=333`、`financial_core_facts_hot=4`、`financial_facts=4`，四个 instrument-period 的 6 个核心字段均非空；生产写入前备份为 `/tmp/research_pre_sse_prod_2x2_20260508.db`
     - 同日扩大到前 `10` 个 SSE 活跃标的 x `2023Q4 / 2024Q1`：dry-run evidence `20/20` instrument-period 成功，耗时 `28.354s`，写入临时库 `60` 条 source manifest、`1,778` 条 numeric facts；随后 production gate accepted，生产写入耗时 `33.673s`，`post_readiness=ready`、fallback share `0.0`、parser failure `0`，生产库复核为 `financial_source_files=61`、`financial_numeric_facts=1,778`、`financial_core_facts_hot=20`、`financial_facts=20`；同 checkpoint 复跑时 `pending_instrument_period_count=0`、跳过 `20` 个 instrument-period、写入计数为 `0`；生产写入前备份为 `/tmp/research_pre_sse_prod_10x2_20260508.db`
     - `2026-05-09` CNInfo data20 isolated live validation：`000001.SZ / 2025Q4` 通过，写入 `3` 个 payload、`24` 个 numeric facts；`920833.BJ / 2025Q4` 通过，写入 `3` 个 payload、`19` 个 numeric facts；SZSE 前 5 与 BSE 前 5 的 bounded dry-run 均为 `15` 个 payload、失败 instrument-period `0`，分别写入 `102` 与 `96` 个 numeric facts；同日直接探测确认 `600000.SH` 可通过 CNInfo data20 返回三张表结构化 JSON
     - `2026-05-10` SSE 同样本 source profile 对比：`scripts/dev_validation/compare_official_financial_source_profiles_live.py --exchange SSE --sources sse,cninfo --limit 5 --report-period 2023Q4` 写 `/tmp` 隔离库；字段语义保护后，`sse_commonquery` 耗时 `6.563s`、吞吐 `45.714` instrument-period/min、写入 `444` 个 numeric facts，`cninfo_data20` 耗时 `11.635s`、吞吐 `25.783` instrument-period/min、写入 `102` 个 numeric facts；SSE 失败 instrument-period `0`、核心字段覆盖 `30/30`，CNInfo 因 `equity` 语义不兼容被判为 degraded，核心字段覆盖 `25/30`。CNInfo 的 `所有者权益` 仍保留在 raw numeric facts，不再写入标准归母 `equity`；结论是 SSE 默认继续使用 `sse_commonquery`，CNInfo data20 作为显式 cross-check/fallback profile
     - `2026-05-12` 标准长表字段元数据落地：`financial_numeric_facts(_hot/_history)` 新增 `canonical_fact_name / canonical_statement_family / canonical_semantic / canonical_unit / canonical_version`，parser 在 SSE commonQuery、CNInfo data20 和 XBRL numeric facts 中统一写入 `standard_financial_numeric_facts.v1`。重新验证 `000001.SZ / 2025Q4` 与 `920833.BJ / 2025Q4` 后，临时库中 `所有者权益 -> equity_total`、`归属母公司净利润 -> net_income_parent`、`营业收入/营业总收入 -> revenue`，完整 numeric facts 可按标准字段查询；核心 `equity` 仍因缺少归母权益或少数股东权益组件而不自动填充
     - `2026-05-13` 统一字段目录继续收口：`S2010_0700 / 实收资本（或股本） / TOTAL_SHARE / 股本` 统一映射为 `share_capital_amount`、单位 `CNY`，`shares_outstanding` 仅用于明确股数口径字段，避免把资产负债表股本金额污染为股数；AkShare fallback bundle 已转换为 `financial_numeric_facts(_hot/_history)` long-form facts，保留 source-file lineage、statement family、parser version、source mode、raw value 和 canonical metadata，未知数值字段保留为 unmapped facts 且不默认填单位；新增 `scripts/dev_validation/audit_financial_numeric_fact_coverage.py`，用于有界审计 native 字段数量、canonical 覆盖、缺失必需字段、单位冲突和核心事实语义 warning
     - `2026-05-13` 生产前 gate 加严：`scripts/research_financial_statements_backfill.py` 的 dry-run evidence 会自动附带 `numeric_fact_coverage`；write-enabled 生产写入若发现 evidence 缺失 numeric coverage、coverage 非 `passed`、缺少 required canonical facts 或存在 canonical unit conflict，会阻断写入，除非显式提供 operator override。默认 required canonical facts 为 `revenue / net_income_parent / total_assets / total_liabilities / equity_parent / operating_cf`，可用 `--required-canonical-facts` 显式调整；`scripts/dev_validation/compare_official_financial_source_profiles_live.py` 也会在 same-sample profile comparison 中输出 long-form coverage summary 和 coverage warning
     - `2026-05-13` 随机小样本 dry-run：`SSE 4` 家、`SZSE 3` 家、`BSE 3` 家，报告期 `2024Q2 / 2025Q4`，全部写 `/tmp` 临时 SQLite。`SSE/sse_commonquery` 在 `2024Q2` required canonical facts 齐全，但 `2025Q4` 四个 instrument-period 未取到 numeric/core facts，整体 `degraded`；`SZSE/BSE + cninfo_data20` 均能稳定写入 long-form numeric facts 且无单位冲突，但所有 instrument-period 都缺 `equity_parent`，只拿到 `equity_total` 并触发 `equity_total_vs_parent_ambiguous`。该结果证明新 gate 能拦住字段语义不完整的结构化数据，不应直接生产推广 CNInfo profile
     - `2026-05-13` coverage gap 分类已接入 `scripts/dev_validation/audit_financial_numeric_fact_coverage.py` 和 backfill compact evidence：每个 instrument-period 输出 `gap_reasons`、`required_fact_gaps`，summary 输出 `gap_reason_counts`。当前分类包括 `missing_numeric_rows`、`missing_required_canonical_fact`、`semantic_gap`、`derivation_component_gap`、`alias_gap_candidate`、`canonical_unit_conflict`、`unmapped_nonrequired_fields`。该分类只解释原因，不放松生产 gate；`equity_total` 仍不能直接替代 `equity_parent`
     - `2026-05-13` SSE 年报期无数值已用 `scripts/dev_validation/probe_sse_official_period_availability.py` 定位：`600000.SH` 的 `2023Q4` 和 `2024Q2` 通过 `COMMON_MAP_INCOMESTATEMENT_C / COMMON_MAP_BALANCESHEET_C / COMMON_MAP_CASHFLOW_C` 返回 structured numeric rows，但 `2024Q4 / 2025Q4` 为 HTTP 200 空结果；同脚本探测官方页面使用的 `COMMON_MAP_MAXYEAR_L`，返回 semi-annual `1000 -> 2024`、annual `5000 -> 2023`。`601033.SH` annual max year 也为 `2023`。当前结论是：SSE commonQuery 端点未整体停更，但 annual `REPORT_PERIOD_ID=5000` 的官方 XBRL structured data 当前最大年份仍停在 `2023`；`2024Q4 / 2025Q4` 在 commonQuery profile 下应归类为 official period availability gap，而不是 parser 字段映射问题
   - 当前已新增 `/api/v1/research/financial-statements/readiness`，并扩展 `/api/v1/research/valuation/readiness` 返回 metric coverage、financial readiness 摘要和 rollout blockers
   - valuation rollout 必须依赖 financial readiness 和 industry readiness，不再只看估值历史表是否有行

工程约束：

- 不在 provider、sync service、API 中硬编码官方 URL、报告期、字段 alias、限流、重试或估值常数
- 不让 API 请求时访问外部财务源；读取链路只查本地存储
- 存储访问必须收口到 storage/repository 层，避免未来从 SQLite 拆分到 `data/financials.db`、DuckDB 或 PostgreSQL 时修改业务逻辑
- hot/cold tier 是存储优化，不是业务语义；常用 API 默认查 hot tier，长历史查询通过 repository union hot/history，调用方不直接拼表
- 现有 `financial_statements_raw / financial_facts / financial_indicator_snapshots` API 响应尽量保持兼容，新表和新字段先通过 optional fields / readiness 暴露

当前 blocker：

- `SSE/sse_commonquery` 已找到官方 `commonQuery.do` structured JSON 路线，并已补上 `S2020_xxxx / S2010_xxxx / S2030_xxxx` 到 numeric facts 与核心事实的配置化 alias/parser；`300` 标的单报告期 dry-run 和 `100x2` 多报告期 dry-run 已通过，但在生产 backfill gate、字段稳定性和 readiness 通过前，仍不启用生产官方源
- `SSE/SZSE/BSE + cninfo_data20` 已找到巨潮结构化 JSON 路线；SZSE/BSE 已通过 1+5 标的小样本临时库验证，并已把真实返回字段统一落到 `canonical_*` 标准长表字段。下一步 blocker 是扩大 SZSE/BSE bounded dry-run、确认 CNInfo 是否存在可直接映射归母权益的行标签或可稳定提供少数股东权益组件，并把 CNInfo profile 纳入生产写入前的字段稳定性和 readiness gate
- `BSE` 官网 metadata/PDF 仍未发现可解析 XBRL/XML/ZIP artifact；该问题保留为审计源探索，不再作为 BSE 结构化财务数据接入的前置 blocker
- 当前 probe 只做有界只读验证，不写入生产表；官方源启用前必须记录 payload hash、parser diagnostics、source profile、parser profile、request policy、字段稳定性、失败模式、fallback share 和覆盖率
- 限流/反爬：当前没有确认 SSE、CNInfo data20 或 BSE 官网对这些公开 endpoint 的稳定公开配额；生产前必须按保守策略执行 `request_interval`、短 timeout、retry/backoff、低并发、batch timeout、checkpoint 续跑和失败隔离。AkShare fallback 继续按结构波动和反爬风险更高的第三方源管理，不允许静默覆盖官方事实

申万行业自身后续只保留两类工作：

- 主线维护：以全量本地化为核心，固定执行 `taxonomy 全量刷新 -> 三级成分股集合全量刷新 -> 当前 membership 全量重建/重映射 -> targeted gap fill -> readiness`
- 审计旁路：继续保留 `stock_industry_clf_hist_sw()`、official-code mapping cache、backlog/override review，但它们只服务历史分类审计，不再参与当前归属和相对估值同行分组
- valuation readiness：通过 `/api/v1/research/valuation/readiness` 同时观察估值历史覆盖、模块 gate 与相对估值行业前置条件
- valuation input sync runner：通过 `python scripts/research_valuation_input_sync.py --exchanges SSE,SZSE,BSE --sync-mode incremental` 做日更输入同步；全量回填用 `--sync-mode full --start-date 1990-01-01`，必须按交易所/limit 分批放量
- valuation rollout validation runner：通过 `python scripts/research_valuation_rollout_validation.py --sync-inputs --exchanges SSE,SZSE` 将 `valuation_input_sync -> valuation_history_rebuild -> readiness` 串成可重复执行的仓库级复核命令；支持 `--window-mode trading_days|last_12_quarters` 与 `--write-policy missing_only|overwrite`
- `industry` / `valuation` 两条 rollout validation runner 在只读复核场景下已切换为轻量初始化路径，不再默认初始化 `DataSourceFactory`、`pytdx`、`baostock`、`akshare`、`yfinance`
- technical readiness：通过 `/api/v1/research/technical/readiness` 观察最新技术快照缓存覆盖、口径、状态与信号分布

申万行业正式工程主线定义如下：

1. `industry_taxonomy`
   - 全量下载申万一、二、三级行业字典
   - 本地保存代码、名称、层级、父子关系、版本、来源和更新时间
2. `industry_component_sets`
   - 全量下载全部申万叶子行业成分股集合
   - 叶子节点定义为“全部三级节点 + 无三级子节点的二级 leaf 节点”
   - 本地保存 `leaf_industry -> symbols` 的最新集合快照，作为 current membership 的主数据来源
3. `industry_memberships`
   - 基于本地叶子行业成分股集合反向生成 `instrument -> sw_l1/sw_l2/sw_l3`
   - 当行业归属变更或行业成分变动时，通过下一轮全量同步直接重写当前映射，不做运行时远程推断
4. 定向补缺与验收
   - 对全量同步后的残余缺口，再执行 `industry_standard_gap_fill`
   - 最终统一通过 readiness / coverage gap 读链路验收，而不是靠单标的远程试探

#### Mainline 1：行业严格标准化

目标：

- 将当前行业参考层升级为 **严格申万一/二/三级标准层**
- 建立可版本化、可全量刷新、本地优先读取的 `industry_taxonomy`
- 建立 `industry_component_sets`，将申万叶子行业成分股集合作为本地缓存资产维护
- 建立股票到申万一/二/三级的 authoritative `industry_memberships`
- 建立行业变更同步任务，默认按每周运行，必要时可提升到日更

约束：

- 免费源返回的行业/板块字段，在未完成标准映射前只能作为 reference
- relative valuation 尚未开始前，必须先完成可用的申万二级映射

当前状态：

- 严格申万 taxonomy / membership 的代码链路已经落地
- 申万叶子行业成分股集合已落地到 `industry_component_sets`
- legacy 逐页 membership 抓取的真实上游曾确认为 `legulegu.com`，当前主线已改为优先使用 `sw_index_third_cons`
- research 域 `AkShare` provider 现已具备显式的 `direct / proxy_patch` mode 语义
- `industry_standard_sync` 现可在 direct 失败时保留 taxonomy 进度并继续尝试下一个候选源
- 提权真实环境复验已经确认：
  - `akshare:direct` taxonomy 可写入 `413` 行，但 membership 会遭遇 `legulegu.com` `429`
  - `akshare:proxy_patch` loader 安装成功，且对 `legulegu.com` 的单请求返回 `200`
- 后续确认：`stock_industry_clf_hist_sw()` 只适合保留为历史分类审计源，当前股票归属应直接由申万叶子行业成分股集合反向生成

#### Mainline 2：行业标准层加固与上线解锁

目标：

- 将 strict Shenwan 维护链路收敛成正式的全量本地化流程，而不是继续围绕代表样本和远程映射 probe 打转
- 固化“全量刷新 -> 本地重映射 -> 定向补缺 -> readiness 验收”的执行顺序
- 明确 `industry_standard_sync` 是全量本地主数据更新任务，`industry_standard_gap_fill` 只是补缺任务
- 仅在 `config/03_data.json` 的 `akshare.proxy_patch.hook_domains` 已覆盖 `legulegu.com` 且真实环境复验通过后，才将 `akshare:proxy_patch` 视为有效 fallback

优先原因：

- `financial_statements` 已通过代表样本校验，但 `industry_standard` 当前仍无法作为生产 authoritative membership 主链
- relative valuation 之前必须先解除行业标准层阻塞

当前完成判定：

- `industry_standard_sync` 可在不传 `limit_per_exchange` 时按交易所执行全量同步
- `industry_standard_sync` 在代表性 `SSE / SZSE` 样本上返回非空 authoritative membership
- `taxonomy_nodes_written` 与数据库真实写入一致，不再出现“taxonomy 已写入但结果口径为 0”的状态失真
- direct 失败时，若允许 `proxy_patch`，结果中能够继续尝试下一个候选而不是直接终止
- `proxy_patch` 对 `legulegu.com` 的单请求在真实环境中确认为 `200`
- 当前仍未完全满足的是“全市场全量 authoritative membership 覆盖率收口”；其主要矛盾已从“代码缺失”转为“上游限流、全量导入时延、以及缺口补齐效率”

#### Mainline 3：申万叶子行业成分股当前归属主源

目标：

- 接入 `AkShare sw_index_first_info / sw_index_second_info / sw_index_third_info` 构建申万一、二、三级 taxonomy
- 接入叶子行业成分股集合，按 `leaf_industry -> symbols` 反向生成当前 `instrument -> sw_l1/sw_l2/sw_l3` membership
- 将 `stock_industry_clf_hist_sw()` 仅保留为官方历史分类审计层，不再用其六位 `industry_code` 反推当前三级行业
- 对未出现在叶子行业成分股集合中的标的，先执行配置化行业名称补源，再允许 bounded legacy fallback 补缺

当前实现状态：

- 已新增 `AkShareOfficialShenwanHistoryProvider`
- 已新增 `OfficialIndustryHistoryProviderRegistry`
- 已新增 `industry_official_classifications` 原始层表，用于保留官方六位码、官方起始日期、更新时间、映射状态和置信度
- `industry_standard_sync` 现优先执行：
  - taxonomy：`sw_index_first_info / second_info / third_info`
  - current membership：叶子行业成分股集合反向生成股票当前申万叶子归属，并通过 taxonomy parent links 派生申万二级、一级；若叶子节点本身是二级 leaf，则 `sw_l3_code` 保持为空
  - official history audit：`stock_industry_clf_hist_sw()`
  - official code mapping：仅用于 `industry_official_classifications` 审计字段和 backlog 观察，不再阻塞当前 membership
  - name supplement：对 component miss 标的先走 `manual:configured` 配置化人工补源，再走 `sina:direct` 结构化申万相关资料补源，最后走 `eastmoney:direct` 个股行业名补源，并按本地申万 taxonomy 行业名匹配
  - fallback：legacy `legulegu` membership 抓取
- 已补齐单测覆盖：
  - official raw classification 落表
  - 叶子成分股主链 membership 正常写入
  - official unmapped 但叶子成分存在时仍写入 current membership
  - 成分股缺失时先尝试东财行业名补源；三级名称可直接映射，二级名称仅在本地 taxonomy 中没有三级子节点时可作为 leaf-level membership 写入
  - 成分股缺失时的 legacy fallback
  - degraded / proxy fallback 兼容行为

`2026-04-24` 新主线决策：

- 上述 AkShare/Legulegu 叶子成分股主线保留为已实现能力，但不再作为下一阶段 strict Shenwan 分类主事实
- 下一阶段新增 `swsresearch_classification_direct`：
  - 主文件：`StockClassifyUse_stock.xls`
  - 代码表：`SwClassCode_2021.xls`
  - 主标识：申万官方六位行业分类代码
  - alias：`801xxx.SI / 85xxxx.SI` 申万行业指数代码，仅用于指数行情、指数成分和行业指数分析数据 join
- 库表结构建议随迁移调整：
  - 增加 source-file manifest，记录 URL、`ETag`、`Last-Modified`、sha256、行数、最大 `更新日期`、parser version 和 ingestion run id
  - 增加或改造 full classification history 存储，保留官方股票分类历史全量行
  - `industry_taxonomy.industry_code`、`industry_memberships.industry_code`、`sw_l1_code / sw_l2_code / sw_l3_code` 在新结构中均指官方分类代码
  - 另增 `sw_l1_index_code / sw_l2_index_code / sw_l3_index_code` 或 `aliases_json` 保存指数代码
  - `industry_official_code_mappings` 不再参与 current membership，可归档为旧 index-code 映射审计资产
  - `industry_component_sets` 不再参与分类 membership，可保留为指数成分诊断或后续 index 数据资产
- 现有 strict Shenwan 行业表数据可按迁移脚本整组清空并重建；清理范围仅限行业标准层表，不影响财务、估值、技术、股东等其他 research 域
- 每日同步机制应优先走条件 GET；申万站点 `HEAD` 不稳定，`GET` 返回的 `ETag / Last-Modified` 与文件 sha256、表内最大 `更新日期` 一起作为是否重写落库的判断依据
- 若官方直连不可用，`AkShare stock_industry_clf_hist_sw()` 可作为股票分类历史 fallback；但代码表仍应来自官方直连或上次有效缓存，不能用覆盖较低的 index taxonomy 页面静默重建完整分类标准
- 行业估值/指数表现另拆 `swsresearch_index_analysis_direct`，调用申万 `index_analysis_report` API，按指数代码和日期保存收盘指数、成交量、涨跌幅、换手率、PE、PB、均价、成交额占比、流通市值、平均流通市值、股息率等字段；该表不参与股票分类判定

`2026-04-25` 官方分类主源落地状态：

- 已新增 `SWSResearchShenwanClassificationProvider`，直连解析 `StockClassifyUse_stock.xls` 与 `SwClassCode_2021.xls`
- 已新增 `industry_source_files` 与 `industry_classification_history`；`industry_taxonomy` / `industry_memberships` 已支持官方六位分类代码作为主键，并保留 `sw_l1_index_code / sw_l2_index_code / sw_l3_index_code` alias 字段
- `industry_standard_sync` 已切到显式配置开关 `classification_primary_enabled=true` 后走 `swsresearch -> akshare fallback`；当官方文件未变化时可根据 manifest/sha256 短路，当官方主源不可用时只允许 AkShare 结合缓存的官方六位代码表补 membership
- readiness 已在官方主源模式下重新要求 active `SSE/SZSE/BSE` authoritative coverage，不再把 BSE 作为行业标准层 optional-empty
- 新增受控重建脚本：`python scripts/research_ops/official_shenwan_rebuild.py --exchanges SSE,SZSE,BSE --drop-existing --force-refresh`
- 新增现场验证脚本：`python scripts/dev_validation/validate_swsresearch_shenwan_classification_live.py --exchanges SSE,SZSE,BSE --use-db-manifest`
- 后续指数分析已拆为独立 OpenSpec 变更：`add-swsresearch-index-analysis-direct`，避免把 index analysis 误并入分类同步

已确认事实：

- `stock_industry_clf_hist_sw()` 在提权真实环境中可拉取约 `12717` 行股票分类历史
- 其原始字段当前为 `股票代码 / 计入日期 / 行业代码 / 更新日期`
- 样本 `600519` 与 `000001` 均存在最新分类记录
- `stock_industry_clf_hist_sw()` 返回的六位行业分类代码不能稳定映射到旧 `sw_index_third_info` 指数代码体系；`2026-04-24` 后的修正方向是不再把这种映射作为 current membership 前置，而是直接以官方六位分类代码为 strict Shenwan 主键
- 官方分类文件实测覆盖当前 active `SSE/SZSE/BSE` universe；此前 BSE optional-empty 对 `industry_standard` 是缺少稳定上游时的保护策略，官方主源落地后应重新收紧该 readiness 口径
- `2026-04-23` 对当前剩余钢铁缺口 live 验证：东财 `push2.eastmoney.com/api/qt/stock/get` 的 `f127` 字段对 `600117 / 600399 / 600507 / 603995 / 000708 / 000825 / 002075 / 002318 / 002443 / 002478 / 300881 / 301160` 均返回 `特钢Ⅱ`
- 因东财返回的是行业名而不是申万代码，补源只允许“本地 taxonomy 名称匹配”；不会把东财结果写入 `industry_component_sets`，也不会凭空构造不存在的申万三级节点
- 如果行业名命中二级节点且该二级节点仍有三级子节点，则不自动降级写入，避免把粗粒度行业名伪装成三级归属
- `2026-04-23` 已用标准 `industry_standard_gap_fill` 入口验证并落库：SSE/SZSE 缺口从 `12` 降为 `0`，12 只残余钢铁标的均以 `eastmoney:direct / 申万行业名称补源` 写入 `801045.SI / 特钢Ⅱ`；该节点在当前本地 taxonomy 中为二级 leaf，因此 `sw_l3_code` 保持为空，relative valuation 所需 `sw_l2_code` 已可用
- `2026-04-23` 又为个别已确认但主源未覆盖的股票补上了统一的 `manual:configured` 补源入口；配置位置是 `industry.standard.name_supplement.manual_entries`。当前首个落地样本为 `688781.SH -> 面板(850831.SI)`，它仍走既有 `name_supplement` 主链，不单独旁路写库
- `2026-04-23` 对新浪 / 东财 / 雪球的补源候选重新比对后，当前结论是：
  - 新浪 `相关资料` 页最适合作为后续结构化补源候选，因为同页会列出 `电子 / 光学光电子 / 面板` 及对应代码，信息粒度能直接覆盖 `L1/L2/L3`
  - 东财当前已接入的 `push2` 个股接口更适合作为“行业名称补源”，因为它稳定给出单个行业名（如 `特钢Ⅱ`），但并不保证返回完整 `L1/L2/L3`
  - 雪球更适合作为人工复核参考，不适合作为自动补源主链；页面更多是聚合展示或用户内容，结构稳定性和反爬风险都弱于新浪/东财
- `2026-04-24` 已将新浪补源从候选设计推进为默认结构化补源链路：
  - provider：`sina:direct`
  - 页面：`vip.stock.finance.sina.com.cn/corp/go.php/vCI_CorpXiangGuan/stockid/{stockid}.phtml`
  - 解析策略：只采纳当前有效的申万行业相关行，优先 `85xxxx` 三级行业候选，再把 `801xxx` 候选交给本地 taxonomy 做 level-2 leaf-only 校验
  - 默认顺序：`manual:configured -> sina:direct -> eastmoney:direct`
  - 仍不把新浪结果写入 `industry_component_sets`，只在 component miss 时写 `industry_memberships`，并保留 supplement provenance
  - 雪球暂不进入自动补源链路；它更适合作为人工复核参考，除非后续有无需 cookie/token 且结构稳定的官方接口可验证
- `2026-04-24` 同步优化了定向缺口补齐路径：当 `industry_standard_gap_fill` 已经枚举到具体 missing instrument ids 时，`industry_standard_sync` 会优先复用本地 `industry_taxonomy` 缓存，并跳过 audit-only 的 official classification 拉取；这避免单标的补缺仍被远端 taxonomy/official audit 阶段拖慢。full sync 和 `industry_official_mapping_refresh` 仍负责完整刷新与审计资产维护
- `2026-04-23` 进一步排查 live 维护阻塞时确认：`DataManager.run_industry_standard_sync()` 原先通过 `from research import IndustryStandardSyncService` 走包根导入，会把整个 `research` 包及其无关服务一起拉起；在当前环境下，这会放大 strict Shenwan CLI/runner 的初始化副作用。现已改为按模块精确导入，并在 `DatabaseOperations` 中新增 `get_research_target_instruments_by_exchange(_sync)` 轻量研究标的读取入口，`IndustryStandardSyncService` 优先复用这条路径，不再强依赖通用 `get_instruments_by_exchange()` 异步大查询
- `2026-04-25` 申万官方分类主源已从手工运行推进为系统功能：
  - 日更任务：`scheduler.tasks.industry_standard_sync` / `config/05_scheduler.json -> industry_standard_sync`，默认使用 source manifest/sha256 短路，Telegram 命令为 `/industry_standard_sync [force]`
  - 全量重建任务：`scheduler.tasks.industry_standard_rebuild_official` / `DataManager.rebuild_official_industry_standard()`，清理范围仅限 strict Shenwan 行业标准层，Telegram 命令为 `/industry_standard_rebuild [force] [drop_source_files]`
  - 正式运维 CLI 归入 `scripts/research_ops/`；开发验证 CLI 归入 `scripts/dev_validation/`；旧脚本路径已清理，不再保留兼容包装
- `2026-04-25` 已开始落地独立的申万指数分析直连源：
  - provider：`SWSResearchIndexAnalysisProvider`
  - 存储表：`industry_index_analysis_daily`，主键为 `(taxonomy_system, taxonomy_version, sw_index_code, trade_date)`
  - 系统入口：`DataManager.run_industry_index_analysis_sync()` 与调度任务 `industry_index_analysis_sync`，当前已启用；Telegram 命令为 `/industry_index_analysis_sync [limit=N]`
  - `2026-04-26` 已执行最新日频全维度同步：`525` 行、`525` 个指数代码，最新交易日 `2026-04-24`，分布为 `市场表征 9 / 一级行业 31 / 二级行业 131 / 三级行业 337 / 风格指数 17`
  - 现场验证脚本：`scripts/dev_validation/validate_swsresearch_index_analysis_live.py`
  - 已确认官方前端当前可用日频接口为 `day_week_month_report`，返回 `2026-04-24` 最新行；provider 已接收 latest/date-range 参数、重试和超时配置，并对返回结果做本地日期过滤；但历史日期参数在现场探测中会被上游忽略，因此不把真实历史回补能力标记为完成
  - 该链路只写指数分析指标，不写 `industry_memberships`，也不从指数代码反推股票分类
- `2026-04-26` 已补充 AkShare 历史申万指数分析回补入口：
  - provider：`AkshareSWSResearchIndexAnalysisProvider`，复用 AkShare `index_analysis_daily_sw` 的 SWS `index_analysis_report` 上游语义，并由项目代码控制 timeout/retry，避免 AkShare 内部无 timeout 请求卡住系统任务
  - 系统入口：`DataManager.run_industry_index_analysis_backfill()`；正式 CLI 为 `scripts/research_ops/industry_index_analysis_backfill.py`；CLI 默认按“月份 + index_type”分块提交，避免一次性全区间请求超时且支持失败后续跑；遇到单月分页 404、上游限流或局部缺口时，可用 `--chunk-frequency day` 按日补缺；Telegram 命令为 `/industry_index_analysis_backfill start=YYYY-MM-DD end=YYYY-MM-DD [limit=N]`
  - 调度配置项：`industry_index_analysis_backfill` 已加入 `config/05_scheduler.json`，默认 disabled，避免误触发大范围历史抓取
  - 字段单位：`bargain_volume` 为成交量，单位亿股；`markup / turnover_rate / bargain_sum_rate / dividend_yield` 保持百分数值而非小数；`negotiable_share_sum / average_negotiable_share_sum` 单位为亿元；`pe / pb` 为倍数
  - 历史覆盖：实测同日值与直连最新源一致；三级行业历史可返回但历史行数随申万分类版本变化，回补结果必须查看 coverage/missing_metrics，不能用当前 taxonomy 行数判断历史是否失败
  - `2026-04-26` 已通过正式 CLI 执行单日小样本回补验证：`2024-10-25`、每个 index_type 限制 2 行，写入 `10` 行，coverage 显示五类指数均命中 1 个交易日；当前 `industry_index_analysis_daily` 合计 `535` 行
- `2026-04-27` 已完成申万指数分析全量回补落库复核：
  - 覆盖范围：`2010-01-04` 至 `2026-04-24`，共 `3,937` 个交易日、`529` 个指数代码、`1,104,548` 行
  - 分类型覆盖：市场表征 `40,224` 行 / `3,937` 日，一级行业 `100,295` 行 / `3,937` 日，二级行业 `305,800` 行 / `3,937` 日，三级行业 `590,998` 行 / `3,936` 日，风格指数 `67,231` 行 / `3,937` 日
  - 已修复 `2023-12` 三级行业和风格指数的月度分页缺口，采用按日分块补入 `7,413` 行
  - 唯一剩余日期缺口为 `2012-03-05` 的三级行业；按日补跑上游返回 `0` 行，同日市场表征、一级行业、二级行业和风格指数均有数据，因此记录为上游历史缺失，不视为本地落库失败
  - 主键重复检查为 `0`；旧 `bargain_amount` 字段已完成迁移，当前只保留 `bargain_volume`
- `2026-04-27` 已补齐 component-set API 的本地派生 fallback：
  - `industry_component_sets` 物理缓存为空或未命中时，`GET /api/v1/research/industry/component-sets` 会从 authoritative `industry_memberships` 按行业代码派生股票组合
  - 当前库中官方 membership 可派生 `337` 个叶子行业股票组合；这条派生链路不访问外部源，也不改变 `industry_memberships`
- 这次修复后的验证结论是：strict Shenwan live sync 已能重新创建新的 `ingestion_runs` 记录并正式进入远程 component-set 抓取阶段；此前“sync 启动前卡住、连 run row 都写不进去”的问题已解除，但仍需要完成一次长时 `--force-component-refresh` 全量回刷，把当前库中的 `336` 条三级 component set 升级为真正的 leaf component set 缓存
- `2026-04-23` 继续收口时又确认了一条数据一致性问题：当目标标的在本轮 `component -> name supplement -> fallback` 全链路都未命中时，旧的 authoritative `industry_memberships` 会被保留，导致 readiness 被历史脏数据虚高。现已补上统一 stale cleanup：strict Shenwan sync 会在目标标的本轮最终未命中时删除旧的 current membership。这样下次更新不会再把旧数据误当成当前归属；如果上游仍无法给出当前分类，该标的会正确暴露为 coverage gap，而不是继续显示陈旧行业
- 估值与统计口径现明确为：调用方可选 `L1 / L2 / L3` 作为同行分组层级；未显式指定时默认使用 `L2`。`L3` 统计只覆盖 `sw_l3_code` 非空样本，不能把二级 leaf 强行伪装成三级

当前下一步：

- `industry_standard_gap_fill`、Sina/Eastmoney/manual supplement、`industry_component_sets` 与 `official-mapping-backlog` 保留为异常诊断或旧链路资产，不再作为官方分类主源下 membership rollout 的前置条件
- 申万行业分类与指数分析主线已完成；后续仅保留日更监控、异常补缺和 readiness/coverage 查询体验优化，继续推进时应转入 Phase 1 其它默认 disabled 模块的 rollout 复核；其中 shareholders 先使用 `scripts/research_shareholder_rollout_validation.py` 做可重复样本验证
- API 可访问性要求：服务侧会归一化重复斜杠，`//api/v1/...` 会按 `/api/v1/...` 路由处理；线上进程需要重启后才能加载该中间件
- 统一 research API 阶段继续覆盖其它本地 research 表，保持“落库即必须可 API 查询或明确 gated/disabled”的完成标准

`2026-04-19` live validation 结论：

- direct 模式下：
  - `taxonomy_nodes_written = 413`
  - `total_official_classifications_written = 2`
  - `total_memberships_written = 1`
  - `600519.SH` 已通过 official source 成功映射到 authoritative `sw_l2 = 801125.SI`
  - `000001.SZ` 的 official code `480301` 已写入 `industry_official_classifications`，但当前仍为 `unmapped`
  - 对 `000001.SZ` 的 legacy fallback direct 路径仍会遭遇 `legulegu.com` `429`
- `proxy_patch` 模式下：
  - 全链路 representative validation 在 `90s` 窗口内未完成
  - 单标的 fallback membership probe 在 `90s` 窗口内同样未完成
- `sw_index_third_cons` 已作为当前叶子行业成分股主源；`index_component_sw` 仅作为兼容 fallback。早前验证中 `index_component_sw` 对相当数量的三级节点会返回异常列结构，因此不能再作为主路径。

因此 `2026-04-21` 当时的工程结论曾调整为：

- strict Shenwan 当前归属主线应以叶子行业成分股集合为准
- official stock-history path 当时仅保留历史分类审计价值，不作为 relative valuation 的同行分组依据
- 解除 `valuation` production rollout 阻塞的关键变为：全市场 authoritative `industry_memberships` 覆盖率，而不是六位官方历史码 mapping backlog

`2026-04-24` 再次修正：上述结论是建立在“必须把官方六位码映射到 `801xxx.SI / 85xxxx.SI` index taxonomy”这一旧前提上。现在的推进方向是把官方六位行业分类代码提升为 strict Shenwan 主键，industry index code 退为 alias；因此 official stock-history path 将重新成为 current membership 主链，而不是仅作为审计层。

#### Mainline 4：官方映射缓存资产化（审计层，非当前归属主线）

目标：

- 将 `official_code -> taxonomy node` 从运行时临时对象升级为可持久化映射缓存
- 让独立的 `industry_official_mapping_refresh` / 审计读链路优先读取缓存，仅在缓存缺失或显式允许时才 live rebuild
- 保留 unmapped official codes 的 ranked candidate diagnostics，便于后续补全

当前口径：

- 本节能力已经降级为 official history audit 支撑能力
- 它可以帮助解释历史官方六位码、排查异常、沉淀后续研究线索
- 它不能再作为生成 current `industry_memberships` 的路径
- 它不能再作为 relative valuation 的同行分组依据
- 它不再作为 strict Shenwan readiness 或 valuation rollout 的前置 blocker

当前实现状态：

- 已新增 `industry_official_code_mappings`
- 已新增 `industry_component_sets`
- 已扩展映射对象，`unmapped` 行也会保留 `best_taxonomy_industry_code`
- 已扩展 mapping payload，持久化 `candidate_rankings` top-N 候选诊断
- `industry_standard_sync` 已不再复用或重建 official-code 映射；当前 membership 只依赖叶子行业成分股集合和 bounded legacy fallback
- official mapping live rebuild 现已优先复用 `industry_component_sets`；仅在组件集缓存缺失或过期时才调用 live component source，且 provider 优先使用 `sw_index_third_cons`，再 fallback 到 `index_component_sw`
- 已新增独立 refresh 入口 `industry_official_mapping_refresh`
- `DataManager` 与 scheduler 已可单独触发 official mapping cache refresh；该任务仅用于历史分类审计，不是 current membership 同步前置
- 已新增审计读链路：
  - `/api/v1/research/industry/official-mappings`
  - `/api/v1/research/industry/official-mappings/{official_industry_code}`
  - `/api/v1/research/industry/official-mapping-backlog`
  - `/api/v1/research/industry/official-mapping-override-candidates`
  - `/api/v1/research/industry/official-mapping-override-review`
  - 可用于分页查看 `unmapped` backlog、确认 `manual_override` 命中情况、读取原始 mapping payload
  - backlog 行当前还会额外返回：
    - `review_priority`
    - `override_candidate_ready`
    - `override_candidate_reason`
    - `candidate_count`
    - `top_candidate_overlap_gap`
    - `manual_override_suggestion`
  - 用于区分“值得优先人工核验”的高信号行与普通待观察行
  - backlog 响应还会返回：
    - `override_candidate_total`
    - `review_priority_counts`
  - 并支持 `override_candidate_ready_only=true`，用于只查看已经值得优先人工核验的候选
  - `official-mapping-override-candidates` 则会把 ready rows 汇总为独立导出视图，并额外给出：
    - `manual_overrides`
    - 可直接对接 `industry.standard.official_mapping.manual_overrides` 的配置片段
  - `official-mapping-override-review` 则会进一步聚合：
    - 当前配置中的 `manual_overrides`
    - 当前 ready candidate 导出结果
    - 当前 mapping cache 中已生效的 `manual_override`
  - 并返回：
    - `status_counts`
    - `pending_manual_overrides`
    - `configured_and_applied / ready_candidate_pending_config / configured_not_applied / applied_not_configured` 等审阅状态
  - 该接口支持定向过滤：
    - `attention_only=true`：只看非 `configured_and_applied` 状态
    - `review_status=...`：只看指定审阅状态，可重复传参
  - 过滤后的 `status_counts / pending_manual_overrides / items` 会按过滤结果重算，适合直接排查 stale 或 mismatch override
  - `industry/standard-readiness` 当前也会直接给出：
    - `override_review.requires_attention`
    - `override_review.status_counts`
    - `override_review.pending_manual_override_total`
    - `override_review.top_items`
  - `override_review` 仅作为审计提示，不再进入 current membership readiness blocker
- 已为 legacy fallback 增加总时长预算 guard，避免 `proxy_patch` 下整任务长时间悬挂
- 配置项已新增：
  - `cache_max_age_days`
  - `minimum_mapping_rows`
  - `minimum_mapped_rows`
  - `readiness_override_review_limit`
  - `allow_live_rebuild_on_cache_miss`
  - `component_cache.cache_max_age_days`
  - `component_cache.minimum_component_sets`

当前工程意义：

- strict Shenwan membership sync 已从 official-code mapping 推断链路切换为三级成分股 current membership 主链
- component-set 已从纯运行时对象升级为可持久化缓存，current sync 会优先复用该缓存
- strict Shenwan 主线现已具备推荐执行顺序：`industry_standard_sync -> industry/standard-readiness`
- `industry_official_mapping_refresh` 只作为历史分类审计缓存刷新任务，需要时手工或定期单独执行
- backlog 审计不再需要直接查库；当前已经可以通过 backlog API 按 `current_classification_count` 优先级筛出 high-impact unmapped codes，再决定是否写入 `manual_overrides`
- `2026-04-19` 的 official-code override validation 结论已被后续主线替代：
  - 当时的 `official_code -> taxonomy` 映射可作为审计缓存验证记录保留
  - 当时通过 `480301 -> 857831.SI` override 解锁代表样本的逻辑，不再作为 current membership 的推荐路径
  - 现在 `000001.SZ / 600000.SH` 这类股票的当前归属应直接由叶子行业成分股集合确认
- `2026-04-21` 使用仓库级 rollout runner 做 `SSE/SZSE` 小样本验证后确认：
  - `industry_official_mapping_refresh` 成功写出 `433` 条映射缓存，其中 `304` 条 mapped、`129` 条 unmapped
  - `industry_standard_sync` 在 `--limit-per-exchange 2` 下成功写入 `4` 条 official classifications 和 `3` 条 authoritative memberships
  - readiness 仍为 `not_ready`，原因是 readiness 按全市场 `5191` 个目标标的计算，而本轮 sync 是代表样本验证
  - `480101` 仅影响 official history 审计层，样本标的为 `600000.SH`
  - 后续确认 `600000.SH` 应通过叶子行业成分股集合确认落入 `857831.SI`，而不是通过 `480101` overlap 反推
- 这不会立刻解锁 valuation rollout，但会显著降低后续继续打磨时的波动面

#### Mainline 5：股东域

目标：

- 股东摘要
- 前十大股东
- 股权结构线索

显式 source policy：

- `free_chain`
  - 空；当前未确认存在可覆盖 whole-domain baseline 的稳定免费主源
- `paid_chain`
  - `akshare:proxy_patch`
  - `efinance:proxy_patch`
- `fallback_chain`
  - `cninfo:direct`
  - `akshare:direct`
  - `efinance:direct`

`2026-04-19` live probe 结论补充：

- 已对 `cninfo` 官方 webapi 的 `股东人数及持股集中度`、`实际控制人持股变动`、`公司股本变动` 做直连验证
- 单次时延约 `0.24s - 1.7s`；`10` 次 burst 请求未出现 `403 / 429 / busy` 类报错
- 说明 `cninfo:direct` 在当前环境下**速度与反爬压力都可接受**
- 但当前可稳定验证的官方字段主要是 `holder_count / actual_controller_change / share_change`
- `top10_holders` 与更完整的股权结构字段尚未确认存在同等级、稳定、可工程化的 `cninfo` 官方接口
- 因此，即使当前同步已支持“缺失标的继续补跑 fallback”，也**不应直接把整个 shareholders 域切换为 `cninfo` 主源**
- 当前已按新的工程决策将 `AkShare proxy_patch` 设为 shareholders 主源，`cninfo:direct / akshare:direct / efinance:direct` 放到 fallback
- 其中 `efinance` 已进入 fallback 路由顺序，但当前全局 source 开关仍保持 disabled；要让它实际参与运行，还需要显式启用依赖与配置
- 当前已新增 instrument-level fallback pass：
  - 若主源只返回部分股票，剩余股票会继续进入后续 fallback source
  - 同一股票一旦已有首个成功 snapshot，当前不会再做字段级 merge 或覆盖
- 若后续要将 `cninfo` 前置，前提应是：
  - 要么先实现 `cninfo + efinance/akshare` 的多源合并
  - 要么先把 shareholders 拆成 `official_holder_count / top10_holders / ownership_structure_clues` 等子域分别路由

显式预算闸门：

- 仓库级 research 路由默认值已经是 `availability_first + allow_paid_proxy=true`
- `shareholders` 模块是否真正开放读取，仍由 `delivery_mode` 和 `snapshot_api_requires_mode` 单独控制
- `free_best_effort`
  - 仅用于影子同步或内部验证
  - 即便仓库允许 paid proxy，`shareholders` 读取接口也不会在该模式下开放正式 snapshot 读取
  - 只承诺股东摘要、股东户数、前十大股东等可得摘要域
- `paid_high_availability`
  - 保持 `AkShare proxy_patch` 主链与补充 fallback 同时可用
  - 才进入标准化 snapshot/API 的正式实施与开放
  - 当前生产配置已采用该模式，API gate 已满足

当前实现状态补充：

- 已新增 `/api/v1/research/shareholders/readiness`
- 可直接读取：
  - `shareholder_snapshots` 总量、缺口与最近更新时间
  - `coverage_status / source / source_mode` 聚合分布
  - `required_scope / scope_counts / scope_coverage`
  - 按交易所的 snapshot coverage 与 `ready` 判定
  - 模块 gate 状态与 `paid_high_availability` rollout blockers
- 后续 shareholders 是否真正开放正式读取，应优先由这条 readiness 接口来判断，而不是只看配置或单次样本同步结果
- 当前已新增仓库级 rollout validation runner：
  - 只读 readiness：`python scripts/research_shareholder_rollout_validation.py --exchanges SSE,SZSE --skip-sync`
  - 小样本 paid 链路验证：`python scripts/research_shareholder_rollout_validation.py --exchanges SSE,SZSE --limit-per-exchange 20 --enable-module --delivery-mode paid_high_availability`
  - runner 只在内存中应用 `--exchanges / --enable-module / --delivery-mode` 等 scope/gate override，不改写 `config/10_research.json`
  - 若需在 CI 或运维脚本中硬失败，可追加 `--fail-on-not-ready`，not ready 时退出码为 `2`
- 当前 shadow sync 已进一步升级为“缺失标的 + 缺失 required scope 继续 fallback 并按 scope merge”
  - 同一标的不再是简单“首个成功 snapshot 生效”
  - 当前会保留首个成功 snapshot 的 `source/source_mode` 作为 primary source，同时继续从后续 provider 补齐 `holder_count / top10_holders / reference_only_ownership_clues`
  - `snapshot.scope_sources` 会记录每个 scope 的实际来源
- `2026-04-19` representative live validation 已确认：
  - `cninfo:direct` 在代表性 `SSE / SZSE` 样本上可稳定返回 `holder_count + reference_only_ownership_clues`
  - `AkShare:proxy_patch` 在同样样本上可稳定返回 `holder_count + top10_holders + reference_only_ownership_clues`
  - 同步链路中 raw payload 审计此前存在 `date` 序列化缺陷，已修复后复验通过；当前 `direct / proxy_patch` 两条代表性链路均已实现 `2/2` 成功
  - 但这仍只是代表性样本验证，不等于 whole-domain baseline 已完成全市场 rollout
- `2026-04-19` 扩大样本验证（`SSE 5 + SZSE 5`）已确认：
  - `cninfo:direct`
    - `snapshot_total = 10 / 10`
    - 但 `scope_counts.holder_count = 6`，且 `top10_holders = 0`
    - 因此 scope-aware readiness 会返回 `required_scope_coverage_incomplete`，不能被视作 rollout-ready 主链
  - `AkShare:proxy_patch`
    - `snapshot_total = 10 / 10`
    - `holder_count / top10_holders / reference_only_ownership_clues` 三个 required scope 均为 `10 / 10`
    - 在该扩大样本下可视作满足 `paid_high_availability` 的字段覆盖要求
  - 这说明 shareholders 的 rollout 判定不能只看“是否有 snapshot”，必须同时看 required scope 覆盖
- `2026-04-19` 进一步扩大样本复验（`SSE 20 + SZSE 20`）后，结论明显收紧：
  - `direct` 链路（实际执行顺序：`cninfo:direct -> akshare:direct`）
    - `snapshot_total = 40 / 40`
    - `scope_counts.holder_count = 40`
    - `scope_counts.reference_only_ownership_clues = 40`
    - `scope_counts.top10_holders = 31`
    - `SSE` 达到 `20 / 20` required scope 完整覆盖，但 `SZSE` 仅 `11 / 20`
    - readiness 仍会返回 `required_scope_coverage_incomplete`
  - `proxy-first` 链路（实际执行顺序：`akshare:proxy_patch -> cninfo:direct -> akshare:direct`）
    - `snapshot_total = 40 / 40`
    - 但 `scope_counts.top10_holders = 0`
    - `SSE / SZSE` 两个交易所都未满足 required scope，`resolved_instruments = 0`
    - ingestion metadata 已确认三段 source 都被实际尝试且写入成功，因此问题不在 routing，而在 `top10_holders` 批量抓取稳定性
  - 对 `600000 / 000001 / 600519` 的单标的 probe 已确认 `AkShareShareholdersProvider` 在 `direct / proxy_patch` 下都能产出 `top10_holders = 10`
    - 这说明当前问题更像是批量运行时的上游节流/稳定性退化，而不是 provider 字段提取逻辑本身错误
  - 因此 shareholders whole-domain baseline 目前**仍不能**按 `paid_high_availability` 进入 rollout
  - 为了支撑下一轮排障，`AkShare` 股东 provider 现已把分字段抓取异常写入 `raw_payload.fetch_errors`
- `2026-04-20` 针对 `AkShare` 股东 provider 新增了 `top_holders_request_interval_seconds / top_holders_retry_attempts / top_holders_retry_backoff_seconds`
  - 配置入口：
    - `research.sources.akshare.shareholders.top_holders_request_interval_seconds`
    - `research.sources.akshare.shareholders.top_holders_retry_attempts`
    - `research.sources.akshare.shareholders.top_holders_retry_backoff_seconds`
  - 当前仓库默认值：
    - `request_interval = 0.2`
    - `retry_attempts = 2`
    - `retry_backoff = 0.5`
  - 重跑大样本 live validation 后，结论变为：
    - `direct` 链路（`cninfo:direct -> akshare:direct`）
      - `top10_holders = 0 / 40`
      - 最终 `source_counts = {"cninfo": 40}`
      - `raw_payload_audit` 中可见 `38` 条 `akshare:direct` 的 `fetch_errors.top_holders = "No tables found"`
      - 因此 current direct batch path 不能承担 whole-domain baseline
    - `proxy-first` 链路（`akshare:proxy_patch -> cninfo:direct -> akshare:direct`）
      - `top10_holders = 33 / 40`
      - `SSE = 20 / 20` 完整覆盖，`SZSE = 13 / 20`
      - `source_counts = {"akshare": 39, "cninfo": 1}`
      - `raw_payload_audit` 中仅剩 `6` 条 `akshare:proxy_patch` 的 `fetch_errors.top_holders = "No tables found"`；另有 `000028.SZ` 退回 `cninfo:direct`
  - 这说明新的节流+重试对 `proxy_patch` 主链是有效的，已把大样本 `top10_holders` 从此前的 `0 / 40` 拉回到 `33 / 40`
  - 但当时 shareholders readiness 仍然是 `required_scope_coverage_incomplete`，whole-domain `paid_high_availability` rollout 依旧不能开放
- `2026-04-20` 又为 shareholders sync 增加了 bounded same-source recovery pass
  - 新增配置入口：
    - `research.modules.shareholders.same_source_recovery_candidates`
    - `research.modules.shareholders.same_source_recovery_batch_size`
    - `research.modules.shareholders.same_source_recovery_max_instruments`
  - 同步 metadata 已新增：
    - `same_source_recovery_runs`
    - `same_source_recovery_attempted_instruments`
    - `same_source_recovery_resolved_instruments`
  - 单元测试已覆盖 recovery 行为和 ingestion metadata 记录
  - 对上一轮遗留的 `SZSE 7` 个缺口标的做 focused live validation 后：
    - `direct` 链路（`cninfo:direct -> akshare:direct`）达到 `7 / 7`
    - `proxy_patch` 链路（`akshare:proxy_patch`）也达到 `7 / 7`
    - 两条链路的 `same_source_recovery_*` 计数都为 `0`
    - 这说明上一轮 gap 在当前环境下已不再稳定复现；recovery pass 目前更像是 bounded protection，而不是当前样本下的必经路径
  - 同日重新执行 `SSE 20 + SZSE 20` 的 `proxy_patch` 大样本复验后：
    - `snapshot_total = 40 / 40`
    - `scope_counts.holder_count = 40`
    - `scope_counts.top10_holders = 40`
    - `scope_counts.reference_only_ownership_clues = 40`
    - `SSE = 20 / 20`、`SZSE = 20 / 20`
    - `source_counts = {"akshare": 40}`
    - readiness 在该样本上返回：
      - `ready_for_paid_high_availability_rollout = true`
      - `blockers = []`
    - ingestion metadata 仍显示 `same_source_recovery_runs = 0`
  - 当前更新后的工程判断：
    - `AkShare:proxy_patch` 已从“唯一现实候选但未达标”推进到“在当前 20+20 shadow 样本上满足 required scope 的主链候选”
    - 但这仍然是样本级 live validation，不等于已经完成全市场或多轮次稳定性确认
    - 当前项目决策已明确：不再把继续扩大样本或全市场 shadow validation 作为后续推进前置；工程主线改为保持 `AkShare:proxy_patch` 导入主链，模块是否正式开放仍由 `delivery_mode / snapshot_api_requires_mode` 单独控制
- `2026-04-27` 增量更新策略与 BSE 覆盖评估：
  - 股东户数存在轻量变化检测路径；此前工程策略按业务要求统一收口为周更，但 `2026-05-23` 已新增 OpenSpec `add-shareholder-incremental-sync` 并落地通用 CNInfo 公告元数据扫描/过滤/水位模块，股东域已改造为“每日公告驱动增量检查 + 周期性/手工全量刷新”双模式：
    - `akshare.stock_zh_a_gdhs(symbol="最新")` 可一次返回当前全市场股东户数列表，现场探测返回 `5505` 行，其中 `920` 前缀 BSE-like 代码 `311` 行
    - `cninfo.stock_hold_num_cninfo(date=YYYYMMDD)` 可按季末报告期批量读取股东人数及持股集中度，适合作为报告期级别的官方口径交叉验证
    - 新增通用公告扫描能力不按单标的逐个查询公告，而是按 CNInfo 市场/栏目和时间窗口分页扫描公告元数据，支持按业务传入标题/分类/谓词过滤与独立水位；股东增量作为首个消费者，筛出相关公告后再对候选标的拉取 `holder_count / top10_holders / ownership_clues` 并做规范化 hash 比较；只有 hash 或覆盖范围变化才写入快照
    - `shareholder_incremental_sync` 默认每日 `06:30` 运行，写入 `cninfo_announcement_scan_state / cninfo_announcement_audit / shareholder_change_manifest` 以解释水位、候选、hash 和 pending recheck；同一批公告的 pending 截止时间固定为第一次进入 pending 的 `first_pending_at + pending_recheck_days`，不会因每日扫描滚动延长
    - `shareholder_shadow_sync` 保留为 `manual_only=true` 的手工全量刷新、灾后补齐和抽样审计入口，不再作为常驻周六定时任务；该任务仍出现在 `/status` 中，展示为仅手工执行入口；每日任务承担增量发现与定向刷新，每周六 `12:30` 的 `shareholder_reconciliation_sync` 做全量读取 + changed-only hash 对比，只补写变化、缺失或 required scope 不完整标的
  - 前十大股东当前仍缺少足够轻量的全市场变更清单：
    - 当前主链依赖 `akshare.stock_main_stock_holder(stock=...)`，本质是按单标的读取新浪财经主要股东历史数据
    - 不建议对全 A+BSE 每日全量刷新 `top10_holders`；调度层已将 shareholders 固定为每日公告驱动增量检查和周六 changed-only 周期复核，`shareholder_shadow_sync` 仅作为手工全量刷新入口，默认不设置 `limit_per_exchange`
  - BSE 数据源结论：
    - 北交所官网公开 `董监高及相关人员持股变动`，`akshare.stock_share_hold_change_bse(symbol=...)` 可返回结构化持股变动事件；现场样例 `430489` 返回 `8` 行
    - 该接口覆盖的是董监高及相关人员持股变动事件，不等价于标准化股东快照，不能补足 `holder_count / top10_holders`
    - 东财股东户数在 `stock_zh_a_gdhs("最新")` 中可覆盖 BSE-like `920xxx` 代码；`stock_zh_a_gdhs_detail_em("920833")` 现场样例可返回历史股东户数详情
    - 新浪主要股东 `stock_main_stock_holder("920833")` 现场样例可返回主要股东历史数据；旧 BSE 代码 `430489` 对东财详情和新浪主要股东会失败，而 `920489` 可返回同一公司历史股东户数与主要股东数据
    - 当前 provider 层已补齐 `430/83/87 -> 920xxx` 请求候选；snapshot 仍按原 `instrument_id` 落库，raw payload 记录实际请求代码候选
    - `cninfo.stock_hold_control_cninfo("全部")` 现场样例可返回 BSE `920833` 实际控制人；同步层已新增 `force_merge_candidates=["cninfo:direct"]`，用于在 AkShare 股东快照成功后继续合并 CNInfo 实控人线索
    - `cninfo.stock_hold_num_cninfo(date=...)` 现场未返回 BSE 股东户数；BSE `holder_count / top10_holders` 以 AkShare/东财/新浪链路为准，`control_owner` 以 CNInfo 控制人链路补充
    - 已补齐股东 provider 对 `NaN / nan / null` 比例字段的数值归一化，避免将上游缺失比例误落为有效浮点值
    - 当前仍保留 `BSE` 在 shareholders 域的 optional-empty 保护；全量下载后已确认 BSE snapshot 有数据，后续可单独评估是否把 BSE 纳入必达 target 口径
- `2026-04-30` 全量落库、API gate 与 scheduler 收口：
  - 当前配置：
    - `research.modules.shareholders.enabled = true`
    - `research.modules.shareholders.delivery_mode = paid_high_availability`
    - `research.modules.shareholders.snapshot_api_requires_mode = paid_high_availability`
    - `scheduler.jobs.shareholder_shadow_sync.enabled = true`
    - `scheduler.jobs.shareholder_shadow_sync.manual_only = true`
    - `scheduler.jobs.shareholder_reconciliation_sync.enabled = true`
  - 当前全量刷新策略：`shareholder_shadow_sync` 仅手工触发，`SSE / SZSE / BSE` 全量刷新，`limit_per_exchange = null`，`max_runtime_seconds = 18000`；常驻调度改为每日 `06:30` 的 `shareholder_incremental_sync` 和每周六 `12:30` 的 `shareholder_reconciliation_sync`
  - 三种股东更新任务的边界：
    - `shareholder_incremental_sync`：每日公告驱动增量检查，不逐标的扫描公告；只读取公告候选、缺失 required scope 和 pending recheck 标的；只写变化、缺失或覆盖不完整标的
    - `shareholder_reconciliation_sync`：每周六周期复核与补足，全量读取目标股票池，但按 `changed_only` 写入；本地 `snapshot_json` hash 和 required scope 相同则跳过
    - `shareholder_shadow_sync`：仅手工 `/run` 的全量刷新，`manual_only=true`，不注册自动 cron；用于初始化、灾后修复或明确需要全量重写的场景
  - 只读 readiness 复核命令：
    - `python scripts/research_shareholder_rollout_validation.py --exchanges SSE,SZSE,BSE --skip-sync --fail-on-not-ready`
  - 当前复核结果：
    - `status = ready`
    - `snapshot_api_enabled = true`
    - `ready_for_paid_high_availability_rollout = true`
    - `blockers = []`
    - `target_instrument_count = 5191`
    - `snapshot_total = 5191`
    - `scope_counts.holder_count = 5191`
    - `scope_counts.top10_holders = 5191`
    - `scope_counts.reference_only_ownership_clues = 5191`
    - `source_mode_counts.proxy_patch = 5191`
  - `BSE` 当前已有 `300` 条 shareholder snapshot；但由于 `optional_empty_exchanges=["BSE"]` 仍保留，readiness target 仍只把 `SSE / SZSE` 作为必达目标。
  - 股东域当前可视为已完成本阶段实现，后续只保留周更维护、异常补缺、BSE target 口径是否收紧以及上游稳定性监控。

优先原因：

- 业务价值高
- 但缺乏可靠 free whole-domain 主源，因此需要显式区分“仓库默认路由”与“模块正式开放条件”

#### Mainline 6：strict Shenwan rollout readiness

目标：

- 将 `industry_standard -> valuation` 的上线判断从文档描述升级为标准 read model
- 基于 research 域真实落库状态输出 readiness 与 blockers
- 让后续 rollout、shadow validation 和手工 override 回修有统一观测入口

当前实现状态：

- 已新增 `/api/v1/research/industry/standard-readiness`
- 已聚合：
  - authoritative membership coverage
  - 当前 research 股票池按交易所 coverage
  - `relative valuation` rollout blockers
  - official mapping / override review 的审计摘要

当前工程意义：

- strict Shenwan 主线的“是否 ready”现在可以被程序化读取
- 即使代表样本已经 `2/2` 成功，只要全市场 authoritative coverage 仍不足，readiness API 仍会明确返回 `not ready`
- 后续继续打磨全量 current membership sync 后，可以直接用这条接口复核 readiness，而不需要再从多张表人工拼状态
- 现已新增仓库级 rollout validation runner：
  - `scripts/research_industry_standard_rollout_validation.py`
  - 默认串联 `industry_standard_sync -> industry/standard-readiness`
  - 输出结构化 JSON，包含 `refresh / sync / readiness / summary`
  - `industry_official_mapping_refresh` 默认跳过；如需同步审计缓存，可显式传入 `--include-official-refresh`
  - 支持 `--skip-refresh / --include-official-refresh / --skip-sync / --fail-on-not-ready`，可用于 shadow validation、人工排障或后续 CI gate
  - CLI 生命周期已补齐 `DataManager.close()`，避免 live validation 结束时遗留 aiohttp client session
- 现已新增 strict Shenwan 覆盖率收口链路：
  - `data_manager.get_research_industry_standard_coverage_gaps()`
  - `data_manager.run_industry_standard_gap_fill_sync()`
  - `scripts/research_industry_standard_gap_fill.py`
  - `scripts/research_industry_standard_maintenance.py`
  - `scheduler.tasks.industry_standard_gap_fill`
  - `config/05_scheduler.json -> scheduler_config.jobs.industry_standard_gap_fill`
- `industry_standard_sync` 已支持 `instrument_ids_by_exchange` 定向同步；gap fill 会先枚举 missing instrument ids，再只对缺口标的触发 current membership 同步
- 现已新增仓库级维护命令 `scripts/research_industry_standard_maintenance.py`：
  - 默认执行全量 `industry_standard_sync`
  - 然后执行 `industry_standard_gap_fill`
  - 最后统一输出 readiness/coverage 摘要
- 这意味着 strict Shenwan authoritative membership 收口流程已从“读 readiness -> 手工猜测 -> 全量重跑”升级为“枚举缺口 -> 定向补齐 -> 再读 readiness”的闭环
- `2026-04-22` 已补齐 live 运行时性能问题：
  - `coverage_gaps` 不再依赖 `db_ops.get_instruments_by_exchange()` 的 async ORM 读取
  - research target universe 已统一收口到 `DatabaseOperations.get_research_target_instrument_ids_by_exchange()`
  - `DataManager` 不再直接访问 `sqlite3`，而是只依赖统一数据访问层
  - `industry_memberships` 新增 strict Shenwan gap lookup 组合索引
  - 当前真实环境下 `SSE` coverage probe 约 `9ms`，全市场 summary probe 约 `19ms`
- `2026-04-22` 已完成 live gap-fill pilot：
  - `SSE x5`：authoritative memberships `1 -> 6`
  - `SSE x100`：authoritative memberships `6 -> 105`
  - `SSE x100`：authoritative memberships `105 -> 204`
  - 当前全市场 readiness：`authoritative = 206 / target = 5191`，`industry_standard_ready = false`
  - 当前已验证 gap-fill 批处理可以稳定复用 `akshare:proxy_patch`
  - 当前已观测到少量 fallback 未命中样本，例如 `600117.SH`，原因为 `constituent fetch time budget exceeded before any target instrument was matched`

---

## 6. 当前推荐推进顺序

### Iteration 1

`industry` 参考层已完成；严格申万标准层已进入实现态，但真实环境验证尚未完成。

已交付：

- research 存储新增行业表
- industry provider 抽象与 registry
- `BaoStock` 主源 + `PyTDX` 次级支持
- `company -> industry` 标准化 membership
- `/api/v1/research/company/{instrument_id}/industry`
- `overview` 中的标准化行业整合

尚未交付：

- 代表性 A 股样本的真实环境影子同步验证
- 行业版本维护与异常回退策略的线上确认
- 可直接用于 relative valuation 的申万二级比较中枢

### Iteration 2

先把行业从“参考层”升级为“严格申万标准层”。

交付要求：

- `industry_taxonomy` 明确维护申万一/二/三级体系与版本，并作为全量本地维表维护
- `industry_component_sets` 明确维护申万叶子/三级行业成分股集合，作为指数成分诊断与后续成分资产使用；官方分类主源落地后，不再作为当前股票归属的主数据来源
- `industry_memberships` 明确维护股票到申万一/二/三级的映射，行业归属变更后由全量同步直接重映射
- `industry_index_analysis_daily` 明确维护申万行业指数分析日频指标，并作为行业 PE/PB/股息率等官方 benchmark 数据源；该表只通过 `sw_index_code` 或显式 alias 与 taxonomy join，不参与股票归属判定
- 对叶子成分股主源残余缺口，允许配置化人工补源、结构化新浪补源和东财行业名补源按本地 taxonomy 名称补齐；补源结果只写 `industry_memberships`，不回写 `industry_component_sets`
- 对无法映射到申万标准的记录标记为 `reference_only`
- 增加每日/每周行业更新任务，默认每周全量，必要时可提升为日更；缺口补齐任务可独立调度
- `industry_official_mapping_refresh` 不再排在 current membership 同步之前；如需审计历史分类映射，可单独执行
- 约定 relative valuation / 行业统计支持显式选择 **申万一级 / 二级 / 三级** 作为分组层级；未指定时默认使用 **申万二级行业**
- 标准运维顺序明确为：
  - `industry_standard_sync`：按官方 source manifest 增量刷新 taxonomy、classification history 和 current memberships
  - `industry_standard_rebuild_official`：在 schema/源口径变更或需要清理重建时受控全量重建 strict Shenwan 标准层
  - `industry_standard_gap_fill`：仅作为官方主源后残余异常缺口的诊断/补救入口
  - `/api/v1/research/industry/standard-readiness`：统一验收 current authoritative 覆盖率
- 标准 API 收口要求明确为：
  - 所有申万一/二/三级行业 taxonomy、股票 membership、叶子/三级 component set、指数分析 benchmark 都必须可通过 research API 读取
  - 所有本地 research 表的 API 暴露纳入后续统一 API 阶段，不再允许“只落库、不可读”的完成状态

### Iteration 3

推进 shareholders，但在开发前先重新确认预算策略。

原则：

- `BaoStock / PyTDX` 不足以独立承担标准化股东域主源
- 若采用 `free_best_effort`，则股东域只做摘要与 reference-only 输出
- 若采用 `paid_high_availability`，则允许进入标准化 snapshot/API 实施

### Iteration 4

推进 valuation，在 strict Shenwan 二级行业与完整财务域之上建立默认估值基线。

交付前置条件：

- strict Shenwan 行业映射真实环境验证完成
- `financial_statements` 影子同步代表性样本校验完成
- 申万二级 peer group 规则与估值口径完成配置化定义
- `data/valuation.db` 存储边界、备份策略和 repository 访问层完成
- market-cap/share-count 估值输入源、单位、可得日期和缺失诊断完成审计

---

## 7. 开发约束与执行规则

后续继续开发时，统一遵循下面的工程规则：

- 轻量 research 维表默认进入 `research.db`，但高行数或大体量域必须独立存储：财务事实进入 `financials.db`，估值日频派生序列进入 `valuation.db`；任何新物理库都必须通过 storage/repository 层访问
- 所有读取接口统一挂到 `/api/v1/research/*`
- 所有数据源策略必须通过配置表达，不在业务代码里硬编码
- 财务域所有上游 URL、报告期 baseline、rolling window、hot window、anchor policy、限流、重试、并发、parser version、字段 alias、估值指标定义和 fallback policy 必须配置化或版本化
- 行业域必须明确区分：
  - `reference_only`
  - `authoritative`
- relative valuation / 行业统计只能使用 `authoritative` 的申万行业映射；调用方可选 `L1 / L2 / L3`，未显式指定时默认 `L2`
- 所有计算型输出必须保留：
  - `calc_method`
  - `calc_version`
  - `parameter_hash`
- 所有数据型输出尽量保留：
  - `source`
  - `source_mode`
  - `data_as_of`
- 所有用于历史估值的财务事实必须保留：
  - `report_period`
  - `report_type`
  - `publish_date`
  - `data_available_date`
  - `source_file_id` 或等价 source lineage
- 估值历史只持久化 `instrument_id + trade_date + metric variants` 的单标的日频结果；相对估值同行统计不得落全量 subject-peer-date 矩阵
- `DataManager`、API 路由和 provider 不应直接依赖 SQLite 方言；如果必须使用 SQLite 特性，应封装在 storage 层，便于后续拆分 `financials.db`、`valuation.db` 或迁移到 DuckDB/PostgreSQL
- hot/cold 财务表、分区表或历史列式仓都必须由 storage/repository 层屏蔽；业务层只能表达“查最近窗口”或“查长历史”
- 新模块必须先补单元测试，再考虑开放接口
- 文档、配置、代码状态不一致时，以“代码实际能力 + 本文档状态定义”为准，并及时回写文档

---

## 8. 当前结论

截至目前，Research Data Engine 已经从“需求讨论阶段”进入“Phase 0 / Phase 1 基础能力已落地”的状态。

接下来的工作重点不再是继续扩张需求范围，而是把已有成果按工程主线串起来，并按以下顺序持续推进：

1. `industry` 严格申万标准层与 `shareholders` 本阶段均已完成，后续只保留 readiness 复核、异常补缺和调度维护
2. 财务主线已进入生产数据资产与维护任务阶段：后续继续观察 daily catch-up、weekly reconciliation、accepted gap 和 financial readiness
3. 进入 valuation rollout 准备：新建 `data/valuation.db`，补齐 market-cap/share-count 本地输入层，重建日频 PE/PB/PS/market_cap，并通过 `/api/v1/research/valuation/readiness` 验证后再决定是否启用模块
4. 按模块 gate 继续推进 `research metadata` 的 production rollout 决策与质量加固
5. 对 `risk / technical cache` 做参数调优、覆盖率复核和 API 使用体验优化
6. 持续把已交付模块回写到执行文档与 OpenSpec 状态

本文件更新后，后续每次推进都应先更新这里的状态，再继续编码。
