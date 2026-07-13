# 全域日更增量同步变更日志与水位需求说明书

> 更新日期：2026-07-13
> 适用项目：Quote System / Research Data Engine
> 对应 OpenSpec：`openspec/changes/add-daily-sync-change-watermarks/`
> 文档定位：本文是“全域日更增量同步”的独立需求说明，用于定义业务目标、覆盖范围、数据口径、影响面、风险边界、验收标准和后续工程拆解。OpenSpec change 是实施契约；本文是需求与工程评审基准。
> 使用边界：本能力只提供本地已观测数据变更信号，辅助研究系统和外部调用方增量同步；不替代交易决策，不连接真实交易账户，不承诺免费上游源具备完整 CDC。

---

## 1. 结论摘要

全域日更增量同步值得做，但必须按“本地已观测 CDC”设计，而不是假设所有免费上游源都会主动告诉平台历史修订。

当前平台通过“最近 N 天重叠重拉 + 周期对账 + 缺口修复”降低漏数风险。这套机制对免费数据源是必要兜底，但对下游同步方不够友好：调用方不知道本地哪些 `(标的, 日期/期间/事件)` 真正发生变化，只能继续整体重拉。新增变更日志和水位后，下游可以：

1. 保存上次同步水位。
2. 请求“水位之后发生变化的业务键”。
3. 只对这些业务键回查对应 API 或本地读模型。
4. 继续保留重叠重拉和周期对账作为发现上游静默修订的兜底。

本需求优先级：

1. 正确区分“真实语义变更”和“重复拉取无变化”。
2. 不影响现有日更、API、回补、治理、政策发现和研究读取默认行为。
3. 支持所有日更写入域，但分阶段接入，先主行情和复权因子，再扩展期货、FX、特殊商品和研究域。
4. 清晰说明局限：只有平台实际拉取并比较到的数据变化，才会进入 changelog。

---

## 2. 背景与问题

### 2.1 当前痛点

当前日更任务主要依赖窗口式策略：

- A 股、港股、美股行情日更按目标日附近窗口拉取。
- 新股、短缺口和历史修复有额外 catch-up 或 range backfill。
- 复权因子通过日更或周维护补充。
- 期货、FX、特殊商品和研究域任务各自有 lookback、reconciliation、shadow sync 或 changed-only 机制。

这能减少漏数，但不能回答下游最需要的问题：

```text
从我的上次同步水位 N 到现在，本地哪些业务对象变了？
```

特别是以下场景：

- 历史行情被数据源订正。
- 复权因子重算导致 qfq/hfq 历史价格变化。
- 期货主力连续或连续序列因换月规则或源修复变化。
- FX 或商品公开源回补/修订历史观测值。
- 财务、股东、行业、估值输入因公告、官方结构化源或周期对账发生更新。
- 政策发现、主数据、交易日历治理发生变化，但不应误触发行情消费者重拉。

### 2.2 不能直接用 `updated_at`

`updated_at` 是操作元数据，不等同于语义变更。当前部分写入路径在冲突时会更新已有行，即使 OHLCV、因子或事实值完全没变，也可能刷新 `updated_at`。

因此水位不能基于 `updated_at` 直接判断，否则会把所有重叠窗口重拉都误报成变更，反而放大下游同步压力。

---

## 3. 目标与非目标

### 3.1 目标

1. 建立全域统一的本地变更日志和水位语义。
2. 所有日更写入任务最终都能报告 `inserted / changed / unchanged / skipped / changelog_written`。
3. 对重复拉取但内容一致的数据不产生 changelog。
4. 对真实语义变化追加只增不减的 changelog 记录。
5. 通过只读 API 或服务方法提供：
   - 最新水位查询。
   - 按水位增量拉取变更业务键。
   - 按域、数据集、标的、日期区间过滤。
6. 支持下游根据变更键精确补拉，不改变已有数据查询 API 默认响应。
7. 对复权因子、派生序列和研究派生结果明确依赖关系，避免误把“原始行未变”理解为“所有结果未变”。
8. 保持现有重叠重拉、对账、回补、交易日历治理、主数据治理和政策发现任务不受影响。

### 3.2 非目标

1. 不承诺从免费上游源获得完整官方 CDC。
2. 不取消现有 lookback、catch-up、weekly/monthly reconciliation、gap repair。
3. 不自动重算所有下游派生表；第一阶段只记录变更和失效信号。
4. 不改变 `/api/v1/quotes/daily`、`/api/v1/research/*` 等现有端点默认行为。
5. 不在 DCF、API 查询或政策发现中隐式触发远程大规模下载。
6. 不一次性重写全历史数据；历史 hash 回填必须可选、可 dry-run、可分段。

---

## 4. 核心概念

| 概念 | 定义 |
|---|---|
| 语义行 hash | 对业务含义字段做稳定规范化后计算的 hash，不包含 `updated_at`、batch id、ingestion run id、重试次数等操作元数据 |
| row version | 单行或逻辑观测只增不减版本号；仅在语义 hash 首次插入或变化时递增 |
| sequence_id | changelog 的只增不减水位；调用方保存该值作为增量同步 checkpoint |
| change record | 一条本地已观测变化记录，包含域、数据集、业务键、变化类型、old/new hash、版本、来源、运行批次和时间 |
| local-observed CDC | 平台本地任务实际拉取、标准化、比较后发现的变化；不是上游完整修订流 |
| invalidation | 上游或输入变化导致派生数据可能过期，但未必已经重算 |

---

## 5. 覆盖范围

### 5.1 必须覆盖的日更/周期写入域

| 域 | 典型任务 | 变更键 | 初始接入优先级 |
|---|---|---|---|
| 股票/指数/ETF 日行情 | `daily_data_update`、`hk_daily_data_update`、`us_daily_data_update`、range backfill、gap repair | `instrument_id + trade_date` | P0 |
| 复权因子 | 日更 Phase 2、weekly maintenance、factor backfill | `instrument_id + ex_date` | P0 |
| 商品期货 | `futures_market_data_sync`、backfill、continuous series | `contract_id/series_id + trade_date + source_mode` | P1 |
| FX | `fx_rate_sync`、derivation、backfill | `series_id + observation_date + revision_id` | P1 |
| 特殊商品价格 | `special_commodity_price_sync`、`special_commodity_cn_spot_sync`、monthly sync | `series_id + observation_date/period + source_profile` | P1 |
| 特殊商品政策 | policy discovery、candidate review、event promotion | `adapter_id + document/event id + publication/effective date` | P2 |
| 财务披露/事实 | `financial_disclosure_incremental_sync`、reconciliation、broker risk-control | `instrument_id + report_period + fact/source identity` | P2 |
| 股东摘要 | `shareholder_incremental_sync`、reconciliation、shadow sync | `instrument_id + snapshot scope` | P2 |
| 行业数据 | `industry_standard_sync`、gap fill、mapping refresh、index analysis | `instrument_id/index_code + taxonomy_version + effective/as_of date` | P2 |
| 估值输入/估值历史 | `valuation_input_sync`、valuation history rebuild/reconcile | `instrument_id + as_of_date + calc_version + parameter_hash` | P2 |
| 技术/风险快照 | technical snapshot、risk snapshot rebuild | `instrument_id + as_of_date + calc_version` | P2 |
| 无风险利率 | `risk_free_rate_sync` | `series_id + observation_date + revision_id` | P2 |
| 主数据治理 | stock/index/HKEX/futures master governance | `instrument_id + lifecycle/effective key` | P3/可延期 |
| 交易日历治理 | stock/futures/FX/commodity calendars | `exchange/source_profile + date` | P3/可延期 |

### 5.2 只读或诊断任务

只读、诊断、preflight、dry-run 任务不应推进水位。它们可以报告 `would_insert / would_change / would_write`，但不得持久化 changelog。

---

## 6. 变更日志数据契约

### 6.1 最小字段

每条 changelog 至少包含：

| 字段 | 要求 |
|---|---|
| `sequence_id` | 单库或单域内只增不减整数 |
| `domain` | `quotes`、`adjustment_factor`、`futures`、`fx`、`commodity`、`financial`、`industry`、`valuation`、`policy` 等 |
| `dataset` | 具体表或逻辑数据集 |
| `change_type` | `insert`、`update`、`delete_marker`、`invalidate`、`metadata_change` |
| `business_key_json` | 域特有业务键 |
| `instrument_id` | 可选；适用于证券、期货、公司类数据 |
| `series_id` | 可选；适用于 FX、商品、利率、连续序列 |
| `observation_date` | 可选；交易日、观测日或 as-of date |
| `period` | 可选；财报期、月频期、政策有效期 |
| `old_hash` / `new_hash` | 语义 hash 变化前后 |
| `row_version` | 目标业务行版本 |
| `source` / `source_mode` / `source_profile` | 来源与口径 |
| `ingestion_run_id` / `batch_id` | 本次写入运行标识 |
| `changed_at` | 平台本地发现变化的时间 |

### 6.2 sequence 范围

第一阶段允许按数据库/域维护 sequence：

- `quotes.db`：股票/指数/ETF 日行情、复权因子、主数据/交易日历若接入。
- `futures.db`：商品期货合约、连续序列、期货日历/主数据。
- `fx.db`：FX 观测和派生序列。
- `research.db` / `financials.db` / `valuation.db` / `interests.db`：研究域各自维护。

不要求第一阶段提供跨所有数据库的强全局顺序。跨库聚合 API 可以后续做，但必须标明 `database_id/domain`，不能暗示全局事务一致性。

---

## 7. 语义 hash 口径

### 7.1 总原则

hash 输入必须是稳定、规范化、业务含义明确的字段：

- 日期统一为 ISO `YYYY-MM-DD`。
- 数值统一为固定精度字符串或 Decimal 规范形式。
- 空值统一表达。
- JSON 字段按 key 排序。
- 不含操作元数据。

### 7.2 关键域建议字段

| 数据集 | hash 字段建议 |
|---|---|
| `daily_quotes` | `instrument_id,time,open,high,low,close,volume,amount,turnover,pre_close,change,pct_change,tradestatus,factor,adjustment_type,is_complete,quality_score,source` |
| `adjustment_factors` | `instrument_id,ex_date,factor,cumulative_factor,dividend,bonus_shares,rights_shares,rights_price,event_type,source` |
| futures bars | contract/series id、trade date、OHLC、settlement、volume、open interest、amount、source profile、quality flag |
| FX observations | series id、observation date、value、revision id、publication time、quality flag、source profile、lineage hash |
| commodity observations | series id、observation date/period、value、unit、currency、source profile、quality flag |
| financial facts | instrument id、report period、fact name、canonical fact、value、unit、source file、mapping/parser version、data available date |
| industry membership | instrument id、taxonomy version、industry code、effective date、expiry date、source、classification |
| valuation history | instrument id、as_of_date、metric value、calc version、parameter hash、input hash、missing reason |
| policy events | adapter/source、document id、event id、publication date、effective date、event type、payload hash |

---

## 8. API 需求

### 8.1 查询能力

新增只读能力应支持：

```text
GET /api/v1/changes/latest?domain=quotes
GET /api/v1/changes?domain=quotes&since_sequence=12345&limit=1000
GET /api/v1/quotes/daily/changes?since_sequence=12345&exchange=SSE
```

最终路径可在设计阶段微调，但必须满足：

- 可按 `domain/dataset` 过滤。
- 可按 `instrument_id/series_id` 过滤。
- 可按 `observation_date` 或日期区间过滤。
- 分页稳定，按 `sequence_id ASC`。
- 返回 `latest_sequence`、`next_sequence` 或 `has_more`。
- 响应只包含变化键和元数据；完整数据仍通过原有业务 API 获取。

### 8.2 兼容性

现有 API 默认行为不变：

- `/api/v1/quotes/daily` 不要求水位参数。
- `/api/v1/research/*` 不因 changelog 增加默认字段。
- 政策发现、主数据、交易日历的 changelog 不进入 quote-only 查询。

---

## 9. 对业务和关联功能的影响

### 9.1 API 访问

影响：

- 新增只读增量查询端点。
- 原有数据端点作为补拉数据源继续使用。

不允许：

- 改变现有端点默认响应。
- 在增量查询中返回大体积完整行导致分页不稳定。

### 9.2 复权因子

复权因子必须是一等变更源：

- 原始 `adjust=none` 行未变，不代表 qfq/hfq 结果未变。
- 因子变更后，调用方至少应知道 `instrument_id + ex_date` 发生变化。
- 是否返回受影响历史日期范围可作为 phase 2；phase 1 至少要能触发该证券复权口径补拉。

### 9.3 数据回补和缺口修复

回补/修复必须复用同一写入路径：

- 插入新历史行：记录 `insert`。
- 修正已有历史行：记录 `update`。
- 生命周期过滤跳过：不记为 source failure，不产生业务变更。
- dry-run：只报告 would-write，不推进水位。

### 9.4 交易日历治理

交易日历变化可能影响“是否应该有数据”，但不等于行情行变化。

要求：

- 可单独记录 calendar/governance 域变化。
- 不自动进入 quote-only 变更流。
- 不因日历治理变化自动大规模重拉行情，除非后续任务明确消费该治理信号。

### 9.5 主数据治理

主数据变化可能影响 universe、退市、新股、合约生命周期和研究范围。

要求：

- 主数据治理默认保持现有行为。
- 可记录 governance/master 域 changelog。
- 下游增量消费者必须显式订阅该域，避免普通行情同步被主数据噪声影响。

### 9.6 财务数据

财务数据必须按“可得时点”和“报告期”处理，避免未来函数：

- 变更键不得只用自然日。
- 至少包含 `instrument_id + report_period + fact/source identity`。
- 应保留 `data_available_date`、`publish_date`、source profile、mapping/parser version。
- 财务事实变化可以触发估值输入或估值历史失效，但不自动重算全部估值。

### 9.7 行业数据

行业数据必须保留时点化：

- 变更键包含 taxonomy version、effective date、expiry date。
- 当前态行业变更和历史 effective 区间修正都应可区分。
- 行业变更可影响行业中性化、相对估值和分组研究，应记录 input lineage。

### 9.8 估值数据

估值历史和估值输入是派生/半派生数据：

- 估值输入如股本、市值、无风险利率、财务事实变化，应记录 observation/input 变化。
- 估值历史重算后，只有输出语义 hash 变化才记录 valuation change。
- `calc_version`、`parameter_hash`、`input_hash` 必须进入业务键或 lineage，不应混入普通行情变更。

### 9.9 政策发现

政策发现和候选提升是证据域，不是价格域：

- 政策候选新增、审核、提升可记录 policy changelog。
- 价格消费者默认不接收 policy changelog。
- 政策事件若被商品诊断或估值模型使用，应通过 input lineage 体现。

---

## 10. 分阶段落地建议

### P0：股票/指数/ETF行情 + 复权因子

目标：

- 解决最大误报源：`daily_quotes` 冲突更新。
- 新增 quote/factor changelog。
- 新增 quote/factor 只读水位 API。
- 日更报告新增 changed/unchanged 计数。

验收：

- 重复写入相同日线不推进水位。
- 修改 OHLCV 任一业务字段推进水位。
- 修改 `updated_at` 不推进水位。
- 复权因子变更能被独立查询。

### P1：期货、FX、特殊商品价格

目标：

- 接入已有 hash-aware 写入路径。
- 保持 dry-run 不持久化。
- 区分价格观测和政策/事件证据。

验收：

- futures/FX/commodity changed/unchanged 计数与 changelog 一致。
- 派生 FX 和连续期货序列保留 lineage。

### P2：研究域

目标：

- 股东、财务、行业、估值输入/历史、技术/风险、利率接入。
- 明确哪些任务不写 changelog。
- 派生结果记录 input hash/source watermark。

验收：

- 原有 research API 默认响应不变。
- 财务和估值不引入未来函数。
- 行业时点化变更可追踪。

### P3：治理域与聚合能力

目标：

- 主数据治理、交易日历治理、政策发现等域独立记录。
- 评估是否需要跨库聚合 API。
- 制定 changelog retention/compaction 策略。

---

## 11. 配置与运维要求

配置要求：

- 支持按 domain/dataset 开关 changelog emission。
- 支持 dry-run 只报告 would-write。
- 支持最大 changelog 页大小、默认页大小。
- 支持任务报告是否显示零值计数。

运维要求：

- 健康检查能看到各域 latest sequence。
- 报告能看到 changelog 写入数量和 unchanged 数量。
- 可关闭噪声域而不影响其他域。
- 可执行分段 hash backfill，但默认不全量重写。
- 备份策略必须覆盖新增 changelog 表。

---

## 12. 数据质量与金融风险

必须避免：

- 把 `updated_at` 当业务变化。
- 把复权因子变化误归因于原始行情变化。
- 用未来公告或未来财务修订驱动历史研究结果。
- 用完整样本重算后覆盖 PIT 研究结果但不记录版本。
- 把政策发现事件混入价格变更流。

必须保留：

- `data_available_date` / `publish_date` / `as_of_date`。
- source/source_mode/source_profile。
- parser/mapping/calc version。
- input hash / lineage hash。
- ingestion run id 或 batch id。

---

## 13. 测试验收清单

最低测试：

1. `daily_quotes` 插入新行生成 changelog。
2. 相同 `daily_quotes` 重复写入不生成 changelog。
3. OHLCV 变化生成 changelog，row version 递增。
4. 仅 `updated_at/batch_id` 变化不生成 changelog。
5. 复权因子变化生成 factor changelog。
6. API 按 `since_sequence` 升序分页。
7. quote-only 查询不返回 policy/governance/research 变化。
8. dry-run 不推进水位。
9. range backfill 修正历史行生成 changelog。
10. research API 默认响应不变。

集成验收：

- 日更任务报告能区分 changed/unchanged。
- 周期对账发现历史修订后能推进水位。
- 下游用保存的水位可以只补拉变化键。
- 关闭某域 changelog emission 后业务写入仍正常。

---

## 14. OpenSpec Change Review 基准

对 `add-daily-sync-change-watermarks` 的评审要求：

1. proposal 必须明确“本地已观测 CDC”边界，不能暗示上游完整 CDC。
2. design 必须说明 hash、changelog、API、水位、复权因子、派生数据、跨库顺序、rollback。
3. specs 必须包含可测试场景：
   - unchanged 不推进水位。
   - changed 推进水位。
   - API 按水位恢复。
   - default API 不变。
   - dry-run 不持久化。
   - policy/governance 域隔离。
4. tasks 必须按阶段拆分，不允许一口气全域改造。
5. 任务必须覆盖所有品种日更，而不仅股票。
6. 任务必须包含文档、测试、回滚和运维开关。

当前评审结论：

- OpenSpec change 的大方向正确。
- 需要补强：独立需求文档引用、域覆盖矩阵、P0/P1/P2/P3 分期、API 字段契约、hash 字段定义、OpenSpec 评审/验收任务。
- 本文档作为补强后的需求基准，后续实现必须先完成 `tasks.md` 的第 1 组 baseline audit，再进入代码修改。

---

## 15. 后续实施原则

1. 先实现最小可验证闭环：`daily_quotes + adjustment_factors + quote changes API + scheduler report`。
2. 不为了统一而强行重构所有存储层；已有 hash-aware 路径优先复用。
3. 不把 changelog 写失败变成默认业务写失败，灰度期应可配置降级；稳定后再评估是否强一致。
4. 所有新增 changelog 表和字段必须可迁移、可备份、可关闭。
5. 所有涉及财务、估值、行业、复权的变更必须明确可得时点和 lineage，宁可保守标记 invalidation，也不要隐式覆盖历史研究结论。

---

## 16. 2026-07-13 实施落地状态

### 16.1 本次已落地范围

本次实现 P0 闭环：

- `daily_quotes` 增加 `row_hash`、`row_version`。
- `adjustment_factors` 增加 `row_hash`、`row_version`。
- quote DB 增加 `data_change_log` 追加式变更日志。
- `save_daily_data` / `save_daily_quotes` 默认返回值保持 bool；传入 `return_stats=True` 时返回 `inserted / changed / unchanged / skipped / failed / changelog_written`。
- `save_adjustment_factors` 默认返回 int；传入 `return_stats=True` 时返回同样结构化计数。
- A 股主日更报告新增 `changelog_stats`，不改变原有 success/no-op 判断。
- 新增只读 API：
  - `GET /api/v1/changes/latest?domain=quotes&dataset=daily_quotes`
  - `GET /api/v1/changes?domain=quotes&dataset=daily_quotes&since_sequence=123&limit=1000`
  - `GET /api/v1/quotes/daily/changes?since_sequence=123&instrument_id=000001.SZ`
- `/api/v1/quotes/daily` 默认查询参数、响应结构和补拉方式不变。

### 16.2 P0 语义 hash 字段

`daily_quotes` hash 字段：

`instrument_id,time,open,high,low,close,volume,amount,turnover,pre_close,change,pct_change,tradestatus,factor,adjustment_type,is_complete,quality_score,source`

`adjustment_factors` hash 字段：

`instrument_id,ex_date,factor,cumulative_factor,dividend,bonus_shares,rights_shares,rights_price,event_type,source`

明确排除字段：

- `created_at`
- `updated_at`
- `batch_id`
- `ingestion_run_id`
- retry / timeout / diagnostics / report-only metadata
- API pagination/filter metadata

说明：

- `batch_id` 会进入 changelog 记录的追溯字段，但不进入语义 hash。
- 现有历史行如果没有 `row_hash`，第一次被相同业务内容覆盖时只补齐 hash/version，不追加 changelog。
- 复权因子变更使用 `adjustment_factor` 域，不把 raw quote 行误标为 changed。`adjust=none` 消费者只需要关注 raw quote；qfq/hfq 消费者需要同时关注 `quotes` 和 `adjustment_factor`。

### 16.3 调度任务审计与分期矩阵

| 阶段 | 域 | 典型启用任务 | 数据库/模块 | 业务键 | 当前状态 |
|---|---|---|---|---|---|
| P0 | A/H/US 股票、指数、ETF 日行情 | `daily_data_update`, `hk_daily_data_update`; `us_daily_data_update` 当前配置禁用 | quote DB / `database.operations` | `instrument_id + trade_date` | P0 写入路径已接入；调用方默认行为不变 |
| P0 | 复权因子 | 日更 Phase 2、weekly maintenance、factor backfill | quote DB / `adjustment_factors` | `instrument_id + ex_date` | P0 写入路径已接入 |
| P1 | 商品期货行情与连续序列 | `futures_market_data_sync`, `futures_market_data_backfill` | `data/futures.db` / futures storage | `contract_id/series_id + trade_date + source_mode` | 延后；应复用已有 hash-aware counters |
| P1 | FX 观测与派生 | `fx_rate_sync`, `fx_rate_backfill`, `fx_derivation_sync` | `data/fx.db` | `series_id + observation_date + revision/lineage` | 延后；dry-run/manual_only 不推进水位 |
| P1 | 特殊商品价格 | `special_commodity_price_sync`, `special_commodity_cn_spot_sync`, `special_commodity_price_monthly_sync` | research/commodity storage | `series_id + observation_date/period + source_profile` | 延后；价格域和政策域隔离 |
| P2 | 股东/财务/公告事实 | `shareholder_incremental_sync`, `shareholder_reconciliation_sync`, `financial_disclosure_incremental_sync`, `financial_disclosure_reconciliation_sync` | research/financial storage | `instrument_id + period/snapshot + source identity` | 延后；必须保留公告可得时点 |
| P2 | 行业/估值/利率/技术风险 | `industry_standard_sync`, `industry_index_analysis_sync`, `valuation_input_sync`, `valuation_history_rebuild`, `risk_free_rate_sync` | research storage | taxonomy/as-of/calc/input hash | 延后；现有 read API 不加默认字段 |
| P3 | 主数据/交易日历治理 | `trading_calendar_update`, `hkex_instrument_master_sync`, `a_share_stock_master_sync`, `index_master_governance_sync`, futures/FX/commodity governance | quote/futures/FX/research governance storage | lifecycle/calendar effective key | 延后；不进入 quote-only 查询 |
| P3 | 政策发现与候选治理 | `special_commodity_policy_discovery`, `special_commodity_policy_candidate_review`, catalog sync | policy/evidence storage | adapter/document/event/effective date | 延后；不影响市场数据消费者 |
| 不推进 | 只读/诊断/备份/依赖检查 | `system_health_check`, `market_dependency_version_check`, `cache_warm_up`, `database_backup`, diagnostics/recompute read-only jobs | n/a | n/a | 不写 changelog |

### 16.4 迁移与回滚

迁移文件：`database/migrations/004_add_change_watermarks.sql`。

运行时 schema guard：`DatabaseManager.initialize()` 会按需执行非破坏式建表/加列，避免已存在库因为缺列导致保存失败。该 guard 只新增：

- `daily_quotes.row_hash`
- `daily_quotes.row_version`
- `adjustment_factors.row_hash`
- `adjustment_factors.row_version`
- `data_change_log` 及查询索引

回滚方式：

- 不删除源表字段和 changelog 表。
- 如某域出现噪声，先停用该域写入路径的 changelog emission 或回退调用方使用原 API。
- 源表仍是权威数据；changelog 只是本地已观测增量信号。

### 16.5 已验证测试

已新增并通过的聚焦测试：

- `tests/unit/test_database/test_change_watermarks.py`
- `tests/unit/test_api/test_change_watermark_routes.py`
- `tests/unit/test_daily_update_report.py::test_generate_daily_update_report_includes_changelog_stats`
- `tests/unit/test_api/test_quote_capability_improvements.py::test_pagination_omitted_limit_returns_all`

验证点：

- 旧表迁移保留历史行。
- 新库空库支持创建 changelog。
- `daily_quotes` 插入生成 changelog。
- 相同 overlap 重复写入不推进水位。
- 仅 `batch_id` 变化不推进水位。
- OHLCV 变化推进水位并递增 row version。
- 复权因子修订进入 `adjustment_factor` 域。
- API 空水位、分页、domain/dataset 过滤可用。
- `/quotes/daily` 默认行为未回归。

### 16.6 未解决和后续风险

- P1/P2/P3 尚未把 futures、FX、commodity、research、governance、policy 写入路径接入 changelog；本次只是完成 P0 和审计矩阵。
- 当前 `data_change_log.sequence_id` 是 quote DB 内单库水位，不承诺跨 futures/research/FX 多库全局顺序。
- 详细 changelog 暂不做 retention/compaction；在消费者 checkpoint 策略确定前不得清理明细记录。
- 后续如果增加按域开关，默认应先灰度启用，避免某个数据源字段抖动制造噪声。
