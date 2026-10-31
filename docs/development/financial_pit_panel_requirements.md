# 全市场财务横截面物化 PIT 面板需求（financial_pit_panel）

> 版本：v1.2（2026-10-31）
>
> - v1.1：增补 §3.3 实质变更约束（更正防前视）与延期覆盖率验证；
> - v1.2：并入 quantwin 评审反馈（2026-10-31）的 4 项 P0 与 7 项 P1 答复：
>   双源拼接契约、覆盖缺口生命周期推导、归母口径普查结论、行业时点端点（方案 B）、
>   `effective_at`、`attribution_quality`、`is_complete`、changes 游标语义、
>   `revision_guard` 条件开关、report_period 格式统一；里程碑按 quantwin 建议重排。
>
> 数据基线：本库现有数据（2026-10-31 实测），**不做历史回补**。缺失即缺失，如实暴露。

---

## 1. 背景与目标

quantwin 是全 A 股月度调仓多因子选股程序，每期调仓需要对全体股票做横截面打分。
当前财务取数依赖 AKShare 的 `stock_yjbb_em` / `stock_yysj_em`，长期受东财网页端
限流与字段口径漂移影响。本库现有单股接口实测 78–101 秒/只，全市场不可用。

**目标**：基于本库现有数据，提供"按任意历史时点返回全市场当时可知财务横截面"的
批量接口，时点语义正确（无未来函数），性能满足全量拉取分钟级。

**核心方案**：物化 PIT 事件面板 + 服务端装配的时点驱动查询（§3、§4）。

**价值定位（quantwin 2026-10-31 确认）**：本面板自 2024 年起替代 AKShare 的
**增量取数与 PIT 正确性**；quantwin 训练所需更长历史段仍用 AKShare，双源在
2023Q4–2024Q2 重叠校验（§3.5）。

---

## 2. 现有数据基础（2026-10-31 实测）

### 2.1 可用地基

| 项目 | 实测值 |
|---|---|
| 覆盖股票 | 5,595 只（SSE 2,328 / SZSE 2,913 / BSE 354），≈当前在市全市场 |
| 成规模报告期 | **2023-12-31 → 2026-09-30 共 12 期**；历史各期 5,440–5,536 行，无截断；2026Q3 至 10-31 为 4,938 行（披露窗口当日截止，进行中） |
| 双源重叠校验期 | 2023Q4 / 2024Q1 / 2024Q2 = 5,523 / 5,440 / 5,469 行，可支撑 §3.5 对齐验证 |
| instrument-period 总数 | 65,501（`financial_facts`），(instrument, period) 唯一 |
| 宽表字段 | `financial_facts`：revenue、gross_profit、operating_profit、pre_tax_profit、net_income、operating_cf、total_cf、total_assets、total_liabilities、equity、current_assets、current_liabilities、inventory、receivables、fixed_assets、intangible_assets、shares_outstanding |
| 比率指标 | `financial_indicator_snapshots`：gross_margin、operating_margin、net_margin、roe、roa、current_ratio、quick_ratio、asset_liability_ratio、revenue_per_share、book_value_per_share、operating_cf_to_revenue、operating_cf_to_net_income |
| 真实公告日 | `financial_facts.publish_date`：仅 2026Q2（93%）与 2026Q3（99.9%）基本齐全；已抽查确认为真实披露日（2026Q2 落于 07-16~08-31，2026Q3 落于 10-10~10-31，与法定窗口吻合） |
| 披露事件 | `financial_disclosure_event_state`：12,252 行，9,566 行带真实 `announcement_time`，覆盖延期/无法按期披露 |
| 变更日志 | `data_change_log`：563 万行，已有索引 `(domain, sequence_id)`，游标语义现成 |
| 归母口径溯源 | `lineage_json.core_fact_alias_matches` 100% 有记录：equity 有值时 **100% 归母**（parent_attributable_equity）；net_income **95.5% 归母标记**（62,502/65,501），2,970 行（4.5%）有值但未记口径来源（M1 普查分类，见 §6.3-1） |
| 生命周期 universe | `quotes.db` instruments：373 只退市股带 `listed_date`/`delisted_date`，任意报告期末时点在市股票数可推导（§5.3） |
| 交易日历 | `quotes.db` `trading_calendar`：48,337 行，按交易所含 `is_trading_day`，支撑 `effective_at`（§3.2） |

### 2.2 硬限制（如实暴露，不修复）

| 限制 | 事实 | 对 quantwin 的影响 |
|---|---|---|
| 历史深度 | 2023-12-31 之前各期仅 1~3 行 | 回测起点最早 **2024 年初**（2023Q4 数据在 2024-04-30 前后可知）；训练长历史段用 AKShare（§3.5） |
| 退市股财务数据 | 财务表无退市股记录（抽查 5 只已退市股均 0 条） | **存活偏差存在**，但 universe 缺口可按期监控（§5.3 生命周期推导）；`is_delisted` 字段预留（§4.1） |
| 公告日回退 | 2026Q2 之前各期 `publish_date` 仅 1~2% 非空 | 早期时点 as-of 精度受 §3.3 回退策略约束 |
| 生命周期字段质量 | 2024Q1：161 只有财报行但 `listed_date` 晚于报告期末（疑似北交所重编码等）；80 只口径内无财报行（真缺口 ~1.5%） | §5.3 应有数推导需 M1 校准，双向差异均暴露 |
| vintage 体系 | `financial_filing_versions` 等表 0 行，rollout 关闭 | 本需求不启用、不回填 vintage |

基于本面板的回测结论应表述为"**当前在市股票集合内的因子排序证据**"，不得表述为
策略收益预期——存活偏差与窗口长度（12 期）均不支持后者。

### 2.3 与 quantwin 初稿的关键差异（v1.2 并入评审结论后）

| 初稿 | 本文档 | 理由 |
|---|---|---|
| 报告期驱动、客户端拼装 | **时点驱动，服务端装配** | PIT 装配逻辑收在服务端一次做对；`/pit/panel` 原始端点（M1）满足客户端自装配与整缓存 |
| include_delisted=true | **无该参数**；`is_delisted` 预留字段（恒 false）+ coverage 生命周期缺口 | 不做假承诺；接口形状为后续补退市股预留 |
| industry 入面板 | **方案 B：批量时点行业端点**（§5.5，M2） | 最新行业有前视；时点行业已有存储能力，补批量端点 |
| 2012Q1 起 56 期 | 2023Q4 起 12 期，双源拼接（§3.5） | 本库无更早数据，不回补 |
| eps 口径未验证 | 归母口径普查结论（§2.1）+ `attribution_quality` 标记（§4.1） | equity 已 100% 归母；net_income 4.5% 未标记行由 M1 普查分类 |

---

## 3. 语义定义

### 3.1 弱 PIT（时间严格 + 数值最新口径）

- **时间维度严格**：任一 `(instrument, report_period)` 记录只在其 `available_at`
  之后可见；as-of 查询绝不返回 `available_at > T` 的记录。
- **数值维度用最新**：同一报告期的追溯调整/更正取当前库内最新值（`updated_at`
  随之更新），并受 §3.3 实质变更约束防前视。

### 3.2 available_at 与 effective_at

- `available_at` = **公告日**：该期数据首次对市场可知的日期（巨潮/交易所披露日），
  不是报告期末日。例：2025 年报（report_period=2025-12-31）通常公告于
  2026-01~2026-04。
- `effective_at` = **生效日**（v1.2 新增）：`available_at` 之后的**次一交易日**，
  由 `trading_calendar`（SSE）推导。晚间披露的财报实际只能次日交易，服务端统一
  承接"披露日 + 1"规则，避免各客户端自行处理日历（法定节假日时"+1 天"与
  "次一交易日"不同）。客户端做执行时点语义时用 `effective_at`，做信息可知
  语义时用 `available_at`。

### 3.3 available_at 推导链（三级，带质量标记）

| 优先级 | 来源 | `available_at_quality` | 当前覆盖 |
|---|---|---|---|
| 1 | `financial_facts.publish_date`（真实披露日） | `exact` | 2026Q2 起 ~2 期 |
| 2 | `financial_disclosure_event_state.announcement_time`（按 instrument+period 匹配的披露事件） | `exact_event` | 事件驱动的补充 |
| 3 | **法定披露截止日回退**：Q1→04-30、半年报→08-31、Q3→10-31、年报→次年 04-30 | `statutory_deadline` | 2023Q4~2026Q1 各期主体 |

法定截止日是公告日的**上界**，用它回退只会**晚用**数据、不会早用，方向保守、
无前视。代价是截面"变新鲜"的时点被系统性推迟最多一个披露窗口。

**延期股特例**：延期披露的股票公告晚于法定截止日，单纯用截止日回退会造成前视。

1. 若该 `(instrument, period)` 在 `financial_disclosure_event_state` 中存在
   延期/无法按期披露事件，则 `available_at = max(法定截止日, 延期公告日 + 宽限参数)`，
   quality=`delayed_estimate`；
2. 宽限参数初值 90 天，实现阶段以 2026Q2/Q3（publish_date 齐全期）实测的延期股
   事件表命中率校准后固化到配置（§6.3-4）；
3. 覆盖端点（§5.3）暴露各级 quality 行数分布，quantwin 可选择只消费 `exact*`。

**实质变更约束（更正防前视，v1.1）**：弱 PIT 取库内最新数值，若不加约束，更正
公告发布**之前**的历史时点会提前用到更正后的数值（更正多为坏消息，属业绩高估
方向偏差）。规则：

- 面板行 `available_at = max(三级推导结果, 最后一次实质数值变更日)`；
- 实质变更日取 `data_change_log` 中该 `(instrument, report_period)` 相关记录里
  `old_hash ≠ new_hash` 的最大 `changed_at`——hash 未变的重解析不推进；
- 变更日晚于公告推导值时标记 `revised_after_publish = true` 并暴露
  `last_value_change_at`；
- 效果：更正发生前该期数据**整体不可见**（截面回退上一期），方向保守。

**revision_guard 条件开关（v1.2）**：M2 前实测两个数字——实质变更影响的
(instrument, period) 行数占比、更正导致的截面平均回退期数。若影响行数 >5% 或
平均回退 >1 期，cross-section 提供 `revision_guard` 参数：默认 `strict`（上述
约束生效）；`lenient` 时按原公告推导值返回可用性（**重新引入数值前视**，响应
标记 `guard=lenient`，openapi 与本文档明示风险）。未超阈值则不实现该参数。

### 3.4 派生指标的可计算边界（数据边界，非错误）

| 指标 | 首个可计算报告期 | 更早各期 |
|---|---|---|
| rev_yoy / np_yoy（1231 期，vs 上年年报） | 2024-12-31 | null |
| rev_yoy / np_yoy（0331/0630/0930 期，vs 上年同期） | 2025 年对应各期 | null |
| TTM（年报 + 本期累计 − 上年同期累计） | 2025-03-31 | null |

### 3.5 双源拼接契约（v1.2 新增，P0-1）

quantwin 历史段（2023Q4 之前）继续使用 AKShare，本面板自 2024 年起替代其增量
与 PIT 取数。为使拼接点不产生因子突变：

1. **口径对齐承诺**：`net_income` 为归母净利润、`equity` 为归母权益（§6.1 实测
   结论），`eps`/`bps` 按此口径派生；与 AKShare `stock_yjbb_em` 同字段口径一致；
2. **重叠校验**：quantwin 以 2023Q4 / 2024Q1 / 2024Q2 三期做双源对齐，`eps`/`bps`/
   `ocfps`/`roe` 差异 >2% 的股票数须 <1%，且每只差异股票能归因（追溯调整 /
   口径差异 / 单位差异）——验收 #12；
3. **交付说明**随 M4 验收输出拼接建议文档；两侧均为"最新口径"，理论上差异只应
   来自口径、单位与追溯调整时点差。

---

## 4. 核心设计：物化 PIT 事件面板

### 4.1 表结构 `financial_pit_panel`（financials.db）

一行 = 一个 `(instrument_id, report_period)` 事件，事件在 `available_at` 生效：

```text
instrument_id TEXT NOT NULL          -- 600519.SH
symbol TEXT, exchange TEXT           -- SSE/SZSE/BSE
report_period TEXT NOT NULL          -- 库内 ISO（2026-09-30）；API 层统一 YYYYMMDD
report_type TEXT, fiscal_year INT, fiscal_quarter INT
currency TEXT
available_at TEXT NOT NULL           -- §3.3 推导链结果（公告日语义）
announcement_available_at TEXT       -- 推导链原始值（未含实质变更推后），诊断与未来 guard 备用
effective_at TEXT                    -- available_at 后次一交易日（trading_calendar）
available_at_quality TEXT NOT NULL   -- exact / exact_event / statutory_deadline / delayed_estimate
is_delisted INTEGER NOT NULL DEFAULT 0  -- 预留字段：本期恒 0，无数据承诺，后续补退市股时启用
-- 原始报表值（源自 financial_facts，net_income/equity 归母口径，见 §6.1）
revenue, gross_profit, operating_profit, pre_tax_profit, net_income,
operating_cf, total_cf, total_assets, total_liabilities, equity,
current_assets, current_liabilities, inventory, receivables,
fixed_assets, intangible_assets, shares_outstanding
attribution_quality TEXT NOT NULL    -- parent / mixed / unverified（§6.2，D3 决策）
-- 比率（源自 financial_indicator_snapshots）
gross_margin, operating_margin, net_margin, roe, roa, current_ratio,
quick_ratio, asset_liability_ratio, revenue_per_share, book_value_per_share
-- 派生（面板构建时计算，含 §3.4 边界）
eps, bps, ocfps, rev_yoy, np_yoy, ttm_revenue, ttm_net_income
-- 血缘与版本
source_table_updated_at TEXT         -- 源行 updated_at，追溯调整探测
last_value_change_at TEXT            -- 最后一次实质数值变更日（data_change_log，old_hash≠new_hash）
revised_after_publish INTEGER        -- 1 = 变更日晚于公告推导值（available_at 被推后）
panel_version TEXT NOT NULL
built_at TEXT NOT NULL
PRIMARY KEY (instrument_id, report_period)
```

预估规模：65,501 行 × ~0.5KB ≈ 30–60MB。索引：`(available_at)`、
`(instrument_id, available_at)`。

### 4.2 构建与维护

- **一次性构建**：`financial_facts` JOIN `financial_indicator_snapshots`（按
  instrument_id + report_period），叠加 §3.3 推导链（含实质变更约束）与 §3.4
  派生计算；`attribution_quality` 按 lineage 普查结果标记（§6.3-1）；全量写入。
- **增量维护**：挂钩现有财务同步链路（`research/financial_statements_sync.py`
  落库成功后）刷新受影响 `(instrument, period)` 行；disclosure 事件更新触发
  `available_at` 重推导。调度复用现有 scheduler 任务位。
- **写入 owner 唯一**：新建 `research/financial_pit_panel.py`（模式对齐
  `FinancialVintageStore`），写经 financials.db coordinated writes；API 入口只转发
  （治理总纲 §2.1）。
- **幂等**：同一 panel_version 下重复构建结果不变；源行 `updated_at` 未变不重写。

### 4.3 查询语义

as-of 横截面（服务端装配）：

```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY instrument_id
    ORDER BY report_period DESC, available_at DESC
  ) AS rn
  FROM financial_pit_panel
  WHERE available_at <= :as_of
    AND (:exchange IS NULL OR exchange = :exchange)
    AND (:report_period IS NULL OR report_period = :report_period)
) WHERE rn = 1
```

6.5 万行小表 + 索引，单次查询毫秒~秒级；不逐票调用、不做请求期实时聚合。

---

## 5. API 设计

统一前缀 `/api/v1/research/financials/pit`，tags=`["Research Financials PIT"]`，
入口仅转发到 `research/financial_pit_panel.py`。

**格式约定（P1-1）**：API 参数与响应中 `report_period` / `as_of` 统一
`YYYYMMDD`；库内 ISO 日期不透出。openapi description 注明。

### 5.1 时点横截面（主接口）

```text
GET /api/v1/research/financials/pit/cross-section
    ?as_of=20260801              # 必填，YYYYMMDD
    &report_period=              # 可选，YYYYMMDD，时点装配后再按报告期过滤
    &exchange=                   # 可选，SSE/SZSE/BSE，可逗号分隔
    &fields=                     # 可选，逗号分隔字段投影；缺省全字段
    &revision_guard=strict       # 可选，strict|lenient（仅 §3.3 阈值触发时存在）
    &limit=1000&offset=0         # 单页上限 2000
```

响应（节选）：

```json
{
  "as_of": "20260801",
  "report_period": null,
  "total": 5536,
  "limit": 1000,
  "offset": 0,
  "guard": "strict",
  "universe_note": "universe=listed-only (5595 instruments); delisted not covered; latest_complete_period=20260630",
  "stats": {
    "period_lag_distribution": {"0": 5350, "1": 186},
    "attribution_quality": {"parent": 5490, "mixed": 40, "unverified": 6}
  },
  "items": [
    {
      "instrument_id": "600519.SH",
      "symbol": "600519",
      "exchange": "SSE",
      "report_period": "20260630",
      "available_at": "20260726",
      "effective_at": "20260727",
      "available_at_quality": "exact",
      "attribution_quality": "parent",
      "is_delisted": false,
      "currency": "CNY",
      "eps": 31.05, "bps": 199.0, "ocfps": 32.6,
      "roe": 15.6, "gross_margin": 91.4,
      "rev_yoy": 11.2, "np_yoy": 13.8,
      "ttm_revenue": null,
      "source_table_updated_at": "2026-08-29T...",
      "last_value_change_at": null,
      "revised_after_publish": false,
      "panel_version": "v1",
      "built_at": "2026-10-31T..."
    }
  ]
}
```

`stats.period_lag_distribution` = 返回行相对该票最新已公告报告期的滞后分布
（0=当期，1=回退一期），支撑验收 #17；`available_at` 为公告日语义，执行时点
用 `effective_at`（§3.2，P1-2）。

### 5.2 面板原始访问（M1 起可用，客户端整缓存用）

```text
GET /api/v1/research/financials/pit/panel
    ?report_period=20260630       # 可选，YYYYMMDD
    &instrument_id=               # 可选
    &available_at_lte=            # 可选，YYYYMMDD
    &limit=2000&offset=0
```

返回面板原始行（含全部 quality 与血缘字段），供 quantwin 一次性缓存到本地后
离线装配。quantwin 双源拼接依赖此端点（§3.5）。

### 5.3 覆盖自检（含生命周期应有数与完整期标记）

```text
GET /api/v1/research/financials/pit/coverage
    ?report_period=20260930       # 可选，YYYYMMDD；缺省返回逐期汇总
```

逐期返回：

- **实际股票数**（面板行数）；
- **应有股票数**：生命周期推导——`type=stock AND exchange IN (SSE,SZSE,BSE)
  AND listed_date ≤ 期末 AND (delisted_date 为空 OR > 期末)`（instruments 表）；
- **缺口数** = 应有 − 实际；
- **反向差异**（校准指标）：有财报行但生命周期不符的行数（如 2024Q1 为 161），
  M1 校准任务消除或解释（§6.3-5）；
- `available_at_quality` 分布（exact / exact_event / statutory_deadline /
  delayed_estimate 各多少行）、`revised_after_publish` 行数、
  `attribution_quality` 分布（parent/mixed/unverified 占比）；
- **完整期标记**：`is_complete = 评估时点 > 该期法定披露截止日`（时间判据，
  非计数判据），另附 `latest_complete_period`。2026Q3（截止 2026-10-31）自
  2026-11-01 起标记完整；当期不完整时 cross-section 的 `universe_note` 同步携带
  完整状态（P1-6、验收 #15）；
- 派生字段非空率、`source_table_updated_at` 近 N 天变化行数（追溯调整监控）。

应有数推导口径在响应中显式声明；数字须与面板实查一致（验收 #16）。
**应有/缺口仅对成规模报告期（2023-12-31 起）计算**；更早的稀疏期（实际 1~3 行）
只报实际数并标记 sparse，不计算应有/缺口——对稀疏期按生命周期推导会得到
4,000+ 的荒谬缺口，误导存活偏差声明。

### 5.4 增量变更流

```text
GET /api/v1/research/financials/pit/changes
    ?cursor=                      # sequence_id 游标，首次调用省略
    &limit=2000
```

游标语义（P1-5）：以 `data_change_log.sequence_id` **单调递增**为游标（复合游标
不必要），走既有 `(domain, sequence_id)` 索引；同一 sequence_id 重复消费幂等
（消费方按键去重，重放安全）；响应返回 `entries[(instrument_id, report_period,
changed_at, change_type)]` 与 `next_cursor`。追加型变更流，游标只前进。

### 5.5 行业时点批量端点（v1.2 新增，方案 B，M2 交付）

```text
GET /api/v1/research/industries/as-of
    ?as_of=20260801               # 必填，YYYYMMDD
    &taxonomy_system=             # 可选，缺省默认分类体系
    &exchange=                    # 可选
    &limit=2000&offset=0
```

响应 items：`instrument_id / industry_code / industry_name / taxonomy_system /
effective_from / effective_to`。实现基于 `industry_classification_history`
（`official_start_date`，单票 as-of 逻辑已有：`get_industry_membership_as_of`，
storage.py REQ-07.2），本端点补**批量**形态：每票取 as_of 当日生效的最新分类；
**as_of 当日无生效分类的股票不返回**（不回填最新分类，保持时点语义）。
与 P0-4 要求的字段契约一致；quantwin 行业中性化/分组按此关联（验收 #14）。

---

## 6. 字段口径

### 6.1 原始报表值（元 / 股，归母口径实测结论）

`revenue` 营业总收入、`net_income` **归母净利润**、`operating_cf` 经营活动现金
流量净额（累计）、`equity` **归母权益**、`total_assets` 总资产、
`shares_outstanding` 报告期末总股本。均为累计（年初至报告期末）口径，不预年化。

口径依据（2026-10-31 lineage 全量普查）：provider 映射归母优先
（`PARENT_NETPROFIT`、`parent_holder_equity_total` 等在前，合并口径仅兜底）；
equity 有值时 100% 命中 `parent_attributable_equity`；net_income 95.5% 命中
`parent_attributable_net_profit`，2,970 行（4.5%）有值但未记来源（§6.3-1 普查
分类后按 §6.2 标记）。

### 6.2 每股与增长（面板构建时派生）

- `eps = net_income / shares_outstanding`；`bps = equity / shares_outstanding`；
  `ocfps = operating_cf / shares_outstanding`（分母为 0 或缺失时整体 null）；
- **分母解析策略（2026-10-31 实测定案）**：宽表股本 0% 填充，分母回退长表
  `share_capital_amount`（~90% 正确，少数来源把权益科目错映射进该 canonical
  名，如 600398.SH 为 0.31×真值）；仅当窗口内 valuation 隐含总股本
  （market_cap/close，2024-04-30 起）与 fact 偏差 ≥50%（灾难级）时替换，
  其余一律保留 fact——valuation 序列自身对一批股票偏低 ~18% 且缺 920xxx
  迁移代码，不能作普通基准。详见 openspec change 任务记录 §10；
- `rev_yoy / np_yoy`（%）：本期累计 vs 上年同期累计，边界见 §3.4；
- `ttm_revenue / ttm_net_income`：本期累计 + 上年年报 − 上年同期累计；
- **单位契约（v1.2 审计补注）**：`roe / gross_margin / operating_margin /
  net_margin / roa / asset_liability_ratio` 一律**百分数**（15.6 表示 15.6%，
  与 AKShare yjbb 口径一致；指标源表存分数，面板构建时 ×100）；
  `current_ratio / quick_ratio` 为倍数；`revenue_per_share /
  book_value_per_share / eps / bps / ocfps` 为元；`rev_yoy / np_yoy` 为百分数。
- **`attribution_quality`**（D3 决策：标记照算，不返回 null）：
  - `parent`：net_income 与 equity 的 lineage 语义均为归母；
  - `mixed`：任一字段兜底命中合并口径别名；
  - `unverified`：有值但 lineage 无来源记录（当前 4.5%），M1 普查分类后尽可能
    转为前两类；
  - coverage 与 cross-section stats 暴露三类占比；quantwin 按 §3.5 的 >2% 差异
    规则自行处理。剔除会造成截面缺口，比口径偏差伤害更大，故不返回 null。

### 6.3 实现前必须完成的核验（M1/M2 门槛）

1. **归母口径普查（P0-3，阻塞 M1 编码）**：①lineage 程序化普查已在需求阶段完成
   初版（§6.1）；M1 对 2,970 行未标记记录按 source/source_mode/ingestion_run 与
   numeric_facts 长表分类，输出三类占比；②抽 5 家已知公司（贵州茅台、中国石化、
   工商银行、比亚迪、宁德时代——覆盖少数股东权益占比差异大的样本）对公开年报，
   eps/bps 相对误差 ≤1%（验收 #13）；③结论固化于字段注释、openapi description、
   本文档；
2. **publish_date 一致性**：抽 10 只 2026Q3 样本对巨潮披露时间核验；
3. **延期宽限参数**：以 disclosure_event_state 实测分布校准；
4. **延期股事件表覆盖率**：2026Q2/Q3 上统计 `publish_date > 法定截止日` 公司的
   事件表命中率，不足时先调整宽限/回退策略再验收 M2；
5. **生命周期应有数校准（v1.2 新增）**：对 161/274 只 `listed_date` 晚于报告期
   却有财报行的股票逐类归因（北交所重编码 / 吸收合并重新上市 / 字段错误），
   修正 instruments 推导口径或在校验报告中解释；消除后 §5.3 反向差异应趋零；
6. **更正约束影响面（M2 前置）**：data_change_log 实测实质变更影响行数占比与
   平均回退期数，决定 §3.3 revision_guard 是否实现。

### 6.4 缺失值规则（强制）

数值字段缺失一律 JSON `null`。**禁止**返回 `0`、`""`、`"-"`、`"0.00%"` 等占位值
——0 会作为真实财务值参与截面排序，直接污染因子。合法的数值 0 照常返回 0，
与缺失可区分。

---

## 7. 性能要求

| 指标 | 目标 |
|---|---|
| cross-section 单页 1000 行 | 热查询 P95 ≤ 1s，冷启动 ≤ 3s |
| panel / coverage / industries-as-of 分页 2000 行 | ≤ 3s |
| 全量拉取（12 期 × ~5,500 行 ≈ 6.6 万行） | ≤ 2 分钟 |
| 面板增量刷新（单次同步后） | ≤ 30s |
| 服务实现方式 | 预计算落库（§4），禁止请求期实时聚合 |

---

## 8. 验收标准（可测试）

1. **as-of 正确性**：`as_of=20260801` 时，任一返回行 `available_at ≤ 20260801`
   且 `report_period ≤ 20260630`；2026Q2 中披露晚于 08-01 的股票自动回退更早报告期；
2. **延期股不前视**：延期样本在 `delayed_estimate` 的 available_at 前不可见；
3. **法定回退不早用**：`statutory_deadline` 行 available_at ≥ 对应法定截止日；
4. **派生边界**：2023-12-31 期 `rev_yoy` 全 null；2024-12-31 期非 null 率与
   2023 年报覆盖一致；2025-03-31 起 `ttm_*` 可算；
5. **无占位值**：全面板数值字段无 0 占位（合法 0 除外，须能区分）；
6. **覆盖一致**：coverage 各数字（实际/应有/缺口/质量分布）与面板及生命周期
   推导实查一致；
7. **幂等**：同版本重复构建，行数与内容 hash 不变；
8. **增量挂钩**：模拟一次 upsert 后受影响面板行自动刷新且 changes 流可见；
9. **更正防前视**：hash 实质变更且晚于公告推导值时 available_at 推进至变更日、
   `revised_after_publish=1`，此前查询回退；hash 未变不推进；
10. **延期覆盖率实测**：§6.3-4 命中率有数字并记入开发记录，达标后验收 M2；
11. **性能**：§7 各项实测达标；
12. **双源重叠对齐（P0-1）**：2023Q4/2024Q1/2024Q2 与 AKShare 同字段
    （eps/bps/ocfps/roe）差异 >2% 的股票数 <1%，每只差异股票给出原因分类
    （追溯调整/口径/单位）；
13. **归母口径确认（P0-3）**：5 家样本 eps/bps 与公开年报相对误差 ≤1%；
14. **行业可用性（P0-4）**：industries/as-of 端点可对指定 as_of 返回全市场结果，
    字段含 effective_from/effective_to；
15. **完整期标记（P1-6）**：不完整报告期被正确标记，cross-section 的
    universe_note 携带当期完整状态与 latest_complete_period；
16. **覆盖缺口可读（P0-2）**：coverage 逐期输出应有/实际/缺口（生命周期口径），
    允许一次性抽样核对；
17. **回退影响可观测（P1-4）**：cross-section 响应 stats 暴露回退期数分布，
    revision_guard 阈值实测数字记入开发记录。

---

## 9. 非目标

- 不回补 2023Q4 之前的历史财务数据（quantwin 历史段用 AKShare，§3.5）；
- 不补退市股财务数据（存活偏差如实声明，缺口按期监控；`is_delisted` 仅预留）；
- 不做强 PIT / vintage 数值还原（不启用 `financial_filing_vintages`）；
- 不提供 `include_delisted` 参数（无数据支撑，避免误导调用方）；
- 不在本需求内做行情/估值批量接口（见 §10）。

## 10. 关联后续（独立立项，不阻塞本需求）

- **P1**：`/quotes/daily/bulk` 单日全市场行情批量；`turnover` 恒为 0 缺陷修复；
- **P2**：估值批量（`valuation.db` 已有 7.8GB 本地数据，流量可忽略）。

## 11. 里程碑（v1.2 按 quantwin 建议重排，总工作量不变）

- **M1 面板构建与最小可对接**：`financial_facts` 全量构建（12 个成规模报告期 +
  稀疏行，约 6.5 万行）+ cross-section + `/pit/panel` + coverage 最小版（实际/
  应有/缺口 + quality 分布）；§6.3-1/2/5 三项核验完成并固化；
- **M2 推导链完整化**：三级推导 + 实质变更约束 + 延期特则 + §6.3-3/4/6 核验与
  实测（含 revision_guard 阈值判定）+ industries/as-of 端点（§5.5）；
- **M3 派生与增量**：每股/YoY/TTM 派生（含边界）+ attribution_quality 标记 +
  changes 端点 + 同步管线增量挂钩；
- **M4 测试与回归**：§8 全部验收项（含 #12 双源对齐与拼接建议文档）+ 金融语义
  测试（小型 fixture，不依赖实时网络）；
- **M5 清理、Review、提交**。

## 12. 风险与待确认

| 风险 | 说明 | 缓解 |
|---|---|---|
| 早期时点 as-of 精度 | 2026Q2 前各期靠法定截止日回退，截面新鲜度系统性滞后最多一个披露窗口 | quality 分级暴露于 coverage，可只消费 exact* |
| 延期股事件表完整性 | 无事件记录的晚披露公司会造成真前视 | §6.3-4 覆盖率实测门槛 + 宽限参数校准 |
| 4.5% net_income 未标记来源 | 归母口径无法程序确认 | §6.3-1 普查分类 + attribution_quality 标记照算（D3） |
| 生命周期字段质量 | listed_date 晚于报告期的存量行（2024Q1 161 只） | §6.3-5 校准任务 + coverage 双向差异暴露 |
| hash 粒度过细 | 无实质变化的更新被判为实质变更 → 过度保守 | 实现阶段评估字段级比较；宁可保守不可前视 |
| 口径不一致风险 | 与 AKShare 双源口径若不一致，拼接点因子突变 | §3.5 重叠校验 + 验收 #12；两侧同为最新口径 |
| financials.db 活跃写入 | 76GB 库有日常同步 | coordinated writes + 增量刷新 bounded ≤30s |
| 当期披露未完 | 2026Q3 至 10-31 4,938 条（预期 ~5,550） | is_complete 时间判据 + 增量刷新，coverage 反映实际 |
| 交易日历视界有限 | trading_calendar 当前 SSE/SZSE 至 2026-12-30、BSE 仅至 2026-11-06，需随日常维护刷新 | 超出视界时 effective_at 返回 null（不近似）；日历刷新属现有调度职责 |

## 13. quantwin 字段映射（联调对齐，v1.2 新增）

| quantwin 内部 | 面板字段 | 转换规则 |
|---|---|---|
| code | instrument_id | 取前 6 位 |
| period | report_period | API 层已统一 YYYYMMDD |
| avail | available_at / effective_at | 信息可知用 available_at（公告日）；执行时点用 effective_at（次一交易日，服务端已 +1） |
| eps / bps / ocfps / roe / gross_margin | 同名 | 直接使用（累计不年化，客户端按报告期折算）；attribution_quality≠parent 的行自行决定取舍 |
| rev_yoy / np_yoy | 同名 | 直接使用；2024-12-31 前为 null，历史段用 AKShare 自带同比 |
| industry | （§5.5 端点） | 按 as_of 关联 industries/as-of，取 industry_code/industry_name |
| is_delisted | is_delisted | 本期恒 false，预留 |
