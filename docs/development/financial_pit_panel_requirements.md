# 全市场财务横截面物化 PIT 面板需求（financial_pit_panel）

> 版本：v1.1（2026-10-31，基于当日对 `data/financials.db` 的实测；
> v1.1 增补 §3.3 实质变更约束（更正防前视）与 §6.3 延期覆盖率验证）
>
> 本文档在 quantwin 初稿《stock_server 财务数据批量接口需求》基础上重构而成，
> 是后续开发的唯一需求依据。与初稿的差异及理由见 §2.3。
>
> 数据基线：本库现有数据，**不做历史回补**。缺失即缺失，如实暴露。

---

## 1. 背景与目标

quantwin 是全 A 股月度调仓多因子选股程序，每期调仓需要对全体股票做横截面打分。
当前财务取数依赖 AKShare 的 `stock_yjbb_em` / `stock_yysj_em`，长期受东财网页端
限流与字段口径漂移影响。本库现有单股接口
（`/research/company/{instrument_id}/financial-statements/history`）实测 78–101 秒/只，
全市场不可用。

**目标**：基于本库现有数据，提供"按任意历史时点返回全市场当时可知财务横截面"的
批量接口，时点语义正确（无未来函数），性能满足全量拉取分钟级。

**核心方案**：物化 PIT 事件面板 + 服务端装配的时点驱动查询（详见 §3、§4）。

---

## 2. 现有数据基础（2026-10-31 实测）

### 2.1 可用地基

| 项目 | 实测值 |
|---|---|
| 覆盖股票 | 5,595 只（SSE 2,328 / SZSE 2,913 / BSE 354），≈当前在市全市场 |
| 成规模报告期 | **2023-12-31 → 2026-09-30 共 12 期**，每期约 5,500 条 |
| instrument-period 总数 | 65,501（`financial_facts`） |
| 宽表字段 | `financial_facts`：revenue、gross_profit、operating_profit、pre_tax_profit、net_income、operating_cf、total_cf、total_assets、total_liabilities、equity、current_assets、current_liabilities、inventory、receivables、fixed_assets、intangible_assets、shares_outstanding |
| 比率指标 | `financial_indicator_snapshots`：gross_margin、operating_margin、net_margin、roe、roa、current_ratio、quick_ratio、asset_liability_ratio、revenue_per_share、book_value_per_share、operating_cf_to_revenue、operating_cf_to_net_income |
| 真实公告日 | `financial_facts.publish_date`：**仅 2026Q2（93%）与 2026Q3（99.9%）基本齐全**；已抽查确认为真实披露日（2026Q2 落于 07-16~08-31，2026Q3 落于 10-10~10-31，与法定窗口吻合） |
| 披露事件 | `financial_disclosure_event_state`：12,252 行，其中 9,566 行带真实 `announcement_time`，覆盖延期/无法按期披露等异常事件 |
| 变更日志 | `data_change_log`：563 万行，含 domain/dataset/business_key/changed_at，可直接支撑增量同步 |

### 2.2 硬限制（如实暴露，不修复）

| 限制 | 事实 | 对 quantwin 的影响 |
|---|---|---|
| 历史深度 | 2023-12-31 之前各期仅 1~3 行（2021-12-31 1 行、2022-12-31 3 行、2023Q1–Q3 各 2 行），不可用于截面 | 回测起点最早 **2024 年初**（2023Q4 数据在 2024-04-30 前后可知） |
| 退市股覆盖 | 抽查乐视 300104、康得新 002450 等 5 只已退市股均为 0 条；5,595 只 ≈ 当前在市 | 面板存在**存活偏差**，2023Q4 以来退市的股票缺失，需在研究结果中声明 |
| 公告日回退 | 2026Q2 之前各期 `publish_date` 仅 1~2% 非空 | 早期时点的 as-of 精度受 §3.3 回退策略约束 |
| vintage 体系 | `financial_filing_versions` 等表 0 行，rollout 关闭 | 本需求**不启用、不回填** vintage（见 §2.3） |

基于本面板的回测结论应表述为"**当前在市股票集合内的因子排序证据**"，不得表述为
策略收益预期——存活偏差与窗口长度（12 期）均不支持后者。

### 2.3 与 quantwin 初稿的关键差异

| 初稿 | 本文档 | 理由 |
|---|---|---|
| 报告期驱动：`report_period` 必填，as_of 只过滤 publish_date，"未公告→回退上一期"由客户端拼装 | **时点驱动**：`as_of` 为主参数，服务端直接返回每票"截至 T 最新可知报告期"；报告期过滤保留为可选参数 | 装配逻辑（回退、延期、跨期拼接）蕴含 PIT 正确性，收在服务端一次做对，客户端不可能用错；面板只有 6.5 万行，服务端装配成本可忽略 |
| 2012Q1 起 56 期 | 2023Q4 起 12 期 | 本库无更早数据，不回补 |
| include_delisted=true | 参数保留但当前为 no-op（库内无退市股数据），响应显式带 `universe_note` | 不做假承诺 |
| 响应含 industry、is_delisted | 移出本期范围 | industry 随最新年报有前视风险，时点化行业已有独立端点（`get_industry_membership_as_of`，REQ-07.2），另行对接；is_delisted 因数据缺失无意义 |
| eps 口径未验证 | 保留字段但标注口径待核（§6.3） | `financial_facts.net_income` 的归母/合并口径未经样例核验 |

---

## 3. 语义定义

### 3.1 弱 PIT（时间严格 + 数值最新口径）

- **时间维度严格**：任一 `(instrument, report_period)` 记录只在其 `available_at` 之后可见；
  as-of 查询绝不返回 `available_at > T` 的记录。
- **数值维度用最新**：同一报告期的追溯调整/更正，取当前库内最新值，不还原历史口径
  （`updated_at` 随之更新，客户端可感知修正）。

对月频多因子选股，该语义足以消除未来函数；强 PIT（还原当时数值的 vintage 体系）
成本高一个量级，明确不在本期范围。

### 3.2 available_at 语义

`available_at` = 该期数据**首次对市场可知**的日期（公告/披露日），区别于报告期末日。
例：2025 年报（report_period=2025-12-31）通常 available_at 落在 2026-01~2026-04。

### 3.3 available_at 推导链（三级，带质量标记）

| 优先级 | 来源 | `available_at_quality` | 当前覆盖 |
|---|---|---|---|
| 1 | `financial_facts.publish_date`（真实披露日） | `exact` | 2026Q2 起 ~2 期 |
| 2 | `financial_disclosure_event_state.announcement_time`（按 instrument+period 匹配的披露事件） | `exact_event` | 事件驱动的补充 |
| 3 | **法定披露截止日回退**：Q1→04-30、半年报→08-31、Q3→10-31、年报→次年 04-30 | `statutory_deadline` | 2023Q4~2026Q1 各期主体 |

法定截止日是公告日的**上界**（公告必然不晚于截止日），用它回退只会**晚用**数据、
不会早用，方向上保守、无前视。代价是截面"变新鲜"的时点被系统性推迟最多一个
披露窗口。

**延期股特例**：延期披露的股票公告晚于法定截止日，单纯用截止日回退会造成前视。
处理规则：

1. 若该 `(instrument, period)` 在 `financial_disclosure_event_state` 中存在
   延期/无法按期披露事件，则 `available_at = max(法定截止日, 延期公告日 + 宽限参数)`，
   quality=`delayed_estimate`；
2. 宽限参数初值 90 天，实现阶段须用事件表实测分布校准后固化到配置；
3. 覆盖端点（§5.3）暴露各级 quality 的行数分布，quantwin 可选择只消费 `exact*`。

**实质变更约束（更正防前视，v1.1 新增）**：弱 PIT 取库内最新数值，若不加约束，
更正公告发布**之前**的历史时点会提前用到更正后的数值——财报更正多数是坏消息
（虚增修正、审计调整），回测等于提前避开这些问题公司，属**业绩高估方向**的偏差。
规则：

- 面板行 `available_at = max(三级推导结果, 最后一次实质数值变更日)`；
- 实质变更日取 `data_change_log` 中该 `(instrument, report_period)` 相关记录里
  `old_hash ≠ new_hash` 的最大 `changed_at`——hash 未变的重解析、字段映射重跑
  等**不构成**实质变更，不推进 available_at；
- 变更日晚于公告推导值时，面板行标记 `revised_after_publish = true` 并暴露
  `last_value_change_at`，coverage 端点（§5.3）统计此类行数；
- 效果：更正发生前，该期数据对 as-of 查询**整体不可见**（截面回退上一期），
  而非提前使用更正值。代价是极少数股票在更正窗口附近截面变旧，方向保守。

### 3.4 派生指标的可计算边界（数据边界，非错误）

YoY 需要 T-1 年同期、TTM 需要上年年报 + 上年同期累计。按现有覆盖推得：

| 指标 | 首个可计算报告期 | 更早各期 |
|---|---|---|
| rev_yoy / np_yoy（1231 期，vs 上年年报） | 2024-12-31 | null |
| rev_yoy / np_yoy（0331/0630/0930 期，vs 上年同期） | 2025 年对应各期 | null |
| TTM（年报 + 本期累计 − 上年同期累计） | 2025-03-31 | null |

面板构建时直接算好落库，边界之外的字段返回 null，禁止用 0 或插值顶替。

---

## 4. 核心设计：物化 PIT 事件面板

### 4.1 表结构 `financial_pit_panel`（financials.db）

一行 = 一个 `(instrument_id, report_period)` 事件，事件在 `available_at` 生效：

```text
instrument_id TEXT NOT NULL          -- 600519.SH
symbol TEXT, exchange TEXT           -- SSE/SZSE/BSE
report_period TEXT NOT NULL          -- 2026-09-30
report_type TEXT, fiscal_year INT, fiscal_quarter INT
currency TEXT
available_at TEXT NOT NULL           -- §3.3 推导链结果
available_at_quality TEXT NOT NULL   -- exact / exact_event / statutory_deadline / delayed_estimate
-- 原始报表值（源自 financial_facts）
revenue, gross_profit, operating_profit, pre_tax_profit, net_income,
operating_cf, total_cf, total_assets, total_liabilities, equity,
current_assets, current_liabilities, inventory, receivables,
fixed_assets, intangible_assets, shares_outstanding
-- 比率（源自 financial_indicator_snapshots）
gross_margin, operating_margin, net_margin, roe, roa, current_ratio,
quick_ratio, asset_liability_ratio, revenue_per_share, book_value_per_share
-- 派生（面板构建时计算，含 §3.4 边界）
eps, bps, ocfps, rev_yoy, np_yoy, ttm_revenue, ttm_net_income
-- 血缘与版本
source_table_updated_at TEXT         -- 源行 updated_at，追溯调整探测
last_value_change_at TEXT            -- 最后一次实质数值变更日（data_change_log，old_hash≠new_hash）
revised_after_publish INTEGER        -- 1 = 变更日晚于公告推导值，available_at 被推后
panel_version TEXT NOT NULL          -- 构建版本
built_at TEXT NOT NULL
PRIMARY KEY (instrument_id, report_period)
```

预估规模：65,501 行 × ~0.5KB ≈ **30–60MB**，可整体载入内存做向量化回测。
索引：`(available_at)`、`(instrument_id, available_at)`。

### 4.2 构建与维护

- **一次性构建**：`financial_facts` JOIN `financial_indicator_snapshots`（按
  instrument_id + report_period），叠加 §3.3 推导链（含实质变更约束）与
  §3.4 派生计算，全量写入。
- **增量维护**：挂钩现有财务同步链路（`research/financial_statements_sync.py`
  落库成功后）刷新受影响 `(instrument, period)` 行；`available_at` 推导依赖的
  disclosure 事件更新时同样触发。调度复用现有 scheduler 任务位。
- **写入 owner 唯一**：新建 `research/financial_pit_panel.py`（模式对齐
  `FinancialVintageStore`：own 表 + own 构建 + own 查询，经 financials.db
  coordinated writes）；API 入口只转发，不写业务循环（§10.2）。
- **幂等**：同一 panel_version 下重复构建结果不变；源行 `updated_at` 未变不重写。

### 4.3 查询语义

as-of 横截面（服务端装配）：

```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITITION BY instrument_id
    ORDER BY report_period DESC, available_at DESC
  ) AS rn
  FROM financial_pit_panel
  WHERE available_at <= :as_of
    AND (:exchange IS NULL OR exchange = :exchange)
    AND (:report_period IS NULL OR report_period = :report_period)
) WHERE rn = 1
```

6.5 万行小表 + 索引，单次查询毫秒~秒级；不逐票调用、不做实时聚合。

---

## 5. API 设计

统一前缀 `/api/v1/research/financials/pit`，tags=`["Research Financials PIT"]`，
入口仅转发到 `research/financial_pit_panel.py`。

### 5.1 时点横截面（主接口）

```text
GET /api/v1/research/financials/pit/cross-section
    ?as_of=2026-08-01            # 必填，date；返回每票截至该日最新可知报告期
    &report_period=              # 可选，YYYYMMDD；指定时点后再按报告期过滤
    &exchange=                   # 可选，SSE/SZSE/BSE，可逗号分隔
    &fields=                     # 可选，逗号分隔字段投影；缺省返回全字段
    &limit=1000&offset=0         # 单页上限 2000
```

响应：

```json
{
  "as_of": "2026-08-01",
  "report_period": null,
  "total": 5536,
  "limit": 1000,
  "offset": 0,
  "universe_note": "universe=listed-only (5595 instruments); delisted not covered",
  "items": [
    {
      "instrument_id": "600519.SH",
      "symbol": "600519",
      "exchange": "SSE",
      "report_period": "2026-06-30",
      "available_at": "2026-07-26",
      "available_at_quality": "exact",
      "currency": "CNY",
      "revenue": 81000000000.0,
      "net_income": 39000000000.0,
      "operating_cf": 41000000000.0,
      "equity": 250000000000.0,
      "total_assets": 300000000000.0,
      "shares_outstanding": 1256197800,
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

### 5.2 面板原始访问（客户端整缓存用）

```text
GET /api/v1/research/financials/pit/panel
    ?report_period=20260630       # 可选；缺省按 available_at 倒序全量分页
    &instrument_id=               # 可选
    &available_at_lte=            # 可选
    &limit=2000&offset=0
```

返回面板原始行（含全部 quality 行），供 quantwin 一次性缓存到本地后离线装配。

### 5.3 覆盖自检

```text
GET /api/v1/research/financials/pit/coverage
    ?report_period=20260930       # 可选；缺省返回逐期汇总
```

返回：该期应有/实际股票数、缺失数、`available_at_quality` 分布（exact /
exact_event / statutory_deadline / delayed_estimate 各多少行）、
`revised_after_publish` 行数（更正后移监控）、派生字段非空率、
`source_table_updated_at` 近 N 天变化行数（追溯调整监控）。

### 5.4 增量变更流

```text
GET /api/v1/research/financials/pit/changes
    ?since=2026-10-01T00:00:00    # 或 cursor=
    &limit=2000
```

基于 `data_change_log`（domain=financials）过滤面板受影响行，返回
`(instrument_id, report_period, changed_at, change_type)`，客户端据此增量重拉。
数据为追加型变更流，游标向前。

---

## 6. 字段口径

### 6.1 原始报表值（元 / 股）

`revenue` 营业总收入、`net_income` 净利润（**归母 vs 合并口径待 §6.3 核验**）、
`operating_cf` 经营活动现金流净额（累计）、`equity` 净资产、`total_assets` 总资产、
`shares_outstanding` 报告期末总股本。均为累计（年初至报告期末）口径，**不预年化**，
由客户端按报告期折算。

### 6.2 每股与增长（面板构建时派生）

- `eps = net_income / shares_outstanding`；`bps = equity / shares_outstanding`；
  `ocfps = operating_cf / shares_outstanding`（分母为 0 或缺失时整体返回 null）；
- `rev_yoy / np_yoy`（%）：本期累计 vs 上年同期累计，服务端计算，边界见 §3.4；
- `ttm_revenue / ttm_net_income`：本期累计 + 上年年报 − 上年同期累计。

### 6.3 实现前必须核验的口径

1. `financial_facts.net_income` 是归母净利润还是含少数股东——抽 3~5 家已知公司
   对公开数值核验后在字段注释与本文档固化结论；
2. `publish_date` 与交易所披露日的一致性——已抽查分布合理，再抽 10 只对巨潮
   公告时间核验；
3. 延期股宽限参数——用 `financial_disclosure_event_state` 实测分布校准 §3.3；
4. 延期股事件表覆盖率——在 `publish_date` 齐全的 2026Q2/Q3 上，统计
   `publish_date > 法定截止日` 的公司中 `financial_disclosure_event_state` 的
   命中率（库内可直接测得），作为 §3.3 延期特则有效性的直接证据；命中率不足时
   须先调整宽限参数或回退策略，方可验收 M2。

### 6.4 缺失值规则（强制）

数值字段缺失一律 JSON `null`。**禁止**返回 `0`、`""`、`"-"`、`"0.00%"` 等占位值
——0 会作为真实财务值参与截面排序，直接污染因子。

---

## 7. 性能要求

| 指标 | 目标 |
|---|---|
| cross-section 单页 1000 行 | 热查询 P95 ≤ 1s，冷启动 ≤ 3s |
| panel 分页 2000 行 | ≤ 3s |
| 全量拉取（12 期 × ~5,500 行 ≈ 6.6 万行） | ≤ 2 分钟 |
| 面板增量刷新（单次同步后） | ≤ 30s |
| 服务实现方式 | 预计算落库（§4），禁止请求期实时聚合 |

---

## 8. 验收标准（可测试）

1. **as-of 正确性**：`as_of=2026-08-01` 时，任一返回行的 `available_at ≤ 2026-08-01`
   且 `report_period ≤ 2026-06-30`；2026Q2 中披露晚于 08-01 的股票自动落到更早报告期；
2. **延期股不前视**：从 `financial_disclosure_event_state` 抽延期样本，其面板行在
   `delayed_estimate` 的 available_at 之前对 as-of 查询不可见；
3. **法定回退不早用**：`statutory_deadline` 行的 available_at ≥ 对应法定截止日；
4. **派生边界**：2023-12-31 期 `rev_yoy` 全部 null；2024-12-31 期非 null 率与
   2023 年报覆盖一致；2025-03-31 起 `ttm_*` 可算；
5. **无占位值**：全面板数值字段无 0 占位（合法 0 除外，须能区分）；
6. **覆盖一致**：coverage 端点各数字与面板实查一致；
7. **幂等**：同版本重复构建，行数与内容 hash 不变；
8. **增量挂钩**：模拟一次 `financial_facts` upsert（updated_at 变化），受影响面板行
   自动刷新且 changes 流可见；
9. **更正防前视**：构造一条 `old_hash ≠ new_hash` 且 `changed_at` 晚于公告推导值的
   变更记录后，对应面板行 `available_at` 推进至变更日、`revised_after_publish=1`，
   该日之前的 as-of 查询中该股票回退至更早报告期；hash 未变的更新不推进
   available_at；
10. **延期覆盖率实测**：§6.3-4 的命中率有实测数字并记入开发记录，达标后方可
    验收 M2；
11. **性能**：§7 各项实测达标。

---

## 9. 非目标

- 不回补 2023Q4 之前的历史财务数据；
- 不补退市股覆盖（存活偏差作为已知限制声明）；
- 不做强 PIT / vintage 数值还原（不启用 `financial_filing_vintages`）；
- 不做行业时点关联入面板（用现有 industry as-of 能力另行对接）；
- 不在本需求内做行情/估值批量接口（见 §10）。

## 10. 关联后续（独立立项，不阻塞本需求）

- **P1**：`/quotes/daily/bulk` 单日全市场行情批量；`turnover` 恒为 0 的缺陷修复
  （独立 bug，与本需求无关）；
- **P2**：估值批量（`valuation.db` 已有 7.8GB 本地数据，流量可忽略）、
  行业分类批量、数据完整性自检已并入本需求 §5.3。

## 11. 里程碑（纵向切片）

- **M1 最小闭环**：面板一次性构建（先仅 `exact` 质量的 2026Q2/Q3 子集）+
  cross-section 端点 + as-of 语义单元测试；
- **M2 完整回溯**：available_at 推导链（三级来源 + 实质变更约束 + 延期特则）
  + 2026Q2/Q3 延期覆盖率实测 + 2023Q4 起全量构建 + coverage 端点；
- **M3 派生与增量**：每股/YoY/TTM 派生（含边界）+ changes 端点 + 同步管线增量挂钩；
- **M4 测试与回归**：§8 全部验收项 + 金融语义测试（用小型 fixture 数据，不依赖
  实时网络）；
- **M5 清理、Review、提交**。

## 12. 风险与待确认

| 风险 | 说明 | 缓解 |
|---|---|---|
| 早期时点 as-of 精度 | 2026Q2 前各期靠法定截止日回退，截面新鲜度系统性滞后最多一个披露窗口 | quality 标记 + coverage 暴露分布，客户端可选只消费 exact |
| 更正前视（数值维度） | 弱 PIT 提前使用更正后数值，高估业绩方向 | 已由 §3.3 实质变更约束消除；残留仅 hash 粒度过细导致的过度保守，实现阶段评估字段级比较 |
| 延期股前视 | 回退截止日早于延期股实际披露 | §3.3 延期特则 + §6.3-4 覆盖率实测（2026Q2/Q3 库内直接可测）+ 宽限参数校准 |
| 口径不确定 | net_income 归母/合并、publish_date 一致性 | §6.3 实现前样例核验，结论固化 |
| 当期披露未完 | 2026Q3 截至 2026-10-31（法定截止日）仍在入库（4,938/预期 ~5,500） | 面板随增量刷新，coverage 反映实际 |
| BSE 覆盖较薄 | 354 只 | 如实暴露，不特殊处理 |
