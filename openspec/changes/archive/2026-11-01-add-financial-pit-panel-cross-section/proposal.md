## Why

quantwin（全 A 股月度调仓多因子选股程序）当前依赖 AKShare 的 `stock_yjbb_em` /
`stock_yysj_em` 做全市场财务横截面取数，长期受东财网页端限流与字段口径漂移影响；
本库现有单股财务接口实测 78–101 秒/只，全市场 5,300+ 只不可用。回测需要的
"任意历史时点、全市场、当时可知"的横截面能力目前不存在：现有查询均为单票、
最新快照语义，且无公告日过滤。

需求依据：`docs/development/financial_pit_panel_requirements.md`（v1.2，并入
quantwin 2026-10-31 评审的 4 项 P0 与 7 项 P1 答复）。数据基线为本库现有数据，
不做历史回补；quantwin 历史段继续用 AKShare，双源在 2023Q4–2024Q2 重叠校验。

## What Changes

- 新增物化 PIT 面板表 `financial_pit_panel`（financials.db）：一行一个
  `(instrument_id, report_period)` 事件，事件在 `available_at`（公告日语义）生效；
  由 `financial_facts` JOIN `financial_indicator_snapshots` 一次性构建，约 6.5 万行。
- 新增 `available_at` 推导链：真实披露日 → 披露事件 → 法定披露截止日回退；
  延期股特则；实质变更约束（`available_at = max(推导值, data_change_log 实质变更日)`，
  old_hash ≠ new_hash）封死更正前数值前视；超阈值时提供 `revision_guard` 条件开关。
- 新增 `effective_at`（公告日后次一交易日，`trading_calendar` 推导），服务端统一
  承接"披露日 +1"执行时点语义。
- 新增时点驱动横截面查询（服务端装配）及面板原始访问、覆盖自检（生命周期
  应有/实际/缺口 + 完整期标记）、增量变更流（sequence_id 单调游标）API 端点，
  入口仅转发；API 层 report_period/as_of 统一 YYYYMMDD。
- 新增 `attribution_quality`（parent/mixed/unverified）标记 net_income/equity
  归母口径状态（lineage 普查：equity 有值 100% 归母，net_income 95.5% 归母、
  4.5% 未标记），标记照算不置 null；`is_delisted` 预留字段（恒 false）。
- 新增批量时点行业端点 `GET /api/v1/research/industries/as-of`（基于
  `industry_classification_history`，quantwin P0-4 方案 B）。
- 派生指标（eps/bps/ocfps/rev_yoy/np_yoy/ttm_*）构建时计算落库，含可计算边界；
  数值缺失一律 null，禁止占位值。
- 双源拼接支撑（quantwin P0-1）：归母口径对齐承诺 + 2023Q4/2024Q1/2024Q2
  重叠校验（差异 >2% 股票 <1% 且逐只归因）+ M4 输出拼接建议文档。
- 面板增量维护挂钩现有财务同步链路，调度复用现有 scheduler 任务位。
- 语义为弱 PIT：时间严格、数值最新口径；不启用、不回填 vintage 体系。

## Capabilities

### New Capabilities

- `financial-pit-panel`: 全市场财务 PIT 面板的构建、可得性推导、时点横截面查询、
  覆盖自检、增量维护与 API 暴露。
- `industry-membership-as-of`: 批量时点行业归属查询（按 as_of 返回全市场生效分类）。

## Impact

- Affected code:
  - new `research/financial_pit_panel.py`（own 表 + own 构建 + own 查询，模式对齐
    `FinancialVintageStore`；唯一写入 owner，经 financials.db coordinated writes）
  - `api/routes.py`（仅转发，不写业务循环，遵循治理总纲 §2.1）
  - `research/storage.py` 或独立模块：industries as-of 批量查询（复用
    `get_industry_membership_as_of` 语义的批量形态）
  - `research/financial_statements_sync.py`（落库成功后的增量刷新挂钩点）
  - `scheduler/tasks.py`（增量刷新任务位，若需）
  - tests: panel build/derivation/query/边界与金融语义测试（小型 fixture 数据）
- Affected storage:
  - `data/financials.db` 新增 `financial_pit_panel` 表（预估 30–60MB）及
    `(available_at)`、`(instrument_id, available_at)` 索引
  - 只读 `financial_facts`、`financial_indicator_snapshots`、
    `financial_disclosure_event_state`、`data_change_log`
  - 只读 `data/quotes.db`：`instruments`（生命周期应有数推导）、
    `trading_calendar`（effective_at 推导）
  - 不修改现有表结构，不启用 vintage rollout 配置
- Known data limits（如实暴露，不修复）:
  - 成规模报告期仅 2023-12-31 → 2026-09-30 共 12 期；2026Q3 披露窗口至今日未完
  - 真实 publish_date 仅 2026Q2 起基本齐全，此前依赖法定截止日回退
  - universe 为当前在市 5,595 只，退市股无财务数据（存活偏差声明 + 缺口按期监控）
  - instruments 生命周期字段需校准（2024Q1：161 只 listed_date 晚于报告期、
    80 只口径内无财报行）
