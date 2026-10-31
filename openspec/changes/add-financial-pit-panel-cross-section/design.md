## Context

`data/financials.db`（76GB）已有 5,595 只在市股票、2023-12-31 起 12 个报告期的
宽表事实（`financial_facts`，65,501 行，(instrument, period) 唯一）与比率指标
（`financial_indicator_snapshots`），但所有查询入口均为单票、最新快照语义；真实
公告日仅 2026Q2 起基本齐全；vintage 体系 0 行且 rollout 关闭；退市股无财务数据。
消费方 quantwin 需要月度调仓的全市场 PIT 横截面，历史段继续用 AKShare 双源拼接。

需求全文与实测数据见 `docs/development/financial_pit_panel_requirements.md`（v1.2，
含 quantwin 评审结论与 2026-10-31 二次核验数据）。

## Goals / Non-Goals

**Goals:**

- 任意 as_of 时点一次查询返回全市场"每票最新可知报告期"横截面，无未来函数；
- available_at/effective_at 语义可解释、质量可分级、退化可观测；
- 归母口径状态可标记（attribution_quality），覆盖缺口可用生命周期推导监控；
- 派生指标预计算落库，请求期零实时聚合；
- 面板增量维护与现有财务同步链路衔接，不新建平行同步循环；
- 数值缺失一律 null，杜绝 0/""/"-" 占位污染因子。

**Non-Goals:**

- 不回补 2023Q4 之前历史、不补退市股财务数据、不修正存活偏差（声明 + 监控）；
- 不做强 PIT / vintage 数值还原（schema 已备，留作后续升级路径）；
- 不提供 include_delisted 参数（无数据支撑）；
- 不在本 change 内做行情/估值批量接口（独立立项）。

## Decisions

### D1 物化事件面板而非请求期实时聚合

面板 6.5 万行 × ~0.5KB ≈ 30–60MB，as-of 查询退化为小表上的窗口函数 + 索引扫描
（毫秒~秒级）。实时聚合在 76GB 库上不可行；逐票调用实测 78–101 秒/只，排除。

### D2 时点驱动、服务端装配

PIT 正确性蕴含在装配逻辑中（未公告回退、延期推后、更正推后）。收在服务端一次
做对；`/pit/panel` 原始端点（M1）满足 quantwin 整缓存与双源拼接自装配需求；
报告期过滤保留为可选参数。

### D3 弱 PIT + 实质变更约束 + attribution_quality 标记

- 时间维度严格不可妥协：任一记录仅在 available_at 后可见。
- 数值维度取最新口径，以 `available_at = max(推导值, 实质变更日)` 封死"更正前
  使用更正值"的高估方向偏差；代价是更正窗口附近截面变旧，方向保守。
  变更日志过滤口径：`domain='financial'`，datasets `financial_facts`（26,607 条，
  宽表）与 `financial_numeric_facts`（560 万条，长表）——初始入库无变更记录的行
  不推进 available_at（"无记录即无变更"分支）。
- 归母口径以 lineage 全量普查为准（2026-10-31 实测：equity 有值 100% 归母、
  net_income 95.5% 归母、4.5% 未标记），未标记行**标记照算不置 null**（剔除制造
  截面缺口，比口径偏差伤害更大）；M1 普查将 2,970 行未标记记录分类收敛。
- 买回剩余差距需完整 vintage（约 20GB+）；12 期窗口、月频、通用因子场景下
  边际收益不成立。面板结构对将来升级 vintage 兼容。

### D4 available_at 三级推导 + 延期特则 + effective_at

`exact`（publish_date）→ `exact_event`（disclosure_event_state.announcement_time）
→ `statutory_deadline`（Q1→04-30、半年报→08-31、Q3→10-31、年报→次年 04-30）。
法定截止日是公告日上界，回退只会晚用、不会早用。延期股存在事件时
`available_at = max(法定截止日, 延期公告日 + 宽限参数)`，quality=
`delayed_estimate`；宽限参数以 2026Q2/Q3 实测延期股事件命中率校准（M2 门槛）。

`effective_at` = available_at 后次一交易日（`quotes.db` `trading_calendar`，
48,337 行实测可用），服务端统一承接 quantwin"披露日 +1"执行时点规则，避免各
客户端自行处理日历（节假日时"+1 天"≠"次一交易日"）。

### D5 模块归属与写入 owner

新建 `research/financial_pit_panel.py`，模式对齐 `FinancialVintageStore`：
own 表 DDL、own 构建/增量刷新、own 查询；写经 financials.db coordinated writes。
API 入口只转发。行业 as-of 批量查询复用 `get_industry_membership_as_of`
（REQ-07.2）语义补批量形态，不复制单票循环。

### D6 派生指标边界内置于构建

YoY 需上年同期、TTM 需上年年报 + 上年同期累计。按现有覆盖：年报 YoY 自
2024-12-31、季度 YoY 自 2025 年对应各期、TTM 自 2025-03-31 可算；更早各期该
字段为 null（数据边界，非错误）。

### D7 批量时点行业端点（quantwin P0-4 方案 B）

行业入面板（最新分类）有前视且污染 PIT 语义一致性；改为独立
`GET /api/v1/research/industries/as-of`：基于 `industry_classification_history`
的 `official_start_date`，每票取 as_of 当日生效的最新分类，返回
instrument_id/industry_code/industry_name/taxonomy_system/effective_from/
effective_to。quantwin 行业中性化/分组按 as_of 关联。

### D8 覆盖缺口的生命周期推导与完整期标记

应有数 = instruments（quotes.db）中 `type=stock AND exchange IN (SSE,SZSE,BSE)
AND listed_date ≤ 期末 AND (delisted_date 为空 OR > 期末)`；缺口 = 应有 − 实际。
初测存在双向偏差（2024Q1：161 只 listed_date 晚于报告期却有财报行、80 只口径内
无财报行），故 M1 增加生命周期校准任务，coverage 同时暴露反向差异。
完整期标记用时间判据：`is_complete = 评估时点 > 该期法定披露截止日`，避免计数
判据被披露节奏噪声干扰。

### D9 双源拼接：重叠校验而非历史回补

quantwin 历史段（2023Q4 前）继续用 AKShare，本面板自 2024 年起替代其增量与
PIT 取数。拼接风险集中在口径与追溯调整时点：两侧同为"最新口径"，理论上重叠期
差异只应来自口径（归母/合并）、单位与追溯调整时点差。因此以 2023Q4/2024Q1/
2024Q2 三期重叠校验（>2% 差异股票 <1%、逐只归因）作为拼接质量门槛（验收 #12），
而非回补历史。归母口径对齐由 D3 的 lineage 普查与 attribution_quality 支撑。

## Risks / Trade-offs

| 风险 | 处置 |
|---|---|
| 早期时点 as-of 精度靠法定截止日回退，截面新鲜度滞后最多一个披露窗口 | quality 分级暴露于 coverage，可只消费 exact* |
| 延期股事件表完整性未验证 | M2 门槛：2026Q2/Q3 实测命中率，不足先调策略 |
| 4.5% net_income 行未记口径来源 | M1 普查分类 + attribution_quality 标记照算；验收 #13 五家样例对公开年报 |
| instruments 生命周期字段质量（listed_date 晚于报告期） | M1 校准任务 + coverage 反向差异暴露 |
| data_change_log hash 粒度过细 → 过度保守 | 实现阶段评估字段级比较；宁可保守不可前视；超阈值时提供 revision_guard=lenient 显式降级 |
| 双源口径不一致造成拼接点因子突变 | 2023Q4–2024Q2 重叠校验（验收 #12），两侧同为最新口径，差异只应来自口径/单位/追溯时点 |
| financials.db 活跃写入（76GB） | coordinated writes；增量刷新 bounded（≤30s） |
| 2026Q3 当期披露未完（至 10-31 为 4,938 只） | is_complete 时间判据 + 增量刷新，coverage 反映实际 |
| trading_calendar 视界有限（SSE/SZSE 至 2026-12-30、BSE 至 2026-11-06），依赖日常刷新 | 超视界 effective_at 为 null 不近似；日历刷新属现有调度职责 |
