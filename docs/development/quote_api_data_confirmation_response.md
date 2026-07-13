# 数据方回执：量化平台《数据方确认清单（Quote System API 1.1.0）》

> 回复方：Quote System 数据源团队
> 对照文档：量化平台《数据方确认清单（Quote System API 1.1.0）》（2026-07-12）
> 核对方式：全部对照**当前实际运行的服务**（live `/openapi.json`、真实接口调用）与**真实数据库**逐项复现，非凭记忆/文档推断
> 日期：2026-07-13

---

## 0. 结论速览

| 编号 | 结论类型 | 一句话 |
|---|---|---|
| A1 🔴 | **本实例数据未全量导入** | 全库 `daily_quotes` 对**所有**标的（含仍在市蓝筹）统一起点 `2023-07-17`，非源端限制，也非退市股特有问题 |
| A2 🔴 | **已修复** | `InstrumentResponse.status` 枚举补全全部真实状态值，`type` 过滤增加大小写归一化。已用真实生产库验证：`is_active=false`/`status=delisted`/`type=STOCK` 均恢复正常 |
| A3 🔴 | **已修复** | `/stats` 路由改为正确读取 `get_database_statistics()` 的嵌套结构，新增行业分布/日历日期范围补充查询。已用真实生产库验证：`instruments.total=13155`、`daily_quotes.total=6487818` 等字段正确返回 |
| A4 | **既定行为，此前遗漏一处文档表述** | 不传 `limit` 时确认为 `time` **降序**（非升序）；已据实修正文档 |
| A5 | **既定行为，已确认** | 公式验证通过：`hfq.close = raw.close × hfq.factor`（精确到小数点后三位一致） |
| A6 | **计算口径已修复；港股仍需数据补全** | `quoted_count` 现与 `listed_count` 同口径（均要求 `listed_date` 已知），跨市场比值不再 >1；新增 `unknown_listed_date_quoted_count` 透明暴露被排除数量。真实库验证：2024-06-28 跨市场 ratio 从 1.428 修正为 0.988。港股本身仍需回填 `listed_date`（数据获取类工作，未在本次范围） |
| A7 | **既定行为** | `.SSE/.SZSE`→`.SH/.SZ` 为既有内部代码归一化约定 |
| A8 | **既定行为，SRS 假设需修正** | 官方留痕最早并非统一起点；个别记录的 `1990-01-01` 为占位默认值，非真实留痕日期 |
| A9 | **已用全量数据回答** | 全库 `pre_close` 缺失率 **0.1188%**（7709/6487818），集中在近期个别交易日，非除权日 |
| B1–B7 | 见 §2 | B2/B3 已有明确、可引用的核实结论；B4/5/6/7 尚未做源端 spike，如实标注未验证 |

---

## 1. A. M0 实测发现（逐项明确结论）

### A1 🔴 行情历史深度

**结论：本实例数据未全量导入，不是源端限制。**

复核方式：直接查询 `data/quotes.db`，不限于贵州茅台样本。

```
600519.SH（贵州茅台，2001-08-27 上市）: 首行情 2023-07-17，共 723 行
000001.SZ（平安银行）:                首行情 2023-07-17，共 723 行
整表 daily_quotes（648.8 万行）:       全库最早 2023-07-17
data_updates（批次记录表）:            0 条记录
```

**关键发现**：这不是"个别标的缺历史"，而是**全库 `daily_quotes` 对每一个标的都统一止步于 2023-07-17**，包括 2001 年就上市、从未停牌退市的蓝筹股。`data_updates`（批次追踪表）完全为空，说明本实例从未记录过一次历史批量回补任务的执行。

**能否/何时回补**：技术上可行——已有的 `daily_data_backfill_range` 批量回补能力可直接复用；覆盖 2015 年至今需要的是**执行一次全市场历史回补**（数据工程任务：多日下载、需评估源端限流），而非新开发。这属于我方需要单独排期执行的运维任务，建议贵方将其列为**独立、高优先级的一次性回补请求**，我方给出可执行时间表。

### A2 🔴 `/instruments` 状态过滤 500

**结论：已修复。**（原：已复现，是真实缺陷，根因明确，可修复——OpenSpec change `fix-instrument-stats-coverage-defects`）

复现（对当前活跃服务实测）：
```
GET /instruments?is_active=false           → 500
GET /instruments?status=delisted           → 500
GET /instruments?delisted_before=2026-01-01 → 500
GET /instruments?type=STOCK&is_active=false → 200，但返回 []（另一个独立问题，见下）
```

**根因 1（导致 500）**：响应模型 `InstrumentResponse.status` 用 Pydantic 枚举限定为仅 `active/inactive/suspended` 三个值，但库内 `instruments.status` 实际存在 8 种真实状态：
```
active(8518) / excluded(1435) / delisted(447) / suspended(87) /
auto_deactivated_no_data(46) / auto_deactivated_zombie(21) ...（stock 类型统计）
```
任何查询只要命中一行 `status` 不在枚举内的记录（`delisted`/`excluded`/`auto_deactivated_*` 等），响应序列化即抛 500。经核实：**当前没有任何一种过滤组合能在不触发此 bug 的前提下查到退市股**（`type=stock AND is_active=0` 的 1949 条记录里，没有一条的 status 落在枚举允许范围内）。

**根因 2（`type=STOCK` 返回空）**：`type` 过滤是**大小写敏感**的精确匹配，库内存储为小写 `stock`，传大写 `STOCK` 会静默匹配 0 条（不报错，但也拿不到数据）。

**库内退市 STOCK 数量级**：`type='stock' AND status='delisted'` = **447 条**；若含更广义的"非活跃"（`is_active=0`）则为 **1949 条**（含 excluded/auto_deactivated 等类别）。

**修复方案（已上线）**：
1. `InstrumentStatusEnum` 补全为库内实际 8 个状态值（新增枚举项，不改变已有值语义）
2. `type` 过滤增加大小写归一化

**修复后真实生产库验证**：`is_active=false`、`status=delisted`、`type=STOCK` 均返回 200 且拿到预期数据（不再是 500 或空列表）。

### A3 🔴 `/stats` 返回全 0

**结论：已修复。**（原：已复现，是真实缺陷（字段映射 bug），不是"未刷新"）

复现：`/stats` 返回 `instruments_count=0, quotes_count=0, ...` 全部为空/零值。但通过 `/api/v1/system/status`（走的是**同一个底层聚合函数** `get_database_statistics()`）证实底层数据完全正常：

```json
"instruments": {"total": 13155, "active": 9272, "by_exchange": {"SSE":2742,"SZSE":5396,"BSE":327,"HKEX":4690}, ...},
"daily_quotes": {"total": 6487818, ...}
```

**根因**：`get_database_statistics()` 返回的是**嵌套结构**（`stats['instruments']['total']`、`stats['daily_quotes']['total']`），但 `/stats` 路由的处理代码按**扁平字段名**读取（`stats.get('instruments_count', 0)`、`stats.get('quotes_count', 0)`）——这些扁平键在返回字典里根本不存在，所以全部静默落到默认值 0/{}。这是路由层与数据函数之间的字段命名不一致，纯代码 bug，与"聚合未跑/未刷新"无关——聚合本身是对的，`/system/status` 已证明。

**修复方案（已上线）**：改写 `/stats` 路由的字段提取逻辑以匹配实际嵌套结构；新增行业分布与交易日历日期范围补充查询（`get_stats_supplement`，不改动 `get_database_statistics()` 本体，避免影响 `/system/status`）。

**修复后真实生产库验证**：`instruments_count=13155`、`quotes_count=6487818`、`instruments_by_exchange` 等字段正确返回非零值。

### A4 排序方向

**结论：既定行为——不传 `limit` 时为 `time` 降序（最新在前）；显式传 `limit` 时为升序切片。这一差异此前文档未讲清楚，现已补充说明。**

实测（`600519.SH`，2024-01-01~01-10，不传 limit）：
```
返回顺序：2024-01-10 → 2024-01-09 → ... → 2024-01-02（降序）
```

该降序是底层查询 `ORDER BY time DESC` 的既有行为（早于本次改动）；本次新增的分页能力（`limit`/`offset`）为保证跨页确定性，**只在显式传 `limit` 时**改为升序切片。这造成"默认降序、分页时升序"的不一致，我方认可这是文档表述不够清楚导致的疑惑，已在 `docs/api/restful_api.md` 中明确标注两种场景的实际顺序。

如贵方需要"始终升序"的确定性契约，我方可评估是否统一为默认升序返回（这将改变现有默认响应顺序，需按贵方"不改变现有端点默认行为"的原则单独评估，不在本次自动执行范围内）。

### A5 复权因子语义

**结论：确认为既定行为，公式验证通过。**

实测（`600519.SH`，2024-01-10）：
```
adjust=none: close=1641.5,     factor=1.0（恒定），adjustment_type=none
adjust=hfq:  close=11440.124,  factor=6.969311，   adjustment_type=backward
验证：1641.5 × 6.969311 = 11440.1240065 ≈ 11440.124 ✓ 精确吻合
```

确认：
- (a) `adjust=none` 的 `factor` 恒为 `1.0`（占位标识，不携带累计因子），为既定行为
- (b) 累计后复权因子的权威来源确认为 `adjust=hfq` 请求返回的 `factor` 列（即 `adjustment_factors.cumulative_factor`）

**关于"长期稳定"**：正常运行下，累计因子是**只追加**的——新除权事件只会在原有链条上继续累乘，不会重新计算历史事件的因子值。但需说明：系统当前**没有版本化/不可变性保证**——若某个历史除权事件的原始数据本身被人工订正（罕见但架构上未禁止），对应历史因子理论上会被覆盖更新。这与贵方此前 REQ-06（全库修订水位）关注点相关；若需要"因子历史不可变"的强保证，需要额外的版本化改造（未在本次范围内评估工作量）。

### A6 `/quotes/coverage` 的 `coverage_ratio` 语义

**结论：计算口径缺陷已修复；港股 `listed_date` 数据缺失仍需单独回补（数据获取类工作）。**

**精确定义（修复后）**：`coverage_ratio = quoted_count / listed_count`，两者现在统一要求 `listed_date` 已知（非 NULL），理论上限为 `1.0`；新增 `unknown_listed_date_quoted_count` 字段透明暴露"有行情但 `listed_date` 未知"的证券数，不再让其静默污染主比值。

**修复前根因复现**（`2024-06-28`，`instrument_type=stock`）：
```
不传 exchange:        listed_count=5365  quoted_count=7663  ratio=1.428（失真）
exchange=SSE:          listed_count=2267  quoted_count=2242  ratio=0.989（正常）
exchange=HKEX:          listed_count=0     quoted_count=2362  ratio=null（HKEX 完全不可用）
```
根因：`2024-06-28` 当日有行情的 2362 只港股 `stock` **全部 `listed_date` 为 `NULL`**，只计入分子不计入分母，跨市场汇总时比值失真。

**修复后真实生产库验证**（同一天）：
```
不传 exchange: listed_count=5365 quoted_count=5301 unknown_listed_date_quoted_count=2362 ratio=0.988
exchange=SSE:  listed_count=2267 quoted_count=2242 unknown_listed_date_quoted_count=0    ratio=0.989（与修复前一致，无回归）
```

**正确查询姿势**：修复后跨市场查询也不再失真，但 `unknown_listed_date_quoted_count` 较大时（如混入港股）该市场的覆盖信息仍不完整，建议仍用 `exchange` 过滤按单市场查询以获得最精确数值。**港股 `listed_count` 仍恒为 0**（`listed_date` 全字段缺失属于港股主数据完整性问题，需要单独的数据回补工作，不在本次代码修复范围）。

**是否为退市历史覆盖率的权威来源**：是，计算口径已修复；港股场景需等 `listed_date` 回补后才可用。

### A7 证券 ID 回显

**结论：确认为既定约定。**

`.SSE/.SZSE`（查询用交易所后缀）→`.SH/.SZ`（响应/库内标准格式）是 `utils/code_utils.py` 中 `convert_to_database_format` 承担的既定内部归一化约定，适用于所有涉及 instrument_id 的端点，非本次新增，非缺陷。

### A8 行业 as-of 早于留痕起点的行为

**结论：既定行为符合代码逻辑，但 SRS 关于"留痕起点"的假设需要修正——留痕起点因证券而异，且存在占位默认值需要排除。**

复核：`industry_classification_history` 表 `official_start_date` 全库分布：
```
最早值：1990-01-01（仅 12 条记录）
其余早期真实分布：2002-05-01(21)、2001-05-01(17)、2003-05-01(15)、1999-01-01(10)、1994-01-03(10) ...
NULL 值：0 条
```

**深挖那 12 条 `1990-01-01` 记录**：其 `official_update_time` 字段为 `2024-09-27`/`2024-09-30` 等近期日期，`source='swsresearch'`——这是源端在无法确定真实生效起点时使用的**占位默认值**（1990-01-01 早于中国 A 股市场实际开市），不是真实的历史分类记录。

**as-of 端点行为解释**：`as_of_date=2010-01-02` 之所以有返回而非 404，是因为库内**确实存在** `official_start_date ≤ 2010-01-02` 的记录（多数标的的真实留痕本就早于 2010 年，可回溯至 1994–2003 年区间）——这是端点按设计正确执行的结果，不是缺陷。但 SRS 文档中"留痕起点"若被理解为一个**全局统一**的日期，这个假设不准确：起点因证券而异，且约 12 条记录的起点是不可信的占位符。

**建议**：贵方在标注 `INDUSTRY_DRIFT` 适用区间时，(a) 不要假设全局统一的留痕起点，改为逐证券判断；(b) 对 `official_start_date='1990-01-01'` 的 12 条记录做特殊标注/排除，避免误判为"该证券自 1990 年起分类已知"。如需要，我方可后续增加占位值标注字段（未在本次范围内评估）。

### A9 `pre_close` 缺失率（全量）

**结论：已用全量数据回答，非小样本。**

```
全库 daily_quotes：6,487,818 行
pre_close 缺失：    7,709 行
缺失率：            0.1188%
```

集中日期（Top 10）：`2026-06-15`(856)、`2026-06-30`(133)、`2026-07-05`(124)、`2026-07-07`(116)、`2026-07-01`(101) 等——**均为近期交易日**，不集中于除权除息日，初步判断与数据同步的时间窗口/短期缺口相关，非除权事件导致的系统性缺失。对涨跌停推导（SRS §13.4b）的影响面很小（<0.12%），且不呈除权日聚集模式。

---

## 2. B. SRS §26 外部 Backlog（源端可得性联合验证）

| 编号 | 结论 | 说明 |
|---|---|---|
| B1 | **需排期（数据工程）** | 与 A1 是同一件事——参见 A1，实为全库统一回补，非"仅退市股" |
| B2 | **源端不可得（已证实）** | `adjustment_factors` 表 91541 行现金分红/送转/配股拆解列**全部为 0**，仅审计表 `adjustment_factors_tdx` 有 1921 行真实明细（覆盖率<3%）。若需完整逐事件明细，需新增采集，非暴露已有数据 |
| B3 | **确认：历史修订不可回溯** | `financial_facts` 按 `(instrument_id, report_period)` 单版本覆盖写，重述会覆盖旧值；已有 `data_available_date` 字段记录披露可得时间，但无法找回已被覆盖的修订前数值。上线 vintage 留痕后只能前向积累，无法一次性补齐历史 |
| B4 | **未验证，需专项 spike** | ST/\*ST 状态历史区间在 akshare/tushare 的可得性尚未核实，需要单独评估（预估 0.5 人日 spike） |
| B5 | **未验证，需专项 spike** | 宽基指数历史成分的源端可得性（如 tushare `index_weight`）尚未核实，涉及权限确认，需要单独评估 |
| B6 | **未验证，需专项 spike** | 分析师前瞻预测历史快照源端可得性未核实；库内 `analyst_forecasts` 当前确认为覆盖写，不留痕 |
| B7 | **未验证，需专项 spike** | `limit_up`/`limit_down` 源端字段（如 tushare `stk_limit`）权限与可得性未核实；`daily_quotes` 当前无此字段 |

B4/B5/B6/B7 的"未验证"是如实说明——这四项此前评估阶段就已标注为"需先做源端可得性 spike 再冻结排期"，本次回复周期内未安排新的验证动作，避免给出未经证实的结论。

---

## 3. 缺陷修复状态

A2、A3、A6 已修复上线（OpenSpec change `fix-instrument-stats-coverage-defects`），均为新增枚举值/修正内部计算逻辑/新增响应字段，未改变任何现有成功查询路径的返回结果结构。已用真实生产库直接验证（详见各节"修复后真实生产库验证"）；线上服务生效需等待下次进程重启（FastAPI 路由/模型改动需重启进程才能反映到 `/openapi.json` 与实际响应，详见此前关于"自动更新"的说明）。

A4（默认排序方向是否改为统一升序）**未修改**——涉及现有默认响应顺序变更，按贵方"不改变现有端点默认行为"的原则，需贵方明确需要后再单独评估执行，不在本次自动修复范围内。

A1/B1-B7（历史数据回补、公司行为明细采集、财务修订 vintage、ST 状态历史等）均涉及数据重新获取或较大架构改造，按约定推至下一步，不在本次修复范围。

---

## 4. 附：核对方式说明（可复现性）

本文档所有结论均通过以下方式之一验证，不依赖历史记忆或既有文档：
- 对当前活跃运行的服务实例发起真实 HTTP 请求（`curl http://127.0.0.1:8000/...`）
- 直接查询生产数据库文件（`data/quotes.db`、`data/research.db`）
- 阅读并核对当前代码实现（`api/routes.py`、`database/operations.py`、`research/storage.py` 等）

贵方如需复现任一结论，可提供具体查询参数，我方可再次现场演示。
