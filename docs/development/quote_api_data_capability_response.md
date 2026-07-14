# 《Quote System API 数据能力改进需求》落地情况回复

> 回复方：Quote System 数据源团队
> 对照文档：量化选股与回测平台团队提交的《Quote System API 数据能力改进需求》（2026-07-11）
> 基准：Quote System API **1.0.0 → 1.1.0**
> 首次回复：2026-07-12　|　**本次更新：2026-07-14（历史行情全量回补 + M0 实测反馈的三处缺陷修复后重新对照）**

---

## 更新说明

自首次回复以来，项目又有两方面进展，直接影响本文档结论：

1. **REQ-01 从"部分实现"升级为"已实现"**：贵方完成了 A 股全市场历史行情回补（含退市股专项回补）。`daily_quotes` 从 648.8 万行增至 **3075.6 万行**，深度覆盖 1990-12-19 至今；447 只已退市 A 股 STOCK 中 **446 只**已有完整历史行情（此前仅 138 只，覆盖率从 3.5% 提升到 99.8%）。详见更新后的 §2.3。
2. **额外修复了三处影响数据可用性的缺陷**：在贵方 M0 实测过程中发现并修复了 `/instruments` 状态过滤 500 错误、`/stats` 统计全零、`/quotes/coverage` 跨市场比值失真三处问题（均不在原始 13 项需求编号内，但直接影响 REQ-01/REQ-12 相关能力的实际可用性）。详见新增的 §2.7。完整过程见[《数据方确认清单回执》](quote_api_data_confirmation_response.md)。

其余各项（REQ-02~13，除 REQ-01 外）结论不变，仍维持首次回复的判断。

---

## 0. 一句话总结

13 项需求中，**7 项已实现**（含 1 项原计划实现但验证后撤下、改为下方"未实现"列出），**6 项未实现**。已实现部分均为**只读新增**，不改变任何现有端点的默认参数与响应结构，无需量化平台改动现有对接代码。

---

## 1. 分类总览

| 编号 | 需求 | 类型 | 状态 | 备注 |
|---|---|---|---|---|
| REQ-01 | 退市证券历史行情可得性与覆盖率 | 能力增强 | ✅ **已实现（本次更新）** | 全量历史回补 + 退市股专项回补均已完成，见 §2.3 |
| REQ-02 | 公司行为明细（现金分红/送转/配股） | 能力增强 | 🔴 **未实现（验证后撤下）** | 前提不成立，见 §3.1 |
| REQ-03 | 财务数据的时点/修订版本（vintage） | 能力增强 | 🟡 **部分实现** | 契约澄清已做；完整 vintage 留痕未做 |
| REQ-04 | 日 K 契约澄清 | 契约澄清 | ✅ **已实现** | |
| REQ-05 | 证券状态历史（ST/\*ST/退市整理） | 能力增强 | 🔴 **未实现** | |
| REQ-06 | 全库修订水位/增量变更标识 | 能力增强 | 🟡 **部分实现** | `batch_id`语义契约澄清已做；变更日志端点未做 |
| REQ-07 | 行业归属时点化 | 能力增强 | ✅ **已实现** | |
| REQ-08 | 宽基指数历史成分 | 能力增强 | 🔴 **未实现** | |
| REQ-09 | 估值历史 PIT 保证与前瞻指标 vintage | 能力增强+契约澄清 | 🟡 **部分实现** | PIT/percentile 契约澄清已做；前瞻 vintage 未做 |
| REQ-10 | 涨跌停参考价字段 | 能力增强 | 🔴 **未实现** | |
| REQ-11 | 分页与批量导出的稳定性/效率 | 契约澄清+能力增强 | 🟡 **部分实现** | 分页已落实；Parquet 批量导出未做 |
| REQ-12 | 证券主数据补充字段（lot_size/tick_size） | 能力增强 | ✅ **已实现** | 港股 tick_size 仍为 null，见 §2.6 |
| REQ-13 | 无风险利率序列 | 能力增强 | ✅ **已实现** | |

---

## 2. 已实现 — 做了什么 & 如何使用

### 2.1 REQ-04 日 K 契约澄清

**做了什么**：`/quotes/daily` 的 OpenAPI 200 响应此前为空对象 `{}`，现已补全完整 Schema；并在文档中明确了此前需要探测的全部语义。

**如何使用**：无需改动调用方式，直接查看契约即可。
- 访问 `GET /openapi.json` 或 `/docs` 查看 `/quotes/daily` 的完整响应 Schema
- 复权公式：后复权价 = 原始价 × `cumulative_factor(t)`；前复权价 = 原始价 × `cumulative_factor(t)/cumulative_factor(latest)`
- `tradestatus`：`1`=正常交易，`0`=停牌；`pre_close` 在除权日为除权调整后的参考价
- `time` 为 Asia/Shanghai 时区；`volume` 单位为股，`amount` 为人民币元
- 详见 [`docs/api/restful_api.md`](../api/restful_api.md#get-apiv1quotesdaily)

### 2.2 REQ-11.1 分页语义

**做了什么**：`/quotes/daily` 的 `limit`/`offset` 此前声明但未生效，现已落实为真实分页；不传 `limit` 时行为与之前完全一致（不影响现有对接）。

**如何使用**：
```bash
curl "http://localhost:8000/api/v1/quotes/daily?instrument_id=600000.SH&start_date=2024-01-01&end_date=2024-12-31&limit=100&offset=0"
```
返回体新增 `pagination` 字段：`{limit, offset, total_available, returned_records}`。排序固定为 `time` 升序，跨请求分页结果稳定。

### 2.3 REQ-01 退市证券历史行情可得性与覆盖率（含 REQ-01.2 全量历史回补，本次更新为已实现）

**做了什么**：
1. `/quotes/daily` 新增可选参数 `include_delisted`（默认 `false`，不改变现有默认行为）
2. `/instruments` 新增 `delisted_after`/`delisted_before` 过滤
3. 新增覆盖率自查端点 `GET /quotes/coverage`
4. **（本次更新）A 股全市场历史行情回补，含退市股专项回补**：`daily_quotes` 从 648.8 万行增至 **3075.6 万行**，深度覆盖 **1990-12-19 至今**；447 只已退市 A 股 STOCK 中 **446 只**已有完整历史行情（回溯到真实上市日、延续到退市前夕），覆盖率从首次回复时的 3.5%（138/3883）提升到 **99.8%**。

**如何使用**：
```bash
# 取回已退市证券的历史行情（现在能查到真实的、深度完整的历史）
curl "http://localhost:8000/api/v1/quotes/daily?instrument_id=000003.SZ&start_date=1995-01-01&end_date=1995-01-10&include_delisted=true"
# 000003.SZ 已于 2002-06-14 退市，1991 年即上市；上述查询能正常取到 1995 年真实历史行情

# 按退市日期筛选证券
curl "http://localhost:8000/api/v1/instruments?delisted_after=2023-01-01&delisted_before=2024-01-01"

# 覆盖率自查：某日上市证券数 vs 库内有行情证券数（务必按 exchange 分市场查询，见下方"已知限制"）
curl "http://localhost:8000/api/v1/quotes/coverage?date=2024-06-03&exchange=SSE"
```

**验证方式**：以上数字均对真实生产库直接查询、并对真实运行服务发起 HTTP 请求验证，非估算。

**已知限制**（非阻塞，供知悉）：
- 唯一剩余的 1 只无行情证券是 `03588.HK`（富途控股-W），这是**港股**且 `delisted_date` 字段为空（退市状态本身未最终确认），不在本次 A 股退市股回补范围内。
- `/quotes/coverage` 不传 `exchange` 时，跨市场查询会因港股 `listed_date` 主数据缺失（见 §2.7）导致比值不准确；请始终按 `exchange` 分市场查询。

### 2.4 REQ-07.2 行业归属时点化

**做了什么**：新增只读端点，基于官方分类变更留痕，返回给定历史日期"当时生效"的行业归属及其区间。既有当前态查询端点行为不变。

**如何使用**：
```bash
curl "http://localhost:8000/api/v1/research/company/600000.SH/industry/as-of?as_of_date=2021-03-15"
```
返回 `effective_date`/`expiry_date` 区间（`expiry_date=null` 表示至今仍生效）。该日期早于任何已知分类时返回 `404`。

### 2.5 REQ-13 无风险利率序列

**做了什么**：新增独立数据库 `data/interests.db`（比照 `financial.db`/`valuation.db` 的域隔离模式，便于未来扩展 SHIBOR/LPR/回购/美债等利率产品）；新增中国 10 年期国债到期收益率日更任务与只读端点。

**如何使用**：
```bash
# 列出已定义的利率序列
curl "http://localhost:8000/api/v1/research/risk-free-rate/series"

# 按日期查询
curl "http://localhost:8000/api/v1/research/risk-free-rate?series_id=china_treasury_10y&start_date=2026-01-01&end_date=2026-07-11"
```
数据来源 akshare，周一至周五 `17:30` 自动日更（一次拉取为全量幂等 upsert，无需单独回补任务）；无数据时返回空序列而非报错。当前只有 `china_treasury_10y` 一条序列，SHIBOR 等按需后续新增。

### 2.6 REQ-12 证券主数据补充字段（lot_size/tick_size）

**做了什么**：`instruments`/`/instruments` 响应新增 `lot_size`（每手股数）、`tick_size`（最小价位）字段，由主数据同步任务回填。

**如何使用**：
```bash
curl "http://localhost:8000/api/v1/instruments?exchange=HKEX&limit=10"
```
- A 股：`lot_size=100`、`tick_size=0.01`（常量）
- 港股：`lot_size` 从官方证券名单 board lot 回填（如 `00001.HK→500`）；`tick_size` 因港股为随价格分档的价位表、无法用单一标量表达，暂为 `null`
- 期货 `contract_multiplier` 字段名请参照 `futures` 相关响应模型，本次未新增字段（属契约澄清范畴，已在原字段基础上确认）

**已知覆盖缺口**（非阻塞，仅供知悉）：港股约 1.7%（82/4690）"active"状态证券因双柜台/规范性过滤逻辑（既有代码行为，非本次改动引入）尚未拿到 `lot_size`；全部港股的 `tick_size` 仍为 `null`（港股最小报价单位是随价格分档的价位表，非单一数值，现有字段设计无法表达，需先做数据模型决策）。均已列入内部待办清单跟踪。

### 2.7 M0 实测发现并修复的三处缺陷（不在原始 13 项需求编号内，本次更新新增）

贵方在 M0 数据契约闸门实测中发现三个直接影响可用性的问题，均已定位根因、修复、并用真实生产库与真实运行服务验证：

| 问题 | 修复前现象 | 修复内容 |
|---|---|---|
| `/instruments` 状态过滤 500 | `is_active=false`/`status=delisted` 等查询必定报错 500，**无法枚举退市证券** | `InstrumentResponse.status` 枚举补全库内实际使用的全部状态值；`type` 过滤增加大小写归一化 |
| `/stats` 返回全零 | `instruments_count`/`quotes_count` 等字段恒为 0/空 | 路由改为正确读取底层聚合函数的实际返回结构；另发现并修复了历史回补后触发的响应超时（33 秒→2 秒） |
| `/quotes/coverage` 比值 >1 | 跨市场查询 `coverage_ratio` 可能超过理论上限 1.0 | `quoted_count` 改为与 `listed_count` 同一统计口径，新增字段透明暴露被排除的证券数 |

**如何使用（现在都能正常工作）**：
```bash
curl "http://localhost:8000/api/v1/instruments?status=delisted&limit=10"      # 枚举退市证券
curl "http://localhost:8000/api/v1/stats"                                     # 返回真实统计（约 2 秒）
curl "http://localhost:8000/api/v1/quotes/coverage?date=2024-06-28&exchange=SSE"  # 比值恒 ≤1.0
```

完整根因分析、修复前后对比数据见[《数据方确认清单回执》](quote_api_data_confirmation_response.md) A2/A3/A6 章节。

---

## 3. 未实现 — 原因说明

### 3.1 REQ-02 公司行为明细【原计划实现，验证后撤下】

**原因**：实现前评估认为"数据已在 `adjustment_factors` 表、只差暴露"，属低成本。但实现验证时查库发现该表**现金分红/送转/配股拆解列全部为 0**（91541 行中无一行有值），只存了合成复权因子；唯一有真实明细的审计表覆盖极小（1921 行）。

**结论**：这不是暴露问题，而是需要**新增数据采集**（分红明细 provider + 存储 + 回补），是一个全新的数据域，工作量与 REQ-05/08/10 同级（净新增+源端依赖），不再属于低成本项。

### 3.2 REQ-03.2 财务数据完整 vintage 留痕

财务事实当前按 `(instrument_id, report_period)` 单版本存储，重述会覆盖旧值。**已被覆盖的历史修订版本无法回溯找回**——即使现在开始做 vintage 留痕，也只能从改造上线那一天起前向积累。工作量评估为高（8–15 人日，改主键+写入路径+ as_of 查询），且收益要等较长时间积累才能体现，故本轮未做。

### 3.3 REQ-05 证券状态历史（ST/\*ST/退市整理）

需要新建状态区间表 + 采集任务，且依赖上游源（akshare/tushare）是否能提供完整 ST 变更历史区间，做之前需要先验证源端可得性。

### 3.4 REQ-06.2 全库变更日志/水位

REQ-06.3（`batch_id` 语义澄清）已在文档中说明；但"变更日志端点"（返回近期哪些分区被修订）和"全库单调 revision_seq"属架构级改动，本轮未做。

### 3.5 REQ-08 宽基指数历史成分

沪深300/中证500/中证1000/上证50 的历史成分与 in/out 日期，本库当前**完全没有**相关数据模型，需新建表+采集+历史推导，是一个完整的新数据域。

### 3.6 REQ-09.2 前瞻指标（pe_forward/ps_forward）历史预测 vintage

需要把 `analyst_forecasts` 从当前覆盖写改为留痕存储，且历史分析师预测快照本身能否从源端找回存疑，未做。

### 3.7 REQ-10 涨跌停参考价字段

`daily_quotes` 目前没有 `limit_up`/`limit_down` 字段，源端是否有现成字段（如 tushare `stk_limit`）需要先确认数据源权限和可得性；若无则要退化为"pre_close×幅度+制度表"推导，且需覆盖新股/科创创业板20%/ST档位切换等复杂制度，加上全量历史回补，工作量较大，本轮未做。

### 3.8 REQ-11.2 Parquet/Arrow 批量导出

本库目前未引入 `pyarrow`，属新增基础设施；报告原文本身标注为"可选"，本轮优先做了分页语义修复，批量导出未做。

---

## 4. 小结

已实现部分建议贵方按以下顺序验收接入：REQ-04（契约澄清，零迁移成本）→ REQ-01/REQ-11.1（历史深度与分页，直接解除幸存者偏差与分页保守假设，**本次更新重点**）→ REQ-07.2/REQ-13（新读接口，按需接入）→ REQ-12（lot_size 主要服务港股扩展场景）。§2.7 的三处缺陷修复建议一并回归验证，尤其 `/instruments` 退市枚举和 `/stats` 是否已符合预期。

未实现部分中，REQ-02、REQ-05、REQ-08、REQ-10 的共同前提是**先验证上游数据源是否真的能提供所需数据**，建议贵方与我方共同评估源端可得性后再排期；REQ-03.2/REQ-09.2 的核心制约是"历史数据不可回溯"，需要提前对齐预期（只能前向积累，不能一次性补齐历史 vintage）。这 6 项未实现的详细现状、验证过程与后续排期建议已整理在[《待解决问题清单》](open_issues_backlog.md)中，供内部排期参考。
