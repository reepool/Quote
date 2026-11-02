# 全市场财务 PIT 面板使用说明（quantwin 对接指南）

> 版本：v1.0（2026-11-01）
>
> 面板需求与设计：`docs/development/financial_pit_panel_requirements.md`（v1.3）；
> 交付与验收记录：`openspec/changes/archive/2026-11-01-add-financial-pit-panel-cross-section`
> 与 `.../2026-11-01-cross-validate-share-capital-denominators`。
> 本文档面向调用方（quantwin），只讲怎么用、怎么解读、有什么限制。

---

## 1. 一句话定位

**在任意历史时点 T，一次调用返回全市场每只股票"截至 T 已公告的最新一期财务横截面"**——每票自动回退到各自最新可知报告期，无未来函数（弱 PIT：时间严格、数值取最新口径并带更正防护）。

## 2. 快速开始

```bash
# 调仓日横截面（主接口）
curl "http://<host>:8000/api/v1/research/financials/pit/cross-section?as_of=20260801&limit=2000"

# 全量缓存（离线自装配用）
curl "http://<host>:8000/api/v1/research/financials/pit/panel?limit=2000&offset=0"   # 翻页至取完

# 覆盖与数据质量自检
curl "http://<host>:8000/api/v1/research/financials/pit/coverage"

# 增量变更流（游标轮询）
curl "http://<host>:8000/api/v1/research/financials/pit/changes?cursor=<next_cursor>"
```

日期参数统一 **YYYYMMDD**（响应同格式）。

## 3. API 参考

### 3.1 `GET /research/financials/pit/cross-section`（主接口）

| 参数 | 必填 | 说明 |
|---|---|---|
| `as_of` | 是 | 时点，YYYYMMDD；返回每票 `available_at ≤ as_of` 的最新报告期 |
| `report_period` | 否 | 时点装配后再按报告期过滤 |
| `exchange` | 否 | SSE/SZSE/BSE，逗号分隔 |
| `fields` | 否 | 字段投影，逗号分隔 |
| `limit/offset` | 否 | 单页上限 2000 |

响应关键字段：`guard`（恒 `strict`）、`universe_note`（含 latest_complete_period 与披露窗口内新期的 incomplete 状态）、`stats`（period_lag_distribution / available_at_quality / attribution_quality 分布）、`items[]`（字段字典见 §4）。

**时间语义**：`available_at` = 公告日（信息可知）；`effective_at` = 公告后**次一交易日**（执行时点语义，服务端已按交易日历 +1，客户端无需再加）。

### 3.2 `GET /research/financials/pit/panel`

原始行分页（`report_period` / `instrument_id` / `available_at_lte` 过滤），供整缓存后离线装配。双源拼接校验建议用本端点取重叠期全量。

### 3.3 `GET /research/financials/pit/coverage`

逐期：实际数、生命周期应有数与**集合差缺口**、反向差异数（pre-IPO 行）、`available_at_quality` / `attribution_quality` / **`share_validation`** 三组分布、`revised_after_publish` 行数、近 7 天源变更行数、`is_complete`（时间判据：评估时点严格晚于法定截止日）与 `latest_complete_period`。

### 3.4 `GET /research/financials/pit/changes`

追加型变更流，游标为 `data_change_log.sequence_id`（单调递增、重放幂等）：

```
首次省略 cursor → 用返回 next_cursor 轮询 → 对命中 (instrument, report_period) 重拉 panel 行
```

### 3.5 `GET /research/industries/as-of`

批量时点行业：`as_of` 必填；返回 `instrument_id / industry_code / industry_name / taxonomy_system / effective_from / effective_to`；**as_of 当日无生效分类的股票不返回**（不回填最新分类，行业中性化请按此关联）。

## 4. 字段字典

### 4.1 单位契约（重要，勿再换算）

| 字段 | 单位 |
|---|---|
| `roe / gross_margin / operating_margin / net_margin / roa / asset_liability_ratio / rev_yoy / np_yoy` | **百分数**（15.6 = 15.6%，与 AKShare yjbb 口径一致） |
| `current_ratio / quick_ratio` | 倍数 |
| `eps / bps / ocfps / revenue_per_share / book_value_per_share` | 元（累计口径，**不预年化**，按报告期自行折算） |
| `revenue / net_income / equity / operating_cf / total_assets ...` | 元（net_income/equity 为**归母**口径） |
| `shares_outstanding` | 股（交叉验证后的解析值，见 4.3） |

### 4.2 `available_at_quality`

`exact`（真实披露日）> `exact_event`（披露事件日）> `statutory_deadline`（法定截止日回退，最多晚用一个披露窗口，方向保守）> `delayed_estimate`（延期股估计）。2026Q2 起 exact 基本全覆盖，更早各期以 statutory 为主——做早期时点研究可只消费 `exact*`。

### 4.3 `share_validation`（股本交叉验证，过滤建议见 §6）

- `consensus`：多个独立锚一致（97.3%），直接使用；
- `discrepancy`（1.7%）：**期末口径锚之间真实不一致**——已按优先序取值并附全部锚数值，建议剔除或人工核对该子集；
- `single_source`（~1%）：仅单一锚可用，无交叉验证；
- `null`：无任何可用锚（bps/ocfps 为 null）。

`share_basis` 记录采用锚名（末位兜底时标注 `paid_in_capital_cny_par_assumption`）；`share_candidates` 含各锚数值、absent 原因、`unit_mismatch`（面值≠1 的可解释差异）与 `eps_weighted_drift_advisory`（加权/期末漂移，建议性注记，不构成分歧）注记。

### 4.4 其他

`attribution_quality`（parent/mixed/unverified：归母口径标记）；`revised_after_publish` + `last_value_change_at`（更正防护：更正日前该期不可见）；`is_delisted`（预留恒 false）。

## 5. 覆盖与已知限制（如实声明）

- 报告期 **2023-12-31 起 12 期**（更早仅 6 行稀疏数据）；回测起点最早 2024 年初；
- universe 为**当前在市 5,595 只**（SSE/SZSE/BSE），无退市股财务数据——存在存活偏差，缺口经 coverage 逐期可监控，回测结论应表述为"在市集合内的因子排序证据"；
- 当期披露窗口内的最新一期可能不完整：以 `universe_note` 的 incomplete 标记与 coverage `is_complete` 为准；
- BSE 覆盖较薄（354 只）；`effective_at` 受交易日历视界限制（超界返回 null 而非近似）。

## 6. 双源拼接指南（历史段 AKShare ↔ 面板）

重叠期（2023Q4/2024Q1/2024Q2）三期全量实测（>2% 相对差异率）：

| 字段 | 结果 | 处理建议 |
|---|---|---|
| eps | 0.26–0.58% ✅ | 直接使用（披露值优先） |
| ocfps | 0.53–1.20% ✅ | 直接使用 |
| bps | ~10% ⚠️ | **口径差**：AKShare 每股净资产对含永续债/其他权益工具的公司做扣除。建议拼接比对时先按 `share_validation=consensus` 过滤；逐股归因清单可在联调窗口共同核对 |
| roe / gross_margin | 定义差（AK 加权 ROE vs 面板期末；营业总收入 vs 营业收入） | 拼接点两侧口径声明，勿混排 |

历史段（2023Q4 前）继续用 AKShare；面板自 2024 年起替代增量与 PIT 取数。

## 7. 增量与缓存模式（推荐）

1. **初次**：`/pit/panel` 翻页取全量（~66k 行，实测全量 <2 分钟）本地缓存；
2. **日常**：定时轮询 `/pit/changes?cursor=...`，对命中键重拉对应 panel 行；
3. **调仓日**：直接调用 cross-section（服务端装配，热查询 P95 <1s），或用本地缓存离线装配（同语义：每票 `available_at ≤ T` 的最新报告期）。

## 8. 监控建议（coverage 端点 + 服务日志）

- **服务日志关注** `financial mapping registry has no rows for mapping_version=...`
  警告（出现即映射目录装载缺失，全市场就绪将报 policy gap；运行
  `scripts/financial_pit_panel/load_mapping_catalog.py` 修复）；
- `share_validation` 分布漂移（consensus 占比骤降 = 某锚数据源异常）；
- `available_at_quality` 分布（exact 占比下降 = 披露日采集退化）；
- `discrepancy` 基准 ~1.7%、`revised_after_publish` 行数突增（更正季）；
- `is_complete` 与 latest_complete_period 推进是否符合法定节奏。

## 9. 性能实测（供容量参考）

cross-section 热查询 P95 **0.61s** / 冷 3s；panel 2000 行页 **0.18s**；coverage **1.6s**；全量拉取 ≈2 分钟；面板随财报/披露同步自动增量刷新（best-effort，失败不影响同步）。

## 10. 已知后续（不阻塞使用）

- 双源 bps 口径逐股归因清单（联调窗口共同完成）；
- AK 侧 roe/毛利率定义差的对齐决策（quantwin 侧）；
- 可选增强（默认不做）：外部期末总股本采集、批量同步末聚合刷新优化。

## 11. 变更历史

需求 v1.3 与三个已归档 change（面板交付 / 股本交叉验证 / 连接治理）为唯一权威记录；API 契约变更将走 openspec 流程并同步更新本文档。
