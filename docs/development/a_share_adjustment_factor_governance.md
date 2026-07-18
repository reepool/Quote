# A 股复权因子治理与 BaoStock 退出方案

## 结论

BaoStock 不再作为任何业务下载主源。现有 `adjustment_factors` 继续作为兼容读取表，
但不得再把不同供应商的原始累积因子直接拼接进去。AkShare 的 A 股因子来自新浪
`stock_zh_a_daily(adjust="hfq-factor")`，其绝对累积基准与 BaoStock 不兼容，不能按
`(instrument_id, ex_date)` 直接覆盖 BaoStock 路径。

生产库诊断曾发现 417 只股票存在 BaoStock/AkShare 混合路径；416 个源切换点中，
157 个偏差超过 0.1%，114 个超过 5%，最大偏差约 85.39%。BaoStock 还存在
`factor == cumulative_factor` 的旧语义记录，以及 `000001.SZ` 在 2020-12-31 的
无公司行动累积因子重置。因此 BaoStock 只能保留为历史证据，不能作为权威真值。

## 数据分层

- `adjustment_factor_observations`：按来源隔离保存供应商原始累积因子、事件因子、
  归一化事件比率、来源 profile、质量状态和回补批次。
- `adjustment_factors_canonical`：按 `series_version` 保存从 1 基准开始、由相邻事件
  比率累乘得到的标准序列，不复制供应商的绝对累积基准。
- `adjustment_factor_series_status`：保存全市场版本的覆盖率、冲突、TDX 对账、
  前复权路径误差和生产晋级资格。
- `adjustment_factor_instrument_status`：逐证券记录 `complete_with_events`、
  `complete_no_events` 或 `incomplete`，避免把数据缺失误判为从未发生公司行动。
- `adjustment_factors`：现有生产兼容表。新 A 股事件只能按事件比率接在旧表尾部；
  历史日期只写 observation，不覆盖旧表。

## 分阶段实施

### 0. CNInfo 官方事件基线与多源因子比较模型

公司行动原始证据和因子路径分开维护：

- `corporate_action_observations` 中保留 CNInfo 分红/配股全历史，CNInfo 是官方事件基线；
- `adjustment_factors_tdx` 继续保留 TDX XDXR 全历史，作为独立备份和缺失补充；
- `adjustment_factor_observations` 分别保存 `cninfo_event_derived_v1` 与
  `tdx_event_derived_v1`，不把两家来源拼成一条未经标注的序列；
- `adjustment_factor_series_status` 保存全市场 benchmark，比较 CNInfo 自研、TDX 自研、
  Sina 和 BaoStock 路径的覆盖率、P50/P95、最大误差和阈值超限比例；
- 默认重建不创建 `adjustment_factors_canonical`。只有显式传入 `build_canonical=true`
  才创建隔离 staging 候选，且仍不影响生产读取；
- `a_share_cninfo_corporate_action_daily_sync` 在每日行情更新后按最近 7 天滚动刷新活跃股票的
  两个原始来源，再利用本地全历史重建累计因子；任务单实例运行且不会晋级生产；
- `a_share_cninfo_corporate_action_backfill` 和
  `a_share_cninfo_adjustment_factor_rebuild` 仍是手工任务，分别负责官方事件全量回补和
  全历史因子重建/对账。

自动日更只刷新源数据的近期窗口，因子重建不能只从这个窗口起算，否则累计因子会错误地从
1 重新开始。CNInfo 是事件来源候选，不代表其自研累计因子已被选为生产主源；主源选择必须
等待全市场 benchmark 完成并人工评审。

### 1. 全市场 CNInfo 官方事件回补

先完成 CNInfo 分红和配股结构化历史，不以前置公告语义分析阻塞全市场处理：

```text
/run a_share_cninfo_corporate_action_backfill start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE,BSE scopes=dividends,allotments chunk_size=50 request_interval_seconds=1.0 resume=true dry_run=true
```

```text
/run a_share_cninfo_corporate_action_backfill start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE,BSE scopes=dividends,allotments chunk_size=50 request_interval_seconds=1.0 resume=true dry_run=false
```

历史冲突先记录为异常类别。只有累计路径无法解释的重大边界才进入后续公告补证。

### 2. CNInfo/TDX 独立路径和多源 benchmark

默认 `build_canonical=false`。预演只计算报告，不写 observation、benchmark 或 canonical：

```text
/run a_share_cninfo_adjustment_factor_rebuild start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE,BSE dry_run=true
```

确认后写入 CNInfo 自研、TDX 自研独立路径，并保存 benchmark 状态：

```text
/run a_share_cninfo_adjustment_factor_rebuild start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE,BSE dry_run=false
```

任务返回 `source_selection_status=deferred`，即使来源存在差异也不阻止独立路径落库。
报告中的 `benchmark_series_version` 可用于质量 API 查询。

### 3. 参考累计因子补足

#### 3.1 定向预演

预演只统计股票池和已有 observation，不访问外部因子接口、不写数据库、也不创建
checkpoint。

```text
/run a_share_adjustment_factor_rebuild start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE instrument_ids=600000.SH,000001.SZ source=akshare chunk_size=2 request_interval_seconds=1.0 resume=false dry_run=true
```

#### 3.2 定向写入与核对

先用长历史、已知 BaoStock 异常和无分红样本进行 smoke test。任务顺序调用新浪接口，
每只股票后保存 checkpoint。`request_interval_seconds` 默认 1 秒，不允许并发任务。

```text
/run a_share_adjustment_factor_rebuild start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE instrument_ids=600000.SH,000001.SZ source=akshare chunk_size=2 request_interval_seconds=1.0 resume=false dry_run=false
```

定向任务只写独立 staging 版本，不会改写配置中的生产 canonical 版本。

#### 3.3 全市场回补

必须先 dry-run，再执行 write。全市场任务预计约 5,000 至 6,000 次股票级请求；
不得与其他 AkShare/Sina 全市场任务并发，也不得注册为 cron。

```text
/run a_share_adjustment_factor_rebuild start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE,BSE source=akshare chunk_size=100 request_interval_seconds=1.0 resume=true dry_run=true
```

```text
/run a_share_adjustment_factor_rebuild start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE,BSE source=akshare chunk_size=100 request_interval_seconds=1.0 resume=true dry_run=false
```

中断后使用相同参数和 checkpoint 继续。不要把 dry-run checkpoint 用于 write；当前实现
已保证 dry-run 不创建或读取 checkpoint。

参考路径补足后，重新运行第 2 步 benchmark，使 Sina 等来源覆盖率进入同一份报告。

### 4. 显式候选构造

只有全市场 benchmark 已完成并确定候选规则后，才允许构造隔离 staging：

```text
/run a_share_cninfo_adjustment_factor_rebuild start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE,BSE build_canonical=true dry_run=false
```

该操作仍不会切换生产读取，也不会自动选择主源。

## Benchmark 指标与候选门禁

Benchmark 本身不选择主源，至少报告：

- 各来源可用股票数和相对全市场覆盖率。
- 与 CNInfo 自研路径可比较的股票数和比例。
- CNInfo 自研、TDX 自研、Sina、BaoStock 所有可用路径的两两比较矩阵。
- 归一化前复权路径的平均、P50、P95 和最大误差。
- 路径比较只在双方共同的最新事件日期内归一化；双方末端事件日期不同的股票
  另计入 `endpoint_mismatch_instruments` 和样本，不能静默视为路径质量误差或一致。
- `available_instruments` 使用股票级完整扫描状态并包含 `complete_no_events`；
  `path_instruments` 只统计实际存在因子事件行的股票，两个口径不得混用。
- 超过 0.1%、0.5% 和 1% 的点数及比例。
- 事件精确匹配、错位匹配、经济字段冲突和来源单边事件。

后续显式构造的 canonical 候选必须同时满足：

- 股票级完成覆盖率不低于 99%，确认无公司行动的股票也计为已覆盖。
- 回补结束日期覆盖本地交易日历中 SSE、SZSE、BSE 各自最近交易日。
- 源内事件因子冲突率不高于 0.1%。
- 与 TDX XDXR 的精确日期或三交易日内错位事件完成对账。
- TDX 事件差异率不高于 2%，事件因子误差不高于 0.5%。
- canonical 与 TDX 的前复权等价路径最大误差不高于 0.5%。
- 下载过程没有未解决错误。

与 legacy BaoStock 路径的前复权误差是诊断指标，不把 BaoStock 当作权威答案。较大差异
必须结合 TDX XDXR、交易所/巨潮已实施分红公告以及价格连续性进一步确认。

旧版本 TDX 回补没有持久化股票级空结果。升级后需至少重新执行一次对应范围的 TDX XDXR
写入回补，系统才会把确认无事件的股票记录为 `source=tdx / source_profile=tdx_xdxr /
complete_no_events` 并纳入 benchmark 覆盖率；已有事件路径仍会作为最低可用覆盖证据保留。

质量查询：

```text
GET /api/v1/corporate-actions/adjustment-factor-quality?series_version=a_share_event_product_v1
GET /api/v1/corporate-actions/adjustment-factor-observations?instrument_id=000001.SZ&source=cninfo&source_profile=cninfo_event_derived_v1&limit=100
GET /api/v1/corporate-actions/adjustment-factor-canonical?instrument_id=000001.SZ&series_version=a_share_event_product_v1&limit=100
```

## 晋级与回滚

默认配置保持：

```json
{
  "read_dataset": "legacy",
  "canonical_series_version": "a_share_event_product_v1",
  "allow_legacy_fallback": true
}
```

默认 write 只写独立 observation 和由参数哈希标识的 benchmark 版本，不写 canonical。
显式候选操作才写入 staging 版本。只有全市场任务全部通过质量门禁，
系统才在同一数据库事务中把 staging 行、逐证券覆盖状态和生产版本状态一起晋级；定向、
部分完成或中断任务不能修改已晋级版本。只有生产版本
`adjustment_factor_series_status.promotion_eligible=true` 且人工复核通过后，才将
`read_dataset` 改为 `canonical`。报价 API 会在 `factor_metadata` 中披露请求数据集、
实际数据集、版本、逐证券覆盖状态和 fallback 状态。

回滚只需把 `read_dataset` 改回 `legacy` 并重启应用。不要删除 BaoStock observation、
legacy 表或 canonical 版本；保留这些证据用于追溯和复核。

## 已知限制

- AkShare/Sina 不是官方公司行动源，可能缺少早期、退市或特殊证券数据。
- TDX XDXR 也是待核对证据，不能单独证明无遗漏。
- 官方公告能验证现金分红、送转和配股语义，但通常不直接提供完整累积复权序列。
- 最终替换 BaoStock 前，仍需对重大差异样本做事件级官方公告核验，并对全市场前复权
  收益率连续性、年末锚点和最新锚点做统计复核。
