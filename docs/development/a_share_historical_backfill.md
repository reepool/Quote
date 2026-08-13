# A 股历史全量回补

## 任务定位

Scheduler 手工任务 `a_share_daily_data_historical_backfill` 统一编排以下阶段：

1. 当前 A 股股票主数据治理。
2. 冻结包含 inactive/delisted 股票的历史 repair universe，并按上市、退市日期裁剪。
3. 交易日历刷新和目标区间连续覆盖校验。
4. 历史日线行情分 chunk 回补。
5. TDX XDXR 原始分红、送转、配股事件回补。
6. 生产复权因子同步和 TDX 审计因子派生。
7. 显式选择 `index_composition` 时，使用共享 BaoStock 额度和 session lock 回补核心指数历史成分。

任务为 `manual_only`，没有 cron trigger，默认 `dry_run=true`。部署或重启 Scheduler 不会自动执行历史下载。

执行模式分为三类：

- `dry_run`：只校验参数、治理历史股票池并生成 chunk 计划，不请求行情或 TDX 数据源。
- `dry_run scan_sources=true`：真实请求 TDX XDXR 和可选因子派生，统计事件、空响应、超时和错误，但不写数据库、不保存 checkpoint。
- `write`：真实请求数据源并持久化结果，成功 chunk 写入 checkpoint。

## 推荐执行顺序

先运行全范围 dry-run：

```text
/run a_share_daily_data_historical_backfill start_date=1990-12-19 end_date=2025-12-31 exchanges=SSE,SZSE,BSE scopes=master,calendar,quotes,dividends,factors dry_run
```

再使用少量明确股票进行 TDX 真实源扫描：

```text
/run a_share_daily_data_historical_backfill start_date=1990-12-19 end_date=2025-12-31 exchanges=SSE,SZSE instrument_ids=600000.SH,000001.SZ scopes=dividends,factors chunk_size=2 dry_run scan_sources=true
```

确认扫描返回非零 `raw_events`、合理的 `derived_factors`，并且 `saved_events=0` 后，再进行小范围写入验证：

```text
/run a_share_daily_data_historical_backfill start_date=2018-01-01 end_date=2025-12-31 exchanges=SSE,SZSE instrument_ids=600000.SH,000001.SZ scopes=master,calendar,quotes,dividends,factors chunk_size=2 write
```

确认主数据、日历、行情、`adjustment_factors` 和 `adjustment_factors_tdx` 的报告后，再扩大股票范围。不要在首次验证时直接运行全市场写入。

## 参数

- `start_date`、`end_date`：必填，格式为 `YYYY-MM-DD`。
- `exchanges`：默认 `SSE,SZSE,BSE`。
- `scopes`：可选 `master,calendar,quotes,dividends,factors,index_composition,security_state,price_limits,corporate_actions`；除指数成分外的可选历史项仍需各自来源完成后才能写入。
- `instrument_ids`：可选，用于小样本验证或定向恢复。
- `dry_run` / `write`：默认 dry-run；只有 `write` 才执行生产写入。
- `scan_sources`：默认 `false`；仅允许与 `dry_run=true` 组合，并且 scopes 必须包含 `dividends` 或 `factors`。
- `resume`：默认 `true`，复用参数完全一致的 checkpoint。
- `chunk_size`：默认 100，允许 1 至 1000。
- `repair_universe_limit`：可选，每个交易所限制候选数量，适合验证。
- `per_instrument_timeout_sec`：单股票行情或 XDXR 请求超时。
- `force_current_master_refresh`：写入模式默认先刷新当前股票主数据。
- `override_lifecycle_filter`：仅用于明确的取证型修复，正常回补保持 `false`。
- `repair_pending_factor_quotes`：默认 `false`；仅可在包含 `factors` 的写入模式启用。启用后只对当前范围内 `pending_factor_missing_pre_close` 的股票调用既有退市 A 股历史行情回补，并重新派生 TDX 因子。
- `index_instrument_ids`：指数历史范围，默认 `000300.SH,000905.SH,000016.SH`；未验证的 `000852.SH` 会零请求返回 unavailable。
- `index_sampling`：默认 `daily`，最终历史基线必须逐交易日观察；`monthly` 仅用于小范围源探针和临时验证。
- `index_max_queries_per_run`：默认 `4000`，单次最多查询次数，不超过项目 BaoStock 每小时 5000 次限制。
- `index_daily_request_reserve`：默认 `5000`，为日更和其他任务保留的当日额度。
- `index_checkpoint_path`：可选指数专用 checkpoint；默认位于主任务 checkpoint 同目录。

## Checkpoint

Checkpoint 位于：

```text
data/backfill_checkpoints/a_share_history_<parameter_hash>.json
```

其身份绑定日期、交易所、scopes、股票过滤、repair-universe 策略、chunk 大小和超时参数。参数变化后不会静默复用旧完成状态。

每个成功 chunk 完成后原子更新 checkpoint。失败或超时的 chunk 不会标记完成，使用相同参数再次执行时会继续处理。

普通 dry-run 不创建或更新 checkpoint，也不会调用行情、XDXR 或主数据写入接口。`scan_sources=true` 会读取 TDX，但仍不会创建、更新或完成 checkpoint chunk。

## 核心指数历史成分

指数成分复用同一个手工任务，但该 scope 在股票 universe 解析前独立执行，不会下载股票行情或刷新股票主数据。BaoStock 只提供历史成员，不提供历史权重；落库成员的 `weight` 保持 `NULL`，API readiness 明确返回 membership ready、weights deferred。

先做零网络全范围 dry-run，核对交易日数量、总请求量、单批请求量和剩余额度：

```text
/run a_share_daily_data_historical_backfill start_date=2005-01-01 end_date=2026-08-04 scopes=index_composition index_instrument_ids=000300.SH,000905.SH,000016.SH index_sampling=daily index_max_queries_per_run=4000 index_daily_request_reserve=5000 dry_run
```

单日三指数源探针使用 write 模式和临时数据库环境执行。不要把 `monthly` 探针结果当成最终历史基线：

```text
/run a_share_daily_data_historical_backfill start_date=2020-06-30 end_date=2020-06-30 scopes=index_composition index_instrument_ids=000300.SH,000905.SH,000016.SH index_sampling=monthly index_max_queries_per_run=3 index_daily_request_reserve=5000 write
```

生产回补前必须先在临时 `quotes.db` 完成多年验证，再备份生产库。生产每轮最多 4000 次查询，返回 `batch_query_limit_reached` 属于正常的可恢复 partial；使用完全相同参数和 checkpoint 重跑，直到所有逐交易日 observation 完成：

```text
/run a_share_daily_data_historical_backfill start_date=2005-01-01 end_date=2026-08-04 scopes=index_composition index_instrument_ids=000300.SH,000905.SH,000016.SH index_sampling=daily index_max_queries_per_run=4000 index_daily_request_reserve=5000 resume=true write
```

每次真实调用都先由 `BaostockAccessGovernor` 写入 `data/runtime/baostock/api_usage.json`，完整登录会话持有 `data/runtime/baostock/session.lock`。额度不足、锁冲突、网络异常或成员数量越界时停止当前批次，不绕过治理器。历史观测日期与本地实际获取时间分开保存；严格 `known_at` 早于获取时间时不会返回后来回补的数据。

## 数据治理规则

- 历史股票池来自本地主数据，包含 inactive 和 delisted 股票，不使用 active-only 当前股票池。
- 每只股票按 `listed_date`、`delisted_date` 裁剪下载区间。
- inactive/delisted A 股缺少 `delisted_date` 但存在本地最后行情日时，以最后行情日作为只读降级边界纳入历史回补；该日期不会写回主数据，并会以 `a_share_stock_delisted_last_quote_fallback` 报告。
- 行情请求前必须通过目标交易所日历连续覆盖校验。
- BSE 日历和股票范围从 2021-11-15 起计算，不要求北交所成立前的伪历史。
- 日历覆盖失败会阻断对应交易所行情回补，并在报告中给出缺失日期样本。

## 分红与复权隔离

- TDX category-1 XDXR 原始事件写入 `adjustment_factors_tdx`。
- XDXR 返回空且同股票行情探针也为空时，会刷新 TDX 长连接并重试一次，避免把静默断线误判为无分红历史。
- 缺少前收盘价时仍保存分红、送转和配股字段，并标记 `pending_factor_missing_pre_close`。
- 停牌除权日优先使用本地停牌占位行的正数 `pre_close`；正常交易的除权日不使用同日除权参考价，而取除权日前最后一个 `tradestatus=1` 的正数收盘价。
- 本地行情仍无有效证据时，TDX 因子引擎按 XDXR 事件日寻找严格早于该日的最近 K 线，不要求除权日本身存在 K 线。
- 后续存在前收盘价时，factor 阶段可以更新同一审计行。
- 原始事件刷新不会覆盖已有的有效计算因子和验证结论。
- TDX 数据不会写入生产表 `adjustment_factors`；生产因子仍使用现有正式数据源路由。

## 完整性门禁与对账

写入模式下，只要 scopes 包含 `dividends` 或 `factors`，任务都会在 provider chunk 完成后读取数据库最终状态并生成 `completeness` 阶段：

- 汇总 persisted XDXR 事件、pending 因子、受影响股票和现金分红事件。
- 将 TDX 公司行动事件与 BaoStock/AkShare 生产复权因子变化证据进行本地只读对账。
- 对账优先匹配同日且因子幅度一致的记录，再使用治理后的交易日历在 3 个交易日窗口内进行确定性一对一匹配；默认相对因子容差为 5%。
- 报告 exact factor match、shifted factor match、factor conflict、reference factor change only、TDX event only、provider-empty、生命周期排除和降级边界样本。
- 参考因子变化没有现金分红、送转或配股字段，不能单独证明 TDX 漏掉了具体公司行动。
- BaoStock 使用涨跌幅复权算法，TDX 审计因子使用公司行动字段推导理论除权价；同日但幅度不一致的记录归类为 factor conflict，不强制合并。
- 已解释的日期偏移不会使任务变成 `partial`；pending 因子、未解决生命周期边界、降级边界、provider 错误/超时、因子冲突、未解释参考因子变化或参考证据不可用时，顶层状态返回 `partial`，即使全部 chunk 已完成。

`resume=true` 可以跳过已经完成的 provider chunk，但不会跳过持久化完整性检查，因此报告反映当前数据库状态，而不是只显示本轮新增计数。

对已知 pending 股票进行显式行情修复时，推荐先使用 instrument filter 控制范围：

```text
/run a_share_daily_data_historical_backfill start_date=1990-12-19 end_date=2026-07-15 exchanges=SSE,SZSE instrument_ids=000040.SZ,000584.SZ scopes=dividends,factors chunk_size=2 repair_pending_factor_quotes=true resume=false write
```

该选项会产生真实行情请求和数据库写入，不允许与 `dry_run` 或 `scan_sources=true` 同时使用。

审计事件可通过只读 API 查询：

```text
GET /api/v1/corporate-actions/xdxr?instrument_id=000001.SZ&start_date=2007-01-01&end_date=2008-12-31&limit=100&offset=0
```

接口还支持 `validation_result` 过滤，响应固定包含 `audit_only=true` 和 `dataset=adjustment_factors_tdx`，不能作为生产复权因子接口使用。

## 公司行动多源验证

手工任务 `a_share_corporate_action_validation` 对已入库 TDX XDXR 做三层只读验证：

1. **事件字段层**：通过 AkShare 的 `stock_fhps_em` 读取东方财富已实施分红方案，按除权除息日比较每 10 股现金分红和送转总比例。AkShare 是适配器，报告中的上游来源明确标记为 Eastmoney。
2. **官方公告层**：对冲突和单边事件的有限股票扫描 CNInfo 权益分派实施公告。该层只证明实施公告元数据存在，不在未解析 PDF 的情况下声称金额已经获得官方验证。
3. **累计结果层**：将 TDX 与 BaoStock/AkShare 的事件日因子都归一为 1，从同一窗口重新连乘，在各年末和最新日期比较累计因子。默认误差不超过 0.1% 为 acceptable，0.1% 至 0.5% 为 warning，超过 0.5% 为 conflict。

推荐先运行少量股票和近期区间：

```text
/run a_share_corporate_action_validation start_date=2020-01-01 end_date=2026-07-15 exchanges=SSE,SZSE instrument_ids=600000.SH,000001.SZ reference_sources=baostock,akshare scan_official_announcements=true official_sample_limit=2
```

扩大到全市场时，CNInfo 不会扫描全部股票，只会优先选择事件冲突和单边事件股票，并受 `official_sample_limit` 限制：

```text
/run a_share_corporate_action_validation start_date=1990-12-19 end_date=2026-07-15 exchanges=SSE,SZSE,BSE reference_sources=baostock,akshare scan_official_announcements=true official_sample_limit=50 official_lookback_years=3
```

关键参数：

- `field_tolerance`：每 10 股现金或送转字段的绝对差容限，默认 `0.0001`。
- `acceptable_cumulative_error_pct`：累计因子可接受误差百分数，默认 `0.1`。
- `warning_cumulative_error_pct`：累计因子 warning 上限百分数，默认 `0.5`。
- `official_sample_limit`：本轮最多进行 CNInfo 定向扫描的股票数量。
- `official_lookback_years`：官方公告扫描只覆盖截止日前最近若干年，默认 3 年；更早事件不会被误报为“官方公告不存在”。
- `per_source_timeout_sec`：单报告期 Eastmoney 请求或单股票 CNInfo 请求的超时。

该任务不更新 `adjustment_factors`、`adjustment_factors_tdx` 或主数据。事件字段冲突、任一来源单边事件、累计 warning/conflict、源请求失败或官方证据未解决时返回 `partial`。累计因子收敛不会覆盖事件层冲突，因为不同错误可能在最终累计结果中相互抵消。

## 巨潮官方公司行动结构化回补

手工任务 `a_share_cninfo_corporate_action_backfill` 将巨潮历史分红和配股实施方案写入隔离的官方观测层：

- 上游来源固定标记为 `cninfo`。
- `akshare.stock_dividend_cninfo` 和 `akshare.stock_allotment_cninfo` 仅作为巨潮 Web API 的传输适配器。
- 现金分红、送股、转增和配股比例统一保存为每股值。
- 巨潮返回缺少数值时，可从标准的 `10派`、`10送`、`10转增`实施描述中解析，并标记为 `parsed_description`。
- 缺少除权日的记录不会被移动到交易日或补成虚假日期，而是标记为 `partial_missing_ex_date`。
- 接口异常、畸形空响应和超时标记为 `indeterminate`，不能解释为确认无公司行动。
- AkShare 巨潮适配器在自身 HTTP 请求层设置超时，任务严格等待当前请求结束后再访问下一个接口，不通过取消后台线程继续下载。
- CNInfo 适配层对缺少 `records`、JSON 解码失败和连接/超时类瞬时错误执行最多 3 次串行请求，按 1 秒、2 秒退避；成功空响应和字段确定缺失的业务记录不会重试。
- 重试耗尽时只记录 HTTP 状态、巨潮结果码/消息和响应字段名，不记录完整响应正文；对应接口继续标记为 `indeterminate`，留待相同 checkpoint 续跑。
- 显式存在配股失败退款日的记录标记为 `failed`；实际配股数量为零但失败状态不明确时标记为部分完整，不能进入后续因子计算。
- 完整接口快照中消失的旧事件保留审计记录并标记为非当前记录；异常或无法判定的响应不会使已有事件失效。
- 覆盖状态按股票、来源、接口和请求起止区间分别保存，窄区间检查不会覆盖全历史结论。

新增数据集：

```text
corporate_action_observations
corporate_action_instrument_status
corporate_action_effective_date_evidence
```

它们不参与当前 `adjustment_factors`、`adjustment_factors_tdx` 或 canonical 生产读取。

CNInfo 与 TDX 始终保持独立来源事实：`source=cninfo` 的观测只允许保存 CNInfo 返回或由其实施描述确定性解析出的字段。TDX 可用于后续对账，但不得写入 CNInfo 观测、伪装为 CNInfo 补录，也不得改变 CNInfo 的 `partial_missing_fields` 或 `indeterminate` 结论。CNInfo 定时日更只覆盖沪深，北交所明确标记为 `source_not_supported`；TDX 的沪深北日更继续运行，历史 CNInfo 北交所观测保留审计但不再补齐。

首次运行前先做定向预演：

```text
/run a_share_cninfo_corporate_action_backfill start_date=1990-12-19 end_date=2026-07-18 exchanges=SSE,SZSE instrument_ids=600000.SH,000001.SZ,000003.SZ scopes=dividends,allotments chunk_size=3 request_interval_seconds=1.0 resume=false dry_run
```

确认规划后进行定向写入：

```text
/run a_share_cninfo_corporate_action_backfill start_date=1990-12-19 end_date=2026-07-18 exchanges=SSE,SZSE instrument_ids=600000.SH,000001.SZ,000003.SZ scopes=dividends,allotments chunk_size=3 request_interval_seconds=1.0 resume=false write
```

全市场任务必须保持单请求流并启用 checkpoint。先完成结构化基线，不要求逐股票公告分析：

```text
/run a_share_cninfo_corporate_action_backfill start_date=1990-12-19 end_date=2026-07-18 exchanges=SSE,SZSE scopes=dividends,allotments chunk_size=50 request_interval_seconds=1.0 resume=true write
```

2026-07-18 全市场首轮回补完成后，如需使用已生成的 checkpoint 重试剩余 `indeterminate` 股票，必须保持原参数完全一致：

```text
/run a_share_cninfo_corporate_action_backfill start_date=1990-12-19 end_date=2026-07-18 exchanges=SSE,SZSE scopes=dividends,allotments chunk_size=50 request_interval_seconds=1.0 resume=true dry_run=false
```

不要修改日期、交易所、scopes、chunk 大小或请求间隔；任一参数变化都会生成不同的 checkpoint 身份，无法只续跑原任务剩余股票。

全量结构化回补完成后，对缺少 `ex_date` 但具有实际经济内容的沪深事件执行公告候选发现。
先用少量股票预演；此处 dry-run 会发送受限、串行、按事件窗口约束的巨潮公告请求：

```text
/run a_share_cninfo_special_action_discovery start_date=1990-12-19 end_date=2026-07-18 exchanges=SSE,SZSE instrument_ids=600108.SH max_events=10 target_offset=0 window_before_days=10 window_after_days=30 max_window_days=180 request_interval_seconds=0.5 dry_run=true
```

确认候选标题和窗口合理后落库：

```text
/run a_share_cninfo_special_action_discovery start_date=1990-12-19 end_date=2026-07-18 exchanges=SSE,SZSE max_events=500 target_offset=0 window_before_days=10 window_after_days=30 max_window_days=180 request_interval_seconds=0.5 dry_run=false
```

全市场目标超过单批上限时，任务返回 `partial` 和 `targets.next_target_offset`；使用该值作为
下一次 `target_offset` 继续，直至 `targets.has_more=false`。发生请求或写入错误时优先使用
`targets.retry_target_offset` 重试当前批次，不要直接跳到下一批。

候选公告仅标记 `resolution_status=candidate`。系统不会根据标题、下一交易日、TDX 日期或
价格跳变自动填写生效日期。只有明确的官方正文或人工复核证据写为 `resolved` 后，因子重建
才会使用该日期；`corporate_action_observations.ex_date` 不被覆盖。

全量事件完成后，默认运行独立路径和 benchmark，不创建 canonical 候选：

```text
/run a_share_cninfo_adjustment_factor_rebuild start_date=1990-12-19 end_date=2026-07-17 exchanges=SSE,SZSE,BSE dry_run=true
```

```text
/run a_share_cninfo_adjustment_factor_rebuild start_date=1990-12-19 end_date=2026-07-17 exchanges=SSE,SZSE,BSE dry_run=false
```

该任务分别保存 CNInfo 自研和 TDX 自研路径，比较已有 Sina、BaoStock 参考路径，并返回
`source_selection_status=deferred`。只有后续明确决定候选规则时才传入
`build_canonical=true`。

只读查询：

```text
GET /api/v1/corporate-actions/official-observations?instrument_id=000001.SZ&source_profile=cninfo_dividend&limit=100&offset=0
GET /api/v1/corporate-actions/official-coverage?coverage_status=partial_missing_fields&limit=100&offset=0
GET /api/v1/corporate-actions/effective-date-evidence?instrument_id=600108.SH&resolution_status=candidate&limit=100&offset=0
```

官方观测接口默认只返回当前有效记录。审计已从完整快照中消失的历史记录时，增加 `include_inactive=true`。

当前已知边界：

- 巨潮结构化分红中的股权分置、重整转增等记录可能缺少生效日期，需要独立公告证据。
- 部分早期退市股票的分红接口返回畸形空结构，需要公告恢复，不能标记为 `complete_no_events`。
- 北交所不进入 CNInfo 定时日更，也不使用 TDX 回填 CNInfo 表；TDX 独立表继续覆盖北交所。
- 当前公告任务只发现候选元数据，不自动解析任意 PDF 正文；未 resolved 的事件继续阻断对应 CNInfo 累积因子路径。

## 对现有业务的影响

- 不修改 `daily_data_update` 的定时计划或默认参数。
- 不修改旧 `/backfill`、`/backfill_factors` 和 gap repair 的入口行为。
- `update_daily_data_range()` 仅增加可选股票过滤和 factor-sync 参数，省略时保持原默认行为。
- 禁用新任务只需将 `config/05_scheduler.json` 中该 job 的 `enabled` 改为 `false`，无需数据库回滚。
