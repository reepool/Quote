# A 股历史全量回补

## 任务定位

Scheduler 手工任务 `a_share_daily_data_historical_backfill` 统一编排以下阶段：

1. 当前 A 股股票主数据治理。
2. 冻结包含 inactive/delisted 股票的历史 repair universe，并按上市、退市日期裁剪。
3. 交易日历刷新和目标区间连续覆盖校验。
4. 历史日线行情分 chunk 回补。
5. TDX XDXR 原始分红、送转、配股事件回补。
6. 生产复权因子同步和 TDX 审计因子派生。

任务为 `manual_only`，没有 cron trigger，默认 `dry_run=true`。部署或重启 Scheduler 不会自动执行历史下载。

## 推荐执行顺序

先运行全范围 dry-run：

```text
/run a_share_daily_data_historical_backfill start_date=1990-12-19 end_date=2025-12-31 exchanges=SSE,SZSE,BSE scopes=master,calendar,quotes,dividends,factors dry_run
```

再使用少量明确股票进行写入验证：

```text
/run a_share_daily_data_historical_backfill start_date=2018-01-01 end_date=2025-12-31 exchanges=SSE,SZSE instrument_ids=600000.SH,000001.SZ scopes=master,calendar,quotes,dividends,factors chunk_size=2 write
```

确认主数据、日历、行情、`adjustment_factors` 和 `adjustment_factors_tdx` 的报告后，再扩大股票范围。不要在首次验证时直接运行全市场写入。

## 参数

- `start_date`、`end_date`：必填，格式为 `YYYY-MM-DD`。
- `exchanges`：默认 `SSE,SZSE,BSE`。
- `scopes`：可选 `master,calendar,quotes,dividends,factors`。
- `instrument_ids`：可选，用于小样本验证或定向恢复。
- `dry_run` / `write`：默认 dry-run；只有 `write` 才执行生产写入。
- `resume`：默认 `true`，复用参数完全一致的 checkpoint。
- `chunk_size`：默认 100，允许 1 至 1000。
- `repair_universe_limit`：可选，每个交易所限制候选数量，适合验证。
- `per_instrument_timeout_sec`：单股票行情或 XDXR 请求超时。
- `force_current_master_refresh`：写入模式默认先刷新当前股票主数据。
- `override_lifecycle_filter`：仅用于明确的取证型修复，正常回补保持 `false`。

## Checkpoint

Checkpoint 位于：

```text
data/backfill_checkpoints/a_share_history_<parameter_hash>.json
```

其身份绑定日期、交易所、scopes、股票过滤、repair-universe 策略、chunk 大小和超时参数。参数变化后不会静默复用旧完成状态。

每个成功 chunk 完成后原子更新 checkpoint。失败或超时的 chunk 不会标记完成，使用相同参数再次执行时会继续处理。

Dry-run 不创建或更新 checkpoint，也不会调用行情、XDXR 或主数据写入接口。

## 数据治理规则

- 历史股票池来自本地主数据，包含 inactive 和 delisted 股票，不使用 active-only 当前股票池。
- 每只股票按 `listed_date`、`delisted_date` 裁剪下载区间。
- 行情请求前必须通过目标交易所日历连续覆盖校验。
- BSE 日历和股票范围从 2021-11-15 起计算，不要求北交所成立前的伪历史。
- 日历覆盖失败会阻断对应交易所行情回补，并在报告中给出缺失日期样本。

## 分红与复权隔离

- TDX category-1 XDXR 原始事件写入 `adjustment_factors_tdx`。
- 缺少前收盘价时仍保存分红、送转和配股字段，并标记 `pending_factor_missing_pre_close`。
- 后续存在前收盘价时，factor 阶段可以更新同一审计行。
- 原始事件刷新不会覆盖已有的有效计算因子和验证结论。
- TDX 数据不会写入生产表 `adjustment_factors`；生产因子仍使用现有正式数据源路由。

## 对现有业务的影响

- 不修改 `daily_data_update` 的定时计划或默认参数。
- 不修改旧 `/backfill`、`/backfill_factors` 和 gap repair 的入口行为。
- `update_daily_data_range()` 仅增加可选股票过滤和 factor-sync 参数，省略时保持原默认行为。
- 禁用新任务只需将 `config/05_scheduler.json` 中该 job 的 `enabled` 改为 `false`，无需数据库回滚。
