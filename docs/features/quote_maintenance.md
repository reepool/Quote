# 行情维护与历史回补

本手册是日线行情下载、补数、缺口维护和已退市 A 股历史回补的当前运维入口。
它只说明 `daily_quotes` 的维护，不替代证券主数据、交易日历、复权因子或公司行动的专门手册。

## 边界与安全规则

- 日常自动维护由 scheduler 的 `daily_data_update` 和周度
  `find_gap_and_repair` 配置负责；不要在任务运行时启动另一条写入流程。
- 日期区间补数使用本地历史时点 universe。除非任务明确要求，不以今天的
  主数据刷新替代历史股票池。
- 先使用最小日期、市场或标的范围验证结果，再扩大范围。所有写入结果都要
  检查成功/失败计数、交易日覆盖和报告中的生命周期裁剪原因。
- 当前 CLI 和 Telegram 仍有兼容入口。W3
  `unify-quote-maintenance-command-paths` 会将它们收口到一个应用服务；在此之前，
  不要从入口复制或改造下载、缺口或写入循环。

## 日常维护

`config/05_scheduler.json` 中的 `daily_data_update` 是日线例行更新，
`find_gap_and_repair` 每周日 15:00 运行。后者默认检查 SSE、SZSE、BSE、HKEX，
从 2025-01-01 起检测，并通过本地 lifecycle 规则裁剪停编指数、退市后区间和
不应请求的长期停牌标的。

日常 A 股更新会刷新所需主数据，并对新股和短缺口执行受配置限制的小窗口追补。
超出该窗口的缺口才进入下列人工流程。

## 指定日期或区间补数

Telegram `/backfill` 是当前人工补数入口：单日调用现有
`daily_data_update`，日期区间调用 `daily_data_backfill_range`，避免逐交易日重复
全市场扫描。

```text
/backfill 2026-03-27
/backfill 2026-03-27 SSE
/backfill 2026-04-09 2026-05-21 SSE SZSE BSE
```

支持的交易所为 `SSE`、`SZSE`、`BSE`、`HKEX`、`NASDAQ`、`NYSE`。省略市场时，
使用当前 A 股预设。开始前确认 scheduler 没有同一行情任务正在运行；完成后查看
Telegram 结果中的成功日期与失败日期，失败不能被当作已覆盖。

单日 CLI 兼容入口如下，仅用于受控的本地人工操作，不作为日常调度的替代：

```bash
python main.py update --exchanges SSE SZSE BSE --target-date 2026-03-27
```

## 缺口检查与修复

正式周度任务 `find_gap_and_repair` 先检测再修复，并保留失败段跳表、生命周期
过滤和 HKEX 防护。需要即时查看缺口时，CLI 的 `gap` 是只读检测入口：

```bash
python main.py gap --exchanges SSE SZSE --start-date 2026-01-01 --end-date 2026-03-31 --detailed
```

`/find_gap_and_repair` 和 `/smart_fill_gaps` 当前仍会启动历史脚本子进程。它们不是
新增生产流程的模板；若必须使用，先将参数限制到单个市场或小样本，并保留报告。
这两个兼容路径的替代和退出由 W3 管理。

## 全量和单标的下载

`main.py download` 仍是兼容 CLI。它会刷新请求市场的标的列表，并写入现有行情存储；
适合隔离环境或受控的小范围修复，不应与 scheduler 并行执行。

```bash
python main.py download --exchanges SSE SZSE --start-date 2024-01-01 --end-date 2024-12-31 --types stock
python main.py download --instrument-id 000001.SZ --start-date 2024-01-01 --end-date 2024-12-31
```

`--instrument-id` 必须同时提供起止日期。可用交易所、日期、品种和续传参数以
`python main.py download --help` 为准；不要依赖旧文档中的输出样例、质量阈值或
进度文件字段。

## 已退市 A 股回补

已退市标的是独立 operator workflow：先 dry-run，再以很小的 `--limit` 或
`--instrument-ids` 进行写入验证。脚本只通过现有 `daily_quotes` upsert 路径写入，
不修改标的生命周期字段，也不会删除行情行。

```bash
/home/python/miniconda3/envs/Quote/bin/python scripts/backfill_delisted_a_share_quotes.py \
  --delisted-year-start 1999 --delisted-year-end 2024 --limit 20

/home/python/miniconda3/envs/Quote/bin/python scripts/backfill_delisted_a_share_quotes.py \
  --execute --instrument-ids 000508.SZ --timeout-sec 120
```

结果中的 `missing`、`partial` 和 `covered` 是按上市至退市的生命周期覆盖窗口
判断。上游返回空或失败应保留为 `source_empty`/failure，而不能视作已覆盖。

## 大范围历史回补

`a_share_daily_data_historical_backfill` 是 scheduler 中 `manual_only` 的综合任务，
默认 `dry_run=true`。它可以覆盖主数据、交易日历、行情、分红和因子等 scope，属于
长时间 operator 变更，不是普通 `/backfill` 的扩展。执行前必须明确日期、scope、
数据源和 checkpoint，并先完成临时数据库或小范围验证。

## 相关文档

- [交易日历管理](trading_calendar_management.md)
- [调度系统](scheduler_system.md)
- [Telegram 运维入口](../telegram_task_manager.md)
- [证券主数据同步与治理](../development/instrument_master_sync.md)
- [框架改造总纲](../development/framework_refactoring_program.md)
