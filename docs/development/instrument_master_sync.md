# A 股证券主数据同步

## 背景

2026-05-21 的本地排查发现，`quotes.db` 中 `600355.SH` 仍被标记为 `status=active,is_active=1,source=akshare,updated_at=2026-04-09 21:46:05.599604`，而该股实际已在 2026 年 4 月退市；同时 BSE 本地活跃股票数量为 300 只，低于当时公开当前列表约 314 只。这说明只依赖历史缓存会让股票数据字典滞后，进而影响日更股票池。

## 日更行为

`DataManager.update_daily_data()` 在普通 A 股日更前会先执行 `sync_instrument_master()`：

- 同步范围默认是 `SSE`、`SZSE`、`BSE` 的 `stock` 主数据。
- 同步完成后，日更重新从数据库读取 active instruments，再开始行情抓取。
- 当前退市或停用的 instrument 只改变主数据状态，不删除历史行情。
- 历史回补默认跳过当前主数据同步，避免用今天的股票状态改写历史回补语义；报告中会记录 `historical_backfill_current_master_sync_skipped`。

相关配置位于 `config/03_data.json` 的 `data_config.instrument_master_sync`。

## 数据源优先级

- `BaoStock` 是 A 股主数据主源，负责 `ipoDate`、`outDate`、`status` 等字段。
- `AkShare` 是备用源和补充源，尤其用于发现 BSE 新股或 BaoStock 临时不可用时的当前列表。
- `pytdx` 只做当前列表差异诊断，不写入 `listed_date`、`delisted_date`、`status` 或最终 `is_active`。2026-05-21 实测 SSE/BSE 返回 0、SZSE 覆盖不完整，因此默认关闭 `pytdx_validation_enabled`，只在人工诊断时显式开启。

现有 `DataSourceFactory.get_instrument_list(force_refresh=True)` 会按 `instrument_list.a_stock = ["baostock", "akshare"]` 路由获取数据；BaoStock 正常返回时，AkShare 只补充主源缺失的 instrument。若 BaoStock 抛异常，AkShare 会作为备用源接管，但报告会提示该结果不具备退市日期权威性。

## Upsert 防线

数据库写入保留既有 `delisted_date`，且基于已过期的 `delisted_date` 强制设置 `is_active=False`、`status=delisted`。对已有 `delisted` 或 `auto_deactivated*` 状态的记录，缺少 `delisted_date` 的 AkShare/pytdx 等当前名单源不能自动重新激活，避免备用源把退市股或幽灵股唤醒。

## 报告

日更 JSON 报告包含 `instrument_master_sync`：

- 总体状态、耗时、数据源优先级。
- 分市场同步前后总数、活跃数、停用数。
- 新增和停用品种样例。
- pytdx 差异诊断。
- warnings/errors。

Telegram 日更报告会渲染“证券主数据同步”段落；即使行情更新成功，主数据同步警告也会暴露给操作员。

## HKEX 边界

本轮同步只覆盖 A 股 `SSE/SZSE/BSE`。2026-05-21 只读检查显示，HKEX 本地 `stock` 主数据为 `active=3022`、`inactive=1604`；直接调用 AkShare `stock_hk_spot_em` 当前列表返回约 `4641` 条。两者存在差异，但这不应直接套用 A 股规则：

- AkShare 港股 spot 列表覆盖范围较宽，样例中包含旧代码、债券/RMB 柜台等非普通股票。
- 本地 HKEX inactive 多数来自 `auto_deactivated_no_data` 与 `auto_deactivated_zombie`，属于历史行情可得性/品种类型混杂问题，不等同于交易所正式退市日期。
- 港股没有 BaoStock 这种可提供统一 `outDate/status` 的主源；若要做 HKEX 主数据日更，应先新增 HKEX 专用 OpenSpec：定义普通股票过滤、代码/柜台映射、退市/停牌来源、以及是否允许 AkShare/yfinance 重新激活自动封禁品种。

## 验证记录

2026-05-21 实施验证：

- `sqlite3 data/quotes.db` 只读确认：`600355.SH|600355|*ST精伦|SSE|stock|active|1|||akshare|2026-04-09 21:46:05.599604`。
- `sqlite3 data/quotes.db` 只读确认：`BSE stock active=300`，`SSE stock active=2307/inactive=3`，`SZSE stock active=2883/inactive=5`。
- 手动执行 `DataManager.sync_instrument_master(['SSE','SZSE','BSE'])` 后，`600355.SH` 更新为 `status=delisted,is_active=0,delisted_date=2026-04-27,source=baostock`；BSE active 从 300 更新到 314，新增 `920011.BJ`、`920012.BJ`、`920200.BJ` 等 14 只。
- 同次实测显示 pytdx 当前列表不适合作为主源：SSE=0、BSE=0、SZSE=1494 且覆盖不完整。
- HKEX 只读检查：本地 `active=3022,inactive=1604`；AkShare 当前列表探测返回 `4641` 条。该差异需要单独的 HKEX 清洗/主数据设计，不纳入本次 A 股同步。
- 聚焦单测覆盖 BaoStock 过期 `outDate` 停用、AkShare 备用源接管、pytdx 只诊断不写库、历史回补 skip、日更报告 `instrument_master_sync` 渲染。
