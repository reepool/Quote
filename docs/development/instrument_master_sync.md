# A 股证券主数据同步

## 背景

2026-05-21 的本地排查发现，`quotes.db` 中 `600355.SH` 仍被标记为 `status=active,is_active=1,source=akshare,updated_at=2026-04-09 21:46:05.599604`，而该股实际已在 2026 年 4 月退市；同时 BSE 本地活跃股票数量为 300 只，低于当时公开当前列表约 314 只。这说明只依赖历史缓存会让股票数据字典滞后，进而影响日更股票池。

## 通用治理层

主数据新鲜度现在由 `DataManager.ensure_instrument_master_fresh()` 统一治理。该入口包装现有 `sync_instrument_master()`，只负责判断是否需要同步、是否复用本地新鲜状态、历史任务是否跳过、失败后是否继续，以及把结果写入报告；A 股 BaoStock/AkShare/pytdx 的实际源优先级和 upsert 规则仍只在 `sync_instrument_master()` 中维护。

普通行情日更、研究同步、财务同步和当前快照类任务都应通过这个共享治理入口后再读取 `instruments.is_active` 股票池。行情日更保留 `instrument_master_sync` 报告字段作为兼容字段，同时也会产生同一份 `instrument_master_governance` 结果；研究和财务维护任务使用 `instrument_master_governance` 报告段。最终架构不再保留一套行情专用主数据前置逻辑和另一套研究/财务主数据逻辑。

## 日更行为

`DataManager.update_daily_data()` 在普通 A 股日更前会先执行共享主数据治理：

- 同步范围默认是 `SSE`、`SZSE`、`BSE` 的 `stock` 主数据。
- 同步完成后，日更重新从数据库读取 active instruments，再开始行情抓取。
- 当前退市或停用的 instrument 只改变主数据状态，不删除历史行情。
- 历史回补默认跳过当前主数据同步，避免用今天的股票状态改写历史回补语义；报告中会记录 `historical_backfill_current_master_sync_skipped`。

兼容配置位于 `config/03_data.json` 的 `data_config.instrument_master_sync`；通用治理配置位于 `data_config.instrument_master_governance`。治理配置默认继承同步配置中的超时、新鲜度阈值、历史回补 skip、失败继续和 pytdx 诊断开关。

## 数据源优先级

- `BaoStock` 是 A 股主数据主源，负责 `ipoDate`、`outDate`、`status` 等字段。
- `AkShare` 是备用源和补充源，尤其用于发现 BSE 新股或 BaoStock 临时不可用时的当前列表。
- `pytdx` 只做当前列表差异诊断，不写入 `listed_date`、`delisted_date`、`status` 或最终 `is_active`。2026-05-21 实测 SSE/BSE 返回 0、SZSE 覆盖不完整，因此默认关闭 `pytdx_validation_enabled`，只在人工诊断时显式开启。

## BSE 退市确认

北交所当前上市名单使用 BSE 官方 `nqxxController/nqxxCnzq.do` 当前列表口径。2026-05-26 现场验证显示，`xxfcbj[]=2` 返回当前 BSE 上市股票 `315` 只，且包含停牌未退市股票：名单中 `xxtpbz=T` 的 `920058.BJ`、`920305.BJ` 仍在当前列表内；已公告终止上市并摘牌的 `920680.BJ` 不在当前列表中。因此，BSE 当前名单“消失”可以作为退市候选信号，但不能单独作为正式退市依据。

`DataManager.sync_instrument_master()` 在 BSE 主数据刷新后会执行二次确认：

- 用本地刷新前 active BSE 股票集合减去本次 BSE 当前名单，得到候选。
- 通过现有 CNInfo 公告扫描器按市场级入口扫描 `column=neeq, plate=bj`，不逐股抓取。
- 只有标题命中“股票终止上市暨摘牌”“摘牌”等终局公告时，才调用 `DatabaseOperations.mark_instrument_delisted()` 写入 `status=delisted,is_active=0,trading_status=0,delisted_date`。
- “可能被终止上市”“拟终止上市”“退市风险警示”“事先告知书”等只作为风险或待确认事项，不反写正式退市状态。
- 若当前名单消失但未找到终局公告，报告中的 `bse_delisting.unconfirmed_count` 和样例会提示人工或后续周度复核，不会直接误杀。

该逻辑补齐 BSE 无 BaoStock `outDate/status` 的缺口，并确保停牌不会被误判为退市。

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

研究、财务和快照维护任务的结构化结果包含 `instrument_master_governance`：

- `status/action/reason` 表示已同步、复用新鲜本地状态、按历史语义跳过、按市场策略跳过、warning 或 error。
- `summary` 展示新增、停用、活跃合计。
- `warnings/errors` 保留 BSE fallback-only、unsupported market、失败继续等治理诊断。

Telegram 维护报告会独立渲染“证券主数据治理”段落，避免把业务任务成功误解为主数据来源完全权威。

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
