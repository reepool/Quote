# 证券主数据同步与治理

## 背景

2026-05-21 的本地排查发现，`quotes.db` 中 `600355.SH` 仍被标记为 `status=active,is_active=1,source=akshare,updated_at=2026-04-09 21:46:05.599604`，而该股实际已在 2026 年 4 月退市；同时 BSE 本地活跃股票数量为 300 只，低于当时公开当前列表约 314 只。这说明只依赖历史缓存会让股票数据字典滞后，进而影响日更股票池。

2026-06-02 已新开 OpenSpec `add-hkex-instrument-master-sync`，准备建设港股专用主数据系统。港股主数据的定位与 A 股主数据一致：它不是单个港股研究模块的内部实现，而是未来所有港股行情、研究、公司信息、财务、披露、估值、风险与事件模块解析当前 universe 的统一基础设施。

## 通用治理层

主数据新鲜度现在由 `DataManager.ensure_instrument_master_fresh()` 统一治理。该入口包装现有 `sync_instrument_master()`，只负责判断是否需要同步、是否复用本地新鲜状态、历史任务是否跳过、失败后是否继续，以及把结果写入报告；A 股 BaoStock/AkShare/pytdx 的实际源优先级和 upsert 规则仍只在 `sync_instrument_master()` 中维护。

普通行情日更、研究同步、财务同步和当前快照类任务都应通过这个共享治理入口后再读取 `instruments.is_active` 股票池。行情日更保留 `instrument_master_sync` 报告字段作为兼容字段，同时也会产生同一份 `instrument_master_governance` 结果；研究和财务维护任务使用 `instrument_master_governance` 报告段。最终架构不再保留一套行情专用主数据前置逻辑和另一套研究/财务主数据逻辑。

## 日更行为

`DataManager.update_daily_data()` 在普通 A 股日更前会先执行共享主数据治理：

- 同步范围默认是 `SSE`、`SZSE`、`BSE` 的 `stock` 主数据。
- 同步完成后，日更重新从数据库读取 active instruments，再开始行情抓取。
- 当前日更读取会要求 `tradable_only=True`，因此 `is_active=1` 但 `trading_status=0` 的停牌品种不会进入当日日更抓取；历史区间回补仍使用原 active universe 语义。
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

## HKEX 边界与新需求

已归档的 A 股同步只覆盖 `SSE/SZSE/BSE`。2026-05-21 只读检查显示，HKEX 本地 `stock` 主数据为 `active=3022`、`inactive=1604`；直接调用 AkShare `stock_hk_spot_em` 当前列表返回约 `4641` 条。2026-06-02 复核显示本地 HKEX `stock` 为 `active=3020`、`inactive=1606`，active 行的 `updated_at` 仍主要停留在 2026-04-09，最近更新来自 `auto_deactivated_zombie` 等行情可得性清理，不是完整主数据刷新。

这些差异不应直接套用 A 股规则：

- AkShare 港股 spot 列表覆盖范围较宽，样例中包含旧代码、债券/RMB 柜台等非普通股票。
- 本地 HKEX inactive 多数来自 `auto_deactivated_no_data` 与 `auto_deactivated_zombie`，属于历史行情可得性/品种类型混杂问题，不等同于交易所正式退市日期。
- 港股没有 BaoStock 这种可提供统一 `outDate/status` 的主源；HKEX 主数据必须独立定义普通股票过滤、代码/柜台映射、退市/停牌来源、以及是否允许 AkShare/yfinance 重新激活自动封禁品种。

新 OpenSpec 的目标是新增 HKEX 专用策略，而不是把 `HKEX` 直接加入 A 股 `sync_instrument_master()`：

- 官方 HKEX / HKEXnews 清单、退市和停牌证据作为生命周期权威来源。
- AkShare / 东方财富作为高速候选发现、字段补充和差异预警来源，不单独决定 `active/delisted`。
- yfinance 和本地行情缺口只作为行情可得性诊断，不作为正式退市证据。
- 港股 product scope 必须先分类：普通股、REIT、ETF、债券、窝轮、牛熊证、inline warrant、旧代码、Trading Only Securities、人民币柜台、临时柜台、Stock Connect 特殊代码区和其他预留/过渡区等。
- `auto_deactivated_no_data` / `auto_deactivated_zombie` 只代表行情不可得，不等同退市。
- 第一阶段默认 `audit_only`，先输出官方源、本地库、AkShare/东方财富之间的差异和样例；确认口径后再进入 safe write 和 lifecycle write。

港股研究模块的默认规则：

- 所有 HKEX 当前 universe 研究任务必须先经过共享 HKEX 主数据治理，再读取本地 active instruments。
- 各研究域不得各自抓一份港股代码表作为自己的业务股票池。
- 如果 HKEX 主数据治理降级但业务任务按配置继续运行，研究 readiness 和任务报告必须暴露降级原因。

## HKEX 实现入口

HKEX 主数据已通过 `DataManager.sync_hkex_instrument_master()` 接入共享治理入口：

- `audit_only`：只读取本地 `instruments`、官方/补充源快照和行情可得性，输出差异，不写 `instruments`、metadata 或 lifecycle 字段。
- `safe_write`：只写官方 in-scope 新标的和安全范围内存量标的的非破坏性基础字段、`instrument_master_metadata` 和 `instrument_master_discrepancies`；不会退市、停牌或重新激活。
- `lifecycle_write`：在 `safe_write` 基础上，只有官方 active/suspended/delisted 证据通过后，才调用 `mark_instrument_active()`、`mark_instrument_suspended()` 或 `mark_instrument_delisted()`。

写入门控由 `HKEXSourceEvidencePolicy` 统一输出：

- 主 active 源 `ListOfSecurities.xlsx` 可用且无源错误时，才允许 `safe_write` 和 reactivation 写入。
- 如果主源不可用但 HKEXnews active JSON 可用，系统会报告 `active_fallback_used=True`；该 fallback 可用于 audit 差异判断，但不允许直接 safe-write 或 reactivation。
- delisting 写入要求 delisted 证据源可用且存在确认行。
- suspension 写入要求 PDF parser 或人工 review 回灌产生结构化 suspended 行。

辅助表：

- `instrument_master_metadata`：保存 HKEX product type、research scope、双柜台 canonical 映射、官方源 URL、parser version 和 snapshot hash。
- `instrument_master_discrepancies`：保存每次同步中需要人工复核的差异样本。

人工复核回灌：

- `manual_review_file` 默认是 `data/hkex_manual_review.json`，可配置为 JSON 或 CSV；文件不存在时按空 review 处理。
- 推荐字段：`instrument_id`/`code`、`action`、`effective_date`、`reason`、`evidence_url`、`reviewed_by`。
- `action=delisted/suspended/active` 会被解析成经过人工确认的生命周期证据；这用于处理普通证券中“本地 active、官方 active 不存在、官方 inactive JSON 也未覆盖”的历史差异。
- HKEX 非研究证券不进入人工复核链路，直接在产品分类层排除。当前按 HKEX Stock Code Allocation Plan 的官方区间与官方子类别排除：
  - 临时柜台：`02900-02999`、`08551-08600`、`82900-82999`。
  - Trading Only / Nasdaq-Amex Pilot：官方子类别 `Trading Only Securities`，以及 `04330-04399`。
  - 人民币柜台：`80000-89999` 以及 `Trading Currency=CNY/RMB` 或 `RMB Counter=Y`。
  - 衍生品与结构化产品：`10000-29999` Derivative Warrants、`47000-48999` Inline Warrants、`49500-69999` CBBCs、SPAC Warrants。
  - 特殊/过渡/非普通证券区：Stock Connect 特殊代码区、专业投资者债券/政府债/票据、专业优先股、HDR、受限证券、Leveraged and Inverse Products、预留或 future-use 区间。
- 不按证券简称里的 `RTS`、`-500` 等后缀做临时代码判断，避免误伤有连续行情的正式代码，例如 `00290.HK`、`08239.HK`。
- 上述 excluded 产品不允许作为 `safe_write` 新标的写入 `instruments`；已历史落库的 excluded 产品应在 `instrument_master_metadata.research_scope='exclude'` 留痕，并在 `instruments` 中标为 `status='excluded', is_active=0, trading_status=0`，让港股日更和区间回补直接跳过。
- `safe_write` 和 `lifecycle_write` 写入 metadata 后，会把官方分类为 `research_scope='exclude'` 的历史落库标的批量标为不可用；该处理只改主数据可用性，不删除历史行情。
- 在 `audit_only` 下仍只报告；在 `lifecycle_write` 下才会根据该证据改写状态。
- API：
  - `GET /instruments/hkex/master/review-required`：现场运行 `audit_only`，返回当前待复核样本。
  - `GET /instruments/hkex/master/manual-review`：返回已回灌的 review 记录。
  - `POST /instruments/hkex/master/manual-review`：追加一条 review 结论；可设置 `run_audit_after=true` 立即复核 audit-only 结果。
- Telegram：
  - `/hkex_review pending [limit]`：查看当前待复核样本。
  - `/hkex_review list [limit]`：查看已回灌记录。
  - `/hkex_review 08888.HK delisted 2026-05-30 已确认退市 evidence=https://...`：追加一条 reviewed lifecycle evidence。

手工主数据任务：

- `config/05_scheduler.json` 中的 `hkex_instrument_master_sync` 是 `manual_only=true`，没有自动运行时间。
- Telegram 可用 `/run hkex_instrument_master_sync` 手工触发，默认参数为 `mode=safe_write, timeout_sec=60`；会写入安全新增/metadata 候选，但不会写退市、复牌、停牌生命周期字段。
- `lifecycle_write` 是更高权限模式，会先执行 `safe_write` 能做的安全写入，再按官方/人工证据改写退市、复牌和停牌状态；它不是和 `safe_write` 并列同时打开的第二个开关。
- `config/03_data.json` 的 HKEX governance 默认仍保持 `audit_only`，所以自动港股行情日更的前置治理暂时不写库；当前只启用手工任务写入。
- 本地直接调用可用：

```bash
/home/python/miniconda3/envs/Quote/bin/python -c 'exec("""import asyncio
from scheduler.tasks import scheduled_tasks
async def main():
    ok = await scheduled_tasks.hkex_instrument_master_sync(mode="safe_write", timeout_sec=60)
    print({"success": ok})
asyncio.run(main())
""")'
```

- 未来需要自动运行时，把 `manual_only` 改为 `false` 并补充 `trigger`；切换到 `lifecycle_write` 前必须先复核报告中的退市、复活和停牌候选样本。
- `lifecycle_write` 上线前必须先在复制库运行 `scripts/dev_validation/validate_hkex_lifecycle_write_on_copy.py`。通过条件是：source evidence policy 全部满足、`review_required=0`、停牌样本来自结构化 PDF/人工证据、复活样本在主 active 源中、退市样本在官方 delisted 源中、excluded 产品同步后无 active/tradable 残留、日更读取 `tradable_only=True` 可跳过 `trading_status=0` 标的。

当前生产配置的官方源：

- 主 active 源：HKEX `ListOfSecurities.xlsx`，来自 HKEX Securities Lists 的 Full List of Securities。
- 辅 active 源：HKEXnews `activestock_sehk_e.json`。
- delisted 源：HKEXnews `inactivestock_sehk_e.json`。
- suspension 源：HKEXnews prolonged suspension monthly PDF（Main Board / GEM）；系统通过 `pypdf` 提取文本并解析代码。若运行环境未安装 `pypdf` 或 PDF 格式变化导致解析失败，结果会降级为 warning，不会静默作为停牌证据。
- PDF 文本解析只接受报告表格行块：行首序号、公司名称中的括号代码、以及同一块内的日期行必须同时存在；说明文字里的普通数字不会被当成港股代码，避免把 `00001.HK`、`00005.HK` 等误标停牌。
- HKEXnews delisted JSON 若不提供真实退市日期，系统只写 `status=delisted,is_active=0,trading_status=0`，不再用同步日伪造 `delisted_date`；精确退市日后续应从 HKEX 交易安排公告或公司公告补录。

研究模块应通过 `DataManager.resolve_hkex_current_universe()` 获取港股当前研究 universe。该 resolver 会复用共享 governance，优先按 metadata 过滤普通股/REIT/ETF 和 canonical counter；metadata 不可用时允许降级回退到本地 active instruments，并在 `readiness='degraded'` 和 warnings 中暴露原因。

## 验证记录

2026-06-04 HKEX lifecycle-write 复制库 gate 验证：

- 使用 `scripts/dev_validation/validate_hkex_lifecycle_write_on_copy.py` 从 `data/quotes.db` 复制到 `/tmp/hkex_lifecycle_validation_20260604_pass2.db` 后运行 `mode=lifecycle_write`，生产库未执行 lifecycle 写入。
- 结果文件：`/tmp/hkex_lifecycle_validation_20260604_pass2.json`，`validation.status=pass`，`blockers=[]`。
- source usage：`hkex_securities_list=17749`、`hkexnews_active_list=18217`、`hkexnews_delisted_list=864`、`hkexnews_suspension_report=12`。
- source evidence policy 全部通过：`safe_write_allowed=true`、`reactivation_write_allowed=true`、`delisting_write_allowed=true`、`suspension_write_allowed=true`，`source_error_count=0`。
- 复制库运行前：HKEX `total=4649`、`active=2948`、`tradable_stock=2948`，`excluded_active_or_tradable=0`。
- 复制库运行后：HKEX `total=4649`、`active=3034`、`tradable_stock=2960`，状态分布为 `active=2960`、`suspended=74`、`delisted=113`、`excluded=1435`、`auto_deactivated_no_data=46`、`auto_deactivated_zombie=21`；`excluded_active_or_tradable=0`。
- lifecycle diff 合计 `282`：`suspended=74`、`delisted=113`、`reactivated=53`、`excluded=42`。这些变化只发生在复制库，用于证明 gate 规则和写入路径符合预期。
- 本次 gate 前发现并修复三个上线风险：PDF parser 曾把说明文字数字误解析为停牌代码；HKEXnews active fallback 曾覆盖主源 XLSX 的产品分类字段；metadata 标为 exclude 的历史落库标的曾未同步改写 `instruments.status/trading_status`。
- 当前生产配置仍未切到 lifecycle：`config/05_scheduler.json` 的手工任务仍为 `mode=safe_write`，`config/03_data.json` 的自动 HKEX governance 仍为 `mode=audit_only`。

2026-06-03 HKEX live audit-only 验证：

- 使用生产配置官方 URL 只读验证，未写入生命周期字段或辅助表。
- source usage：`hkex_securities_list=17671`、`hkexnews_active_list=17838`、`hkexnews_delisted_list=864`。
- 合并后官方 active `17854`，official delisted 原始 `864`，按 instrument_id 去重后 `781`。
- 本地 HKEX stock：`total=4626, active=3020, inactive=1606`。
- 决策计数：`insert_candidates=13419`、`metadata_update_candidates=3019`、`reactivation_candidates=1416`、`delisting_candidates=359`、`review_required=1`。这些是原始官方全证券差异，不等于可写入研究 universe。
- safe-write 仍按 `allowed_product_types=["ordinary_equity","reit","etf"]`、canonical counter 和产品分类过滤；报告会输出 `safe_write_preview_count` 和允许生命周期候选数，防止把债券/窝轮/牛熊证误读为研究股票池。
- safe-write 产品分类会排除 HKEX 官方临时代码区间；这类代码不应进入研究 active universe。
- 行情诊断只读输出：`no_local_quote_count=1314`、`stale_local_quote_count=1611`、`mutation_candidates=[]`。

2026-06-02 HKEX audit-only 开发验证：

- 使用 fixture 官方/补充源对当前 `data/quotes.db` 只读验证，未写入生命周期字段。
- 本地 HKEX stock：`total=4626, active=3020, inactive=1606`，`source_counts={"akshare": 4626}`。
- fixture 源计数：`hkex_securities_list=8`、`hkexnews_active_list=4`、`hkexnews_delisted_list=2`、`akshare_hk_spot_em=5`、`eastmoney_hk_profile=4`；补充源按 instrument_id 去重后 `supplemental=5`。
- 决策计数：`insert_candidates=2`、`metadata_update_candidates=5`、`reactivation_candidates=1`、`suspension_candidates=0`、`delisting_candidates=2`、`review_required=3015`。由于官方 fixture 是小样例，`review_required` 高是预期现象。
- 行情诊断只读输出：`no_local_quote_count=1314`、`stale_local_quote_count=1611`、`mutation_candidates=[]`。

2026-05-21 实施验证：

- `sqlite3 data/quotes.db` 只读确认：`600355.SH|600355|*ST精伦|SSE|stock|active|1|||akshare|2026-04-09 21:46:05.599604`。
- `sqlite3 data/quotes.db` 只读确认：`BSE stock active=300`，`SSE stock active=2307/inactive=3`，`SZSE stock active=2883/inactive=5`。
- 手动执行 `DataManager.sync_instrument_master(['SSE','SZSE','BSE'])` 后，`600355.SH` 更新为 `status=delisted,is_active=0,delisted_date=2026-04-27,source=baostock`；BSE active 从 300 更新到 314，新增 `920011.BJ`、`920012.BJ`、`920200.BJ` 等 14 只。
- 同次实测显示 pytdx 当前列表不适合作为主源：SSE=0、BSE=0、SZSE=1494 且覆盖不完整。
- HKEX 只读检查：本地 `active=3022,inactive=1604`；AkShare 当前列表探测返回 `4641` 条。该差异需要单独的 HKEX 清洗/主数据设计，不纳入本次 A 股同步。
- 聚焦单测覆盖 BaoStock 过期 `outDate` 停用、AkShare 备用源接管、pytdx 只诊断不写库、历史回补 skip、日更报告 `instrument_master_sync` 渲染。
