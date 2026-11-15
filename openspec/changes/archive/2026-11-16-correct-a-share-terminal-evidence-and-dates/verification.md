# 验证与交付记录

本轮是固定26项历史差异的小型修复，未扩为全市场清理或架构改造。生产操作已获得用户授权，经现有 `DatabaseOperations.mark_instrument_delisted` 执行；没有直接SQL更改生产生命周期。

## 业务结果

- 2026-11-16 新取 SSE 官方 A 股终止名单148条、SZSE名单190条，来源均完整无解析错误。
- 固定26项：23条已核实退市日更正（SSE20、SZSE3）、1条600680.SH解析误报消除且数据不变、2条上市日差异保留待核。
- 定向audit：更正前25个真实冲突（SSE20、SZSE5），更正后2个（SSE0、SZSE2）。原第26项为600680.SH的A/B混用误报，在解析修复后已消除。
- 全部26项保持 `status=delisted / is_active=0 / trading_status=0`，不在当前财务采集池。没有更改上市日或历史财务事实。
- audit明确限定固定26项，未获取current全名单，报告保留current覆盖未核验warning；未伪造全市场healthy、未追加生产verification或清除历史差异记录。下一次正常完整治理仍可能报告两条上市日待核。

## 实际更正清单

|证券|原退市日|官方退市日|
|---|---|---|
|000015.SZ|2001-10-22|2001-10-25|
|000556.SZ|2002-05-29|2002-06-03|
|000588.SZ|2001-06-15|2001-06-16|
|600069.SH|2020-08-27|2020-08-28|
|600074.SH|2020-06-02|2020-06-03|
|600086.SH|2021-03-17|2021-03-18|
|600175.SH|2020-08-14|2020-08-17|
|600190.SH|2025-07-25|2025-07-28|
|600240.SH|2020-02-05|2020-02-06|
|600401.SH|2019-07-12|2019-07-15|
|600555.SH|2022-07-13|2022-07-14|
|600614.SH|2021-07-21|2021-07-22|
|600625.SH|2001-04-23|2001-04-25|
|600634.SH|2021-07-21|2021-07-22|
|600646.SH|2003-09-22|2003-09-23|
|600687.SH|2021-03-03|2021-03-04|
|600695.SH|2022-06-14|2022-06-15|
|600701.SH|2021-04-30|2021-05-06|
|600709.SH|2003-05-23|2003-05-26|
|600747.SH|2019-12-12|2019-12-13|
|600786.SH|2008-03-18|2008-03-24|
|600799.SH|2005-12-31|2006-01-04|
|601558.SH|2020-07-02|2020-07-03|

## 保留项

|证券|本地上市日|本轮官方上市日|处理|
|---|---|---|---|
|000003.SZ|1991-07-03|1991-01-14|保留待核；退市日两侧均2002-06-14|
|000004.SZ|1991-01-14|1990-12-01|保留待核；退市日两侧均2026-07-14|
|600680.SH|1993-10-18|1993-10-18|不改数据；退市日两侧均2019-05-24|

两条上市日冲突不能仅凭一个列表确定旧数据错误或主体沿革，因此不修改上市日，不补造lineage。后续处理需要同一证券历史上市文件或已审核主体区间证据；在该证据出现前保留真实warning。两项未决不影响已退市状态及当前财务池排除。

## 来源与复现

- [preview.json](evidence/preview.json)：写前冻结的26项旧值、分类、逐证券官方证据、实际请求参数、抓取时间、原始来源hash。
- [result.json](evidence/result.json)：实际23项更正、前后audit摘要、写后生命周期、更正记录与业务不变量检查。证据正文通过preview引用，避免重复保存。
- [SSE原始响应](evidence/sse-terminal-1.json)及[SZSE原始XLSX](evidence/szse-terminal-1.xlsx)：一套权威新来源。原测试fixture不改写，继续回归真实A/B类别及数值单元格。
- 获准联网环境已检查 `query.sse.com.cn` DNS/TLS1.2、`www.szse.cn` DNS/TLS1.3，使用现有provider默认URL和请求参数，无新增外传对象或逐股网络抓取。

离线复现：以两个原始来源路径分别配置 `sse.terminal_list_file` 和 `szse.terminal_list_file`，调用 `AShareOfficialStockMasterSource.get_delisting_snapshot`，对照preview中的旧值和相同reconciler执行audit_only。该操作不需要联网；不得用冻结旧值再次覆盖生产。

受控纠错入口为现有 `mark_instrument_delisted` 的 `date_correction` 可选参数，结构见design D2及主数据runbook。reference为 `correct-a-share-terminal-evidence-and-dates`，实际审计保存在quotes.db差异表的 `a_share_terminal_date_corrected` 行，每个证券一条。无新表、无第二writer、无生产代码证券特判。

## 直接验证

- 新增更正相关15个离线场景通过：成功/幂等、普通冲突拒绝、预期旧值/身份/上市日/证据错误、未来日期、active拒绝、metadata/审计失败回滚、校验后并发变化拒绝。
- 真实fixture验证600680只有A股证据，B-only不生成A股终态，优先A_STOCK_CODE；沪深真实请求顺序为acquire→HTTP，本地fixture零HTTP/零acquire。
- 生产前后断言通过：instruments总数与清单外证券hash不变；26项除23条日期/source/updated_at外不变；metadata的正式证据与preview一致。
- 同一reconciler的audit前后业务状态完全相同，生产verification hash不变；生产重试一条更正后业务状态不变、无重复更正记录。
- 受影响财务记录hash保持一致：financial_statements_raw 30行、financial_core_facts_hot 10行、financial_indicator_snapshots_hot 10行；financial_disclosure_event_state和financial_summaries各0行。财务库仅通过mode=ro读取。

## 回归、Review与收尾

最终相关回归 **289 passed / 1 deselected**，耗时300.44秒。OpenSpec strict、5个修改Python文件语法检查及git diff --check通过。最后一次兼容性复验确认SZSE旧COMPANY_CODE列别名和数值证券代码仍可读取。

需求对照Review：原有A类问题（SSE类别混用、SZSE限流遗漏、已确认旧日期缺受控更正路径）已处理；最终未发现剩余阻塞缺陷。调用链继续收敛到原生命周期owner；生产代码无固定证券特判或第二套同目的业务循环。未扩充可选架构重构。Git基线比对确认既有改动的状态条目全部仍在，且本轮仅新增明确6个代码/测试/runbook修改及本卡材料。

实际回归命令（使用项目统一开发环境，SQLite位于QuoteBak）：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_a_share_delisting_reconciliation.py \
  tests/unit/test_a_share_official_stock_master.py \
  tests/unit/test_instrument_master_sync.py \
  tests/unit/test_master_governance_orchestration.py \
  tests/unit/test_scripts/test_prepare_sina_ths_local_core_import_manifest.py \
  tests/unit/test_research/test_financial_disclosure_incremental_sync.py \
  tests/unit/test_scheduler_instrument_master_governance_report.py \
  tests/unit/test_delisted_a_share_quote_backfill.py \
  tests/unit/test_hkex_instrument_master.py \
  -k 'not test_build_local_core_import_manifest_profiles_and_mapping_readiness'
```

排除的是前轮已在基线确认失败的无关profile readiness场景，不属于本次主数据改动。初次新增测试出现14项夹具失败：普通BaoStock入口正确拒绝伪造历史终态；改为在隔离测试库重建历史存量行后，15个更正场景全部通过。未放松生产入口保护，也未更改正确的测试预期。未主动重启常驻服务，源码部署后的首次自然全市场治理结果不在本次定向audit声明范围。旧生产verification/告警会保留，直到正常完整治理实际刷新。


临时产物收尾：确认本轮pytest会话全部退出、无.lock后，清理 `pytest-of-python/pytest-27, pytest-of-python/pytest-28, pytest-of-python/pytest-29` 的隔离测试库、两个一次性operator脚本及已转存摘要的XML，共约6.43 MiB。`pytest-of-python/pytest-26` 已由pytest保留策略自动回收。来源只保留本卡evidence中的一套权威版本；共享缓存/开发日志与其他任务目录未删除。Git基线暂留至提交复核，提交后删除本轮工作目录；不保留开发库或一次性执行脚本。
