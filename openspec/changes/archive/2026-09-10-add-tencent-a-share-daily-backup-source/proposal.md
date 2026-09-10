## Why

A 股股票日线的权威主源是 `pytdx`（SSE/SZSE/BSE 三市均是），但第一备源层薄弱：SSE/SZSE 的降级链直接落到 `baostock`（单连接、需要独立限流治理），BSE 更是直接落到 `akshare`。当通达信服务器池整体异常时，日更压力会集中涌向容量最紧张的 baostock。

2026-09-10 实测（本机直连，盘中）确认腾讯行情接口是当前唯一兼具以下条件的免费候选：

- 免费、无需 token、与 pytdx/baostock/akshare **故障域完全独立**（腾讯自有 CDN）；
- 高速：newfqkline 单请求 ≤800+1 根日线 0.14–0.21s；串行逐只实测约 11 只/s；批量实时快照 60 只/请求 0.17s；
- 全历史：按 800 根/请求翻页可取至上市日（600000 实测翻 3 页到 1999-11-10）；
- 覆盖面：920xxx 北交所有**全历史**日线（`fqkline` 端点对 BSE 只返回最新 1 根，选型依据见 design）；退市股返回至最后交易日；
- 口径可对账：`000001.SZ` 1105134×100=110,513,400、`300750.SZ` 262864×100=26,286,400、`920000.BJ` 11810×100=1,181,000，在 2026-09-03 与 `quotes.db` 中 pytdx 行**恰好精确一致**（跨日不保证：000001.SZ 2026-09-07 即差 100 股）；`600000.SH` 898172×100=89,817,200 vs pytdx 89,817,100，相差 1 手（腾讯按手四舍五入、pytdx 截断）。**一般规则：与 pytdx 对账允许 ≤100 股的手级取整容差；sh688*/sh689* 原始值已是股**（见 design 决策 3）。

将其插入为 `pytdx` 之后的第一备源，可以在主源故障时让日更优先走一个吞吐足够、独立的源，而不是立刻压向 baostock/akshare。日更链路是 DataManager 逐只串行，全市场一次回退约 9–13 分钟——备源的价值是**主源挂了以后不打满 baostock**，不是缩短日更时长。

## What Changes

- 新增 `data_sources/tencent_source.py`（`TencentSource(BaseDataSource)`）：
  - `get_daily_data`：**单一 URL** `https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get`，空复权参数（`day` 节点，不复权），11 列映射，按窗口有界翻页（≤800 根/请求，实返 800+1 根按日期去重）；
  - `get_latest_daily_data`：`qt.gtimg.cn` 批量实时端点（GBK 解码、固定字段位映射、**量纲同日 K 前缀规则**）；
  - `health_check`：探针票日线校验；
  - **不提供** `get_instrument_list` / `get_trading_calendar` / `get_adjustment_factors`（保持基类默认，路由不引用）。
- `data_sources/source_factory.py` 的 `_create_source_instance` 增加 `tencent` 分支；`data_sources/__init__.py` 导出。
- `config/03_data.json`：
  - `data_sources_config.tencent` 新增条目（enabled、exchanges_supported、rate limit 等同构字段）；
  - `routing.daily` 仅改三条股票链，在 `pytdx` 之后插入 `tencent`：
    - SSE stock：`pytdx → tencent → baostock → akshare`；
    - SZSE stock：`pytdx → tencent → baostock → akshare`；
    - BSE stock：`pytdx → tencent → akshare`。
- `docs/configuration/config_file.md` 增加 `tencent` 数据源小节（与 pytdx/baostock 小节同构）。
- **不改变**：既有三个源、`routing.instrument_list` / `routing.calendar` / `routing.factor`、指数与 HKEX 链、`routing.daily_behavior` 熔断参数、DataManager 写入链、调度任务、存储格式。

## Capabilities

### New Capabilities

- `tencent-daily-quote-source`: 定义腾讯 A 股日线备源的合同——不复权口径、11 列字段映射与按前缀区分的量纲规则、窗口覆盖与有界翻页、节假日容忍的 pre_close 回看、与既有日更熔断/降级链兼容的失败语义（含 403/429 字面）、`pytdx` 之后第一备源的路由位置、单一 URL 与共享传输/限流约束。

### Modified Capabilities

- None. `data-source-routing` 已发布的"路由由配置驱动 + 启动校验"需求语义不变，本次只改路由配置数据；路由插入位置作为新 capability 的可观察行为固化，便于审核。

## Impact

- 代码：`data_sources/tencent_source.py`（新增）、`data_sources/source_factory.py`（一个 elif 分支）、`data_sources/__init__.py`（导出）、`config/03_data.json`（数据源配置 + 三处路由插入）、`docs/configuration/config_file.md`（新增 tencent 小节）。
- 测试：11 列映射与字段序防回归、前缀量纲（×100 与 ×1 各有对账例）、amount 万元换算、count+1 去重翻页、节假日 pre_close 回看、403/429 异常字面、快照字段位与量纲、工厂接线与降级链（全部 mock 共享传输层，不打真实网络）。
- 复用既有已实施约束：共享 HTTP 传输层（standardize-http-transport）、源级熔断与半开探测（harden-a-share-daily-source-failover）、日线完整性校验与合法空区分（fix-a-share-daily-update-integrity）。
- 治理合规：腾讯源只拥有上游协议与源字段语义；canonical 写入 owner 仍是 `DataManager → database/operations.py`，不新增平行下载循环，不改 `data_manager.py` / `scheduler/tasks.py`。不复用 AkShare `stock_zh_a_hist_tx`（按年切分、单位启发式不可靠、与东财 AkShare 源耦合）。
