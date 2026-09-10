## Why

A 股股票日线的权威主源是 `pytdx`（SSE/SZSE/BSE 三市均是），但第一备源层薄弱：SSE/SZSE 的降级链直接落到 `baostock`（单连接、需要独立限流治理），BSE 更是直接落到 `akshare`。当通达信服务器池整体异常时，日更压力会集中涌向容量最紧张的 baostock。

2026-09-10 实测（本机直连，盘中）确认腾讯行情接口是当前唯一兼具以下条件的免费候选：

- 免费、无需 token、与 pytdx/baostock/akshare **故障域完全独立**（腾讯自有 CDN）；
- 高速：日线单请求 800 根 0.11–0.21s；16 线程实测约 **110 只/s**（100 只样本 0.9s），全市场 5558 只推算约 1 分钟；批量实时快照 60 只/请求 0.17s；
- 全历史：单票按 800 根/请求翻页可取至上市日（1999 年上市股票实测可翻页到 1999-11-10）；
- 覆盖面：920xxx 北交所在用代码实测 5/5 有数据；退市股（sz000003）返回至最后交易日；
- 口径可对账：腾讯 2026-09-03 `600000.SH` 成交量 898172 手 × 100 = 89,817,100 股，与 `quotes.db` 中 pytdx 入库行**精确一致**（不复权收盘价 9.27 亦一致）。

将其插入为 `pytdx` 之后的第一备源，可以在主源故障时让日更优先走一个高吞吐、独立的源，而不是立刻压向 baostock/akshare。

## What Changes

- 新增 `data_sources/tencent_source.py`（`TencentSource(BaseDataSource)`）：
  - `get_daily_data`：`web.ifzq.gtimg.cn` fqkline 接口，**空复权参数（不复权）**，按窗口有界翻页（≤800 根/请求）；
  - `get_latest_daily_data`：`qt.gtimg.cn` 批量实时端点（GBK 解码、固定字段位映射）；
  - `health_check`：探针票日线校验；
  - **不提供** `get_instrument_list` / `get_trading_calendar` / `get_adjustment_factors`（保持基类默认，路由不引用）。
- `data_sources/source_factory.py` 的 `_create_source_instance` 增加 `tencent` 分支；`data_sources/__init__.py` 导出。
- `config/03_data.json`：
  - `data_sources_config.tencent` 新增条目（enabled、exchanges_supported、rate limit 等同构字段）；
  - `routing.daily` 仅改三条股票链，在 `pytdx` 之后插入 `tencent`：
    - SSE stock：`pytdx → tencent → baostock → akshare`；
    - SZSE stock：`pytdx → tencent → baostock → akshare`；
    - BSE stock：`pytdx → tencent → akshare`。
- **不改变**：既有三个源、`routing.instrument_list` / `routing.calendar` / `routing.factor`、指数与 HKEX 链、`routing.daily_behavior` 熔断参数、DataManager 写入链、调度任务、存储格式。

## Capabilities

### New Capabilities

- `tencent-daily-quote-source`: 定义腾讯 A 股日线备源的合同——不复权口径与字段映射/单位、窗口覆盖与有界翻页、与既有日更熔断/降级链兼容的失败语义、`pytdx` 之后第一备源的路由位置、共享传输与限流约束。

### Modified Capabilities

- None. `data-source-routing` 已发布的"路由由配置驱动 + 启动校验"需求语义不变，本次只改路由配置数据；路由插入位置作为新 capability 的可观察行为固化，便于审核。

## Impact

- 代码：`data_sources/tencent_source.py`（新增）、`data_sources/source_factory.py`（一个 elif 分支）、`data_sources/__init__.py`（导出）、`config/03_data.json`（数据源配置 + 三处路由插入）。
- 测试：字段序防回归、手→股单位对账、翻页覆盖、空结果与传输错误分类、盘中当日 bar 透传、`get_latest_daily_data` 字段位、工厂接线与降级链（全部 mock 共享传输层，不打真实网络）。
- 复用既有已实施约束：共享 HTTP 传输层（standardize-http-transport）、源级熔断与半开探测（harden-a-share-daily-source-failover）、日线完整性校验与合法空区分（fix-a-share-daily-update-integrity）。
- 治理合规：腾讯源只拥有上游协议与源字段语义；canonical 写入 owner 仍是 `DataManager → database/operations.py`，不新增平行下载循环，不改 `data_manager.py` / `scheduler/tasks.py`。
