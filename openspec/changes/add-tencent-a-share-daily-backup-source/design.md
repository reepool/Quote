# Design: add-tencent-a-share-daily-backup-source

## Context

当前 `config/03_data.json` 的 `routing.daily` 中，A 股股票日线降级链为：

- SSE stock / SZSE stock：`pytdx → baostock → akshare`
- BSE stock：`pytdx → akshare`

`pytdx` 是三市股票日线的权威主源（不复权原始价 + 本地复权因子治理）。备源层缺少一个与主源故障域独立、吞吐足够的"第一备源"。

2026-09-10 实测证据（本机直连、盘中）：

| 项目 | 结果 |
|---|---|
| 日线单请求（800 根，不复权） | 0.11–0.21s |
| 并发 16 线程逐只日线 | 100 只样本 0.9s ≈ 110 只/s，零失败、无限流 |
| 批量实时快照 `qt.gtimg.cn` | 60 只/请求 0.17s |
| 全历史翻页 | 600000（1999-11 上市）可逐页翻至上市日 |
| BSE 920xxx 在用代码 | 5/5 有数据（中科美菱/海泰新能/锦波生物/晶赛科技/开特股份） |
| 退市股 sz000003 | 返回至最后交易日 2002-04-26（空窗口语义正确） |
| 成交量单位对账 | 腾讯 898172 手 ×100 = 89,817,100 股 == `quotes.db` pytdx 行（600000.SH 2026-09-03） |
| 字段顺序 | `[日期, 开, 收, 高, 低, 量]` —— **收盘价在第 2 位**，非 OHLC 序 |

对照项：东财直连数据最全（单请求全历史）但同一 IP 高频会触发持续数分钟的动态断连（实测 curl/requests 一律断连，冷却数分钟恢复），且与现有 `akshare` 同源，边际价值低；新浪日线接口上限 1023 根；网易 CSV 接口已 502 下线；Tushare 需 token/积分；efinance/adata 与东财同源或与现有路由式多源架构重复。

## Goals / Non-Goals

**Goals**

- 为 SSE/SZSE/BSE 股票日线提供一个紧随 `pytdx` 的免费、高吞吐、故障域独立的备源。
- 复用既有 `BaseDataSource` / 工厂 / 路由 / 熔断 / 完整性校验设施，实现为 drop-in adapter。
- 成交量单位、复权口径与现有入库行精确可对账。

**Non-Goals**

- 不接腾讯分钟线（项目无分钟线下载路径）。
- 不接指数（指数主源为 csindex/cnindex，非 tdx；如需另立 change）。
- 不接港股/美股（HKEX 链不动）。
- 不提供股票池/交易日历/复权因子（`routing.instrument_list` / `routing.calendar` / `routing.factor` 不引用 tencent）。
- 不接入 `adaptive-source-throttling`（opt-in；腾讯实测未见 403/429，保持最小实现，见 Risks）。
- 不改 `routing.daily_behavior` 熔断阈值、不建平行下载循环。

## Decisions

1. **端点与复权口径**：`GET https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code},day,{beg},{end},{count},{fq}`，`fq` 固定为空串（响应节点 `day`，不复权原始价）。接口支持 `qfq`/`hfq` 节点但本源不使用——与项目"全源统一下载不复权原始价、复权由本地因子引擎计算"的既定口径一致（`routing.factor` 不变）。
2. **字段映射**：day 行字段序为 `[date, open, close, high, low, volume]`。收盘价在第 2 位、高低在后，与直觉 OHLC 不同，是最高风险的静默错价来源；单测必须锁定该映射（含"close 介于 low/high 之间"不变量）。
3. **单位**：成交量为**手**，×100 转为股（与 `TdxSource` 的 `int(vol_shou * 100)` 先例一致，已与 `quotes.db` pytdx 行精确对账）。腾讯不提供历史成交额 → `amount=0.0`、`turnover=0.0`（后者与 tdx 先例一致）；0.0 是显式的"源不提供"语义，不伪造。
4. **代码映射**：`instrument_id` 后缀 → 腾讯前缀：`*.SH → sh`、`*.SZ → sz`、`*.BJ → bj`（如 `600000.SH → sh600000`、`920002.BJ → bj920002`）。
5. **翻页策略**：`count = batch_size = 800`；从窗口 `end_date` 向前取，下一页 end 边界 = 当前页最早 bar 日期 − 1 天；终止条件：已覆盖 `start_date`、页返回空或不足覆盖、或安全阀 `max_iterations = 100`（与 tdx 相同）。日更常规窗口 `[T-1, T]`（calendar-day）通常 1 个请求；新票 catchup 窗口 ≤10 天约 1–2 个请求。
6. **盘中当日 bar**：fqkline 盘中返回当日未收盘 bar（实测确认），与 pytdx 行为一致。源内**不做特判**，原样返回；完整性由调度端"收盘 + 15 分钟"等待（`_wait_for_markets_close`）与 DataManager 的 `is_complete`/质量校验处理。非交易日 bar 由工厂既有 `_drop_non_trading_day_daily_quotes` 丢弃。
7. **失败语义（对齐 harden-a-share-daily-source-failover）**：数据级失败（空节点/死代码/畸形行）→ 返回 `[]` + `last_fetch_diagnostic`，不抛异常；传输级故障（连接拒绝/重置/超时）→ 抛 `ConnectionError`，计入既有 transport 熔断。健康空结果**不**置 `connection_unhealthy`，避免污染熔断计数。日更主循环内**禁止**请求级 sleep 退避。
8. **传输层**：所有 HTTP 请求必须经 `utils/http_transport.py` 共享传输层（standardize-http-transport 已实施约束），禁 `verify=False`；UA、超时（`connection_timeout_sec: 10`）与重试（`retry_times: 3`、`retry_interval: 1.0`）在源内配置。
9. **限流**：走基类 `RateLimiter`，`data_sources_config.tencent` 与 `RateLimitConfig` 同名字段。配置值取实测容量的保守折扣：`max_requests_per_minute: 3600`（实测 ~6600/min 无限流）、`max_requests_per_hour: 60000`、`max_requests_per_day: 1000000`。全市场 5558 只一次全量回退在限额内约 2 分钟完成。
10. **路由插入位置**：仅 `routing.daily` 三条股票链插入 `tencent` 紧随 `pytdx`；`exchanges_supported: ["a_stock"]`（与 pytdx 同粒度，启动路由校验按此判定 eligibility）；指数/HKEX/instrument_list/calendar/factor 链一律不动。
11. **BSE 纳入依据**：北交所在用代码已完成 920 换码（东财 BSE 列表已无 43/83/87 在用代码），920xxx 实测 5/5 有数据；老代码/死代码返回空 `day` 节点，按决策 7 的"数据级空"语义自然降级到 akshare，无熔断污染。
12. **`get_latest_daily_data`**：`GET https://qt.gtimg.cn/q={code}`，GBK 解码，无需 Referer（实测）。字段位：idx3 现价、idx4 昨收、idx5 今开、idx6 成交量（手，×100 转股）、idx30 报价时间（`yyyyMMddHHmmss`）、idx33 最高、idx34 最低、idx37 成交额（万元，×10000 转元）。死代码返回 `{}`（基类契约）。
13. **`health_check`**：探针票（如 `600000.SH`）拉取一根可解析日线并校验字段，模式与 `TdxSource.health_check` 一致。
14. **实现位置**：单文件 adapter `data_sources/tencent_source.py`，不新建共享抽象（当前无第二调用方，符合治理总纲"新增共享抽象需两个真实调用方"）。

## Risks / Trade-offs

- **[非官方接口无 SLA]** 字段序/节点名/域名可能变化 → 探针 `health_check` + 既有熔断把故障隔离为降级；字段序单测防静默错价。腾讯该接口多年稳定、被广泛使用，风险可接受。
- **[amount 恒 0.0]** 腾讯不提供历史成交额 → 接受显式 0.0；影响面仅 `amount` 列与 row_hash，OHLCV 与复权口径不受影响。若主源重拉同 bar 覆盖，走既有 row_hash changelog 语义，不新增处理。
- **[服务端限流策略未知]** 本机高强度实测（110 只/s）未触发限流，但无公开阈值 → 保守 RateLimiter + 既有熔断兜底；若未来出现 403/429，可 opt-in `adaptive-source-throttling`（记录为 follow-up，不在本 change 范围）。
- **[沙箱环境未验证项]** 腾讯分钟线接口（`web3.ifzq.gtimg.cn` DNS 在沙箱不解析）未验证——本 change 不涉分钟线，无影响；实测数据来自单机单网络环境，接入后以 M1 端到端对账为准。
