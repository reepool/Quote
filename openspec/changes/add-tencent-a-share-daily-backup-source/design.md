# Design: add-tencent-a-share-daily-backup-source

## Context

当前 `config/03_data.json` 的 `routing.daily` 中，A 股股票日线降级链为：

- SSE stock / SZSE stock：`pytdx → baostock → akshare`
- BSE stock：`pytdx → akshare`

`pytdx` 是三市股票日线的权威主源（不复权原始价 + 本地复权因子治理）。备源层缺少一个与主源故障域独立、吞吐足够的"第一备源"。

2026-09-10 实测证据（本机直连、盘中，端点为最终选定的 newfqkline）：

| 项目 | 结果 |
|---|---|
| 日线单请求（≤800+1 根，不复权） | 0.14–0.21s |
| 串行逐只（贴近日更真实形态） | ≈11 只/s，全市场 5558 只推算 ≈9–13 分钟 |
| 并发 16 线程 | 54.5 只/s（仅作容量参考，**不进验收**） |
| 批量实时快照 `qt.gtimg.cn` | 60 只/请求 0.17s |
| 翻页至上市日 | 600000（1999-11-10 上市）翻 3 页可达；与 `fqkline` 在 2005/2007/2010/2017/2019/2021 各历史边界返回完全一致 |
| BSE 920xxx | newfqkline 全历史（bj920000 两域名均 801 根、2023-05-26 起）；`fqkline` 对 BSE **只有最新 1 根**（弃用主因） |
| 退市股 sz000003 | 返回至最后交易日 2002-04-26（空窗口语义正确） |
| 量纲对账 | 手：000001.SZ 1105134×100=110,513,400、300750.SZ 262864×100=26,286,400、920000.BJ 11810×100=1,181,000（与库内 pytdx 行精确一致）；600000.SH 898172×100=89,817,200 vs pytdx 89,817,100（差 1 手：腾讯按手四舍五入、pytdx 截断）；股：688981.SH 22,153,826 vs 22,153,800、688111.SH 4,181,985 vs 4,181,900、689009.SH 7,250,217 vs 7,250,200（pytdx 按手取整损失零股） |
| 字段 | newfqkline 空复权返回 **11 列**：`[日期, 开, 收, 高, 低, 量, {}, 换手类比率, 额(万元), 0.00, 0.00]`——**收盘在 idx2** |

对照项：东财直连数据最全但同一 IP 高频会触发持续数分钟的动态断连，且与现有 `akshare` 同源，边际价值低；新浪日线接口上限 1023 根；网易 CSV 已 502 下线；Tushare 需 token/积分；efinance/adata 与东财同源或与现有路由式多源架构重复。

## Goals / Non-Goals

**Goals**

- 为 SSE/SZSE/BSE 股票日线提供一个紧随 `pytdx` 的免费、吞吐足够、故障域独立的备源。
- 复用既有 `BaseDataSource` / 工厂 / 路由 / 熔断 / 完整性校验设施，实现为 drop-in adapter。
- 成交量/成交额单位、复权口径与现有入库行精确可对账。

**Non-Goals**

- 不接腾讯分钟线（项目无分钟线下载路径）。
- 不接指数（指数主源为 csindex/cnindex，非 tdx；如需另立 change）。
- 不接港股/美股（HKEX 链不动）。
- 不提供股票池/交易日历/复权因子（`routing.instrument_list` / `routing.calendar` / `routing.factor` 不引用 tencent）。
- 不接入 `adaptive-source-throttling`（opt-in；实测未见 403/429，保持最小实现，见 Risks）。
- 不改 `routing.daily_behavior` 熔断阈值、不建平行下载循环、不做运行时双端点切换。

## Decisions

1. **端点（单一 URL）**：`GET https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get?param={code},day,{beg},{end},{count},{fq}`，`fq` 固定为空串（响应节点 `day`，不复权原始价）。备注：`web.ifzq.gtimg.cn` 同路径亦可用且历史覆盖一致，但 spec 只钉上述一条 URL，**运行时不做双端点切换/探测**。`qfq`/`hfq` 参数存在但本源不使用——与项目"全源统一下载不复权原始价、复权由本地因子引擎计算"的口径一致（`routing.factor` 不变）。选 newfqkline 弃 fqkline 的决定性原因：fqkline 对 BSE 只返回最新 1 根（对 BSE 备源形同虚设），且 newfqkline 自带成交额列。
2. **字段映射（11 列）**：锁定 `idx0..idx5 = [date, open, close, high, low, volume]`——**收盘价在 idx2、高低在后**，与直觉 OHLC 不同，是最高风险的静默错价来源；`idx6` 为动态对象（实测常为 `{}`）跳过；`idx7` 为换手类比率（实测不是涨跌幅），**不使用**；`idx8` 为成交额（万元）；`idx9`/`idx10` 不使用。单测必须锁定该映射（含"close 介于 low/high 之间"不变量）。
3. **量纲（按代码前缀，不按交易所板块标签）**：`sh688*` / `sh689*` → 原始值已是股（×1）；其余本源覆盖的 A 股股票（SSE 主板、SZSE 主板/创业板、BSE）→ 手，×100 转股。依据为 7 只票 ×100/×1 双向对账（见 Context 表）。对账容差：×100 票允许 ≤100 股（1 手）的手级取整容差——腾讯按手四舍五入、pytdx 截断、baostock 为精确股（实测 600000 差 1 手、09-09 baostock 差 42 股）；×1 票保留腾讯原始股数，不为对账取整。**不采用 AkShare `stock_zh_a_hist_tx` 的 `startswith(("sh688","sz399","sh000","sz000"))` 启发式**——`sz000` 会错杀深主板（000001.SZ 实测必须 ×100）。`get_latest_daily_data` 快照的 idx6 适用**同一条前缀规则**。
4. **代码映射**：`instrument_id` 后缀 → 腾讯前缀：`*.SH → sh`、`*.SZ → sz`、`*.BJ → bj`（如 `600000.SH → sh600000`、`920000.BJ → bj920000`）。
5. **翻页策略**：`count = batch_size = 800`，端点实返 **800+1 根**，按日期去重；从窗口 `end_date` 向前取，下一页 end 边界 = 当前页最早 bar 日期 − 1 天；终止条件：已覆盖 `start_date`、页返回空（**含 end 早于上市日的正确空**）、或安全阀 `max_iterations = 100`（与 tdx 相同）；跨页按日期去重、升序输出。日更常规窗口 `[T-1, T]`（calendar-day）通常 1 个请求。
6. **pre_close 回看（节假日容忍）**：SSE 相邻交易日最大间隔可达 **11 个日历日**（2026 春节 02-13→02-24、2024 春节 02-08→02-19、2023 国庆 09-28→10-09 实测），日更窗口 `[T-1, T]` 剔除节假日后常等效单交易日。因此推算 `pre_close`/`change`/`pct_change` 时，回看起点必须**从 `start_date` 再往前延伸 15 个日历日**（或取到上一根有效 bar 为止，上限 20 个日历日——一次请求成本不变），推算完成后输出**滤回 `[start_date, end_date]`**；IPO 首日（回看内无更早 bar）`pre_close=None`，不伪造。`turnover=0.0`（与 tdx 先例一致，不猜 idx7）。
7. **盘中当日 bar**：端点盘中返回当日未收盘 bar，与 pytdx 行为一致。源内**不做特判**，原样返回；完整性由调度端"收盘 + 15 分钟"等待与 DataManager 的 `is_complete`/质量校验处理。非交易日 bar 由工厂既有 `_drop_non_trading_day_daily_quotes` 丢弃。
8. **失败语义（对齐 harden-a-share-daily-source-failover）**：数据级失败（空节点/死代码/畸形行）→ 返回 `[]` + `last_fetch_diagnostic`，不抛异常；传输级故障（连接拒绝/重置/超时）→ 抛 `ConnectionError`（被工厂 `_is_daily_source_unavailable_error` 的 isinstance 分支命中）；**HTTP 403/429 必须抛出能被 `_is_daily_http_throttle_error`（source_factory.py:622-641）识别的异常**：异常带 `code`/`status` 属性 ∈ {403, 429}，或消息（小写化后）含 `http error 403` / `http error 429` / `403`+`forbidden` / `too many requests` / `status code: 403` / `status_code=403` 之一——吞成 `[]` 会逐只空转且永远计不进 throttle 熔断。健康空结果**不**置 `connection_unhealthy`。日更主循环内**禁止**失败退避 sleep（`RateLimiter.acquire()` 的 pacing 等待不属于此列）。
9. **传输层**：所有 HTTP 请求必须经 `utils/http_transport.py` 共享传输层（standardize-http-transport 已实施约束），禁 `verify=False`；UA、超时（`connection_timeout_sec: 10`）与重试（`retry_times: 3`、`retry_interval: 1.0`）在源内配置。偶发瞬时抖动（同批代码 16 线程 0 失败、串行复跑 2 只失败）由重试 + 健康空语义兜底。
10. **限流**：走基类 `RateLimiter`，`data_sources_config.tencent` 与 `RateLimitConfig` 同名字段：`max_requests_per_minute: 3600`、`max_requests_per_hour: 60000`、`max_requests_per_day: 1000000`。推导：新端点串行实测 ≈11 只/s ≈ 660 次/分，3600/min 约为 5 倍余量；并发 54.5 只/s 仅说明容量上限充裕。全市场一次串行回退 ≈9–13 分钟，在限额内。
11. **路由插入位置**：仅 `routing.daily` 三条股票链插入 `tencent` 紧随 `pytdx`；`exchanges_supported: ["a_stock"]`（与 pytdx 同粒度，启动路由校验按此判定 eligibility）；指数/HKEX/instrument_list/calendar/factor 链一律不动。
12. **BSE 纳入依据**：库内 BJ 股票前缀全部为 `92`（全量 344、近 30 天活跃 343，无 43/83/87），newfqkline 对 920xxx 提供全历史（两域名对 bj920000 均 801 根、2023-05-26 起）；920000.BJ 对账 11810 手 ×100 = 1,181,000 股与 pytdx 行精确一致。老代码/死代码返回空 `day` 节点，按决策 8 的"数据级空"语义自然降级到 akshare，无熔断污染。
13. **`get_latest_daily_data`**：`GET https://qt.gtimg.cn/q={code}`，GBK 解码，无需 Referer（实测）。字段位：idx3 现价、idx4 昨收、idx5 今开、idx6 成交量（**量纲同决策 3 的前缀规则**）、idx30 报价时间（`yyyyMMddHHmmss`）、idx33 最高、idx34 最低、idx37 成交额（万元，×10000 转元）。快照盘中是未收盘价，**不得当历史日线使用**——日更主路径只用 `get_daily_data`。死代码返回 `{}`（基类契约）。
14. **`health_check`**：探针票（如 `600000.SH`）拉取一根可解析日线并校验字段，模式与 `TdxSource.health_check` 一致。
15. **实现位置与已知边界**：单文件 adapter `data_sources/tencent_source.py`，不新建共享抽象（当前无第二调用方，符合治理总纲）。已知边界：个别在市代码腾讯日 K 端点全形态空返回而快照正常（典型为上市首日尚未入腾讯 K 线库；实测样本 688837/688801 为**池外**代码，`instruments` 表中不存在），按健康空降级 baostock，不计失败、不进熔断。

## Risks / Trade-offs

- **[非官方接口无 SLA]** 字段序/节点名/域名可能变化 → 探针 `health_check` + 既有熔断把故障隔离为降级；字段序与量纲单测防静默错价。腾讯该接口多年稳定、AkShare `stock_zh_a_hist_tx` 同路径生产使用多年，风险可接受。
- **[精度微差]** 成交额为万元两位小数（与 pytdx 精确元值差 ±数百元以内）、科创板/CDR 原始量含零股（比 pytdx 按手取整更精确，如 22,153,826 vs 22,153,800）→ 跨源同 bar 的 row_hash 允许微差，覆盖时走既有 changelog 语义，不新增处理。
- **[服务端限流策略未知]** 本机高强度实测未触发，但无公开阈值 → 保守 RateLimiter（串行速率约 5 倍余量）+ 既有熔断兜底；若未来出现 403/429，可 opt-in `adaptive-source-throttling`（follow-up，不在本 change 范围）。
- **[快照非历史]** `get_latest_daily_data` 盘中是未收盘快照价 → 已在 spec 钉死"不得当历史日线使用"；日更主路径只消费 `get_daily_data`。
- **[沙箱环境未验证项]** 腾讯分钟线接口（本沙箱 DNS 不解析）未验证——本 change 不涉分钟线，无影响；实测来自单机单网络环境，接入后以 M1 四票对账为准。
