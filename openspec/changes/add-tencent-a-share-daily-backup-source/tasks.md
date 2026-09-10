# Tasks: add-tencent-a-share-daily-backup-source

## 1. Milestone 1：最小业务闭环

- [x] 1.1 新建 `data_sources/tencent_source.py`：`TencentSource(BaseDataSource)` 骨架；实现单请求日线路径（日更常规窗口 `[T-1, T]`）：单一 URL `https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get`（空复权、`day` 节点）、代码前缀映射（`*.SH/sz/bj`）、11 列映射锁定 `idx0..idx5=[日期,开,收,高,低,量]`、`idx6` 跳过、`idx7` 不用、量纲前缀规则（`sh688*`/`sh689*` ×1，其余 ×100）、`amount = idx8 × 10000`、`turnover=0.0`
- [x] 1.2 `data_sources/__init__.py` 导出 `TencentSource`
- [x] 1.3 `data_sources/source_factory.py` `_create_source_instance` 增加 `tencent` 分支
- [x] 1.4 `config/03_data.json`：新增 `data_sources_config.tencent`（enabled、`exchanges_supported: ["a_stock"]`、`connection_timeout_sec: 10`、`retry_times: 3`、`retry_interval: 1.0`、`batch_size: 800`、`max_requests_per_minute: 3600`、`max_requests_per_hour: 60000`、`max_requests_per_day: 1000000`）；`routing.daily` 在 SSE/SZSE/BSE stock 链的 `pytdx` 之后插入 `tencent`；同步在 `docs/configuration/config_file.md` 增加 `tencent` 数据源小节
- [x] 1.5 最小业务对账（允许一次性真实网络，**须直接调用 `TencentSource` 或临时将测试路由设为仅含 tencent——完整路由下 pytdx 先成功不会走到备源**）：`600000.SH`（主板 ×100）、`688981.SH`（科创板 ×1）、`689009.SH`（CDR ×1）、`920000.BJ`（北交所 ×100）各取一个**已收盘**目标交易日，与 `quotes.db` 主源行对账 `close`/`volume`/`amount`；×100 的票 volume 换算后与 pytdx 差异须 **≤100 股（1 手级取整容差：腾讯按手四舍五入、pytdx 截断）**；`sh688*`/`sh689*` 保留腾讯原始股数（×1），允许相差按手取整的零股，**不得为对账取整**；amount 允许万元舍入微差

## 2. Milestone 2：完整核心业务规则

- [x] 2.1 有界翻页覆盖完整窗口：`count=800`（端点实返 800+1 根，按日期去重）、end 边界 = 当前最早 bar 日期 − 1 天向前翻页、终止条件（覆盖 start_date / 空页含 end 早于上市日的正确空 / 安全阀 100 次迭代）、跨页去重升序输出
- [x] 2.2 `pre_close` 节假日容忍回看：**自 `start_date` 再往前延伸 15 个日历日**（或取到上一根有效 bar，上限 20 日历日）推算 `pre_close`/`change`/`pct_change`，输出**滤回 `[start_date, end_date]`**；IPO 首日 `pre_close=None`；`tradestatus = 1 if volume > 0 else 0`
- [x] 2.3 `get_latest_daily_data`：`qt.gtimg.cn` GBK 解码 + 固定字段位映射（idx3 现价、idx4 昨收、idx5 今开、idx6 量、idx30 时间、idx33 高、idx34 低、idx37 额万元→元），**idx6 量纲适用与日 K 相同的前缀规则**，死代码返回 `{}`
- [x] 2.4 `health_check`：探针票日线可解析性校验；`close()` 释放会话资源

## 3. Milestone 3：必要异常与现实边界

- [x] 3.1 失败语义二分并对接工厂匹配器：传输级故障（连接拒绝/重置/超时）抛 `ConnectionError`（`_is_daily_source_unavailable_error` 的 isinstance 分支命中，计入 transport 熔断）；HTTP 403/429 抛出**带 `code`/`status` 属性 ∈ {403, 429} 或消息（小写）含 `http error 403` / `http error 429` / `403`+`forbidden` / `too many requests` / `status code: 403` / `status_code=403` 之一**的异常（`_is_daily_http_throttle_error` 命中，计入 throttle 熔断）；**不得把 403/429 吞成空结果**；数据级失败返回 `[]` + `last_fetch_diagnostic`，健康空结果不置 `connection_unhealthy`
- [x] 3.2 畸形行跳过并在 diagnostic 中记录原因与条数；GBK 解码容错
- [x] 3.3 `RateLimiter` 接入（`rate_limiter.acquire()`），超时/重试按配置生效；日更主循环内无失败退避 sleep（限流器 pacing 等待除外）
- [x] 3.4 所有 HTTP 请求经 `utils/http_transport.py` 共享传输层、单一钉定 URL（运行时不做双端点切换）、TLS 校验开启

## 4. Milestone 4：测试与回归验证

- [x] 4.1 单元测试（mock 共享传输层，不依赖实时网络）：11 列映射与字段序防回归（close 介于 low/high 不变量）、前缀量纲（600000/920000 ×100、688981/689009 ×1 各有对账 fixture）、amount 万元换算、count+1 去重与翻页终止、空 `day` 节点返回空、盘中当日 bar 透传、`get_latest_daily_data` 字段位与同前缀量纲、死代码 `{}`、403/429 异常可被工厂匹配器识别、畸形行 diagnostic
- [x] 4.2 金融语义测试：不复权口径（fq 为空、不误用 qfq 节点）、pre_close 春节 11 日空档（fixture：2026-02-13 → 2026-02-24）、IPO 首日 `pre_close=None`、退市/死代码窗口空结果语义
- [x] 4.3 工厂接线测试：`tencent` 实例化、路由校验通过、降级链顺序（SSE/SZSE：pytdx → tencent → baostock → akshare；BSE：pytdx → tencent → akshare）、tencent 失败时下一源接管
- [x] 4.4 既有测试回归 + 项目既有 lint/格式检查（如已配置）

## 5. Validate and close

- [x] 5.1 `openspec validate add-tencent-a-share-daily-backup-source --strict` 通过
- [x] 5.2 Review 本 change 范围内改动（scoped），发现按 Blocking / Non-blocking / Unrelated 分类处理
- [x] 5.3 `git diff --check` 干净；仅提交本 change 相关文件并推送
