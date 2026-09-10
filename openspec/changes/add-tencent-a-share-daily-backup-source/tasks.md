# Tasks: add-tencent-a-share-daily-backup-source

## 1. Milestone 1：最小业务闭环

- [ ] 1.1 新建 `data_sources/tencent_source.py`：`TencentSource(BaseDataSource)` 骨架；实现单请求日线路径（日更常规窗口 `[T-1, T]`），包含代码前缀映射（`*.SH/sz/bj`）、字段序映射 `[date, open, close, high, low, volume]`、成交量手→股（×100）、`amount=0.0`/`turnover=0.0`
- [ ] 1.2 `data_sources/__init__.py` 导出 `TencentSource`
- [ ] 1.3 `data_sources/source_factory.py` `_create_source_instance` 增加 `tencent` 分支
- [ ] 1.4 `config/03_data.json`：新增 `data_sources_config.tencent`（enabled、`exchanges_supported: ["a_stock"]`、`connection_timeout_sec: 10`、`retry_times: 3`、`retry_interval: 1.0`、`batch_size: 800`、`max_requests_per_minute: 3600`、`max_requests_per_hour: 60000`、`max_requests_per_day: 1000000`）；`routing.daily` 在 SSE/SZSE/BSE stock 链的 `pytdx` 之后插入 `tencent`
- [ ] 1.5 最小业务验证（允许一次性真实网络）：经 `source_factory.get_daily_data` 以 tencent 路由取 `600000.SH` 近两个交易日，与 `quotes.db` 中 pytdx 行对账（close、volume 完全一致）

## 2. Milestone 2：完整核心业务规则

- [ ] 2.1 有界翻页覆盖完整窗口：≤800 根/请求、向前翻页（end 边界 = 当前最早 bar 日期 − 1 天）、终止条件（覆盖 start_date / 空页 / 安全阀 100 次迭代）、跨页按日期去重升序
- [ ] 2.2 窗口内 `pre_close` / `change` / `pct_change` 从前一 bar 推算（与 `TdxSource` 口径一致）、`tradestatus = 1 if volume > 0 else 0`
- [ ] 2.3 `get_latest_daily_data`：`qt.gtimg.cn` GBK 解码 + 固定字段位映射（idx3/4/5/6/30/33/34/37，量手→股、额万元→元），死代码返回 `{}`
- [ ] 2.4 `health_check`：探针票日线可解析性校验；`close()` 释放会话资源

## 3. Milestone 3：必要异常与现实边界

- [ ] 3.1 失败语义二分：传输级故障（连接拒绝/重置/超时）抛 `ConnectionError`（可被既有 transport 熔断计数）；数据级失败返回 `[]` + `last_fetch_diagnostic`，健康空结果不置 `connection_unhealthy`
- [ ] 3.2 畸形行跳过并在 diagnostic 中记录原因与条数；GBK 解码容错
- [ ] 3.3 `RateLimiter` 接入（`rate_limiter.acquire()`），超时/重试按配置生效；日更主循环内无请求级 sleep
- [ ] 3.4 所有 HTTP 请求经 `utils/http_transport.py` 共享传输层，TLS 校验开启

## 4. Milestone 4：测试与回归验证

- [ ] 4.1 单元测试（mock 共享传输层，不依赖实时网络）：字段序防回归（close 介于 low/high 不变量）、手→股换算、翻页覆盖与终止、空 `day` 节点返回空、盘中当日 bar 透传、`get_latest_daily_data` 字段位、死代码 `{}`、传输错误 vs 数据级空
- [ ] 4.2 金融语义测试：不复权口径（fq 为空、不误用 qfq 节点）、停牌/退市窗口空结果语义、BSE 920xxx 代码映射
- [ ] 4.3 工厂接线测试：`tencent` 实例化、路由校验通过、降级链顺序（pytdx → tencent → baostock/akshare）、tencent 失败时下一源接管
- [ ] 4.4 既有测试回归 + 项目既有 lint/格式检查（如已配置）

## 5. Validate and close

- [ ] 5.1 `openspec validate add-tencent-a-share-daily-backup-source --strict` 通过
- [ ] 5.2 `codex review --uncommitted`，发现按 Blocking / Non-blocking / Unrelated 分类处理
- [ ] 5.3 `git diff --check` 干净；仅提交本 change 相关文件并推送
