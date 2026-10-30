# akshare 升级 Runbook（受控升级与自动验证）

> 状态：生效中
>
> 适用对象：Quote 项目的所有开发者和 AI 代理。
>
> 背景：2026-10-29 一次未经流程的 `akshare` 1.19.1→1.20.2 就地升级导致运行中的
> 服务进程找不到 `file_fold/calendar.json`，LME 与 100ppi 全部 akshare 链路 blocked
> 两天。本文档定义升级的受控路径：**版本必须 pin，升级必须验证，升级必须伴随
> 服务重启，失败必须自动回滚。**

## 1. 版本策略

- `requirements.txt` 对 `akshare` 保持**精确 pin**（`akshare==x.y.z`）；
- pin 不是"停在旧版"：升级永远是刚需（上游源改版会逼 akshare 发版），但每次
  升级都必须是一次可审计的显式动作；
- 升级一律使用 `pip install --no-deps`，绝不联动其他依赖；
- **禁止**在服务运行期间裸跑 `pip install -U akshare` 而不重启服务——长驻进程
  的内存模块与磁盘包不一致是 10-29 事故的直接机制。

## 2. 每日自动升级流程

每天 12:00 的 `market_dependency_version_check` 任务（见 `config/05_scheduler.json`）：

```text
检查 PyPI 最新版本
  ├─ akshare 有新版 → 派生独立 worker（scripts/upgrade_akshare.py --target latest）
  │     1. flock 防重入；目标版本 == 当前版本则静默退出
  │     2. pip install --no-deps akshare==<new>
  │     3. 跑 scripts/akshare_smoke.py（生产接口最小真实拉数 + 结构断言）
  │     ├─ 冒烟全绿 → 更新 requirements.txt pin → 发成功报告 → 重启服务
  │     └─ 任一失败 → force-reinstall 回滚旧版 → 复跑冒烟 → 发失败报告 → 仍重启服务
  │     （无论成败都重启：进程内存已被换包污染，重启是恢复一致的唯一途径）
  └─ 其他依赖（pytdx/baostock/yfinance/curl_cffi 等）→ 仅报告新版，不自动升级
```

- worker 以独立会话（`start_new_session=True`）运行，服务重启不会中断它；
- worker 通过 Bot HTTP API 直接发 Telegram 报告（不走服务进程的 telethon
  session，避免争用）；报告无论成功失败都会发送；
- 升级结果快照写入 `log/akshare_upgrade_result.json`，worker 日志在
  `log/akshare_upgrade.log`；
- `requirements.txt` 的 pin 变更**不自动 git 提交**：收到成功报告后人工确认
  diff 并提交（属于仓库 Git 闭环授权的正常提交）。

## 3. 前提配置（一次性）：允许重启服务的 polkit 规则

`python` 用户默认无权 `systemctl restart quote-system.service`。未配置时自动
流程仍能完成升级/冒烟/回滚/报告，但报告会提示**手动重启**。要实现全自动，
由管理员执行一次：

```bash
sudo tee /etc/polkit-1/rules.d/50-quote-system-restart.rules <<'EOF'
polkit.addRule(function(action, subject) {
    if (action.id == "org.freedesktop.systemd1.manage-units" &&
        subject.user == "python") {
        var unit = action.lookup("unit");
        if (unit == "quote-system.service") { return polkit.Result.YES; }
    }
});
EOF
```

规则只放行 `python` 用户对 `quote-system.service` 单个单元的管理，不放开其他
单元。worker 发起重启后自身会被 systemd 终止（属于 `KillMode=control-group`
的预期行为），因此重启始终是 worker 的最后一个动作。

## 4. 手动升级 / 回滚 SOP

```bash
# 预演（只打印计划）
bash scripts/quote_dev_env.sh python scripts/upgrade_akshare.py --target latest --dry-run

# 升级到指定版本（同一条升级→冒烟→报告→重启链路）
bash scripts/quote_dev_env.sh python scripts/upgrade_akshare.py --target 1.21.0

# 人工控制重启时
bash scripts/quote_dev_env.sh python scripts/upgrade_akshare.py --target latest --skip-restart

# 仅跑冒烟（排查"某源突然失效"时）
bash scripts/quote_dev_env.sh python scripts/akshare_smoke.py

# 紧急回滚（冒烟之外的兜底）
/home/python/miniconda3/envs/Quote/bin/python -m pip install --no-deps --force-reinstall akshare==<旧版>
sudo systemctl restart quote-system.service
```

## 5. 冒烟覆盖范围与扩展

`scripts/akshare_smoke.py` 当前覆盖生产在用的六类接口，与服务的
`proxy_patch` 加载路径一致：

| 用例 | 接口 | 断言 |
|---|---|---|
| `lme_sina_cad` / `lme_sina_snd` | `futures_foreign_hist`（LME 新浪主源） | date/open/high/low/close 列 + ≥1 行 |
| `lme_em_lcpt` | `futures_global_hist_em`（东财备源） | 日期/开盘/最高/最低/最新价列 + ≥1 行 |
| `spot_100ppi_ta` | `futures_spot_price_daily`（100ppi 现货） | date 列 + 近 14 天 ≥1 行 |
| `fx_boc_safe` | `currency_boc_safe`（央行汇率） | 日期列 + ≥1 行 |
| `bond_zh_us_rate` | `bond_zh_us_rate`（中国国债 10 年） | 日期 + 10 年收益率列（含 % 变体） |

新增生产接口（如 `data_manager` 的 `stock_fhps_em`）时，在 `build_cases()` 中
追加用例即可；列名必须取自生产代码/配置的真实使用，不得猜测。

## 6. 故障排查

| 现象 | 处置 |
|---|---|
| 调度报 `akshare_unavailable`（file_fold 之类 FileNotFoundError） | 环境被就地换过包：`pip install --no-deps --force-reinstall akshare==<requirements pin>` 后重启服务 |
| 冒烟失败、自动回滚成功 | 看报告中的失败用例；多半是 akshare 新版改了接口/列名 → 等 akshare 修复版，或在 provider 配置层适配 |
| 回滚后冒烟也失败 | 环境/网络问题，人工介入：先跑 `akshare_smoke.py --list-cases` 定位，再检查网络与代理 |
| 报告"服务重启 manual_required" | polkit 规则未配置或被移除：手动 `sudo systemctl restart quote-system.service`，并按第 3 节补规则 |
| 12:00 后没收到任何升级报告 | 查 `log/akshare_upgrade.log` 与 `akshare_upgrade_result.json`；worker 防重入锁 `log/akshare_upgrade.lock` 不会死锁（进程退出自动释放） |

## 7. 相关文件

| 文件 | 职责 |
|---|---|
| `requirements.txt` | akshare 精确 pin（成功升级后由 worker 自动更新，人工提交） |
| `scripts/akshare_smoke.py` | 冒烟用例表与执行 |
| `scripts/upgrade_akshare.py` | 升级编排 worker（升级/冒烟/回滚/报告/重启） |
| `utils/akshare_upgrade.py` | pin 更新、报告组装、Telegram HTTP 通知（纯逻辑，有单测） |
| `scheduler/tasks.py :: market_dependency_version_check` | 每日检测入口，检测到新版时派生 worker |
| `config/05_scheduler.json` | 任务参数：`auto_upgrade_packages: ["akshare"]` |
| `log/akshare_upgrade_result.json` / `akshare_upgrade.log` | 升级审计快照与 worker 日志 |
