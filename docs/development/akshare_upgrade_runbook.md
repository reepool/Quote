# 行情依赖升级 Runbook（受控升级与自动验证）

> 状态：生效中
>
> 适用对象：Quote 项目的所有开发者和 AI 代理。
>
> 背景：2026-10-29 一次未经流程的 `akshare` 1.19.1→1.20.2 就地升级导致运行中的
> 服务进程找不到 `file_fold/calendar.json`，LME 与 100ppi 全部 akshare 链路 blocked
> 两天。本文档定义升级的受控路径：**版本必须 pin，升级必须验证，升级必须伴随
> 服务重启，失败必须自动回滚。**

## 1. 版本策略

- 自动升级成功后，`requirements.txt` 把该发行版写成**精确 pin**（`name==x.y.z`）；
  原来没有这一行时追加到文件末尾；
- pin 不是"停在旧版"：升级永远是刚需，但每次升级都必须是一次可审计的显式动作；
- 升级一律使用 `pip install --no-deps`，绝不联动其他依赖；
- **禁止**在服务运行期间裸跑 `pip install -U` 而不重启服务——长驻进程的内存模块
  与磁盘包不一致是 10-29 事故的直接机制。

## 2. 每日自动升级流程

每天 12:00 的 `market_dependency_version_check` 任务（见 `config/05_scheduler.json`）：

```text
检查 PyPI 最新版本
  └─ auto_upgrade_packages 里有新版
        → 一个 worker（scripts/upgrade_market_dependency.py）
          顺序：curl_cffi → akshare-proxy-patch → akshare → pytdx → baostock → yfinance
          每个包：
            1. flock 防重入；目标版本 == 当前已装版本则跳过
            2. pip install --no-deps <dist>==<new>
            3. 跑该包的生产调用冒烟
            ├─ 全绿 → 更新 requirements.txt pin → 记成功
            └─ 失败 → force-reinstall 回滚该包 → 复跑冒烟 → 记失败
          全部包处理完：先发各包报告并写结果文件，再重启一次服务
未列入 auto_upgrade_packages 的依赖只报告，不安装。
```

- 多个包放在同一个 worker 里。中途不重启，避免 systemd 把还没跑完的升级杀掉；
- 报告在重启之前通过 Bot HTTP API 发出（不走服务进程的 telethon session）；
- 汇总结果写入 `log/dependency_upgrade_result.json`，其中 akshare 的结果同时
  写入 `log/akshare_upgrade_result.json`；worker 日志在 `log/dependency_upgrade.log`；
- `requirements.txt` 的 pin 变更**不自动 git 提交**：收到成功报告后人工确认
  diff 并提交。

## 3. 重启授权（本机已就绪）

升级 worker 通过 `sudo -n systemctl restart quote-system.service` 发起重启。
本机 sudoers 已为 `python` 用户配置单命令免密白名单（`visudo` 查看）：

```text
python ALL=(root) NOPASSWD: /usr/bin/systemctl restart quote-system.service
```

- 只放行对 `quote-system.service` 的 restart 这一个动作，不放开其他命令或单元；
- worker 使用 `sudo -n`（非交互）：白名单命中直接执行，缺失时立即失败并
  在报告中提示手动重启（`sudoers` 变更后无需重启任何服务，立即生效）；
- worker 发起重启后自身会被 systemd 终止（`KillMode=control-group` 的预期
  行为），因此重启始终是 worker 的最后一个动作。

其他机器部署本流程时，若无此类白名单，可由管理员二选一：
  1. `visudo` 添加上述单命令白名单（推荐，范围最小）；
  2. 或按 polkit 规则放行 `python` 用户管理该单元：
     `/etc/polkit-1/rules.d/50-quote-system-restart.rules`
     （`org.freedesktop.systemd1.manage-units` + `unit == "quote-system.service"`
     时返回 `YES`）。

## 4. 手动升级 / 回滚 SOP

```bash
# 预演（只打印计划）
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python scripts/upgrade_market_dependency.py --package pytdx --package baostock --dry-run

# 只升级 akshare（兼容旧入口）
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python scripts/upgrade_akshare.py --target latest

# 升级指定包到指定版本
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python scripts/upgrade_market_dependency.py --package yfinance==1.8.0 --skip-restart

# 只跑某一包的冒烟
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python scripts/dependency_smoke.py --package pytdx
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python scripts/akshare_smoke.py

# 紧急回滚
/home/python/miniconda3/envs/Quote/bin/python -m pip install --no-deps --force-reinstall <发行版>==<旧版>
sudo systemctl restart quote-system.service
```

## 5. 冒烟覆盖范围与扩展

`scripts/akshare_smoke.py` 覆盖 akshare 生产接口。`scripts/dependency_smoke.py`
覆盖其余自动升级包，调用方式与生产代码一致：

| 包 | 用例 | 生产调用 |
|---|---|---|
| `pytdx` | `daily_bars` | `TdxIPManager.refresh` 后 `get_security_bars`（`first_parseable_daily_bars`） |
| `baostock` | `history_k` | `login` + `query_history_k_data_plus`（`sh.600519`，不复权，近 30 天） |
| `yfinance` | `download_aapl` | `yf.download("AAPL", multi_level_index=False)` |
| `curl_cffi` | `chrome_tls_session` | `curl_cffi.requests.session.Session(impersonate="chrome")` |
| `akshare-proxy-patch` | `install_akshare_patch` / `install_yfinance_patch` | `install_patch` / `install_yfinance_patch` |

akshare 冒烟与服务的 `proxy_patch` 加载路径一致：

| 用例 | 接口 | 断言 |
|---|---|---|
| `lme_sina_cad` / `lme_sina_snd` | `futures_foreign_hist_sina`（LME 新浪主源） | date/open/high/low/close 列 + ≥1 行 |
| `lme_em_lcpt` | `futures_global_hist_em`（东财备源） | 日期/开盘/最高/最低/最新价列 + ≥1 行 |
| `spot_100ppi_ta` | `futures_spot_price_daily`（100ppi 现货，`symbols` / `start_date` / `end_date`） | date 列 + 近 14 天 ≥1 行 |
| `fx_central_parity_safe` | `fx_central_parity_safe`（央行汇率） | 日期列 + ≥1 行 |
| `bond_zh_us_yield_em` | `bond_zh_us_yield_em`（中国国债 10 年） | 日期 + 10 年收益率列（含 % 变体） |
| `a_share_hist_600519` | `stock_zh_a_hist_em`（A 股日线） | 日期、收盘列 + 近 14 天 ≥1 行 |
| `trade_calendar` | `calendar_trade_date_hist_sina`（交易日历） | trade_date 列 + ≥1 行 |
| `cninfo_corporate_action_provider_600519` | 项目 `CninfoCorporateActionProvider`（绑定 `akshare.utils.http` 统一入口 + 600519 分红全历史） | instrument_id、action_type 列 + ≥1 行 |

最后一行不是 akshare 接口直调，而是项目自有 CNInfo 公司行动适配器的完整链路：
akshare 1.20.x 起 loader 的 HTTP 调用统一改走 `akshare.utils.http`，适配器依赖在
loader globals 里重绑该入口注入超时与限流（`data_sources/cninfo_corporate_actions.py`
的 `_bind_akshare_http`）。akshare 再换 HTTP 客户端时该绑定会先于生产任务在此断链，
冒烟先行拦截——2026-11-16 升级后绑定失效导致每日公司行动同步连续两晚失败且无报告，
即此用例要防住的事故形态。

新增生产接口（如 `data_manager` 的 `stock_fhps_em`）时，在 `build_cases()` 中
追加用例即可；列名必须取自生产代码/配置的真实使用，不得猜测。

## 6. 故障排查

| 现象 | 处置 |
|---|---|
| 调度报 `akshare_unavailable`（file_fold 之类 FileNotFoundError） | 环境被就地换过包：`pip install --no-deps --force-reinstall akshare==<requirements pin>` 后重启服务 |
| 冒烟失败、自动回滚成功 | 看报告中的失败用例；若报"已在 x.y.z 更名/移除"，按 5.1 节把改名补进兼容层后重试；其余情况等上游修复，或改项目里的调用后再升级 |
| 回滚后冒烟也失败 | 环境/网络问题，人工介入：先跑 `akshare_smoke.py --list-cases` 定位，再检查网络与代理 |
| 报告"服务重启 manual_required" | sudoers 白名单缺失或被移除：手动 `sudo systemctl restart quote-system.service`，并按第 3 节恢复白名单 |
| 12:00 后没收到任何升级报告 | 查 `log/dependency_upgrade.log` 与 `dependency_upgrade_result.json`；锁文件 `log/market_dependency_upgrade.lock` 随进程退出释放 |

## 7. 相关文件

| 文件 | 职责 |
|---|---|
| `requirements.txt` | 成功升级后由 worker 写成精确 pin，人工提交 |
| `scripts/akshare_smoke.py` | akshare 冒烟 |
| `scripts/dependency_smoke.py` | pytdx / baostock / yfinance / curl_cffi / akshare-proxy-patch 冒烟 |
| `scripts/upgrade_market_dependency.py` | 多包升级编排（升级/冒烟/回滚/报告/重启） |
| `scripts/upgrade_akshare.py` | 只升级 akshare 的兼容入口 |
| `utils/dependency_upgrade.py` | 包顺序、单包升级决策 |
| `utils/akshare_upgrade.py` | pin 更新、报告组装、Telegram HTTP 通知 |
| `scheduler/tasks.py :: market_dependency_version_check` | 每日检测入口，检测到新版时派生一个 worker |
| `config/05_scheduler.json` | `auto_upgrade_packages` |
| `log/dependency_upgrade_result.json` / `dependency_upgrade.log` | 升级审计快照与 worker 日志 |
| `log/akshare_upgrade_result.json` | 同一次运行里 akshare 的结果副本 |
