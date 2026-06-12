# Telegram 任务管理器文档

## 📖 概述

Quote System 提供了功能完整的 Telegram 机器人任务管理界面，允许用户通过 Telegram 实时监控和管理系统的定时任务。该管理器支持智能时间显示、任务控制、配置热重载等高级功能。

## 🚀 功能特性

### ✨ 核心功能
- **实时任务监控** - 查看所有定时任务的状态和下次执行时间
- **智能时间显示** - 根据时间差自动选择最佳显示格式
- **任务控制** - 启用/禁用/暂停/恢复任务
- **数据补充** - 指定日期补充缺失的历史数据
- **配置热重载** - 运行时动态更新任务配置无需重启
- **受控服务重启** - 管理员二次确认后重启固定 systemd 服务
- **权限管理** - 基于用户ID的访问控制
- **实时通知** - 任务执行状态和系统告警通知

### 🤖 智能时间显示
系统根据时间差自动选择最合适的显示格式：

| 时间差 | 显示格式 | 示例 |
|--------|----------|------|
| < 1小时 | "X分钟后" | "15分钟后" |
| < 24小时 | "今天 HH:MM" | "今天 20:00" |
| < 7天 | "周一 HH:MM" | "周一 14:00" |
| ≥ 7天 | "MM-DD HH:MM" | "10-25 14:00" |

## 🔧 配置设置

### Telegram 配置
在 `config/config.json` 中配置 Telegram 机器人：

```json
{
  "telegram_config": {
    "enabled": true,
    "api_id": "your_api_id",
    "api_hash": "your_api_hash",
    "bot_token": "your_bot_token",
    "chat_id": ["471105519"],
    "session_name": "MsgBot",
    "connection_retries": 3,
    "retry_delay": 5,
    "auto_reconnect": true,
    "timeout": 30,
    "ops": {
      "service_restart": {
        "enabled": false,
        "mode": "self_exit",
        "service_name": "quote-system.service",
        "systemctl_path": "systemctl",
        "use_sudo": false,
        "delay_seconds": 2,
        "timeout_seconds": 15,
        "exit_code": 1
      }
    },
    "intervals": {
      "tg_msg_retry_interval": 3,
      "tg_msg_retry_times": 5,
      "tg_connect_timeout": 30,
      "tg_auto_reconnect": true,
      "tg_max_reconnect_attempts": 5,
      "tg_reconnect_delay": 10
    }
  }
}
```

### 权限配置
系统只允许配置文件中指定的 `chat_id` 访问任务管理功能。

### 服务重启配置

Telegram 服务重启入口默认关闭；推荐使用 `self_exit` 模式：机器人先发送确认消息，然后当前 Python 进程以非 0 退出码退出，由 systemd 的 `Restart=on-failure` 自动拉起服务。该模式不依赖 `sudo`，适合启用了 `NoNewPrivileges` 的服务进程。

```json
{
  "telegram_config": {
    "ops": {
      "service_restart": {
        "enabled": true,
        "mode": "self_exit",
        "service_name": "quote-system.service",
        "systemctl_path": "systemctl",
        "use_sudo": false,
        "delay_seconds": 2,
        "timeout_seconds": 15,
        "exit_code": 1
      }
    }
  }
}
```

`self_exit` 模式要求 systemd unit 具备自动拉起策略，例如：

```text
Restart=on-failure
RestartSec=10s
```

如果确实需要让机器人直接调用 systemd，可将 `mode` 设为 `systemctl`；此时仍只允许重启配置中的固定服务，不支持从 Telegram 传入任意命令。如果运行 `quote-system.service` 的用户没有 systemd restart 权限，将 `use_sudo` 设为 `true`，并在 sudoers 中只放行固定命令，例如：

```text
quoteuser ALL=NOPASSWD: /bin/systemctl restart quote-system.service
```

不要授予通配符 systemctl 权限，也不要把任意 shell 命令接入 Telegram。

## 📱 使用指南

### 启动任务管理器

```bash
# 启动完整系统（包含任务管理器）
python3 main.py full --host 0.0.0.0 --port 8000

# 或分别启动调度器和API服务
python3 main.py scheduler
python3 main.py api --host 0.0.0.0 --port 8000
```

### 可用命令

#### 基础命令
- `/start` - 显示主菜单和帮助信息
- `/help` - 显示帮助信息

#### 任务管理命令
- `/status` - 查看所有任务状态和下次执行时间
- `/detail <task_id>` - 查看指定任务的详细信息
- `/reload_config` - 热重载配置文件
- `/restart_system` - 显示重启确认提示
- `/restart_system confirm` - 重启配置中的固定 systemd 服务

#### 任务控制命令
- `/run <task_id>` - 立即执行指定任务
- `/run daily_data_update <日期>` - 以补数据模式执行每日更新（跳过交易日检查）
- `/run shareholder_shadow_sync` - 手工执行股东摘要全量刷新，刷新本地 `shareholder_snapshots`
- `/run shareholder_reconciliation_sync` - 立即执行股东摘要周期复核与补足，全量读取后只写变化或本地缺失/覆盖不完整标的
- `/run shareholder_incremental_sync` - 立即执行股东摘要每日增量检查，公告驱动且有变化才写入
- `/run financial_l1_full_import` - 手工执行财务 L1 本地核心层全量导入或补处理，写入 `data/financials.db`
- `/run financial_disclosure_incremental_sync` - 手工执行财务公告驱动增量检查，按定期报告公告候选定向补处理
- `/run financial_disclosure_reconciliation_sync` - 手工执行财务周度对账修复，补缺失或变化的财务报告期
- `/backfill <日期> [交易所...]` - 补充指定日期的缺失数据
- `/backfill <开始日期> <结束日期> [交易所...]` - 以区间模式补充日期范围内缺失数据
- `/industry_standard_sync [force]` - 申万官方分类日更同步；默认使用 source manifest，官方文件未变化时短路
- `/industry_standard_rebuild [force] [drop_source_files]` - 申万官方分类全量重建；清理 strict Shenwan 行业标准层后重载
- `/industry_index_analysis_sync [limit=N]` - 申万行业指数分析日频指标同步；只写 `industry_index_analysis_daily`
- `/industry_index_analysis_backfill start=YYYY-MM-DD end=YYYY-MM-DD [limit=N] [chunk=month|day|quarter|year|none]` - 申万行业指数分析历史回补；只写 `industry_index_analysis_daily`
- `/enable <task_id>` - 启用指定任务（开发中）
- `/disable <task_id>` - 禁用指定任务（开发中）

### 使用示例

```bash
# 在Telegram中发送命令
/status                                  # 显示任务状态
/detail daily_data_update                # 查看每日数据更新任务详情
/run daily_data_update                   # 立即执行每日更新
/run daily_data_update 2026-03-27        # 补充 3/27 的缺失数据
/run shareholder_shadow_sync             # 手工触发股东摘要全量刷新
/run shareholder_reconciliation_sync     # 手工触发股东摘要周期复核与补足
/run shareholder_incremental_sync        # 手工触发股东摘要每日增量检查
/run financial_l1_full_import            # 手工触发财务 L1 全量导入/补处理
/run financial_disclosure_incremental_sync  # 手工触发财务公告驱动增量检查
/run financial_disclosure_reconciliation_sync # 手工触发财务周度对账修复
/backfill 2026-03-27                     # 补充 3/27 所有交易所数据
/backfill 2026-03-27 SSE                 # 仅补充上交所 3/27 数据
/backfill 2026-04-09 2026-05-21 SSE SZSE BSE  # 一次性补充 A 股日期区间
/industry_standard_sync                  # 申万官方分类日更同步
/industry_standard_sync force            # 强制重新拉取官方文件并同步
/industry_standard_rebuild force         # 清理 strict Shenwan slice 后全量重建
/industry_index_analysis_sync limit=20   # 小样本同步申万指数分析指标
/industry_index_analysis_backfill start=2024-10-25 end=2024-10-25 limit=20  # 小样本回补历史申万指数分析指标
/industry_index_analysis_backfill start=2023-12-01 end=2023-12-29 chunk=day # 按日补缺申万指数分析历史缺口
/reload_config                           # 重载任务配置
/restart_system                          # 显示重启确认提示
/restart_system confirm                  # 重启 quote-system.service
```

申万指数分析历史回补的 CLI 入口为 `scripts/research_ops/industry_index_analysis_backfill.py`。
该脚本默认按“月份 + index_type”分块提交，适合直接给大日期范围；失败后可用同一命令重跑，已成功分块会通过主键 upsert 保持幂等。
当某个月份出现分页 404、上游限流或局部缺口时，使用 `--chunk-frequency day` 做按日补缺；Telegram 对应参数为 `chunk=day`。`chunk=month` 适合全量或按年回补，`chunk=day` 适合修复少量缺失交易日。

股东摘要当前没有单独的 Telegram 专用命令，使用通用 `/run shareholder_shadow_sync` 手工触发全量刷新，使用 `/run shareholder_reconciliation_sync` 触发周期复核与补足，使用 `/run shareholder_incremental_sync` 触发公告驱动增量检查。全量刷新任务在 scheduler 中配置为 `manual_only=true`，不会自动注册为周六定时任务，但仍会出现在 `/status` 中，状态为仅手工执行入口；周期复核默认周六 `12:30` 执行，全量读取后通过本地 `snapshot_json` hash 和 required scope 判断，只写变化、本地缺失或覆盖不完整标的；每日增量检查默认 `06:30` 执行。增量检查会按 CNInfo 市场/栏目和时间窗口扫描公告元数据，筛出候选标的后做结构化股东数据 hash 比较，只有变化或 required scope 缺失才写入快照；公告先到、data20 结构化数据滞后的候选会进入 5 个自然日 pending recheck，同一批公告的截止时间固定为第一次进入 pending 的时间加 `pending_recheck_days`，不会滚动延长。配置变更后使用 `/reload_config` 可刷新同进程内的 scheduler 与 `DataManager` 运行时配置；如果 API 是单独进程，需要重启 API 进程才能加载修改后的 research 配置。

股东信息更新任务分工：

| 任务 | 触发方式 | 读取范围 | 写入策略 |
|---|---|---|---|
| `shareholder_incremental_sync` | 每日 `06:30` / `/run` | CNInfo 公告候选、缺失 required scope、pending recheck 标的 | hash 变化、缺失或 required scope 不完整才写 |
| `shareholder_reconciliation_sync` | 周六 `12:30` / `/run` | `SSE / SZSE / BSE` 全量活跃股票 | `changed_only`，本地 hash 和 required scope 相同则跳过 |
| `shareholder_shadow_sync` | 仅 `/run` | `SSE / SZSE / BSE` 全量活跃股票 | `refresh_all`，用于手工全量刷新 |

财务 L1 维护任务分工：

当前实现状态（`2026-06-10`）：`financial_l1_full_import` 已作为 `manual_only` 任务接入 `/run`；`financial_disclosure_incremental_sync` 已启用为每日 `21:45` 自动运行，避开港股日更和 A 股日更主窗口，并通过 `config/05_scheduler.json` 的 `dependencies.post_success` 配置在成功后自动触发券商专项财务后置任务 `broker_risk_control_incremental_sync`；`financial_disclosure_reconciliation_sync` 已启用为周日 `09:30` 自动运行。公告筛选已收紧为正式定报、更正/修订、延期披露和定报相关停牌/退市风险公告；补数源路由为 `CNInfo data20 -> THS -> Sina`，其中 CNInfo data20 是官方结构化优先源，THS/Sina 只做缺失、失败或语义不明确字段的补齐。L1 required facts 使用 `net_income_parent / equity_parent` 等精确口径，不再用旧的 `net_income / equity` 泛化字段作为本地核心层准入要求。券商风控事实使用正式年报/半年报内嵌“净资本及风险控制指标”表，按 `listed_broker_dealer_scope` gate 只处理确认上市券商，写入同一财务事实表；源路由统一封装在服务层，Telegram 任务只展示结果和配置化依赖节点摘要，不直接维护各源 fallback 细节。

| 任务 | 触发方式 | 读取范围 | 写入策略 |
|---|---|---|---|
| `financial_l1_full_import` | 仅 `/run` | `SSE / SZSE / BSE` active 股票池和最近 rolling 报告期 | 可续跑；已完整落库的标的/报告期跳过；用于初始化和大范围补处理 |
| `financial_disclosure_incremental_sync` | 每日 `21:45` / `/run` | CNInfo 正式定报/更正/延期/风险公告候选、pending recheck、本地缺失报告期 | 只对候选补处理；先用 CNInfo data20，缺口再用 THS/Sina；公告先到但结构化源未更新时进入 pending recheck |
| `broker_risk_control_incremental_sync` | `financial_disclosure_incremental_sync` 成功后的后置任务 / `/run` | 确认上市券商范围内最近公告窗口的正式年报/半年报 | 写 `financial_numeric_facts_hot`；提取 `net_capital`、风险覆盖率、资本杠杆率、LCR、NSFR 等券商监管事实 |
| `financial_disclosure_reconciliation_sync` | 周日 `09:30` / `/run` | 最近 rolling 报告期全市场覆盖情况 | 只补缺失、变化或 required core facts 不完整的 instrument-period |

券商风控历史 12 个季度回补不通过 Telegram 自动触发，使用 CLI 显式执行；不带 `--write` 为 dry-run，正式写库命令如下：

```bash
/home/python/miniconda3/envs/Quote/bin/python scripts/dev_validation/backfill_broker_risk_control_reports.py \
  --as-of-date 2026-06-10 \
  --quarters 12 \
  --limit-instruments 0 \
  --per-instrument-max-pages 1 \
  --write \
  --output /tmp/broker_risk_control_all_12q_write.json \
  > /tmp/broker_risk_control_all_12q_write.log 2>&1
```

财务公告驱动任务识别正式年度报告、半年度报告、一季报、三季报、更正/修订公告和披露异常公告。业绩说明会预告、英文版、图文版、问询函/回复、专项说明、投资者接待日和摘要类公告默认过滤，不进入 `pending_recheck`。历史缺报且公告显示停牌、退市风险警示或可能终止上市时，报告中应显示“待退市风险/披露异常待补”，不把该类缺口误报为字段映射错误。
明确无法按期披露的候选记录为 `accepted_disclosure_gap`，不再每日调用 CNInfo/THS/Sina 反复补数；等正式定报主公告出现后再作为新事件触发补处理。
历史 `pending_recheck` 会在再次处理前按当前公告规则复核；旧逻辑遗留的说明会、英文版、图文版、问询函专项说明等噪声会显示在“旧噪声过滤”，不再进入补数源路由。
公告先到但 CNInfo data20 与 THS/Sina 结构化财报未更新时，候选进入 `pending_recheck`；同一公告的重查截止时间以首次 pending 为硬上限，不会因每日扫描滚动延长。

财务任务报告应重点看四组数字：

- `candidate_count`：公告、pending recheck 或对账发现的待处理 instrument-period。
- `changed_count / unchanged_count`：本次实际修复写入与已完整跳过数量。
- `pending_recheck_count / pending_delisting_risk_count`：公告先到但结构化源滞后，或公告解释为待退市风险的数量。
- `source_routing`：CNInfo data20 官方尝试、CNInfo ready、CNInfo 批处理通过、缺失/歧义，以及 THS/Sina fallback 尝试/成功数量。`ready` 才代表 required canonical facts 已满足；批处理通过只代表 CNInfo 源请求没有把该 instrument-period 判为失败。
- `accepted_gap_count / mapping_policy_gap_count / source_missing_gap_count / blocking_gap_count`：可审计解释的缺口、字段标准或映射准入缺口、源数据缺口和其他 blocker 数量。`mapping_policy_gap` 不会反复调用 CNInfo/THS/Sina，应先修正字段标准或 mapping catalog；`source_missing` 才进入补数源路由。

### BotFather 命令映射

提供给 BotFather `/setcommands` 的当前命令列表：

```text
start - 显示主菜单
help - 查看帮助
status - 查看任务状态
detail - 查看指定任务详情
run - 立即执行任务
backfill - 补充指定日期行情数据
backfill_factors - 回填复权因子
industry_standard_sync - 申万官方分类日更同步
industry_standard_rebuild - 申万官方分类全量重建
industry_index_analysis_sync - 申万指数分析日频同步
industry_index_analysis_backfill - 申万指数分析历史回补
audit_factors - 审计自研复权因子
smart_fill_gaps - 智能补足大段缺口
find_gap_and_repair - 精确逐日修复缺口
reload_config - 热重载配置
restart_system - 重启系统服务
```

## 📋 内置任务列表

系统预置了多个定时任务，每个任务都有详细的配置和执行计划：

### 1. 每日数据更新 (daily_data_update)
- **执行时间**: 每周一至周五 20:00
- **功能**: 自动更新当日股票数据
- **特点**: 支持交易日检查、市场收盘等待；普通 A 股日更默认命中 `force_refresh_job_names`，会先强制刷新 `SSE/SZSE/BSE` 股票主数据，再重新读取 active/tradable 股票池抓取行情
- **主数据报告**: 日更报告包含“证券主数据同步”段落，展示新增、停用、活跃数量和 warnings/errors；该段落来自共享 `instrument_master_governance` 治理入口，但保留日更兼容字段 `instrument_master_sync`；历史补数模式默认跳过当前主数据同步，避免用今天的股票池语义污染历史回补

### 1.1 研究/财务任务的主数据治理

通过 `/run` 手工触发或由 scheduler 自动执行的当前研究、财务和快照类任务，在解析 active 股票池前会先经过同一个证券主数据治理入口。包括公司画像、行业、严格申万、股东、财务摘要、完整财报、分析师预测、研报、舆情事件、技术快照和风险快照等任务。

`force_refresh_job_names` 是强制刷新策略列表，不是“接入治理的任务列表”。当前默认强制刷新的自动任务是 `daily_data_update` 和 `industry_standard_sync`；其他已接入治理的当前任务通常采用 freshness-gated 策略，即本地主数据过期时才触发 BaoStock/AkShare 主数据同步。

维护报告会显示“证券主数据治理”段落：

- `fresh/reused_fresh_master` 表示本地 `instruments` 在新鲜度窗口内，任务没有重复访问上游主数据源。
- `synced` 表示本次任务触发了 BaoStock/AkShare 主数据同步。
- `skipped` 表示配置禁用、历史/回补语义跳过，或请求市场未启用主数据策略。
- `warning/error` 会单独显示，即使业务任务本身成功；例如 BSE 只有 AkShare fallback 数据时会继续提示退市日期不具权威性。

历史估值回补和行情 `/backfill` 默认不使用当前股票主数据刷新替代历史时点股票池；如需强制刷新，应通过代码入口显式传入 force refresh，并保留报告证据。

### 2. 系统健康检查 (system_health_check)
- **执行时间**: 每小时执行一次
- **功能**: 检查数据源状态、数据库健康、系统资源
- **特点**: 自动告警、性能监控

### 3. 每周数据维护 (weekly_data_maintenance)
- **执行时间**: 每周日 02:00
- **功能**: 维护前数据库备份、日志清理、幽灵/僵尸标的清理、周度复权因子同步、完整性验证、数据库优化
- **港股因子**: `routing.factor.HKEX.maintenance_sync_enabled=true` 时由本任务周度同步，日更仍由 `routing.factor.HKEX.daily_sync_enabled=false` 控制为跳过

### 4. 行情依赖版本检查 (market_dependency_version_check)
- **执行时间**: 每日 12:00
- **功能**: 检查 `pytdx`、`baostock`、`akshare`、`akshare_proxy_patch`、`yfinance`、`curl_cffi` 是否有 PyPI 新版本
- **特点**: 只发送 Telegram 升级提醒，不自动安装或升级依赖

### 5. 月度数据完整性检查 (monthly_data_integrity_check)
- **执行时间**: 每月1日 03:00
- **功能**: 检查上月数据缺口并自动修复

### 6. 季度数据清理 (quarterly_cleanup)
- **执行时间**: 每季度最后一天 04:00
- **功能**: 清理过期数据和文件

### 7. 数据库备份 (database_backup)
- **执行时间**: 每周六 03:30
- **功能**: 自动备份数据库文件
- **特点**: 支持Telegram通知、自动清理过期备份

### 8. 缓存预热 (cache_warm_up)
- **执行时间**: 每日 08:00
- **功能**: 预加载热门数据到缓存

### 9. 交易日历更新 (trading_calendar_update)
- **执行时间**: 每月1日 01:00
- **功能**: 更新各交易所交易日历

## 📊 状态显示示例

### /status 命令响应示例
```
📊 任务状态概览

• 总任务数：8
• 运行中：8
• 已禁用：0

🟢 运行中的任务：
• 每日数据更新任务 (daily_data_update) - 周一 20:00
• 系统健康检查 (system_health_check) - 45分钟后
• 每周数据维护 (weekly_data_maintenance) - 周日 02:00
• 月度数据完整性检查 (monthly_data_integrity_check) - 下月2日 11:00
• 季度数据清理 (quarterly_cleanup) - 下季度最后一天 14:30
• 缓存预热 (cache_warm_up) - 每天 08:00
• 交易日历更新 (trading_calendar_update) - 下月1日 01:00
• 数据库备份任务 (database_backup) - 周六 03:30

*可用的任务控制命令：*
• /run <task_id> - 立即运行任务
• /enable <task_id> - 启用任务
• /disable <task_id> - 禁用任务
• /detail <task_id> - 查看任务详情
```

## \ud83d\udce5 \u6570\u636e\u8865\u5145\u529f\u80fd

\u5f53\u5b9a\u65f6\u4efb\u52a1\u672a\u6b63\u5e38\u66f4\u65b0\u6570\u636e\u65f6\uff0c\u53ef\u4ee5\u901a\u8fc7\u4ee5\u4e0b\u4e24\u79cd\u65b9\u5f0f\u8865\u5145\u6307\u5b9a\u65e5\u671f\u7684\u7f3a\u5931\u6570\u636e\u3002

### \u65b9\u5f0f\u4e00\uff1a`/backfill` \u547d\u4ee4\uff08\u63a8\u8350\uff09

\u4e13\u7528\u7684\u6570\u636e\u8865\u5145\u547d\u4ee4\uff0c\u652f\u6301\u6307\u5b9a\u4ea4\u6613\u6240\u3002

```
/backfill 2026-03-27              # \u8865\u5145\u6240\u6709\u4ea4\u6613\u6240
/backfill 2026-03-27 SSE          # \u4ec5\u8865\u5145\u4e0a\u4ea4\u6240
/backfill 2026-03-27 SSE SZSE     # \u8865\u5145\u4e0a\u4ea4\u6240\u548c\u6df1\u4ea4\u6240
/backfill 2026-05-20 BSE          # \u8865\u5145\u5317\u4ea4\u6240\u67d0\u4e2a\u4ea4\u6613\u65e5
/backfill 2026-04-09 2026-05-21 SSE SZSE BSE  # \u4ee5\u533a\u95f4\u6a21\u5f0f\u8865\u5145 A \u80a1\u7f3a\u53e3
```

> A 股普通日更已经会先刷新股票主数据。若新股因为历史股票池滞后漏掉了近几天日 K，先确认主数据同步完成，再执行 `/backfill <开始日期> <结束日期> SSE SZSE BSE` 即可。单日 `/backfill` 仍走 `daily_data_update(target_date=...)` 的补数模式；时间段 `/backfill` 走区间补数路径，每个交易所只读取一次 active 股票池、每个品种只请求一次连续日期范围，并默认跳过 TDX Phase 2.5 审计。

### \u65b9\u5f0f\u4e8c\uff1a`/run daily_data_update <\u65e5\u671f>`

\u901a\u8fc7\u5728 `/run` \u547d\u4ee4\u540e\u8ffd\u52a0\u65e5\u671f\u53c2\u6570\u89e6\u53d1\u8865\u6570\u636e\u3002

```
/run daily_data_update 2026-03-27
```

### \u4e24\u79cd\u65b9\u5f0f\u7684\u5dee\u5f02

| \u7ef4\u5ea6 | `/backfill` | `/run daily_data_update <\u65e5\u671f>` |
|------|------------|-----------------------------------|
| \u4ea4\u6613\u6240 | \u9ed8\u8ba4 SSE+SZSE\uff0c\u53ef\u8ffd\u52a0\u6307\u5b9a | \u4ece\u914d\u7f6e\u6587\u4ef6\u8bfb\u53d6 |
| instrument\_types | \u4f7f\u7528\u65b9\u6cd5\u5185\u9ed8\u8ba4\u503c | \u4ece\u914d\u7f6e\u6587\u4ef6\u8bfb\u53d6 |
| \u4f9d\u8d56\u8c03\u5ea6\u5668 | \u4e0d\u4f9d\u8d56\uff0c\u5355\u65e5\u8d70 daily update\uff0c\u533a\u95f4\u8d70 range backfill | \u4e0d\u4f9d\u8d56\uff0c\u76f4\u63a5\u8c03\u7528 |
| \u7b49\u5f85\u6536\u76d8/\u4ea4\u6613\u65e5\u68c0\u67e5 | \u81ea\u52a8\u8df3\u8fc7 | \u81ea\u52a8\u8df3\u8fc7 |

> **\u63d0\u793a**\uff1a\u5355\u65e5 `/backfill` \u4e0e `/run daily_data_update <\u65e5\u671f>` \u57fa\u672c\u4e00\u81f4\uff1b\u65f6\u95f4\u6bb5 `/backfill` \u4f1a\u4f7f\u7528\u533a\u95f4\u8865\u6570\u5165\u53e3\uff0c\u907f\u514d\u5bf9\u6bcf\u4e2a\u4ea4\u6613\u65e5\u91cd\u590d\u8dd1\u4e09\u5e02\u5168\u91cf\u626b\u63cf\u548c TDX \u5ba1\u8ba1\u3002

## 📈 复权因子回补

`/backfill_factors` 现在直接调用系统内置的正式回补逻辑，不再依赖独立脚本子进程。

```text
/backfill_factors
/backfill_factors SSE SZSE
/backfill_factors HKEX missing
/backfill_factors HKEX full
```

参数说明：

- 交易所参数可选，支持 `SSE`、`SZSE`、`BSE`、`HKEX`、`NASDAQ`、`NYSE`
- `missing` 模式：仅处理当前完全没有因子记录的股票，适合常规补缺
- `full` 模式：对目标交易所全部股票执行全量重抓并 upsert，适合清理错误因子后的重建

港股修复建议：

- 替换旧口径或脏数据时，使用 `/backfill_factors HKEX full`；`missing` 模式会跳过已有任意因子记录的股票，不适合全量替换。
- 当前正式路由会按 `routing.factor.HKEX.primary` 执行；生产配置应使用 `akshare` 港股 `qfq-factor` 乘法因子转换逻辑，`yfinance` 只作为失败时的少量补位，不承担主源角色。
- `routing.factor.HKEX.daily_sync_enabled` 控制日更是否自动同步港股因子；`routing.factor.HKEX.maintenance_sync_enabled` 控制周维护是否自动同步港股因子；两者都不会阻止显式 `/backfill_factors`。

## 🔔 任务启动通知

调度器自动触发的定时任务在执行前，会向 Telegram 发送一条启动通知，便于用户感知系统运行状态。

### 配置方式

每个任务可通过 `pre_run_notify` 字段独立控制是否发送启动前通知：

```json
{
  "daily_data_update": {
    "pre_run_notify": true
  },
  "system_health_check": {
    "pre_run_notify": false
  }
}
```

### 默认配置

| 任务 | 通知 | 说明 |
|------|:---:|------|
| daily_data_update | ✅ | 每日数据更新 |
| weekly_data_maintenance | ✅ | 每周数据维护 |
| monthly_data_integrity_check | ✅ | 月度完整性检查 |
| find_gap_and_repair | ✅ | 数据缺口修复 |
| quarterly_cleanup | ✅ | 季度数据清理 |
| trading_calendar_update | ✅ | 交易日历更新 |
| database_backup | ✅ | 数据库备份 |
| system_health_check | ❌ | 每小时执行，过于频繁 |
| cache_warm_up | ❌ | 轻量级任务 |

> **注意**：手动通过 `/run` 触发的任务已有即时回显，不会额外推送通知。通知发送失败不影响任务正常执行。

## 🛠️ 技术实现

### 架构组件
```
utils/task_manager/
├── task_manager.py      # 主控制器类
├── handlers.py         # 消息处理器
├── models.py           # 数据模型
├── formatters.py      # 消息格式化器
├── keyboards.py        # 键盘布局
└── __init__.py
```

### 核心类
- **TaskManagerBot**: 主控制器，管理Telegram连接和消息处理
- **TaskManagerHandlers**: 消息处理器，处理用户命令和交互
- **TaskStatusInfo**: 任务状态信息模型
- **TaskManagerFormatters**: 消息格式化器

### 时间处理机制
系统使用 `utils/date_utils.py` 中的智能时间格式化功能：

```python
# 智能时间格式化示例
from utils.date_utils import DateUtils

# 格式化下次执行时间
next_run_time = task.next_run_time
status = DateUtils.get_task_status_display(next_run_time, "running")
# 结果: "45分钟后" 或 "周一 20:00"
```

## 🔐 安全特性

### 权限控制
- 基于 `chat_id` 的访问控制
- 只有授权用户可以使用任务管理功能
- 防止未授权访问和恶意操作

### 错误处理
- 完善的异常处理机制
- 自动重连和错误恢复
- 详细的错误日志记录

### 会话管理
- 持久化会话连接
- 自动重连机制
- 连接状态监控

## 📝 开发指南

### 添加新命令
1. 在 `handlers.py` 中添加新的命令处理方法
2. 在 `__init__.py` 中注册新命令
3. 更新帮助文档和示例

### 扩展任务管理功能
1. 修改 `models.py` 添加新的数据模型
2. 在 `handlers.py` 中实现相应的处理逻辑
3. 更新 `formatters.py` 添加新的消息格式

### 自定义时间显示
可以在 `utils/date_utils.py` 中修改时间格式化逻辑：

```python
@staticmethod
def format_next_run_time(next_run_time, task_status="running") -> str:
    # 自定义时间显示逻辑
    pass
```

## 🐛 故障排除

### 常见问题

1. **机器人无响应**
   - 检查Telegram配置是否正确
   - 确认 `chat_id` 是否在授权列表中
   - 查看系统日志获取详细错误信息

2. **任务状态显示异常**
   - 检查调度器是否正常运行
   - 确认任务配置是否正确
   - 重启调度器服务

3. **时间显示不准确**
   - 检查系统时区设置
   - 确认 `date_utils.py` 中的时间处理逻辑
   - 验证APScheduler的时间配置

### 日志查看
```bash
# 查看任务管理器日志
grep "TaskManager" log/sys.log

# 查看调度器日志
grep "Scheduler" log/sys.log

# 查看Telegram连接日志
grep "tgbot" log/sys.log
```

## 📈 性能优化

### 缓存机制
- 任务状态信息缓存，减少数据库查询
- 消息格式化结果缓存，提高响应速度

### 异步处理
- 所有网络操作采用异步处理
- 消息发送使用异步队列

### 资源管理
- 定期清理过期会话
- 优化数据库连接池

---

## 📞 技术支持

如需技术支持或报告问题，请查看系统日志或联系项目维护者。

**Note**: Telegram任务管理器是Quote System v2.3.0的核心功能之一，提供了现代化的任务管理体验。
