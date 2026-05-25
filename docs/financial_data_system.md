# 财务数据系统专项文档

> 更新日期：2026-05-24
> 适用范围：A 股 `SSE / SZSE / BSE` 财务报表结构化获取、字段映射、统一存储、本地核心层全量导入与后续运维。  
> 配套工程文档：`implementation_plan.md`、`docs/development/research_data_engine_execution.md`、`config/10_research.json`

## 1. 当前结论

当前财务数据系统已经从“寻找单一完整数据源”调整为分层财务数据服务：

| 层级 | 定位 | 当前源 | 是否本地持久化 | 说明 |
|---|---|---|---|---|
| L1 本地核心层 | 高频、本地、字段统一的核心财务事实 | 同花顺 + 新浪严格语义交集 | 是，写入 `data/financials.db` | 只接收已审核通过的 canonical 字段，按 profile 区分 `nonbank / bank / securities / insurance` |
| L2 官方摘要校验层 | 官方大项校验和结构化披露辅助 | CNInfo data20，历史保留 SSE commonQuery 能力 | 作为 source manifest / numeric facts 能力保留 | CNInfo data20 字段较少，不能当完整三表源；commonQuery 已不适合作为最新年报主源 |
| L3 远程扩展层 | 本地核心字段不足时按需补字段 | 东财 | 默认不写入 L1 核心层 | 字段最宽但慢，仅在显式允许远程扩展时调用，并转换为本地 canonical schema |

关键边界：

- 生产财务数据写入目标是 `data/financials.db`，不是 `data/research.db`。
- `data/research.db` 当前不应保留 `financial_*` 财务物理表；代码初始化时也会在独立财务库配置下移除研究库内的财务表。
- L1 字段必须来自已批准 mapping catalog；字段名相似、单样本数值相近、会计等式另一边相等，都不能自动进入本地核心层。
- CNInfo data20 当前作为官方摘要层和交叉校验层，不再投入大量时间追求完整三表覆盖。
- BSE 与少数上市前科创板历史期存在已确认上游缺口；这些缺口只允许显式登记为 accepted source gap，不能通过放宽映射伪造完整性。
- 全量导入入口默认把 `BSE` required 字段缺失记录为 `exchange_optional_source_gap`，不阻断后续批次；`SSE/SZSE` 仍按严格 gate 处理。
- Manifest 现在记录每个标的的 `listed_date / delisted_date` 和报告期生命周期排除项；上市前历史期、退市后未披露期会进入 accepted source gap，不再被误判为字段映射失败。
- Manifest 后续还必须接收财务披露公告事件：当 CNInfo 公告证明公司存在定期报告无法按期披露、停牌、退市风险警示或可能终止上市等事项时，对应报告期可标记为 `periodic_report_delayed_or_suspended`，作为“披露异常导致的正常待补”处理；该分类不能替代字段映射修复。
- L1 写入允许受控派生 `net_income_parent`：优先使用直接字段；若只有 `净利润 + 少数股东损益`，按 `归母净利润 = 净利润 - 少数股东损益` 派生；若没有少数股东损益和少数股东权益披露，才允许以 `净利润` 作为归母净利润，并在 lineage 中标记 `net_income_total_when_no_minority_interest_disclosed`。

## 2. 数据库与存储边界

### 2.1 数据库分工

| 数据库 | 职责 | 财务域状态 |
|---|---|---|
| `data/quotes.db` | 行情主库 | 不承载财务事实 |
| `data/research.db` | 研究域非财务数据，例如行业、股东、风险、技术快照 | 不承载完整财务报表事实；历史残留财务表已清理 |
| `data/financials.db` | 财务报表、财务事实、字段映射、source manifest、运行审计 | 当前生产财务库 |

`config/10_research.json` 中的关键配置：

```json
{
  "storage": {
    "financials_db_path": "data/financials.db"
  }
}
```

### 2.2 财务相关核心表

当前财务库主要使用以下表族：

| 表族 | 职责 |
|---|---|
| `financial_source_files` | source manifest，记录数据源、报表族、报告期、归档路径、parser 版本等 |
| `financial_statements_raw` | 原始报表 JSON 快照 |
| `financial_numeric_facts_hot/history` | 全数值事实长表的权威物理存储，保留 source lineage 和 canonical 元数据 |
| `financial_numeric_facts` | 兼容读取层；优化后数据库中应为 `hot UNION ALL history` 视图，不再作为重复物理表写入 |
| `financial_core_facts_hot/history` | 核心事实层，服务常用查询 |
| `financial_source_field_mappings` | 持久化字段映射目录，按 mapping version 与 profile 管理 |
| `financial_source_mapping_audits` | 字段映射审计证据 |
| `financial_facts`、`financial_indicator_snapshots` | 旧读取/指标快照兼容层 |
| `ingestion_runs`、`raw_payload_audit` | 财务域运行审计和 payload 审计 |

物理存储采用长表事实模型，不因上游字段变动动态增删宽表列。新增字段、字段削减、字段语义变化通过 mapping version 和审计状态控制，再由 read model 投影为调用方需要的宽表或字段集合。

### 2.3 存储体积优化

`2026-05-25` 已确认 `data/financials.db` 体积约 `78.973 GiB`，`freelist_count=3`，不是删除后未回收空间导致。主要冗余来自 `financial_numeric_facts` 与 `financial_numeric_facts_hot` 同时保存 `23,395,207` 行全量 numeric facts，而 `financial_numeric_facts_history=0`。

当前优化策略：

- 新增写入只落到 `financial_numeric_facts_hot` 或 `financial_numeric_facts_history`，不再双写 legacy 物理表 `financial_numeric_facts`。
- 内部读取和脚本使用 hot/history union；老测试库或迁移前库缺少 tier 表时才回退 legacy 表。
- 后续执行离线优化迁移时，最终库名仍保持 `data/financials.db`；迁移会把 `financial_numeric_facts` 改为兼容视图。
- 生产库迁移前必须先备份到 `/home/python/Quote/data/PVE-Bak/QuoteBak`。

轻量审计命令：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/research_financial_db_storage_audit.py \
  --db-path data/financials.db \
  --sample-size 1000 \
  --format markdown
```

优化迁移 dry-run 命令：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/research_financial_db_optimize.py \
  --db-path data/financials.db \
  --backup-dir /home/python/Quote/data/PVE-Bak/QuoteBak \
  --dry-run
```

正式执行命令只在人工确认维护窗口后使用：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/research_financial_db_optimize.py \
  --db-path data/financials.db \
  --backup-dir /home/python/Quote/data/PVE-Bak/QuoteBak \
  --execute
```

注意：正式执行会先生成 NAS 备份，再构建临时优化库，验证通过后仍替换为原路径 `data/financials.db`。执行期间需要预留较长 IO 时间和足够临时空间；失败时应保留原库和备份，不允许无验证切换。

## 3. 字段统一规则

### 3.1 Profile

当前 L1 本地核心层按公司财报 profile 选择字段映射目录：

| Profile | 说明 |
|---|---|
| `nonbank` | 一般工商企业 |
| `bank` | 银行 |
| `securities` | 证券公司 |
| `insurance` | 保险公司 |

公司 profile 由 `financial_statement_profile` resolver 从行业归属、公司信息和标的元数据中解析，并写入导入 evidence。更新公司财报时需要知道 profile，因为银行、证券、保险与一般企业的科目含义可能不同，不能共享简单别名。

### 3.2 Mapping version

当前 L1 映射目录版本为 `sina_ths_core_financial_facts.v5`。

该版本已经把当前审核通过的 Sina + THS 通用 L1 字段显式写入四个 profile：

| Profile | 当前覆盖边界 |
|---|---|
| `nonbank` | 通用非银字段，当前已审核约 `130` 个 approved 字段 |
| `bank` | 通用字段 + 银行专项修正，当前已审核约 `131` 个 approved 字段，并保留已拒绝映射 |
| `securities` | 当前继承并显式物化非银通用 L1 字段；后续证券专项字段需新版本追加 |
| `insurance` | 当前继承并显式物化非银通用 L1 字段；后续保险专项字段需新版本追加 |

`v5` 的关键修正：

- 银行的“发放贷款及垫款”毛额不能映射到 THS `loans_payments_behalf`。
- 银行的“发放贷款及垫款净额”才与 THS `loans_payments_behalf` 和东财 `LOAN_ADVANCE` 对齐。
- 银行“现金及存放中央银行款项”不能等同通用 `total_cash`。

### 3.3 字段进入 L1 的条件

字段关系必须明确分类：

| 关系 | 是否进入 L1 | 处理 |
|---|---|---|
| `exact_equivalent` | 是 | 字段含义、报表类型、口径、期间属性、单位一致 |
| `equivalent_after_unit` | 是 | 仅单位不同，必须有明确 multiplier |
| `derived_equivalent` | 谨慎 | 必须有稳定公式和完整组件，并标记 derived |
| `broader_than` | 否 | 更宽口径不能覆盖窄口径 |
| `narrower_than` | 否 | 子项不能覆盖总项 |
| `related_only` | 否 | 相关但不是同一财务事实 |
| `rejected` | 否 | 明确拒绝并记录原因 |
| `unknown_candidate` | 否 | 候选待审计，不得进入生产核心层 |

验证顺序：

1. 先做字段语义审查：报表类型、字段层级、归母/总额、合并口径、累计/单季、期末余额/期间发生额。
2. 再做单位审查：`source_unit / canonical_unit / unit_multiplier / value_type`。
3. 再做多公司、多交易所、多 profile、多报告期数值校验。
4. 最后用会计恒等式辅助审计，例如 `资产 = 负债 + 所有者权益`，但恒等式相等不代表两个字段可以合并。

## 4. 已确认的数据缺口

当前 accepted source gap 分为两类：静态已确认缺口，以及 manifest 按上市/退市日期动态生成的生命周期缺口。

### 4.1 生命周期缺口

Manifest 对每个目标标的、每个报告期执行以下判断：

| 分类 | 规则 | 导入处理 |
|---|---|---|
| `pre_listing_period` | 报告期结束日早于上市日 | 记录为正常缺口，不阻断全量导入 |
| `post_delisting_or_no_disclosure` | 报告期结束日晚于退市日，或为退市日前最近一期且退市日在该期披露截止日前 | 记录为正常缺口，不阻断全量导入 |
| `periodic_report_delayed_or_suspended` | 公司仍为 active，但 CNInfo 公告显示对应定报无法按期披露或延期披露 | 记录为披露异常待补，不阻断全量导入；后续公告或结构化源更新后应补处理 |
| `pending_delisting_risk` | 公司仍为 active，但 CNInfo 公告显示停牌、退市风险警示、可能被实施退市风险警示或可能终止上市 | 记录为待退市风险/披露异常待补，不阻断全量导入；不改写股票主数据退市状态 |

该规则解决两类常见误报：

- 新股或科创板上市前历史期没有完整三表。
- 退市标的在退市后不再披露最新季报，例如 `600355.SH / 2026-03-31`。
- 仍为 active 但定报披露异常的标的，例如 `002731.SZ / 688121.SH` 在 `2025-12-31 / 2026-03-31` 尚无结构化三表时，不应被误判为字段映射失败。

公告事件进入 manifest 的要求：

- 事件必须来自可审计公告元数据，至少包含 `instrument_id / report_period / classification / title / announcement_time`。
- 允许的财务披露异常分类目前包括 `periodic_report_delayed_or_suspended` 和 `pending_delisting_risk`；普通“年度报告”“一季报”公告只能作为触发增量更新的候选，不能作为 accepted gap。
- 如果公告显示“更正”“取消”“补充”，只能触发重抓或复核，不得直接接受缺口。
- 如果公告事件缺少明确报告期，只能进入待复核清单，不得自动放宽 readiness gate。

### 4.2 静态已确认缺口

当前内置静态 accepted source gap 只覆盖已经定向确认的真实上游缺口。

| 标的 | 报告期 | 缺失字段 | 分类 |
|---|---|---|---|
| `920020.BJ` | `2024-09-30` | `total_assets / total_liabilities / equity_parent` | BSE 结构化源缺资产负债表核心字段 |
| `920027.BJ` | `2024-09-30` | `total_assets / total_liabilities / equity_parent` | BSE 结构化源缺资产负债表核心字段 |
| `920028.BJ` | `2024-09-30` | `total_assets / total_liabilities / equity_parent` | BSE 结构化源缺资产负债表核心字段 |
| `920045.BJ` | `2024-09-30` | `total_assets / total_liabilities / equity_parent` | BSE 结构化源缺资产负债表核心字段 |
| `688807.SH` | `2024-09-30` | `total_assets / total_liabilities / equity_parent` | `pre_listing_incomplete_structured_statement` |
| `688809.SH` | `2024-09-30` | `total_assets / total_liabilities / equity_parent` | `pre_listing_incomplete_structured_statement` |
| `688816.SH` | `2024-09-30` | `total_assets / total_liabilities / equity_parent` | `pre_listing_incomplete_structured_statement` |
| `688818.SH` | `2024-09-30` | `total_assets / total_liabilities / equity_parent` | `pre_listing_incomplete_structured_statement` |

使用原则：

- 只有经过跨源确认的 issuer-period 缺口才能登记。
- 登记后导入状态可变为 `success_with_accepted_source_gaps`，但这不等同于全字段完整。
- 新发现缺口不得直接加入默认清单，应先做定向跨源审计。

### 4.3 2026-05-22 剩余缺口复核

对全量运行中剩余 `36` 条 blocking issuer-period 做定向真实 dry-run：

| 阶段 | 结果 |
|---|---|
| 字段级跨源合并前 | `36` 条 blocking |
| 增加同一报表字段级来源追踪和缺字段回填后 | 仍为 `36` 条，说明主因不是同义字段未合并 |
| 增加 `净利润 - 少数股东损益` 派生归母净利润后 | 降为 `12` 条 |
| 增加“无少数股东损益/权益披露时净利润即归母净利润”的受控派生后 | 降为 `5` 条 |

剩余 `5` 条为整期无 L1 结构化三表：

| 标的 | 报告期 | 状态 |
|---|---|---|
| `002731.SZ` | `2025-12-31`、`2026-03-31` | active 标的，Sina/THS/Eastmoney 本次探测均未返回该期结构化三表；`2026-05-24` 写入 smoke 命中多条停牌/退市风险提示公告，但标题未指向具体报告期，因此只保留审计证据，不自动放宽 readiness gate |
| `688121.SH` | `2025-12-31`、`2026-03-31` | active 标的，Sina/THS/Eastmoney 本次探测均未返回该期结构化三表；`2026-05-24` 写入 smoke 命中 2025 年年报延期披露公告，并把 `2025-12-31` 记录为 `periodic_report_delayed_or_suspended / pending_recheck` |
| `600355.SH` | `2026-03-31` | 已退市，按 `post_delisting_or_no_disclosure` 记录为正常生命周期缺口 |

### 4.4 2026-05-24 公告驱动写入 smoke

定向命令：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/dev_validation/run_financial_disclosure_targeted_smoke.py \
  --instrument-ids 002731.SZ,688121.SH \
  --exchanges SZSE,SSE \
  --report-periods 2025-12-31,2026-03-31 \
  --lookback-days 180 \
  --max-pages-per-market 20 \
  --db-path data/financials.db \
  --output-path log/financial_disclosure_smoke/20260524_002731_688121.json \
  --write-enabled
```

验证结果：

| 标的 | CNInfo 扫描结果 | 写入结果 | 处理结论 |
|---|---|---|---|
| `002731.SZ` | 扫描 `94` 条公告，选中 `4` 条停牌/退市风险提示 | `candidate_count=0`，`event_count=0` | 公告未映射到具体报告期，只作为审计证据；不能据此接受某个报告期缺口 |
| `688121.SH` | 扫描 `36` 条公告，选中 `5` 条，其中 `3` 条延期 2025 年年报可映射报告期 | `candidate_count=1`，`financial_disclosure_event_state` 写入 `688121.SH / 2025-12-31 / pending_recheck` | 结构化源仍未更新时进入待重查，不作为字段映射 blocker |

该 smoke 同时验证：公告先于结构化财报时，系统会拉取候选对应的最新财务数据；若本地仍缺 required core facts，则写入 `pending_recheck` 并保留 `first_pending_at / pending_recheck_until`。同一公告的 `pending_recheck_until` 使用首次 pending 时间作为硬上限，后续重复扫描不会滚动延长；待结构化源更新后，下一次增量或对账任务会重新拉取并把状态更新为 `changed` 或 `unchanged`。

## 5. 全量导入脚本

正式入口：

```bash
/home/python/miniconda3/envs/Quote/bin/python scripts/research_financial_l1_full_import.py --help
```

脚本定位：

- 生成全市场 manifest、targets 和 batch 文件。
- 按 batch 抓取 Sina/THS L1 本地核心层数据。
- 写入 `data/financials.db`。
- 记录进度、日志、每批 evidence 和最终摘要。
- 支持中断后 `--resume` 续跑。
- 供后续 scheduler 或 Telegram 调用；长期运维不应使用 shell 循环。

### 5.1 推荐全量命令

截至 `2026-05-20`，按当前披露截止日和 `config/10_research.json` 的财务历史配置，最新可用报告期应覆盖到 `2026-03-31`。全量脚本已支持自动滚动窗口，不需要手写每个报告期。

当前配置口径为：

- `baseline_report_period=2024Q1`
- `rolling_min_quarters=8`
- `optional_ttm_anchor_period=2023Q4`
- `include_ttm_anchor_period=true`

因此当前正式全量导入推荐使用 `--period-window latest --rolling-quarters 10`，脚本会自动解析为 `2023-12-31` 到 `2026-03-31` 共 `10` 个报告期：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/research_financial_l1_full_import.py \
  --period-window latest \
  --rolling-quarters 10 \
  --baseline-report-period 2024Q1 \
  --db-path data/financials.db \
  --batch-size 20 \
  --log-dir logs/financial_full_import/20260520_l1_full_2023q4_2026q1
```

中断后续跑：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/research_financial_l1_full_import.py \
  --period-window latest \
  --rolling-quarters 10 \
  --baseline-report-period 2024Q1 \
  --db-path data/financials.db \
  --batch-size 20 \
  --log-dir logs/financial_full_import/20260520_l1_full_2023q4_2026q1 \
  --resume
```

如果已有 CNInfo 公告筛选结果，可把披露异常事件作为 JSON 传给全量脚本；脚本会把 `periodic_report_delayed_or_suspended` 事件合并进 manifest 生命周期缺口，并在 readback gate 中按 accepted source gap 处理：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/research_financial_l1_full_import.py \
  --period-window latest \
  --rolling-quarters 10 \
  --baseline-report-period 2024Q1 \
  --db-path data/financials.db \
  --batch-size 20 \
  --log-dir logs/financial_full_import/20260524_l1_repair_with_disclosure_events \
  --financial-disclosure-events-path data/financial_disclosure_events/latest.json
```

事件文件格式：

```json
{
  "events": [
    {
      "instrument_id": "002731.SZ",
      "report_periods": ["2025-12-31", "2026-03-31"],
      "classification": "periodic_report_delayed_or_suspended",
      "title": "关于无法按期披露2025年年度报告暨股票停牌的公告",
      "announcement_id": "example",
      "announcement_time": "2026-05-06"
    }
  ]
}
```

默认全量脚本会在新建 manifest 前执行通用 A 股主数据治理，刷新上市、退市和 active 股票池。若只做离线复核或复用已确认的新鲜主数据，可显式加 `--skip-instrument-master-governance`。

公告事件文件可由 CNInfo 扫描脚本生成：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/research_financial_disclosure_event_scan.py \
  --exchanges SSE,SZSE,BSE \
  --start-date 2026-04-01 \
  --end-date 2026-05-24 \
  --output-path data/financial_disclosure_events/latest.json \
  --max-pages 20
```

该扫描只输出公告事件，不下载财务报表、不写 `financials.db`。实际生产可先扫描公告，再把输出文件传给全量/补处理脚本。

### 5.2 Telegram 与增量维护目标

财务 L1 全量导入后续应进入 scheduler/Telegram 任务体系，而不是长期依赖 operator 手工敲脚本。

截至 `2026-05-24`，任务化入口已落地到 `config/05_scheduler.json`、`data_manager.py` 和 `scheduler/tasks.py`：

- `financial_l1_full_import` 为 `enabled=true/manual_only=true`，只通过 `/run` 或直接调度调用执行，不注册自动 cron。
- `financial_disclosure_incremental_sync` 与 `financial_disclosure_reconciliation_sync` 默认 `enabled=false`，可先通过 `/run` 验证，稳定后再决定是否启用日更/周更。
- 三个任务均写入 `data/financials.db`，并在报告中显示实际 DB 路径、候选数量、写入/跳过数量、pending recheck、待退市风险、accepted gaps 和 blockers。

| 任务 | 触发方式 | 是否自动定时 | 主要用途 |
|---|---|---:|---|
| `financial_l1_full_import` | `/run financial_l1_full_import` | 否 | 手工执行全量初始化、灾后修复或大范围补处理；封装 `scripts/research_financial_l1_full_import.py` |
| `financial_disclosure_incremental_sync` | 每日或 `/run` | 是，可配置 | 基于 CNInfo 定期报告公告发现候选标的和报告期，只对候选做财务补处理 |
| `financial_disclosure_reconciliation_sync` | 每周或 `/run` | 是，可配置 | 兜底公告漏识别、CNInfo 静默更新和历史缺口；可全量扫描 active 股票池但只补缺失或变化项 |

增量维护规则：

- 定期报告公告包括年度报告、半年度报告、第一季度报告和第三季度报告；普通公告只作为候选触发，不直接代表结构化财报已经可用。
- 公告显示“无法按期披露”“停牌”“退市风险警示”“可能被终止上市”时，对历史缺口先标记为 `pending_delisting_risk` 或 `periodic_report_delayed_or_suspended`，不阻断批次，但必须保留公告 ID、公告标题和首次发现时间。
- 公告先到而 Sina/THS 结构化财报暂未更新时，候选进入 pending recheck；同一公告的重试窗口使用首次 pending 时间作为硬上限，不因每日扫描滚动延长。
- 全量任务、增量任务和周度对账任务都必须写入 `data/financials.db`，并复用相同的字段 mapping、单位转换、生命周期缺口和 accepted gap 规则。

状态表：

- `financial_disclosure_event_state`：按 `instrument_id + report_period + announcement_id` 记录候选、pending recheck、待退市风险、缺失字段、首次 pending 时间、重试截止时间和处理结果。
- `cninfo_announcement_scan_state / cninfo_announcement_audit`：财务任务使用独立 purpose key 记录扫描水位和入选公告证据。

只生成计划，不下载、不写库：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/research_financial_l1_full_import.py \
  --period-window latest \
  --rolling-quarters 10 \
  --baseline-report-period 2024Q1 \
  --batch-size 20 \
  --log-dir logs/financial_full_import/plan_check_2023q4_2026q1 \
  --manifest-only
```

只跑前几个 batch 做小批 smoke。注意：smoke 可以只用两期验证链路；这不是正式全量导入口径。

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/research_financial_l1_full_import.py \
  --report-periods 2024-12-31,2024-09-30 \
  --db-path data/financials.db \
  --batch-size 20 \
  --log-dir logs/financial_full_import/smoke \
  --max-batches 2
```

跑指定 batch 范围：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/research_financial_l1_full_import.py \
  --period-window latest \
  --rolling-quarters 10 \
  --baseline-report-period 2024Q1 \
  --db-path data/financials.db \
  --batch-size 20 \
  --log-dir logs/financial_full_import/20260520_l1_full_2023q4_2026q1 \
  --start-batch 50 \
  --end-batch 80
```

### 5.2 常用参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--report-periods` | 空 | 显式报告期列表，逗号分隔；仅用于复现历史 evidence 或指定固定窗口 |
| `--period-window` | `latest` | 未传 `--report-periods` 时自动解析最新滚动窗口 |
| `--rolling-quarters` | `10` | 最新滚动窗口季度数；`10` 对应当前 `2023Q4 -> 2026Q1` |
| `--baseline-report-period` | `2024Q1` | 自动窗口的配置基准期；若从基准期到最新期不足 `rolling-quarters`，脚本会继续向前补足 |
| `--latest-report-period` | 空 | 手动指定最新披露期，例如 `2026Q1`；默认按当前日期和披露截止日推断 |
| `--optional-anchor-period` | 空 | 可选锚定期，例如 `2023Q4` |
| `--include-optional-anchor` | 关闭 | 是否额外包含 `--optional-anchor-period` |
| `--db-path` | `data/financials.db` | 写入目标库 |
| `--log-dir` | `logs/financial_full_import/<timestamp>` | 本次运行日志与 evidence 目录 |
| `--exchanges` | `SSE,SZSE,BSE` | 导入交易所范围 |
| `--limit-per-exchange` | 空 | 每个交易所最多取多少标的；仅用于 smoke 或抽样 |
| `--batch-size` | `20` | 每批标的数 |
| `--mapping-version` | 当前代码默认 mapping version | 字段映射目录版本，生产应使用配置中的当前版本 |
| `--source-order` | 当前默认 source order | L1 源顺序，当前应为同花顺优先、Sina 互备 |
| `--required-canonical-facts` | 默认六个必达字段 | 启用 gate 检查字段，默认包括收入、归母净利润、资产、负债、归母权益、经营现金流 |
| `--request-interval-seconds` | `0.2` | 单请求间隔 |
| `--request-timeout-seconds` | `20.0` | 单请求超时 |
| `--accepted-source-gap` | 可重复传入 | 额外登记已人工确认的源缺口 |
| `--accepted-source-gap-exchanges` | `BSE` | 指定哪些交易所的 required 字段缺失只记录、不阻断全量；如需严格复核可传空字符串 |
| `--no-default-accepted-source-gaps` | 关闭默认缺口清单 | 用于严格复核，不建议生产导入默认使用 |
| `--stop-on-needs-review` | 关闭 | 默认遇到非 BSE 的 blocking 缺口时记录 review 并继续后续批次；打开后恢复旧行为，遇到首个 `needs_review` 即停止 |
| `--no-skip-ready-targets` | 关闭 | 默认续跑时会检查 `financials.db`，已经覆盖全部 requested report periods 且必达字段齐全的股票会跳过 |
| `--resume` | 关闭 | 使用已有 manifest 和 progress 续跑 |
| `--manifest-only` | 关闭 | 只生成计划 |
| `--start-batch / --end-batch / --max-batches` | 空 | 控制运行批次范围 |

### 5.3 输出文件

运行目录下会生成：

| 文件 | 说明 |
|---|---|
| `manifest.json` | 全量目标、profile、mapping readiness、批次计划 |
| `manifest_summary.json` | manifest 摘要 |
| `targets.txt` | 全部目标行 |
| `batches/batch_*.txt` | 每批目标行 |
| `full_import.log` | 详细运行日志 |
| `progress.log` | 人可读进度日志 |
| `progress_state.json` | 续跑状态，包括已完成批次和失败批次 |
| `batch_0001.json` | 每批完整 evidence |
| `batch_0001_summary.json` | 每批摘要 |
| `final_summary.json` | 全部运行最终摘要 |

观察进度：

```bash
tail -f logs/financial_full_import/20260520_l1_full_2023q4_2026q1/progress.log
```

查看最终结果：

```bash
cat logs/financial_full_import/20260520_l1_full_2023q4_2026q1/final_summary.json
```

### 5.4 状态含义

| 状态 | 含义 | 是否可继续 |
|---|---|---|
| `success` | 所有选中 batch 均 ready，无阻断缺口 | 可以进入后续验证 |
| `success_with_accepted_source_gaps` | 存在已登记真实源缺口，但无 blocking 缺口 | 可以继续，但必须在报告中保留缺口说明 |
| `needs_review` | 单批存在未解释缺口或映射/读取问题 | 默认记录到 `review_batches` 并继续；若传 `--stop-on-needs-review` 则停止 |
| `success_with_review` | 全量批次跑完，但存在需要后续补处理的 batch | 可以作为“下载主流程完成但有补处理清单”的状态 |
| `failed` | 脚本或批次异常失败 | 查看 `full_import.log` 和失败 batch evidence 后续跑 |
| `manifest_ready` | 只生成计划 | 尚未下载和写库 |

### 5.5 耗时估算

已验证样本给出的当前估算：

- `20` 只股票 x `2` 个报告期约 `45~50s`。
- `100` 只股票 x `2` 个报告期约 `226s`。
- 全市场约 `5490` 只股票，`2` 个报告期预计 `3.5~4.5` 小时。
- 正式 `2023-12-31` 到 `2026-03-31` 共 `10` 个报告期尚未做全市场实测；由于接口通常按股票返回多期 DataFrame，耗时不会严格变成两期的 `5` 倍，但 numeric facts 写入、读回校验和 evidence 体积会明显增加。
- 在完成首次全量实测前，正式导入窗口应按 `8~12` 小时级别预留，并通过 `progress.log` 和 `progress_state.json` 观察实际吞吐。

增加报告期不会严格线性增加网络时间，因为 AkShare 三表接口通常按股票返回多期 DataFrame；但会增加 bundle 构造、numeric facts 写入和 local-core 读回校验成本。首次生产导入完成后，应把实际耗时写回本文档。

## 6. 运行前检查

建议全量导入前检查：

```bash
sqlite3 data/research.db "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'financial_%';"
sqlite3 data/financials.db "PRAGMA quick_check;"
```

预期：

- 第一条不返回财务表。
- 第二条返回 `ok`。

只读 manifest 检查：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/research_financial_l1_full_import.py \
  --period-window latest \
  --rolling-quarters 10 \
  --baseline-report-period 2024Q1 \
  --batch-size 20 \
  --log-dir logs/financial_full_import/precheck_2023q4_2026q1 \
  --manifest-only
```

重点看：

- `manifest.json.status` 是否为 `ready`。
- `target_count` 是否符合当前股票池预期。
- profile 是否全部有高置信度来源。
- required canonical facts 是否均有 approved mapping。

## 7. 与行情日更的关系

财务全量导入写入 `data/financials.db`，行情日更主要写入 `data/quotes.db`，非财务研究域写入 `data/research.db`，物理目标不同。

仍需注意：

- 外部数据源请求可能共享网络、代理、AkShare 运行时和 CPU。
- SQLite 写锁仅限各自数据库文件；财务全量导入不应直接锁住行情主库。
- 首次全量导入建议避开行情日更高峰，或降低 batch 范围观察资源占用。
- 后续接入 scheduler/Telegram 时，应配置为手动触发或低峰任务，不应默认和行情日更同时抢占网络预算。

## 8. 后续待办

1. 将 `scripts/research_financial_l1_full_import.py` 接入 scheduler/Telegram 的任务注册和帮助文案。
2. 对 `success_with_review` 输出的 review batch 建立补处理命令，例如退市/未披露最新期、临时源失败、映射缺口三类。
3. 把内部 `dryrun` 命名的批处理函数重命名为中性 `import_batch` 类名称，降低长期维护误解。
4. 继续扩展证券、保险、银行专项字段，但必须通过新 mapping version 和多期证据。
5. 对新增 accepted source gap 建立单独审计文档或结构化清单，避免默认缺口清单膨胀。
6. 后续如迁移 DuckDB/PostgreSQL，只替换 storage adapter 和迁移脚本，不改变 canonical schema、source lineage 和 API 语义。
