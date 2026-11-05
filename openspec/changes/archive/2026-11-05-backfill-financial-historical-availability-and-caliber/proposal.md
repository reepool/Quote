# 回填财报历史可得性日期并量化历史批次口径差

## Why

2026-11-03 三季报抽查后的历史复检发现：宽表 `financial_facts` 的 66,134 行中
**54,389 行（82%）缺 `publish_date` 与 `data_available_date`**，集中于
2023-12-31 ～ 2026-03-31 共十个季度（每期 98–99% 缺失）；2026-06-30 仅缺
7%、2026-09-30 约 0%（当季管道正常，缺口全部来自历史批次导入）。

这两个字段是 PIT（防未来函数）研究的依据：**用 2023Q4–2026Q1 十个季度做
point-in-time 回测时，宽表自身无法回答"该行数据当时何时可得"**。

同时确认（同轮复检证据）：

- 项目既有专项模块 `research/financial_available_date_backfill.py` 的两条本地
  证据源在该区间几乎为空：`financial_source_files` 62,235 行中仅 604 行带
  `published_at`（472 个 instrument×period 对）；事件表经 11-03 修复裁剪后
  仅存近期与少量历史残留，归档 59,232 行中目标区间带 `announcement_time`
  的仅 2 条。
  **模块是对的，但证据原料未进货**。
- 历史批次存在小比例口径差：2025-09-30 抽 300 只，4 只（1.3%）revenue 在
  facts 与数值层不一致，模式统一为"营业总收入 vs 营业收入"（000617.SZ
  256.45 亿 vs 6.82 亿），均为 2026-05-22 同批写入；当季 2026-09-30 全量
  0/5,571 已对齐。真实规模未定量。
- 零星缺口：金刚光伏(300093) 2026H1 缺事实且无事件；2024–2025 各 Q1/Q3 共
  289 行 `total_assets` 为 NULL，其中 40 行数值层有值可本地回填；宽表有效
  历史自 2023Q4 起（更早仅 10 行；数值层更早期次仍有数据，如 2023H1 为
  1,698 行）。

另有 2,010 条历史事件带 11-01 v6 目录空窗期的陈旧 `mapping_catalog_empty`
reason（状态本身正确，均为 `accepted_disclosure_gap`）：滚动窗口内
（2024Q1 起，1,966 条）由周日全量 reconciliation 自然刷新；窗口外的
2023-12-31 期 44 条不会被周日任务覆盖，本 change 以一次定向
reconciliation（`report_periods=['2023-12-31']`）完成刷新并验证清零。

## What Changes

按"先本地、再网络勘察、法定截止日兜底"的顺序修复，网络长任务与应用隔离：

- **本地快修（无网络）**：全期全量口径对账量化脚本（facts vs 数值层，四字
  段 × 0.5% 容差，报告落 `docs/development/`）；40 行 total_assets 本地回
  填；金刚光伏定向 reconciliation 收编；`financial_available_date_backfill`
  证据加载器扩展 `financial_statements_raw` / `financial_filing_versions`
  本地源并试跑。
- **历史披露日批量勘察（唯一网络长任务）**：按市场×季度窗口走既有公告流扫
  描（cninfo 分类查询 + `infer_report_periods_from_title` 从标题推报告期），
  结果写 `announcement_audit`（专用 purpose_key），**禁止写事件状态表**
  （防 11-03 式 5.9 万行膨胀重演）；执行侧遵守 2026-11-03 403 事故教训：
  带完整访问栈（headed Chrome）、温和限速、分窗断点，不在裸独立进程跑
  全量网络任务。
- **可得性日期回填**：证据到位后运行既有回填模块（幂等）；仍缺行以**法定
  披露截止日做保守可得日**（Q1→4/30、H1→8/31、Q3→10/31、年报→次年
  4/30），lineage 标注模块既有兜底标记（`estimated_statutory_deadline`），研究侧可区分实测日
  与保守上界。
- **验证**：覆盖率四类占比报告（observed / surveyed / statutory / 仍缺）、
  PIT 硬性边界校验（`data_available_date` ≥ 报告期期末；兜底行 = 法定截止
  日；实测晚于截止日属延期披露真实情况，超 60 天进异常清单）、抽 20 家对
  cninfo 人工核对、`mapping_catalog_empty` 陈旧 reason 清零验证（窗口内
  周日全量 + 窗口外 2023Q4 定向刷新）。
- **决策门**：口径差经量化后是否对历史行按 v6 管道重派生，由 owner 拍板
  （营业总收入 vs 营业收入对控股公司各有合理性）；未获决策不执行重派生。

## Capabilities

### New Capabilities

- `financial-historical-availability-repair`: 财报宽表历史行的可得性日期
  （`publish_date` / `data_available_date`）具备可验证的 PIT 语义——本地
  证据优先、批量勘察有界、法定截止日保守兜底；历史批次与当季管道的字段
  口径差有全量量化报告与明确的决策门。

### Modified Capabilities

无。复用 `financial_available_date_backfill` 既有模块与公告采集服务契约，
仅做证据源扩展与一次性数据操作；`scheduler-task-loop-isolation`（FR-18）
的执行隔离契约不变，本 change 的网络勘察按该契约在 worker/调度侧执行。

## Impact

- 受影响代码（均为局部改动，总纲 §5 允许级别）：
  - `research/financial_available_date_backfill.py`：`_load_available_date_evidence`
    增加本地源（statements_raw / filing_versions）与 `announcement_audit`
    勘察源（约数十行）；
  - `scripts/maintenance/`：新增口径对账量化脚本与勘察触发脚本（一次性工具，
    遵守 §2.3 TMPDIR）；
  - 勘察的调度侧运行入口：优先复用现有 worker/manual 触发路径；确需新增
    manual_only 调度任务时改 `config/05_scheduler.json`（一条 job 条目）；
  - 不改 `research/storage.py` 表结构与既有方法签名；不新增共享抽象。
- 程序映射：FR-07（W5 存储边界）只读切片不动 repository 划分；FR-18
  （`isolate-long-tasks-from-api-event-loop`）为网络勘察的执行约束来源。
- 生产不变量：表结构、canonical key、写入 owner、job 配置与调度时间全部
  不变；`financial_facts` 仅 UPDATE 空值列（publish_date / data_available_date
  / total_assets 局部）；回填不更新 `row_hash`、不产生 `data_change_log`
  变更记录（`updated_at` 按回填模块既有行为维护，已核实不触发水位级联）。
- 非目标：不做 2023Q4 之前宽表历史扩产（滚动窗口 10 季，2023Q4 已为基线；
  更早历史数值层仍可用）；不做通用披露时间数据平台；不修改当季管道；
  249 行 total_assets 中数值层无值的行以"源文件可重解析则重解析、否则接受
  并记录"处理，不强制补齐；历史事件陈旧 reason 窗口内（2024Q1 起）由周日
  全量任务自然刷新，窗口外 2023Q4 44 条以一次定向 reconciliation 刷新，
  不写并行刷新逻辑。
