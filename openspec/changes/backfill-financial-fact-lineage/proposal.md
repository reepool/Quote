# 存量财务事实血统补盖：终结对账伪缺口死循环

## Why

`financial_disclosure_reconciliation_sync` 连续多个 8~12 小时窗口无法收敛（2026-11-11 12h 上限再次触发）。完整证据链（全部生产实测，2026-11-11）：

- **E1 空转循环**：夜间修复批次以 ~2,800 标的/交易所/报告期的规模、0.24 秒/标的的节奏轮转（真实抓取为 2.4 秒/标的）——抓取层快速确认"源数据未变"后跳过，不产生写入；
- **E2 数据已在库**：各报告期 5,440/5,596 家标的有事实行；
- **E3 判定被血统过滤否决**：数据齐全的标的（000001.SZ 等 5 家抽样）required-5 全部 `missing_local_core_fact`——行存在但 `_numeric_fact_has_local_core_lineage` 因 `raw_fact.local_core_mapping` 缺失而全部过滤；
- **E4 无戳行是历史遗留**：这些行 2026-05-22 由 akshare 备源导入（同批 381 行中 73 行有戳、308 行无戳——按当时目录命中与否分半）；
- **E5 现行目录本可准入**：v6 目录 ths_metric 覆盖 521/521，required-5 的 ths 名（如 `total_debt→total_liabilities`）均有已批准映射——即当前 `_local_core_mapping_lineage` 对这些行**能**算出血统；
- **E6 永不收敛机制**：重抓 → 源负载未变 → 抓取层 unchanged-file 跳过（`financial_statements_sync` 的 `unchanged_files_skipped` 计数器体系）→ 不重解析 → 血统永不补盖 → 下次运行同样目标同样跳过。任何时长的窗口都无法打破。

即：**~2.8 万/报告期的"缺口"绝大多数是伪缺口**（数据在库、目录可准入、唯独缺 5 月时代未盖上的血统戳）。

## What Changes

- **一次性血统补盖迁移**（operator 脚本，维护窗口执行）：按当前 v6 目录对 hot/history 两表存量无戳行重算 `_local_core_mapping_lineage`，命中者补盖 `raw_fact.local_core_mapping` 并同步重算 `row_hash`；未命中者保持无戳（语义不变）；分批事务、可回滚、输出 已补盖/未变化/无映射 三类计数；
- **验收探针固化**：000001.SZ 2024-03-31 的就绪判定从 `ready=False（5 缺）` 变 `ready=True`（修复该标的所需 5 事实均命中目录）；
- **收敛性验收**：迁移后一个对账窗口内修复目标从 ~2,800/交易所/期 骤降至真实缺口（~150/期量级），周度对账总时长回小时级；
- **结构性防复发（登记移交）**：入库去重键未含 mapping_version，目录版本演进后"数据未变但准入语境已变"的行永远不重解析——该结构性修复属映射版本生命周期（V6 owner 域），在本 change 中登记并移交，不静默扩权。

## Capabilities

### New Capabilities

- `financial-fact-lineage-admission`：存量事实血统的补盖规则、语义保持、事务与回滚、收敛性验收。

### Modified Capabilities

无。

## Impact

- 新增 operator 脚本（scripts/），不改任何业务代码路径、不改判定语义、不触写路径；
- 数据影响：仅对"无戳且现行目录命中"的行改写 `raw_fact_json`+`row_hash`（`updated_at` 等其余字段保持原值）；写入前留存受影响行备份以供回滚；
- 非目标：不修抓取层去重键（移交项）、不补目录条目（E5 已证不需要）、不处理每期 ~150 家真缺数据标的（补数由既有任务自然完成）。
