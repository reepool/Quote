# 窄化对账候选构建路径：扫描段压到 1 小时以内

## Why

`financial_disclosure_reconciliation_sync` 的数据补齐已完成大半（抓取批次从 2,217 标的/批 降至 1/1），但**扫描段本身**成为周日全量的主导耗时，阻止总时长回到标准 18000s 窗口（现为临时 43200s）：

实测证据（2026-11-08~10，生产）：

- 扫描 61,284 候选，速率中位 **6.94/秒（144ms/候选）**、最差 3.56/秒（281ms）——扫描段 2.5~4 小时，与数据多少无关；
- py-spy 对扫描期 worker 线程 3 次采样，**2 次命中同一条宽查询链**：`readiness_for_target → _merge_official_cninfo_readiness → _official_cninfo_facts_by_canonical → get_numeric_facts`（宽方法）——对**每个有缺失字段的候选**取回该标的全部事实行（570+ 行 × 35 列肥行），再在 Python 里逐行筛出官方源（cninfo_data20）的少数行；
- `_candidate_for_period` 对**每个候选**（=每标的 × 11 期，同一标的重复 11 次）各自查询行业隶属与公司画像并解析 profile——而代码库已有批量设施 `resolve_financial_statement_profiles_for_instruments` 未被该路径使用（同功能双实现）。

上一 change（`accelerate-financial-readiness-queries`）只窄化了 local-core 读取链；本 change 关闭同规格要求在**官方源合并读取**与**profile 解析**两处的缺口。

## What Changes

- **官方源合并读取窄化**：`_official_cninfo_facts_by_canonical` 不再全量取行后 Python 筛选，改为投影必要列 + 将 cninfo_data20 谓词下推 SQL（覆盖 haystack 语义等价）；复用上一 change 的窄查询设施模式，不新建第三套事实查询实现；
- **profile 解析按标的去重**：同一标的的 11 个报告期共享一次解析（逐候选 → 逐标的；优先接入既有批量解析设施）；
- **等价性门禁复用**：冻结样本双跑（含"仅官方源有数、本地无数"的样本形态）100% 一致才合入；
- **验收**：扫描段 ≤60 分钟；周日全量总时长 ≤2.5 小时；连续两个周日达标后 `max_runtime_seconds` 由 43200 回调 18000。

## Capabilities

### New Capabilities

无。

### Modified Capabilities

- `financial-readiness-query-performance`：其 R1（就绪路径必须窄查询）本已覆盖此语义——本 change 补齐官方源合并读取的执行缺口并新增扫描段时长要求。

## Impact

- 代码影响：`research/financial_statement_maintenance_repair.py`（合并读取窄化）、`research/financial_disclosure_incremental_sync.py`（profile 去重）、`research/storage.py`（窄查询设施按需扩参数）；等价工具复用 `scripts/research_financial_readiness_perf.py`；
- 不改任何就绪判定语义、写路径与报告契约；
- 非目标：不处理 CNInfo 源站限流（另一遗留项）、不做按标的批量就绪（条件项 L5 维持原判）、不动 77GB 库体积治理。
