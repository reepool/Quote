# Design: 存量财务事实血统补盖

## 1. 根因与证据（全部实测，2026-11-11）

```
2026-05-22 akshare 备源导入（v5 时代目录）
  → 同批行按当时目录命中与否：73/381 盖戳、308 无戳（E4）
  → 无戳行被就绪判定的血统过滤器全部否决（E3）
2026-11-08~11 对账窗口
  → 伪缺口标的进入修复循环（~2,800/交易所/期，E1）
  → 重抓：源负载未变 → 入库去重跳过重解析 → 不补戳 → 不收敛（E6）
  → 与此同时现行 v6 目录对这些行的 ths 名全部有已批准映射（E5）
```

关键结论：**这是数据状态问题，不是代码 bug**——写入侧（`financial_statements_sync.py:1555`，lineage 非 None 即盖戳）与读取侧（`storage.py` `_numeric_fact_has_local_core_lineage`）当前均正确；错的是 5 月导入时的目录覆盖面留下的无戳存量，加上去重跳过使其无法自愈。

## 2. 迁移算法

对 `financial_numeric_facts_hot` 与 `financial_numeric_facts_history` 两表：

1. **筛选**：`raw_fact_json NOT LIKE '%local_core_mapping%'` 的行（M1 先做分表计数与按报告期分布，估算批量规模）；
2. **重算**：对每行以与导入路径**完全相同**的入参调用 `_local_core_mapping_lineage(statement_interface=raw_fact['akshare_statement_interface'], fact_name=fact_name, profile=行级 profile 解析结果, mapping_version=当前 MAPPING_VERSION)`——注意接口门仅认 `sina_report`/`ths_report`，其余接口（如 cninfo 官方行本就有戳）不属本迁移范围；
3. **补盖**：命中者将 `local_core_mapping` 写入 raw_fact dict，按**导入路径同一序列化函数**（`_json_text`，键序/ensure_ascii 一致）重写 `raw_fact_json`，并用**同一行哈希函数**（`_financial_numeric_fact_row_hash`）重算 `row_hash`；
4. **保持**：`updated_at` 及其余一切列原值不动（避免扰动"同期同名取最新"的胜负语义）；
5. **事务**：按报告期分批，每批单事务提交并打印 已补盖/未变化/无映射 计数；批前将受影响行写入备份表（`financial_numeric_facts_lineage_backfill_backup_202611`，含全列快照）；
6. **窗口**：与既有 financials.db 写者清单核对（同 readiness 覆盖索引 runbook 的清单），≥02:00 非周日窗口执行。

## 3. 语义与安全不变量

1. 只对"无戳且现行目录命中"的行改写——有戳行零触碰，未命中行零触碰（未来目录补条目后仍可再迁移，幂等可重跑）；
2. 盖戳依据 = 导入路径同一函数 + 当前同一目录 → 补盖结果与"当时目录就覆盖该字段"的导入完全等价；
3. 哈希口径与导入路径同源：`_financial_numeric_fact_row_hash` 对**解析后的 dict**（含 raw_fact_json 反序列化结果，排除 updated_at/ingestion_run_id 等列）计算——故补盖后重算的 hash 只要解析内容一致即与导入路径一致；raw_fact_json 的序列化字节差异**不会**引发 hash 不一致（自审核实：hash 输入是解析 dict 而非字符串），序列化仍按导入同款函数保持人工可读一致性，M2 单测钉死两种口径；
4. 回滚：从备份表按主键还原全列；
5. 迁移期间禁止其他 financials.db 写者（窗口清单核查）。

## 4. 验收

1. **探针**：000001.SZ 2024-03-31 `ready=True`（当前 False、5 缺）；
2. **计数**：迁移报告的补盖总数与 M1 筛选计数吻合；抽样 100 行人工核对戳内容与 `_local_core_mapping_lineage` 直算结果一致；
3. **收敛**：迁移后首个对账窗口，修复目标/交易所/期 从 ~2,800 降至 ~150 量级（真实缺数据标的），窗口不再出现 0.24 秒/标的空转批次；
4. **回归**：周末全量对账总时长回小时级；增量任务行为不变。

## 5. 风险

| 风险 | 缓解 |
|---|---|
| 序列化/哈希与导入路径不同源 | M2 单测：对同一样本行跑"导入路径构造 vs 迁移构造"逐字节比对 |
| profile 解析与导入时不一致 | 迁移用同一解析函数；解析不出的行归"无映射"类不补盖（保守） |
| 大批量 UPDATE 锁/时长 | 按报告期分批 + 窗口执行 + 批计数进度输出；M1 先实测单期批量时长 |
| 误盖（目录命中但语义不应准入） | 盖戳=目录已批准，准入语义即目录语义；备份表可整批还原 |

## 6. 移交登记（不在本 change 实施）

入库去重键不含 `mapping_version`：目录版本演进后"数据未变但准入语境已变"的行不会重解析——本次靠迁移兜底，结构性修复（去重键加入版本维度或版本切换时失效去重）属映射版本生命周期（V6 变更 owner 域），登记移交，避免 V7 重演。
