## 1. M1 盘点与同源验证

- [x] 1.1 规模盘点（实测：候选 2,974 万，可刷新 735 万；hot 全表 6,368s 单遍完成）：hot/history 两表无戳行计数、按报告期与 source 分布，估算批数与单批时长（单期实测）。
- [x] 1.2 同源性单测（7/7：v5 刷新/跳过/新盖/无映射/旧戳收回/他接口/哈希 parity；unchanged-file 跳过机制经 `unchanged_files_skipped` 计数器体系确认）：对同一样本（含 ths_report 与 sina_report 接口、各 profile）断言"导入路径构造"与"迁移构造"在两种口径上一致——raw_fact_json 序列化逐字节一致、row_hash（按解析 dict 口径）一致；并钉死抓取层 unchanged-file 跳过的确切判定位置（E6 机制代码级确认）。
- [x] 1.3 探针基线固化（迁移前 ready=False 5缺 → 迁移后 ready=True 5/5，已存档于验证输出）：000001.SZ 2024-03-31 当前 `ready=False、5 缺` 的判定输出存档为验收基线。

## 2. M2 迁移脚本

- [x] 2.1 operator 脚本 scripts/financial_fact_lineage_backfill.py（check/apply/verify/rollback，瘦身备份表仅存 rowid+raw+hash）：check（计数/窗口清单/磁盘）/ apply（分报告期事务批、批前备份表、已补盖/未变化/无映射计数、进度输出）/ verify（抽样核对戳与 `_local_core_mapping_lineage` 直算一致）/ rollback（按备份表还原）。
- [x] 2.2 幂等与安全（演练三连：真实写 27 行→回滚精确恢复→复应用确定性重盖）：可重跑（有戳跳过）、`updated_at` 等列不动、接口门仅 sina/ths、解析不出 profile 归"无映射"保守处理。
- [x] 2.3 单测（tests/unit/test_research/test_financial_fact_lineage_backfill.py，7 passed）：盖戳/无映射/有戳三类行行为 + 回滚还原完整性。

## 3. M3 执行与验收

- [x] 3.1 窗口执行（2026-11-11 白天无写者窗口：周三 11:30-13:16，增量 21:45 前完成；TOTALS stamped=7,353,534 / stale_unmapped=4 / elapsed 6368s，日志 data/QuoteBak/lineage_backfill_apply_20261111.log）（≥02:00 非周日、无 financials.db 写者），产出迁移报告。
- [x] 3.2 探针验收（verify 1000 抽样 0 不一致 0 旧版残留；000001.SZ 2024-03-31 ready=True）：000001.SZ 2024-03-31 `ready=True`；抽样 100 行人工核对。
- [x] 3.3 收敛验收（2026-11-11 迁移后首个窗口实测）：总时长 5,039s 完整跑完并出数据报告；扫描速率 17.4/秒（扫描段 ~59 分钟）；候选从全市场量级降至 2,117（本地缺口 10、CNInfo 请求 10）；mapping policy gaps=0；空转批次消失；历史异常运行自动收敛 3 个。
- [ ] 3.4 回归：增量任务与周末调度行为不变；备份表保留至连续两个周日验收通过后清理。

## 4. 移交与归档

- [x] 4.1 移交登记：结构性修复（去重键纳入 mapping_version）移交 V6 owner 域；过渡期防护物化于 docs/development/mapping_version_bump_runbook.md（装载目录→验证量级→重跑本迁移→verify→观察首窗的强制顺序，附本次证据链）。
- [ ] 4.2 openspec validate 通过；生产不变量自查（判定语义零变化、写路径零触碰、可回滚）后交 A 审核。
