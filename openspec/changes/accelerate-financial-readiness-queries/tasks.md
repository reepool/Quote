## 1. M1 基线与等价性工具

- [x] 1.1 计时与分解工具（`scripts/research_financial_readiness_perf.py` baseline 子命令）+ 基线报告（docs/development/financial_readiness_query_baseline.md）。结论：查询层 38.1ms→3.9ms/候选（9.7 倍），0.64s 中的非查询部分为候选构建开销（profile/repair/审计加载）——查询侧 9.7 倍收益已足够覆盖 5 小时窗口缺口，L5 批量化不需要。
- [x] 1.2 冻结等价性样本集：220 标的 × 7 期（热表标的 + local-gap 零数据标的 + BSE；历史独有/同键重叠场景由单测 fixture 构造覆盖），入库 tests/unit/test_research/fixtures/financial_readiness_equivalence_sample.json。
- [x] 1.3 双跑等价工具（equivalence 子命令）：宽/窄实现逐候选 diff（ready/missing_fields/facts 语义投影），黄金基线即宽实现现网输出。

## 2. M2 窄方法与调用点切换

- [x] 2.1 窄方法 `get_financial_core_fact_readiness_rows`：显式列投影（含 raw_fact_json——lineage 语义必需，dimensions_json 全库零消费方不产出）+ canonical IN 下推 + updated_at 最新语义 + 历史合并；单测钉住排序/选择/历史合并/并列 updated_at 行为（tests/unit/test_research/test_financial_readiness_narrow.py）。
- [x] 2.2 切换经 storage 层 flag（`database_config.financial_readiness_narrow_rows`，门禁通过后置 true）——调用点与宽方法签名零改动，其他调用方零影响。
- [x] 2.3 连接复用：`pinned_financial_read_connection`（线程本地，路径不匹配回退、退出关闭）接入 `readiness_for_target`；映射注册查询按 (profile, approved, version) 记忆化（TTL 60s，空结果不缓存——V6 事故自愈保证）。
- [x] 2.4 等价性门禁：生产库冻结样本双跑 **1540 次评估，0 不一致**；对账/增量/修复/隔离 84+15 项单测回归通过。

## 3. M3 覆盖索引与运维（条件项：仅当 M4.1 实测不达标）

- [ ] 3.1 在备份副本上实测 CREATE INDEX 时长与空间占用，写死进 runbook（当前生产无现成 77GB 副本；估 20~60 分钟）。
- [x] 3.2 维护窗口核查清单 + operator 脚本 `scripts/financial_readiness_covering_index.py`（check/apply/verify/rollback）+ runbook docs/development/financial_readiness_covering_index_runbook.md（条件项声明与全部写者清单已入）。
- [ ] 3.3 授权窗口内生产建索引（条件触发后）；EXPLAIN 验证；DROP INDEX 回滚演练记录。

## 3. M3 覆盖索引与运维（条件项：仅当 M4.1 实测不达标）

- [ ] 3.1 在备份副本上实测 CREATE INDEX 时长与空间占用，写死进 runbook（当前生产无现成 77GB 副本；估 20~60 分钟）。
- [x] 3.2 产出维护窗口核查清单：枚举 `05_scheduler.json` 全部写 financials.db 任务（当前启用：增量 21:45/日上限 4h、对账周日 09:30/8h、l1 全量导入 manual_only；禁用任务按恢复场景一并列入），确认窗口内无在跑/可能补跑实例（≥02:00 凌晨时段）；operator 脚本 `scripts/financial_readiness_covering_index.py`（check/apply/verify/rollback）已交付，runbook 见 docs/development/financial_readiness_covering_index_runbook.md。
- [ ] 3.3 授权窗口内生产建索引（条件触发后）；EXPLAIN QUERY PLAN 验证；预留 DROP INDEX 回滚演练记录。

## 4. M4 验证与收口

- [x] 4.1 生产实测（2026-11-11，血统迁移后完整窗口）：扫描段 ~59 分钟（61,284 候选 @17.4/秒），总时长 5,039s 含应用与收尾——≤75 分钟达标；dry-run 副本计时不再必要。
- [x] 4.2 心跳与报告暴露每候选速率/阶段耗时：`reconciliation_scan` 检查点新增 `scan_rate_per_sec`，回退可见。
- [ ] 4.3 生产验收：下一个周日 09:30（2026-11-15）自然调度完成，总时长 ≤2.5h、报告正常；`max_runtime_seconds` 回调 18000。
- [ ] 4.4 若 L1-L4 后仍不达标：按 design §3 启动 L5（按标的批量化）并复测；仍不达标则升级再评估，不静默关 change。（查询侧 9.7 倍已达标，达标概率高；剩余变数为候选构建开销，1.1 已界定）

## 5. 清理与交接

- [x] 5.1 基线/等价报告（docs/development/financial_readiness_query_baseline.md）与索引 runbook（docs/development/financial_readiness_covering_index_runbook.md）归档。
- [x] 5.2 openspec validate 通过；对照生产不变量自查（宽方法/其他调用方零改动、写路径零改动、判定语义等价双跑证明、DDL 条件化）后交 A 审核。
