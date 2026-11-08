## 1. M1 基线与等价性工具

- [ ] 1.1 建立单候选计时与调用分解工具：每候选端到端耗时、存储调用次数（连接/查询/取回行数），在 ~200 标的样本上产出基线报告（存 docs/development），确认 0.64s 的构成比例，校准 L4/L5 取舍。
- [ ] 1.2 冻结等价性样本集：≥200 标的 × 全对账报告期，必含银行/非银 profile、BSE、零数据标的（600036 类）、生命周期豁免分类；入库为 tests fixture。
- [ ] 1.3 实现双跑等价工具：同一样本集上新旧实现输出逐候选 diff（ready/missing_fields/facts），当前实现结果作为黄金基线存档。

## 2. M2 窄方法与调用点切换

- [ ] 2.1 storage 新增窄方法 `get_financial_core_fact_readiness_rows`：显式列投影 + canonical IN 过滤下推 + updated_at 最新语义保持；单测钉住排序与选择语义。
- [ ] 2.2 就绪调用点（readiness_for_target → storage 链）切换到窄方法；宽方法及全部其他调用方零改动。
- [ ] 2.3 就绪评估作用域内连接复用（每候选一次连接），消除 5ms×N 叠加；不建常驻池。
- [ ] 2.4 双跑等价 100% 一致后合入；全部既有 storage/对账/增量单测回归通过。

## 3. M3 覆盖索引与运维

- [ ] 3.1 在备份副本上实测 CREATE INDEX 时长与空间占用，写死进 runbook。
- [ ] 3.2 产出维护窗口核查清单（无 financials.db 写任务的时段）与 operator 脚本（显式执行、可中断、进度输出）。
- [ ] 3.3 授权窗口内生产建索引；EXPLAIN QUERY PLAN 验证 index-only scan；预留 DROP INDEX 回滚演练记录。

## 4. M4 验证与收口

- [ ] 4.1 副本库全量对账 dry-run 计时：扫描段 ≤75 分钟。
- [ ] 4.2 心跳与报告暴露每候选速率/阶段耗时（回退可见）。
- [ ] 4.3 生产验收：下一个周日 09:30 自然调度完成，总时长 ≤2.5h、报告正常；`max_runtime_seconds` 回调 18000。
- [ ] 4.4 若 L1-L4 后仍不达标：按 design §3 启动 L5（按标的批量化）并复测；仍不达标则升级再评估，不静默关 change。

## 5. 清理与交接

- [ ] 5.1 基线/等价报告与 runbook 归档 docs/development；openspec validate 通过；对照生产不变量自查后交 A 审核。
