## 1. M1 测量与放大源定型

- [ ] 1.1 受控 instrumented run：临时库 fixture（数千候选 × 多报告期），对就绪度链路的每个读路径（`get_numeric_facts`、`get_industry_membership`、stale 清理状态查询等）计数调用次数与耗时，产出排行表写入 design 附录 A。
- [ ] 1.2 依实测确认/修正 D1–D3 优先级与窄投影列清单（就绪度判定所需标量列集合），固化到 design；若实测推翻假设，更新 design 决策并记录理由。

## 2. M2 run 级输入复用（D1/D3）

- [ ] 2.1 在就绪度服务层实现 run 作用域输入装配：按候选 `(instrument, period)` 分批预取、run 私有缓存、未命中单点回填；生命周期随 run 结束释放。
- [ ] 2.2 `_clear_stale_ready_local_gap_states` 改为复用 run 输入表判定，孤儿状态单点回读兜底；保留既有检查点让出与计数语义（FR-18 契约不动）。
- [ ] 2.3 等价性单测：run 复用路径与逐候选现状路径的就绪度结果、missing 集合、`maintenance_source_routing` 标注逐字段一致（含未命中回填、stale 清理命中/未命中分支）。

## 3. M3 投影收窄（D2）

- [ ] 3.1 `get_numeric_facts` 增加列级投影参数（默认全列全解码，既有调用方零改动）；就绪度路径改用窄投影，需载荷的合并分支按行回读。
- [ ] 3.2 单测：窄投影与全列判定结果等价；公共契约（默认调用）字段集合不变。

## 4. M4 性能合同与交付

- [ ] 4.1 性能合同测试入库：fixture 断言就绪度阶段查询次数上限（首版 ≤ 候选数×3）与耗时上限（按 CI 实测定值），防放大回归。
- [ ] 4.2 全量回归：financial disclosure 相关单测、scheduler/worker isolation 测试全绿；语法与静态检查通过。
- [ ] 4.3 真实夜间观察：部署重启后首个自然调度夜，记录增量任务运行时长与 stale 清理耗时（目标：从小时级回到分钟级），结果回填本 tasks 与 design 附录 A。
- [ ] 4.4 清理本 change 开发产物（临时 fixture、测量脚本按 §2.3 入 QuoteBak 并交接前清理）；openspec validate 通过；更新 framework_refactoring_program 进度矩阵并归档。
