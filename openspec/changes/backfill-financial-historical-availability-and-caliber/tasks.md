## 1. M1 本地快修（无网络，可随时执行）

- [ ] 1.1 口径对账量化脚本：全期 `financial_facts` vs 数值层（hot+history）按 revenue / net_income / total_assets / equity_total 四字段 0.5% 容差全量比对（临时聚合表模式，产物按 §2.3 入 QuoteBak），报告（分期×分字段差异率、典型样本、科目口径归类）落盘 `docs/development/financial_historical_caliber_comparison_<yyyymmdd>.md`（日期以实际运行日命名）。
- [ ] 1.2 40 行 `total_assets` 本地回填：仅数值层有值的 NULL 行（equity 回填同模式，只填 NULL、不动 hash/updated_at），回填后更新 1.1 报告附录记录剩余 249 行清单。
- [ ] 1.3 金刚光伏(300093) 定向 reconciliation（`target_symbols=['300093']`，安静窗口），确认其 2026-06-30 获得事件化处置。
- [ ] 1.4 `financial_available_date_backfill` 证据加载器扩展：新增 `financial_statements_raw` / `financial_filing_versions` 本地披露时间源（先核实两表是否携带可用日期字段，无则记录跳过理由）；dry-run 试跑产出本地证据可覆盖行数。
- [ ] 1.5 M1 单测：证据加载器新源的取最早值与空值跳过行为；回填幂等（重复执行已填值不变）。

## 2. M2 历史披露日勘察（网络，安静窗口，调度侧执行）

- [ ] 2.1 勘察入口实现：复用公告采集服务按市场×季度窗口扫描（窗口日期边界 = 披露季起点至法定截止日 + 延期尾窗 60 天，见 design D1；类别限定定期报告，标题推期），写 `announcement_audit`（purpose_key=`historical_disclosure_date_survey`）；实现访问栈完整性自检（headed Chrome 不可用即拒绝启动）与窗口断点；提供调度侧运行入口（manual_only 调度任务或 worker 入口，必要时新增 `config/05_scheduler.json` 条目）。
- [ ] 2.2 执行约束落地：默认请求间隔 ≥ 1s（可配置）、连续 403/限流停窗、单窗口请求量上限；入口帮助文本注明独立裸进程禁用（2026-11-03 403 事故记录）。
- [ ] 2.3 单测：事件状态零触碰（勘察前后 `financial_disclosure_event_state` 快照一致）；断点续跑不重复请求；标题推期仅接受定期报告类别。
- [ ] 2.4 生产执行：十个季度 × 三市场分窗跑完（安静窗口，避开 21:45/周日 09:30），产出 `(instrument, period, announcement_time)` 覆盖率统计写入 M3 验收报告输入。

## 3. M3 回填与验收（本地为主）

- [ ] 3.1 证据加载器接入 `announcement_audit` 勘察源；回填模块全量运行（本地证据 + 勘察证据），dry-run 先行核对将填行数与样本日期。
- [ ] 3.2 法定截止日兜底：仍缺行按报告期类型填法定截止日到 `data_available_date`（`publish_date` 保持 NULL），lineage `quality='statutory_deadline'`；兜底实现复用回填模块写入路径，不另写平行 UPDATE。
- [ ] 3.3 验收验证一：全部历史缺失行（2023Q4–2026Q1 十季度约 5.4 万行 + 2026H1 残余 388 行 + Q3 残余 2 行）的 `data_available_date` 处置覆盖率 ≥ 99.9%，按 observed / surveyed / statutory / 仍缺 四类给占比；仍缺行逐行说明（预期仅退市/特殊行）。
- [ ] 3.4 验收验证二：PIT 硬性边界校验（`data_available_date ≥ 报告期期末`；兜底行 == 法定截止日），违例阻断验收；实测日晚于法定截止日超 60 天的行进异常清单人工复核（延期披露属真实情况，不计违例）。
- [ ] 3.5 验收验证三：随机 20 家 observed/surveyed 类抽样与 cninfo 公告日人工核对，一致率 ≥ 90%，不一致样本逐家说明。
- [ ] 3.6 验收验证四：`mapping_catalog_empty` 陈旧 reason 清零——首个周日全量 reconciliation 覆盖滚动窗口内（2024Q1 起）各期；窗口外 2023-12-31 期 44 条另跑一次定向 reconciliation（`report_periods=['2023-12-31']`，安静窗口）刷新；SQL 验证全部期间归零或逐条说明，结果记入验收报告。
- [ ] 3.7 249 行无源 total_assets：检查 `financial_statements_raw` 可重解析性，结论（重解析清单或接受记录）写入验收报告。
- [ ] 3.8 口径量化报告（1.1 产物）提交 owner 决策：是否按现行映射重派生历史行；重派生未经决策不执行，如获批准另行排期（不在本 change 内完成重派生）。
- [ ] 3.9 宽表 2023Q4 前边界声明：在需求文档（`financial_equity_semantics_and_balance_identity_requirements.md` 或验收报告）记录"宽表历史自 2023Q4 起，更早历史见数值层"。

## 4. M4 回归、清理与归档

- [ ] 4.1 回归：financial availability/disclosure 相关单测全集 + 新增单测全绿；静态检查通过；生产夜间任务（21:45 增量、周日 reconciliation）首个周期运行正常。
- [ ] 4.2 验收报告落盘 `docs/development/financial_historical_availability_repair_acceptance.md`（覆盖率、PIT 校验、抽样核对、mapping_empty 归零、边界声明、清理记录），作为本 change 唯一权威验收证据。
- [ ] 4.3 清理：勘察/量化/回填的一次性脚本与中间产物按 §2.3 清理或归入 QuoteBak 对应任务子目录；确认无 `/tmp` 残留。
- [ ] 4.4 `openspec validate` 通过；按 A/B 流程交接审核；完成后归档本 change。
