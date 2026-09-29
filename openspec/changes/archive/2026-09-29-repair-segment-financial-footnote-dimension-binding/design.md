## Context

`replay/20260929.2` 已通过控制面审核，独立 source-review 未通过。source recall 102/132，source accuracy 114/142，critical numeric errors 0，`expansion_gates_met=false`。300750 与 603659 的当期单元格已经全量命中。缺口集中在两张附注表：`920015.BJ` 物理页 139 的「地区分部」和「业务分部」被写成默认「报告分部」；`302132.SZ` 物理页 178 的印刷标题「报告分部的财务信息」被写成泛化的「报告分部」。这些记录的金额仍在正文中，来源维度与印刷栏目不一致，因此不能计入召回或准确率。

原 `replay/20260928` 与本次 `.2` 的制品都已冻结。原分部财务 change 的 3.2 继续保持未通过。

## Goals / Non-Goals

**Goals:**

- 只在现有 `extract_segment_financials` 路径上，把附注表的来源维度绑到该表自己的印刷栏目。
- 页 139 的地区表与业务表分别保留，金额相同的合计行、普通行和抵消列不合并。
- 页 178 使用该页正式标题，分部间抵销继续使用 `consolidation_adjustment`。
- 列角色修复、十个已补交单元格和已核对的空值边界保持不变。
- successor 使用新计划和本 change 下的新隔离目录。

**Non-Goals:**

- 不新增发行人，不重选样，不复核 regime。
- 不创建通用表格框架，不扩展其他章节或报告。
- 不改 `replay/20260929.2` 的 enqueue、run、result，也不改 `replay/20260928` 的四份制品。
- 不改原分部财务 change 的 3.2，不把任一失败观察改写成通过。
- 不在范围文档中预填新的 recall、accuracy、critical numeric errors 或 gate。
- 不改 common-core identity、publication、closure、mode、checkpoint 或生产授权。

## Decisions

1. 来源维度取自该表印刷栏目，进入下一张表时重置上一张表的栏目状态。带编号的标题如果本身就是栏目名，先绑定这个栏目名，再重置上一张表的列状态。没有印刷栏目时，不得用泛化的「报告分部」填上该页金额。
2. 页 139 的两个印刷栏目分别是「地区分部」和「业务分部」。记录身份包含栏目、行名和字段。相同金额留在各自栏目中，合计行与抵消列也按栏目分开。
3. 页 178 的来源维度使用该页正式标题「报告分部的财务信息」。分部间抵销保持 `consolidation_adjustment`。空抵消金额继续是 `unclear`，不写成 0。
4. 规则只识别页面上的栏目标题。运行时不写死证券代码、页码、公司名或产品名。样本 bindings 可以继续列出已批准报告和已绑定页。
5. successor 默认计划定为 `manufacturing_materials_stage4_segment_financials.2026-09-29.3`。`.2026-09-29.2` 与 `.2026-09-28.1` 只保留历史读取。新 replay 写入本 change 的 `replay/20260929.3`。
6. 1.1 通过前不实现。实现与定向测试通过后，才用 `.3` 做一次受控 replay，再做独立 source-review。分母仍从四份年报重读，不能用 bundle 条数倒推，也不能复用 102/132、114/142、122/132 或 129/133。

## Risks / Trade-offs

- [编号标题被当成新表后丢掉栏目名] → 栏目名先绑定，再重置上一张表的列角色。
- [相同合计金额被并成一行] → 身份包含印刷栏目，栏目不同则不合并。
- [修复栏目时把页 25 的列角色或十个单元格带回退] → 定向测试同时锁住这些已正确结果和空值边界。
- [新 replay 覆盖旧观察] → 新目录与新计划分开；旧制品只做哈希核对。

## Outcome

`replay/20260929.3` 的独立 source-review 已通过。source recall 132/132，source accuracy 144/144，critical numeric errors 0，`expansion_gates_met=true`。source-review SHA-256 是 `4446fb7619f39b49e865bc563cf7673b98d2288d481c0b229de67394ee9c3c74`。绑定链是 enqueue `0f2ebfa087f8aa4327d15d4e0366af568bcd06a41ab923ba595899ec4e3cdd9b`、run `2ab053d1b3e80b52351ea7963e45f323b97a2706971514c99486eaefd3234d1f`、result `92ec1e2a8be5d4a6ee8bc5b492d7dc09a26dbc84bfbd0ad592ca5aab6cdc1eeb`、run id `stage4-segment-financials-20260929.3`、计划 `manufacturing_materials_stage4_segment_financials.2026-09-29.3`、bundle `segment-financial-stage4-segment-financials-20260929.3`。213 条 fact 和 144 条 Measurement 只是 bundle 规模，不是分母。这个 true 只覆盖四份冻结 2025 年报、该计划和 `extract_segment_financials`。`scale_quality_claim_allowed` 仍为 false，`production_authorization` 仍为 `not_authorized`。`replay/20260929.2` 的 102/132 与 `replay/20260928` 的 122/132 继续作为失败观察保留。
