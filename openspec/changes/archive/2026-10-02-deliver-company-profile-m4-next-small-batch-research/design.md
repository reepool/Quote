## Context

权威入口仍是 `company_profile_common_core`。CLI、Scheduler 和 Telegram 只转发到 `CompanyProfileTaskService`。当前处理身份是 `default_processing_identity()`：`owned_page_facts=v8` 且 `material_input_facts=v1`。

M4 第一轮样本是 `600004.SH` / service 与 `600006.SH` / manufacturing。该轮 closure v2 已是 `completed`。v8-only 的 source-review 是历史观察：recall 8/9、accuracy 8/8、critical numeric errors 0、`expansion_gates_met=false`。同一对公司的后续 successor 已是 9/9，不能回写 v8，也不能把 8/9 再当成这一轮要修的缺口。

`company_profile_first_expansion_mode.v1` 处于 `completed` 时，普通 run 不再限定那两家，也不会自动开始下一轮。下一轮必须是这份经过审核的 change。

## Goals / Non-Goals

**Goals:**

- 选出两份当前 identity 尚未完成的正式年报，一种服务披露，一种制造披露。
- 在 enqueue 前冻结 cutoff、报告身份、文档版本和预算。
- 用现有选证据、抽取、接受、持久化、query、export 跑通两家。先一家，再另一种披露形态。
- 重复提交复用已完成 scope。一家失败不阻塞另一家。
- 三维骨架有实质回答或明确缺项。原文明示的商品角色能查询、能导出。

**Non-Goals:**

- 不重跑、不改写 `600004.SH` 与 `600006.SH` 的历史观察，尤其不把 8/9 修成 9/9。
- 不改 `first_expansion_mode`、plan pointer、closure v2。
- 不新增 published action，不建行业包、六章包，不做框架优化。
- 不授权生产、规模质量、DCF 或交易。本卡不预填召回或准确率。

## Decisions

1. 应用 owner 仍是 `CompanyProfileTaskService`。选样、运行、查询、导出都走已发布动作。允许这个 owner 做本轮最小适配：消费冻结计划、核对有效年报是否偏离冻结引用、把两次运行绑定成一份完整观察，并读写 `reports/m4_next_small_batch/<plan_id>/` 下的独立快照。不新增 published action，也不把 `first_expansion_mode` 改回 `active`。
2. 本轮快照放在 checkpoint `reports/m4_next_small_batch/<plan_id>/`。其中包含 plan、live-run、source-review。不得写入 `reports/company_profile_first_expansion_plan.v1.json`、`reports/company_profile_first_expansion_mode.v1.json` 或 `company_profile_operator_closure.v2.json`。
3. 选样规则：正式年报、本地可读、当前 identity 下还没有已交付运行记录。一家必须是服务披露，另一家必须是制造披露。按交易所 SSE、SZSE、BSE，所内按 `instrument_id` 升序取第一家尚未完成的服务公司和第一家尚未完成的制造公司。不得写死证券代码。若任一类没有合法候选，拒绝记录计划，不得回退到 `600004.SH` 或 `600006.SH`。
4. 两家公司共用一个累计预算 `token_budget=50000`，`max_companies_this_round=2`。第一次运行从 50000 开始。失败调用计入已耗用。已完成 scope 的复用不新增耗用。第二次运行必须显式传入剩余预算，不能再各自拿满 50000。knowledge cutoff、每份报告的 `asset_id`、`report_id`、`report_period`、`document_version` 在第一次成功写入 plan 时固定。普通 run 重新取得有效年报后，必须与这些冻结引用比较；任一字段漂移就拒绝该证券，不得悄悄换版本。
5. 现有 live-run 只记录本次调用的证券。本轮两次运行必须累加到同一份观察：第二次写入时保留第一家的结果，两家都出现在这份观察里。source-review 只读这份完整观察，不读第二次调用单独产生的半份记录。运行顺序仍是先服务公司，再制造公司。一家失败保留在观察里，不阻塞另一家。
6. 观察前判据：每家公司的主营、产品/服务、收入来源必须各有一条带证据的实质回答，或一条带 `missing_reason` 的明确缺项。三个维度都空且没有缺项，不算通过。原文明示的商品销售、原料投入、能源消耗或套保角色必须出现在 query 和 export 中，并带期间和来源。完整检查后没有明示关联，显示未发现明示关联，不得写成零暴露。未检查保持未评估。
7. 召回分母由实施时独立阅读这两份年报确定，覆盖上述三维重要披露和明示商品角色。不得用模型自己列出的字段当全部已披露信息，不得预填 8/9 或 9/9。准确率只评价已交付事实。critical numeric errors 按这次阅读计数。达不到判据也保留观察，不得改写成通过。`scale_quality_claim_allowed` 保持 false，`production_authorization` 保持 `not_authorized`。

## Risks / Trade-offs

- [把 600004/600006 再跑一遍补 8/9] → 这两家已有 v8 历史值和后续 9/9，本轮候选排除它们。
- [改 completed 模式来启动下一轮] → 旧模式、pointer 和 closure 不写。本轮只用新目录。
- [一家失败导致两家都没有结果] → 失败留在完整观察里并计入预算，另一家用剩余预算继续。
- [两次 run 各记各的证券] → 第二次写入必须并入第一家，source-review 只读合并后的观察。
- [查询成功被当成规模质量或生产] → 响应和快照都保持未授权。

## Migration Plan

无部署。1.1 通过前只有范围文档。通过后先做 owner 的最小适配和定向测试，再冻结计划、跑服务公司、跑制造公司、读完整观察做 source-review。失败不回滚旧轮快照。不能从本文件直接 enqueue。

## Open Questions

独立范围审核已接受修订后的合同。下一张才是 2.1：现有 owner 的最小适配和定向测试。本卡不选择证券，不写入 cutoff 或报告版本，也不预填观察分数。
