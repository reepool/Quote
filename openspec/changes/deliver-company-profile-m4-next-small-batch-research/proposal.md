## Why

M4 要验证现有 common-core 能否持续处理新增正式年报。四章受限查询和导出已经闭环，但下一轮新增报告还没有经过审核的计划。现行 first-expansion 规格要求下一轮另立 change，不能把 v8 的 8/9 再当成待修缺口。

## What Changes

- 在当前完整 identity `owned_page_facts=v8` + `material_input_facts=v1` 下，选择两份尚未完成的正式年报，覆盖服务和制造两种披露形态。
- 运行前冻结 knowledge cutoff、两份报告身份和版本，以及两家公司、50000 token 的既有预算。
- 复用 `company_profile_common_core` 的 preview、run、query、export，走通自动选证据、抽取与接受、持久化、查询、导出。先完成一种披露形态，再完成另一种。
- 本轮 plan、live-run、source-review 写入独立快照。旧轮 `completed`、plan pointer、closure v2，以及 v8 的 8/9 和后续 9/9，保持原值。
- 判据在观察前写明。召回由独立阅读原文重算，结果如实保留。研究用途，不授权生产或规模质量。

## Capabilities

### New Capabilities

- `deliver-company-profile-m4-next-small-batch-research`: 当前 identity 下两份新增正式年报的基础画像研究交付。

### Modified Capabilities

- 无。本卡不改 first-expansion 或 owned-page repair 的已归档要求。

## Impact

- 本卡只写范围。审核前不改 Python，不 enqueue，不 replay。
- 以后的实施仍由 `CompanyProfileTaskService` 编排，不新增 published action，不把 `first_expansion_mode` 从 `completed` 改回 `active`。
- 查询和导出继续用现有 query/export。不启动行业包、六章包或框架优化。
