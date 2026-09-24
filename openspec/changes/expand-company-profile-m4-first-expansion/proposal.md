## Why

`owned_page_facts=v4` 已在 302132.SZ / 600000.SH 上通过独立核原文，4.1 数字门槛此次算过。`company_profile_operator_closure.v1` 仍登记 `first_expansion_gates_unmet`。产品 M4 可以提出第一片，但不能把 4.1 通过写成生产或规模质量。现有入口仍是同一 `company_profile_common_core` 的 `run` / `resume`；若无条件要求首次扩大快照，会阻断普通研究运行，或在快照落盘后永久重跑同一批样本。只绑 instrument / strata 也不足以冻结观察对象：更正年报或不同 `knowledge_cutoff` 会让 enqueue 处理另一份报告。

## What Changes

- 打开 M4 的**第一片**：在同一 published owner 内增加首次扩大模式（`inactive` / `active` / `completed`）。不新增 action，不另建执行链。
- 模式 `active` 期间，enqueue / resume 只处理已冻结 work；缺少或不匹配快照才拒绝。普通 `run` / `resume` 在 `inactive` 与 `completed` 下保持现行为。
- 首次扩大完成后进入 `completed`：不得自动开启下一轮，不得继续重跑冻结样本；重复 `run` 在 `active` 且已交付时幂等返回。
- enqueue 前持久化不可变 `company_profile_first_expansion_plan.v1`：绑定 `knowledge_cutoff`、选样所用 universe/registry 时点或身份，以及每家正式年报的 `asset_id`/`report_id`、`report_period`、`document_version`（按现有字段）。漂移拒绝，不得静默换新版本。
- 新 live-run / source-review 写独立快照，携带同一 plan 引用和报告引用；不得覆盖当前两家公司 7/7 的 v1 基线。
- 首次扩大仍只选两家。专用选样预留一个 `service` 名额，另一个名额按现有全局优先级从剩余候选填充。没有合法 `service` 候选则拒绝。普通 live-run 的 `_STRATUM_PRIORITY` 不变。
- 发布 `company_profile_operator_closure.v2` 并保留 v1。v2 使用执行后语义；不授权下一轮扩大、规模质量或生产。
- 不建行业包，不抽净息差 / 成本收入比 / 贷款结构，不预授权 v5。生产继续 `not_authorized`。

## Capabilities

### New Capabilities
- `company-profile-m4-first-expansion`: 同一入口上的首次扩大模式生命周期、冻结报告版本的先验计划、独立审查快照，以及 operator_closure v2 边界。

### Modified Capabilities
- `company-profile-a-share-delivery`: 4.1 通过后首次扩大仍须独立 change；普通 run/resume 不得被首次扩大快照阻断；当前两家样本基线不得覆盖或写成生产上线。

## Impact

- Owner 仍是 `research/company_profile/`：`operations.py`、`live_plan.py`、`live_run.py`、`source_review.py`、`operator_closure.py`、`candidate_registry.py`。
- 入口仍是 `company_profile_common_core` 的已发布动作；writer / reader / unique persist owner 不变。
- 本提案只供范围审核。**任务 1.1 未勾选前不得 `/opsx:apply`，不得改代码。**
- DCF、交易、旧 writer 仍未授权。
