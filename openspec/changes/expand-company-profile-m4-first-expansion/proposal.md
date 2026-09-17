## Why

`owned_page_facts=v4` 已在 302132.SZ / 600000.SH 上通过独立核原文，4.1 数字门槛此次算过（recall 7/7、accuracy 7/7、critical numeric 0、2 份报告、2 个 occupied strata）。`company_profile_operator_closure.v1` 仍登记 `first_expansion_gates_unmet`，与已记录的 `expansion_gates_met=true` 不一致。产品 M4 是「按真实缺口增强关键指标，扩大吞吐」；现在可以提出**第一片**，但不能把 4.1 通过写成生产授权或规模质量达标。当前 live plan 在 enqueue 前不可持久证明预选样本，且 live-run / source-review / operator-closure 都覆盖固定 v1 文件，直接改 v1 待办会毁掉已落盘的合法报告。

## What Changes

- 打开 M4 的**第一片**：在 enqueue / drain 之前，持久化一份不可变的首次扩大计划及预选样本；计划必须绑定 instrument IDs、strata，并包含 `service`。
- 新 live-run / source-review 写入独立快照，并引用同一 plan ID 或内容 hash；不得覆盖当前两家公司 7/7 的权威 v1 报告。
- 按该计划独立核原文；4.1 数字不得预设为再次 7/7。`scale_quality_claim_allowed` 继续为 false。
- 发布 `company_profile_operator_closure.v2` 并保留可读取的 v1；不得在同一 v1 下改唯一合法 backlog。v2 替换项使用执行后稳定语义：首次扩大已按受审计划执行；新 source-review 是本轮权威结果；本 change 不授权下一轮扩大、规模质量声明或生产。
- 本片只做吞吐扩大与待办口径；**不**实现制造/材料生产增强，**不**一次建齐银行/服务/TMT 行业包，**不**抽取 v4 已排除的净息差、成本收入比、贷款结构。
- 若新样本核原文后出现可复用解释缺口，必须另立案、另发 identity，不得在本片预授权。
- 生产继续 `not_authorized`。v1–v4 工作 JSON 与现有两家公司审查基线保留。缺年报资产继续留在分母。

## Capabilities

### New Capabilities
- `company-profile-m4-first-expansion`: M4 第一片的不可变先验计划与预选样本、独立审查快照、operator_closure v2 待办，以及「本片不是生产、不是规模质量、不是全行业包」边界。

### Modified Capabilities
- `company-profile-a-share-delivery`: 明确 4.1 数字门槛已过后，首次扩大仍须独立 change、不可变先验计划和独立快照，不得覆盖当前两家样本基线，也不得把该基线写成规模质量或生产上线。

## Impact

- Owner 仍是 `research/company_profile/`：`live_plan.py`、`live_run.py`、`source_review.py`、`operator_closure.py`、`operations.py`。
- 入口仍是 `company_profile_common_core`；writer / reader / unique persist owner 不变。
- 本提案只供范围审核。**任务 1.1 未勾选前不得 `/opsx:apply`，不得改代码。**
- DCF、交易、旧 writer 仍未授权。
