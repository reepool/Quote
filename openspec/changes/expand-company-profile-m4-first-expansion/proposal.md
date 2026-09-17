## Why

`owned_page_facts=v4` 已在 302132.SZ / 600000.SH 上通过独立核原文，4.1 数字门槛此次算过（recall 7/7、accuracy 7/7、critical numeric 0、2 份报告、2 个 occupied strata）。`company_profile_operator_closure.v1` 仍登记 `first_expansion_gates_unmet`，与已记录的 `expansion_gates_met=true` 不一致。产品 M4 是「按真实缺口增强关键指标，扩大吞吐」；现在可以提出**第一片**，但不能把 4.1 通过写成生产授权或规模质量达标。

## What Changes

- 打开 M4 的**第一片**：在观察新结果前，先写一份新的 `company_profile_live_plan.v1` 扩大运行计划；至少新占一个当前核原文未占用的披露形态层（`service`）。
- 按该计划独立核原文；4.1 数字不得预设为再次 7/7。`scale_quality_claim_allowed` 继续为 false。
- 更新 `company_profile_operator_closure.v1` 待办：退役过时的 `first_expansion_gates_unmet`，改记「当前样本 4.1 已过，首次扩大仍须本 change 审过才执行」。
- 本片只做吞吐扩大与待办口径；**不**实现制造/材料生产增强，**不**一次建齐银行/服务/TMT 行业包，**不**抽取 v4 已排除的净息差、成本收入比、贷款结构。
- 若新样本核原文后出现可复用解释缺口，必须另立案、另发 identity，不得在本片预授权。
- 生产继续 `not_authorized`。v1–v4 JSON 保留。缺年报资产继续留在分母。

## Capabilities

### New Capabilities
- `company-profile-m4-first-expansion`: M4 第一片的先验扩大计划、新增 occupied stratum 独立核原文、operator_closure 待办改写，以及「本片不是生产、不是规模质量、不是全行业包」边界。

### Modified Capabilities
- `company-profile-a-share-delivery`: 明确 4.1 数字门槛已过后，首次扩大仍须独立 change 与先验计划，不得把当前两家样本写成规模质量或生产上线。

## Impact

- Owner 仍是 `research/company_profile/`：`live_plan.py`、`source_review.py`、`operator_closure.py`、`operations.py`。
- 入口仍是 `company_profile_common_core`；writer / reader / unique persist owner 不变。
- 本提案只供范围审核。**未审过范围不得 `/opsx:apply`，不得改代码。**
- DCF、交易、旧 writer 仍未授权。
