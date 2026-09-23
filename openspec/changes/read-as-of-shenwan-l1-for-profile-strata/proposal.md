## Why

正式公司画像登记簿按 `knowledge_cutoff` 读取行业时，时点历史行已经带有申万一级名称，但画像登记只抄顶层 `sw_l1_name`。历史行没有这个顶层字段，分类仍被当成存在，披露层因此落到 `other`。2026-09-17 正式登记因此没有合法 `service` 样本，首次扩大不能在固定两家预算内激活。这是读取缺口，不是抽样预算或封闭申万表的问题。

## What Changes

- 正式 as-of 行业读取把该历史行里已经保存的申万一级名称投影到画像分类使用的 `sw_l1_name`。
- 名称只来自该 `as_of` 当日生效的历史载荷。没有已保存的一级名称时，披露层仍是 `other`。
- 不改用更新的当前 membership 填补历史行，避免把 cutoff 之后的分类当成当时可知。
- 不硬编码证券，不使用 `unknown`，不向封闭申万一级表增加行业名。
- 不修改 `expand-company-profile-m4-first-expansion` 的两家预算，不冻结、不激活、不核原文。

## Capabilities

### New Capabilities
- `company-profile-as-of-shenwan-l1-read`: 正式画像登记从 as-of 行业历史行读取已保存的申万一级名称，再交给现有封闭分层表。

### Modified Capabilities

## Impact

- 读取路径：`research/company_profile/candidate_registry.py` 的正式 industry lookup，以及 `research/storage.py` 的 `get_industry_membership_as_of` 所返回的历史载荷。
- 分层仍由 `research/company_profile/live_plan.py` 的封闭申万一级表决定。本 change 不改那张表。
- 本提案只供范围审核。任务 1.1 未勾选前不得 apply，不得改读取代码。
- 生产继续 `not_authorized`。`scale_quality_claim_allowed` 继续为 false。
