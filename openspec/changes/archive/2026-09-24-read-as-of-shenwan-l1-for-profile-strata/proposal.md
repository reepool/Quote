## Why

正式公司画像登记簿按 `knowledge_cutoff` 读取行业时，as-of 历史行只有 `taxonomy_system`、`taxonomy_version` 和 `official_industry_code`。正式 `classification_json` 不保存申万一级名称，也没有 `classification.levels.sw_l1.industry_name`。画像登记若只抄顶层 `sw_l1_name`，分类仍被当成存在，披露层落到 `other`。2026-09-17 正式登记因此没有合法 `service` 样本，首次扩大不能在固定两家预算内激活。这是读取缺口，不是抽样预算或封闭申万表的问题。

## What Changes

- 没有顶层 `sw_l1_name` 时，用该历史行的 `taxonomy_system`、`taxonomy_version`、`official_industry_code`，沿同版本活动 `industry_taxonomy.parent_code` 父链解析一级名称。
- 顶层 `sw_l1_name` 仍优先保留。
- 不读取 `classification.levels.sw_l1.industry_name`。
- 父链断开、节点缺失或名称不在封闭表内时，披露层仍是 `other`。
- 不回退 cutoff 之后的当前 membership，也不把解析出的名称回写历史表。
- 不硬编码证券，不使用 `unknown`，不向封闭申万一级表增加行业名。
- 不修改 `expand-company-profile-m4-first-expansion` 的两家预算，不冻结、不激活、不核原文，不归档首次扩大 change。

## Capabilities

### New Capabilities
- `company-profile-as-of-shenwan-l1-read`: 正式画像登记用 as-of 历史行的分类版本和官方行业代码，沿活动 taxonomy 父链解析申万一级名称，再交给现有封闭分层表。

### Modified Capabilities

## Impact

- 读取路径：`research/company_profile/candidate_registry.py` 的正式 industry lookup，以及 `research/storage.py` 上沿 `industry_taxonomy` 父链解析一级名称的读取。
- 分层仍由 `research/company_profile/live_plan.py` 的封闭申万一级表决定。本 change 不改那张表。
- 本轮只对齐文档合同，不改读取代码。
- 生产继续 `not_authorized`。`scale_quality_claim_allowed` 继续为 false。
