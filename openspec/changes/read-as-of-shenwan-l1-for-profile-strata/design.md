## Context

`load_official_task_candidate_registry` 通过 `_storage_industry_lookup` 先调用 `get_industry_membership_as_of`。历史行有 `official_industry_code`，以及 `classification.levels.sw_l1.industry_name`。画像分类 `_classification` 只读取顶层 `sw_l1_name`。历史行因此被当成“分类存在但没有一级名称”，`assign_disclosure_form` 返回 `other`。当前 membership 虽有顶层 `sw_l1_name`，只要 as-of 行存在就不会被使用。

2026-09-17 正式登记的可核披露层是 `{other: 5474, manufacturing: 1}`，`service_available_count=0`。首次扩大的两家预算因此不能占到 `service`。

## Goals / Non-Goals

**Goals:**

- 让正式画像登记使用 as-of 历史行里已经保存的申万一级名称。
- 名称缺失时保持 `other`。
- 分层仍只走现有封闭申万一级表。

**Non-Goals:**

- 不改 `industry_classification_history` 的落盘格式，不回填历史列。
- 不硬编码证券，不使用 `unknown`，不扩大封闭申万一级表。
- 不把 cutoff 之后的当前 membership 填进历史时点。
- 不修改首次扩大的两家预算，不冻结、不激活、不核原文，不归档该 change。
- 不授权生产，不把 `scale_quality_claim_allowed` 改为 true。

## Decisions

1. **在画像读取处投影已保存名称，不改历史表。**
   as-of 行没有顶层 `sw_l1_name` 时，从该行 `classification.levels.sw_l1.industry_name` 投影。已有顶层名称时保持原值。备选是回写历史列，会扩大成数据迁移，超出这个读取缺口。

2. **as-of 行存在时不回退当前 membership。**
   当前 membership 可能晚于 `knowledge_cutoff`。历史行存在就只用该行；名称不在该行里则披露层为 `other`。历史行存在但没有 cutoff 前生效的记录时，同样不使用当前 membership。只有该证券完全没有行业历史时，才允许读当前 membership。

3. **封闭表保持唯一分层规则。**
   投影出的名称交给现有 `assign_disclosure_form`。表外名称、空名称和缺失分类都仍是 `other`。

## Risks / Trade-offs

- [历史载荷不是 `levels.sw_l1.industry_name`] → 只认这个已写入形状；认不出就保持 `other`，不另造名称。
- [投影后仍没有 service] → 说明库存历史本身没有可映射的服务层名称。本 change 到此停止，不改首次扩大预算。
- [把当前行业填进历史时点] → as-of 行存在时禁止这条回退。

## Migration Plan

1. 范围审核通过后才改读取。
2. 用夹具证明：历史行只有嵌套一级名称时，封闭表能分出 `service`；没有该名称时仍是 `other`；更晚的当前 membership 不覆盖 as-of 行。
3. 回滚：去掉投影，历史表和封闭表保持原样。

## Open Questions

- 无。实现时若正式行的载荷形状与现有写入器不一致，先记录该形状，不得直接改封闭表或点名证券。
