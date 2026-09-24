## Context

`load_official_task_candidate_registry` 通过 `_storage_industry_lookup` 先调用 `get_industry_membership_as_of`。正式 provider 写入的 `classification_json` 只有股票代码、行业代码、计入日期和更新日期，没有 `classification.levels.sw_l1.industry_name`。一级名称在同版本 `industry_taxonomy` 父链上，例如 `480301` → `480300` → `480000` `银行`。画像分类只读顶层 `sw_l1_name` 时，历史行会被当成没有一级名称，披露层落到 `other`。

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

1. **用历史行上的分类版本和官方代码走既有 taxonomy 父链，不改历史表。**
   as-of 行没有顶层 `sw_l1_name` 时，按 `taxonomy_system`、`taxonomy_version`、`official_industry_code` 在 `industry_taxonomy` 上沿 `parent_code` 找到一级节点名称。已有顶层名称时保持原值。不读取正式生产者不会写的 `levels.sw_l1`。断链或节点缺失则保持 `other`。不回写历史列。

2. **as-of 行存在时不回退当前 membership。**
   当前 membership 可能晚于 `knowledge_cutoff`。历史行存在就只用该行；名称不在该行里则披露层为 `other`。历史行存在但没有 cutoff 前生效的记录时，同样不使用当前 membership。只有该证券完全没有行业历史时，才允许读当前 membership。

3. **封闭表保持唯一分层规则。**
   投影出的名称交给现有 `assign_disclosure_form`。表外名称、空名称和缺失分类都仍是 `other`。

## Risks / Trade-offs

- [taxonomy 父链断开或版本对不上] → 披露层保持 `other`，不改用当前 membership，不硬编码证券。
- [投影后仍没有 service] → 说明库存历史本身没有可映射的服务层名称。本 change 到此停止，不改首次扩大预算。
- [把当前行业填进历史时点] → as-of 行存在时禁止这条回退。

## Migration Plan

1. 范围审核通过后才改读取。
2. 用夹具证明：历史行只有嵌套一级名称时，封闭表能分出 `service`；没有该名称时仍是 `other`；更晚的当前 membership 不覆盖 as-of 行。
3. 回滚：去掉投影，历史表和封闭表保持原样。

## Open Questions

- 无。实现时若正式行的载荷形状与现有写入器不一致，先记录该形状，不得直接改封闭表或点名证券。
