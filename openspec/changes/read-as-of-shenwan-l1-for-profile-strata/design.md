## Context

`load_official_task_candidate_registry` 通过 `_storage_industry_lookup` 先调用 `get_industry_membership_as_of`。正式 provider 写入的 `classification_json` 只有股票代码、行业代码、计入日期和更新日期，没有申万一级名称，也没有 `classification.levels.sw_l1.industry_name`。一级名称在同版本活动 `industry_taxonomy` 的 `parent_code` 父链上，例如 `480301` → `480300` → `480000` `银行`。画像分类只读顶层 `sw_l1_name` 时，历史行会被当成没有一级名称，披露层落到 `other`。

2026-09-17 正式登记的可核披露层是 `{other: 5474, manufacturing: 1}`，`service_available_count=0`。首次扩大的两家预算因此不能占到 `service`。

## Goals / Non-Goals

**Goals:**

- 历史行提供 `taxonomy_system`、`taxonomy_version`、`official_industry_code`。
- 没有顶层 `sw_l1_name` 时，沿同版本活动 `industry_taxonomy.parent_code` 解析一级名称。
- 顶层 `sw_l1_name` 仍优先。
- 不读取 `classification.levels.sw_l1.industry_name`。
- 断链、缺节点或封闭表外名称保持 `other`。
- 不回退 cutoff 之后的当前 membership。

**Non-Goals:**

- 不改 `industry_classification_history` 的落盘格式，不回填历史列。
- 不硬编码证券，不使用 `unknown`，不扩大封闭申万一级表。
- 不把 cutoff 之后的当前 membership 填进历史时点。
- 不修改首次扩大的两家预算。本轮不冻结、不激活、不核原文，也不归档 `expand-company-profile-m4-first-expansion`。
- 不授权生产，不把 `scale_quality_claim_allowed` 改为 true。

## Decisions

1. **用历史行上的分类版本和官方代码走既有 taxonomy 父链，不改历史表。**
   as-of 行没有顶层 `sw_l1_name` 时，按 `taxonomy_system`、`taxonomy_version`、`official_industry_code` 在活动 `industry_taxonomy` 上沿 `parent_code` 找到一级节点名称。已有顶层名称时保持原值。不读取正式生产者不会写的 `levels.sw_l1`。断链或节点缺失则保持 `other`。不回写历史列。

2. **as-of 行存在时不回退当前 membership。**
   当前 membership 可能晚于 `knowledge_cutoff`。历史行存在就只用该行及其 taxonomy 父链；链上没有一级名称则披露层为 `other`。历史行存在但没有 cutoff 前生效的记录时，同样不使用当前 membership。只有该证券完全没有行业历史时，才允许读当前 membership。

3. **封闭表保持唯一分层规则。**
   解析出的名称交给现有 `assign_disclosure_form`。表外名称、空名称和缺失分类都仍是 `other`。

## Risks / Trade-offs

- [taxonomy 父链断开或版本对不上] → 披露层保持 `other`，不改用当前 membership，不硬编码证券。
- [父链解析后仍没有 service] → 说明该 cutoff 的活动 taxonomy 没有可映射的服务层名称。不改首次扩大预算，不归档首次扩大 change。
- [把当前行业填进历史时点] → as-of 行存在，或历史存在但 cutoff 前无生效行时，禁止这条回退。

## Migration Plan

1. 范围审核通过后才改读取。读取实现已经落地；本轮只把文档合同改成这条父链实现。
2. 用正式 provider 形状证明：`480301` 沿父链到 `银行` 后为 `finance`，服务行业代码同理到 `service`；断链或缺失仍是 `other`；更晚的当前 membership 不覆盖 as-of 行。
3. 回滚：去掉父链解析，历史表和封闭表保持原样。

## Open Questions

- 无。正式历史载荷只有股票代码、行业代码、计入日期和更新日期；一级名称只从同版本活动 taxonomy 父链解析。
