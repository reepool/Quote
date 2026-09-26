## Why

首次扩大的正式复核是零交付。缺口集中在两处可复用的 common-core 规则：已拥有标题下的“公司主要从事……”进不了 overview 投影，而“主营业务分析 / 收入和成本分析”里的正式收入构成表没有进入 segment/revenue 投影。v4 身份上的失败观察已经关闭，不能靠重开首次扩大或临时打开 LLM 来掩盖。

## What Changes

- 当前 successor 是 `owned_page_facts=v8`。lineage 是 v5 首次修复、v6 补标题和单位、v7 修正 Activity 但把组合表组单位缩得过窄、v8 恢复正式组合表组的单位作用域。不得覆盖或删除 v1–v7 work JSON。v6 的 8/8 和 v7 的 8/8 source-review 都保留，且都已被后续 Review 否决。
- Overview：在已有 owned 标题下，正文使用“公司主要从事……”时，进入现有 principal-business / products-services 投影。保留原文主体和证据语义。不得按证券、页码或公司硬编码。
- MD&A 收入构成：把“主营业务分析 / 收入和成本分析”下的正式收入构成表送进现有 segment/revenue 投影。保留表格主体、单位、分部维度和来源绑定。后部“分部报告 / 分部信息”不得成为唯一合法路径。不得用跨表、金额相等或无标题匹配放宽路由。
- 不实现机场吞吐量、补偿、汽车产销量、材料成本或原材料行业包。不投影净息差、成本收入比或贷款结构。
- 不改全市场分母、taxonomy、普通 live-run 选样或 publication scope。不启动下一轮扩大，不授权规模质量或生产，不调用旧 writer、DCF 或交易路径。

## Capabilities

### New Capabilities

- `common-core-owned-page-gap-repair`: 当前 successor `owned_page_facts=v8`。v5、v6、v7 观察保留。只覆盖 common-core overview 与收入构成，不扩展原材料行业包。

### Modified Capabilities

## Impact

- Owner 仍是 `research/company_profile/` 的现有 common-core 选择与投影，主要落点是 `core_evidence_selection.py`。不新建 PDF 框架或解析器。
- 正式 600004.SH 与 600006.SH 年报只在实现审核通过后做 successor replay。4.1 不预设通过。
- 本提案只供范围审核。审核通过前不得改业务代码，不得 replay。
- 生产继续 `not_authorized`。
