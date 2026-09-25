## Context

v4 首次扩大已经以失败观察关闭：recall 0/9，accuracy unassessed，`expansion_gates_met=false`，mode `completed`。东风股份的 owned 页面已经定位到“公司主要从事汽车、发动机的开发、设计、生产和销售”，但 `_excerpt_states_owned_overview` 不接受“主要从事”，而 `_PREFERRED_OVERVIEW_STATEMENT` 已经认识这个措辞。两家的营业收入构成都在“主营业务分析 → 收入和成本分析”的业务分析表里，当前 segment 路由却会落到后部“分部报告 / 分部信息”会计模板页。

## Goals / Non-Goals

**Goals:**

- 让 owned 标题下的“公司主要从事……”进入现有 principal-business / products-services 投影，并保留原文主体和证据。
- 让“主营业务分析 / 收入和成本分析”下的正式收入构成表进入现有 segment/revenue 投影，并保留表格主体、单位、分部维度和来源绑定。
- 用 `owned_page_facts=v5` 作为 successor identity。v1–v4 文件保持可读、不被覆盖。

**Non-Goals:**

- 不重开已归档的首次扩大 change，不启动下一轮扩大。
- 不做白云机场吞吐量、土地及广告补偿、东风产销量、材料成本或钢材/铝材/碳酸锂/镍行业包。
- 不投影净息差、成本收入比或贷款结构。
- 不改全市场分母、taxonomy、普通 live-run `_STRATUM_PRIORITY` 或 publication scope。
- 不授权规模质量或生产，不调用旧 writer、DCF 或交易路径。
- 不用跨表匹配、金额相等或无标题匹配放宽收入路由。
- 不按证券、页码或公司硬编码。
- 不打开 LLM 掩盖 `provider_unavailable`。

## Decisions

1. **只对齐现有规则，不新建投影体系。**
   Overview 前置判断补上后文选择规则已经接受的“主要从事”，并且仍然要求落在已有 owned 标题下。主体和证据语义沿用现有投影。

2. **收入构成表与后部分部模板并列，而不是互相替代。**
   “主营业务分析 / 收入和成本分析”下带标题的正式收入构成表必须能进入 segment/revenue 投影。一、/1、这类正式分类行使用 `revenue_composition`，不标成 industry 或 product；分行业、分产品、分地区、分销售模式仍用原维度。投影在下一个成本、产销量、资产负债或后部分部标题处停止。无效收入标题不占用路由，同页或后续页的分部报告/分部信息继续扫描。后部模板仍可匹配，但不再是唯一合法路径。没有标题、跨表拼接或仅凭金额相等的匹配继续拒绝。

3. **v5 是 successor，不是原地覆盖。**
   新 work 使用 `{"rules":"company_profile_common_core.v1","owned_page_facts":"v5"}`。查询按该 identity 选择 successor。v1–v4 JSON 留在磁盘。

4. **正式 replay 晚于范围审核和实现审核。**
   600004.SH 与 600006.SH 只作为实现通过后的 successor replay 样本。recall、accuracy、critical numeric errors 和 elapsed 在 replay 之后独立重算，不预设 4.1 通过。

## Risks / Trade-offs

- [“主要从事”放得过宽] → 仍限制在 owned 标题内，并保留现有主体判断。
- [收入表和分部模板同时命中] → 两处都可以投影，但各自保留自己的来源绑定，不做金额相等合并。
- [v5 replay 仍达不到 4.1] → 记录现场结果，不改门槛，不授权生产。

## Migration Plan

1. 范围审核通过前不改代码。
2. 实现两项规则和反例测试。
3. 审核通过后再对两份正式年报做 v5 successor replay。
4. 独立重算质量指标。回滚时保留 v4 文件和失败的 v4 观察，不删除 v5 work，除非另有审核。

## Open Questions

- 无。正式样本的 4.1 结果留到 replay 之后再记，不在本设计里预设。
