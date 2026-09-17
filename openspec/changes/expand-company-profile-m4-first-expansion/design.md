## Context

当前 published identity 是 `{"rules":"company_profile_common_core.v1","owned_page_facts":"v4"}`。302132.SZ / 600000.SH 的独立核原文已记录 recall 7/7、accuracy 7/7、critical numeric 0，`expansion_gates_met=true`。`scale_quality_claim_allowed` 仍被 schema 锁成 false。生产仍为 `not_authorized`。

当前 live 样本只占两层：SZSE manufacturing、SSE other。600000.SH 虽是银行，分层记为 `other`，因此 `service`、`finance`、BSE 都还没有被这次核原文占用。`operator_closure` 仍写着 `first_expansion_gates_unmet`，与已记录门槛不一致。

`CompanyProfileLivePlan` 要求 `min_independently_reviewed_reports == max_companies_this_round`，新计划的公司数必须等于将要核原文的样本数。阈值必须在观察新结果前写入。

## Goals / Non-Goals

**Goals:**

- 在任何新 live 观察前记录一份新的 `company_profile_live_plan.v1`。
- 新样本至少新占 `service` 披露形态层。
- 按新计划独立核原文并重算门槛，不预设再次 7/7。
- 改写 operator_closure 待办，去掉过时的 `first_expansion_gates_unmet`。
- 保留 v1–v4 JSON；unique owner 与入口不变。

**Non-Goals:**

- 不授权生产、DCF、交易或旧 writer。
- 不声称规模质量达标。
- 不实现制造/材料生产增强，不一次建齐银行/服务/TMT 行业包。
- 不抽取净息差、成本收入比、贷款结构。
- 不在本片预授权新的解释器或 v5 identity。
- 不把缺年报资产踢出分母。
- 不把 Gold24 / fixture 守卫当成 live 质量。
- 本设计不是实施许可；范围未审过不得改代码。

## Decisions

1. **第一片只做吞吐，不做行业包。**
   产品 M4 含「行业深度」和「市场扩展」。本 change 只打开扩展：新增 occupied stratum 的独立核原文。行业包仍留在 operator_closure 其余待办，后续另立案。备选是把所有行业包塞进同一 change，范围不可审。

2. **新计划先写，再跑，再核原文。**
   复用现有分层抽样，不点名证券代码。`max_companies_this_round` 只增加到能新占 `service` 的最小整数；该数字同时成为核原文报告数门槛。302132 / 600000 的 v4 交付只作回归，不覆盖。备选是硬编码下一只股票，会把抽样规则变成样本补丁。

3. **不改 published identity，除非范围审核另批解释器。**
   本片默认继续 v4。若新样本核原文出现可复用缺口，停下来另开 change 发 successor，不在本片预写 v5。

4. **operator_closure 只改过时那一条。**
   用新 item 记录「4.1 已在两家样本上算过，首次扩大仍须本 change 与新先验计划；仍不得声称规模质量或生产」。其余五条待办原文保留。`execute_this_round` 继续锁 false。

5. **600000 分层为 other 不在本片修复。**
   本片用 `service` 证明新层，不重写披露形态分类器。若后续要单独占 `finance`，另审。

## Risks / Trade-offs

- [新样本召回不到 1.0] → 记录真实结果，不扩下一批，不授权生产。
- [分层器把下一家仍标成 other] → 计划必须证明抽到的是 `service`；抽不到则停，不改规则凑层。
- [把本片理解成全 M4] → proposal / spec 写明行业包不在本片。
- [未审范围就开始改代码] → tasks 第一项是范围审核门；未勾选不得 apply。

## Migration Plan

1. 范围审核通过后才允许 apply。
2. 先写新 live plan，再 preview/run 新样本。
3. 独立核原文后写 source review；重算门槛。
4. 最后改 operator_closure 待办。
5. 回滚：停用新 plan，保留 v4 与旧审查报告；生产状态不变。

## Open Questions

- 新计划的精确 `max_companies_this_round` 以分层选择能新占 `service` 的最小值为准，不在审核前锁死具体代码。
- 新样本核原文后若出现可复用解释缺口，另开 change，不在本片预列字段。
