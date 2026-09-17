## Context

当前 published identity 是 `{"rules":"company_profile_common_core.v1","owned_page_facts":"v4"}`。302132.SZ / 600000.SH 的独立核原文已记录 recall 7/7、accuracy 7/7、critical numeric 0，`expansion_gates_met=true`。`scale_quality_claim_allowed` 仍被 schema 锁成 false。生产仍为 `not_authorized`。

当前 live 样本只占两层：SZSE manufacturing、SSE other。600000.SH 虽是银行，分层记为 `other`，因此 `service`、`finance`、BSE 都还没有被这次核原文占用。`operator_closure` 仍写着 `first_expansion_gates_unmet`，与已记录门槛不一致。

`CompanyProfileLivePlan` 只有预算、抽样规则和门槛，不含选中证券或层；选中结果在 `record_live_run_report` 时才出现，并只嵌进运行结束后写入的固定 `company_profile_live_run.v1.json`。`source_review` 同样覆盖固定 `company_profile_source_review.v1.json`。`CompanyProfileOperatorClosureReport` 要求 `m4_backlog` 等于当前 `_declared_m4_backlog()`，在同一 v1 下改清单会让已落盘的合法 v1 JSON 无法读取。

## Goals / Non-Goals

**Goals:**

- 在 enqueue / drain 之前持久化不可变的首次扩大计划及预选样本，并绑定 instrument IDs、strata 与 `service`。
- 新 live-run / source-review 使用独立快照，引用同一 plan ID 或内容 hash；保留当前两家公司 7/7 基线文件。
- 按冻结样本独立核原文并重算门槛，不预设再次 7/7。
- 发布 operator_closure v2，保留 v1；v2 替换项使用执行后稳定语义。
- 保留 v1–v4 工作 JSON；unique owner 与入口不变。

**Non-Goals:**

- 不授权生产、DCF、交易或旧 writer。
- 不声称规模质量达标，不授权下一轮扩大。
- 不实现制造/材料生产增强，不一次建齐银行/服务/TMT 行业包。
- 不抽取净息差、成本收入比、贷款结构。
- 不在本片预授权新的解释器或 v5 identity。
- 不把缺年报资产踢出分母。
- 不把 Gold24 / fixture 守卫当成 live 质量。
- 不覆盖或改写 `company_profile_operator_closure.v1` 的唯一合法清单。
- 本设计不是实施许可；任务 1.1 未勾选不得改代码。

## Decisions

1. **第一片只做吞吐，不做行业包。**
   产品 M4 含「行业深度」和「市场扩展」。本 change 只打开扩展：新增 occupied stratum 的独立核原文。行业包仍留在 operator_closure 其余待办，后续另立案。备选是把所有行业包塞进同一 change，范围不可审。

2. **enqueue 前冻结计划与预选样本，而不是只记规则。**
   现有 `company_profile_live_plan.v1` 只含规则，不持久化、不含选中证券。本片新增独立持久化快照 `company_profile_first_expansion_plan.v1`：嵌入 live-plan 规则，加上 `plan_id` 或内容 hash、`selected_instrument_ids`、`selected_strata`，且 strata 必须含 `service`。抽样仍走现有分层规则，不点名证券代码；一旦写入，快照不可变。没有该快照不得 enqueue / drain。`max_companies_this_round` 等于快照中的预选报告数。备选是继续只在 live_run 结束后嵌入计划，无法证明计划早于观察。

3. **新观察写独立快照，基线 v1 文件不动。**
   新 live-run 与 source-review 必须引用同一 plan ID / hash，并写到不覆盖 `company_profile_live_run.v1.json` / `company_profile_source_review.v1.json` 的路径。那两份文件继续作为当前两家公司 7/7 基线。执行完成后，本轮权威结果是新 source-review 快照，基线仍可读。备选是继续覆盖固定 v1 文件，会毁掉已验收基线。

4. **operator_closure 发 v2，保留 v1。**
   不在同一 v1 下改 `_declared_m4_backlog()`。v1 JSON 继续按原清单可读。新快照使用 `company_profile_operator_closure.v2`，其余五条待办 item_id 保留。备选是给旧 v1 做兼容读取再覆盖当前文件；本片不采用，以免同一文件名下出现两套合法清单。

5. **替换项只写执行后语义。**
   v2 替换 `first_expansion_gates_unmet` 的文案必须在写入时已经为真：首次扩大已按受审计划执行；新 source-review 是本轮权威结果；本 change 不授权下一轮扩大、规模质量声明或生产。因此 closure 仍放在核原文之后写，但文案不得再写成「还需要本 change 和新计划」。

6. **不改 published identity，除非范围审核另批解释器。**
   本片默认继续 v4。若新样本核原文出现可复用缺口，停下来另开 change 发 successor，不在本片预写 v5。

7. **600000 分层为 other 不在本片修复。**
   本片用冻结计划里的 `service` 证明新层，不重写披露形态分类器。若后续要单独占 `finance`，另审。

## Risks / Trade-offs

- [分层器抽不到 service] → 不得写入计划快照，不得 enqueue，不改规则凑层。
- [新样本召回不到 1.0] → 记录真实结果，不扩下一批，不授权生产；基线 7/7 文件仍保留。
- [误覆盖固定 v1 报告] → 验收必须证明旧路径文件内容未变。
- [把本片理解成全 M4] → proposal / spec 写明行业包不在本片。
- [未审范围就开始改代码] → 任务 1.1 是范围审核门；未勾选不得 apply。

## Migration Plan

1. 范围审核通过并勾选 1.1 后才允许 apply。
2. 先按分层规则选出样本，写入不可变 `company_profile_first_expansion_plan.v1` 快照。
3. enqueue / drain 只接受该快照中的证券。
4. 新 live-run / source-review 写独立快照并引用同一 plan ID / hash。
5. 独立核原文后重算门槛；再写 operator_closure v2。
6. 回滚：停用新计划和 v2 当前指针，保留 v4、v1 基线审查报告和 v1 closure；生产状态不变。

## Open Questions

- 新计划的精确 `max_companies_this_round` 以分层选择能新占 `service` 的最小值为准，写入快照后冻结，不在审核前锁死具体代码或证券代码。
- 新样本核原文后若出现可复用解释缺口，另开 change，不在本片预列字段。
