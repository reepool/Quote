## Context

当前 published identity 是 `{"rules":"company_profile_common_core.v1","owned_page_facts":"v4"}`。302132.SZ / 600000.SH 已记录 7/7，`expansion_gates_met=true`。生产仍为 `not_authorized`。

正式入口仍是 `company_profile_common_core` 的 `preview` / `run` / `status` / `pause` / `resume` / `query` / `export`。`run` 走 `enqueue_latest_annual`；`resume` 不重新入队，只 drain 未完成 work。`_run_live` 在有 registry 时现选样本，再 enqueue。`CompanyProfileLivePlan` 不含选中证券或报告版本。`LatestAnnualReport` 已有 `asset_id`、`report_period`、`content_hash`；入队时 `report_id` 来自 `source_asset_id`/`filing_id`，`document_version` 来自 `content_hash`。registry 有 `schema_version`、`as_of`、`universe_snapshot_id`。

若规范无条件要求“没有首次扩大快照就拒绝 enqueue/drain”，普通 `run`/`resume` 会被阻断。若快照落盘后所有后续运行都限定同一批证券，首次扩大会变成永久样本锁。只冻 instrument IDs 也无法防止更正年报或不同 cutoff 换成另一份文件。

## Goals / Non-Goals

**Goals:**

- 在同一 owner 内定义首次扩大模式的激活、恢复和完成；不新增 action。
- 只在该模式 `active` 且快照缺失/不匹配时拒绝；普通路径不回归。
- 计划冻结 `knowledge_cutoff`、registry/universe 身份，以及每家年报的现有身份字段；漂移拒绝。
- 新观察写独立快照；保留两家公司 7/7 基线。
- 发布 operator_closure v2，保留 v1；完成后不得自动下一轮或重跑冻结样本。

**Non-Goals:**

- 不新增 published action 或平行执行链。
- 不授权生产、DCF、交易、旧 writer、规模质量或下一轮扩大。
- 不建行业包，不抽净息差 / 成本收入比 / 贷款结构，不预写 v5。
- 不覆盖 operator_closure v1 清单。
- 任务 1.1 未勾选不得改代码。

## Decisions

1. **第一片只做吞吐，不做行业包。**
   行业包仍留在其余 M4 待办。

2. **首次扩大是同一入口上的模式，不是新 action。**
   在现有 task control / checkpoint 上记录 `first_expansion_mode`：`inactive` | `active` | `completed`。默认 `inactive`，`run`/`resume` 保持今天的全市场 enqueue / 续跑。apply 时由同一 owner 写入不可变计划快照并切到 `active`；这不是新的对外动作。`active` 期间 `run` 只入队冻结报告，`resume` 只 drain 这些 work；快照缺失或不匹配才拒绝。冻结 work 已交付后再 `run`/`resume` 必须幂等返回，不再观察。写入 operator_closure v2 后进入 `completed`：恢复普通 `run`/`resume`，不得自动再激活，不得重跑冻结样本。另开 change 之前禁止再写一份首次扩大计划。备选是无条件拒无快照的 enqueue，会阻断正常研究运行。

3. **计划冻结报告版本，不只冻证券代码。**
   `company_profile_first_expansion_plan.v1` 必须包含：`plan_id` 或内容 hash、live-plan 规则、`knowledge_cutoff`、选样所用 registry 身份（`company_profile_a_share_candidate_registry.v1`、`as_of`、`universe_snapshot_id` 若当时存在）、`selected_instrument_ids`、`selected_strata`（必须含 `service`），以及每家正式年报的 `asset_id`、`report_id`、`report_period`、`document_version`。这些字段按现有入队/registry 语义取值，不发明新身份。enqueue / resume 必须校验同一引用；更正年报、有效资产变化或不同 cutoff 导致漂移时拒绝，不得改用新版本。写入后不可变。样本仍固定为现有两家公司预算，不得扩到三家。首次扩大专用选样必须预留一个 `service` 名额：只从 `asset_status=available` 且有正式有效年报的候选中，按 SSE、SZSE、BSE 和层内 `instrument_id` 升序确定。另一个名额按现有全局优先级从剩余候选中确定性填充。最终样本必须含 `service`，并且至少占用两个不同 strata。没有合法 `service` 候选时拒绝记录和激活。不点名证券，不读模型输出，不改全市场分母，不改普通 live-run 的 `_STRATUM_PRIORITY`。公开计划模型必须拒绝预算、证券、分层和报告同时扩到三家及以上的载荷。

4. **新观察写独立快照，基线 v1 文件不动。**
   新 live-run / source-review 必须携带同一 plan 引用和同一组报告引用，且不覆盖固定 v1 文件。完成后新 source-review 是本轮权威，基线仍可读。

5. **operator_closure 发 v2，保留 v1；文案用执行后语义。**
   不改 v1 唯一合法清单。v2 在新 source-review 之后写：首次扩大已按受审计划执行；新 source-review 是本轮权威；不授权下一轮、规模质量或生产。写入 v2 即完成模式。

6. **不改 published identity，除非另批解释器。**
   默认继续 v4。可复用缺口另开 change。

7. **首次扩大专用选样预留一个 service 名额。**
   2026-09-24 只读复探表明，现有全局优先级的前两层都是 manufacturing，合法 service 候选排在其后，两家预算内记录不出首次扩大计划。只改首次扩大选样，预留一个 service 名额；另一个名额仍走剩余候选的全局优先级。普通 live-run 的 `_STRATUM_PRIORITY` 不变。taxonomy 父链修复后，不再把“600000 分层为 other”当作当前设计结论。

## Risks / Trade-offs

- [模式状态写在 control 上被普通 run 清掉] → 完成标记与计划快照独立持久化；`completed` 后普通 run 不得删除或重写计划。
- [registry 没有合法 service 候选] → 拒绝记录和激活，不得扩大样本，不得改普通 live-run 抽样顺序。
- [年报在计划后更正] → 按冻结引用拒绝，不换新文件。
- [把 active 当成永久样本锁] → `completed` 后恢复普通路径；重复首次扩大必须另审。
- [未审范围就开始改代码] → 任务 1.1 未勾选不得 apply。

## Migration Plan

1. 勾选 1.1 后才允许 apply。
2. 合同 Review 通过后，才实现首次扩大专用的 service 保留名额。两家预算不变。没有合法 service 候选则拒绝，不扩大样本，不激活。
3. 用现有 `run`/`resume` 只处理冻结报告；新 live-run / source-review 写独立快照。
4. 核原文后写 operator_closure v2，模式变为 `completed`。
5. 回滚：回到 `inactive` 或保留 `completed` 而不再激活；保留 v4、v1 基线和 v1 closure；生产状态不变。

## Open Questions

- 2026-09-24 首次扩大零交付集中成两个可复用 common-core 缺口，不在本 change 修复，也不发布 `owned_page_facts=v5`。后续最小 repair change 只覆盖这两项：overview 投影前置判断不接受 owned 标题下的“公司主要从事……”，而后文选择规则已经认识“主要从事”；“主营业务分析 / 收入和成本分析”下的正式收入构成表没有进入 segment/revenue 投影，路由落到后部“分部报告 / 分部信息”会计模板页。该 repair 不得按公司硬编码，不得打开 LLM 掩盖 `provider_unavailable`。白云机场吞吐量、土地及广告补偿，以及东风产销量、材料成本和钢材/铝材/碳酸锂/镍投入角色，留在后续 M4 行业能力，不纳入这次 common-core repair。本卡只登记候选范围，不创建、不 apply。
- 5.9 复验之后的下一优先级不是 5.10。完整复核但零交付的失败观察应能以失败状态离开 `active`；部分样本或真正未完成的语义审查仍必须拒绝。当前阻塞在 `complete_first_expansion` 要求准确率已评估。该合同修正另作决策，不在本 change 顺手修改。
