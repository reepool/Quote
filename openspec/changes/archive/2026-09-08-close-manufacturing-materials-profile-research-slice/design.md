## Context

政策变更后的完整四报告运行 `stage55-policy-four-20260907-a` 已证明验收分层可执行，但研究切片仍有三个局部阻塞：宁德时代分部 scope 的独立 verify 单次执行失败；宁德时代前五名 scope 中一个被正确阻断的非法聚合 Relationship 反向把已有 18 条 accepted 记录的 coverage 降为 `unclear`；璞泰来原文明示的“合并抵消项”被 verify 以 `subject_unsupported` 误拦。

这些问题不需要扩展 schema、Gold、LLM 网关或主体模型。权威业务 owner 继续是 `CompanyProfileSemanticService.run_task`；Stage 5 service 继续只负责编排、隔离 bundle、Benchmark 和报告状态。历史 run 保持不可变，生产授权保持 `not_authorized`。

## Goals / Non-Goals

**Goals:**

- 已有 accepted 记录满足字段时，窄范围容忍同字段额外的、已被安全阻断的 `object_not_allowed` 候选，不让其破坏 coverage 或 task completion。
- 继续阻断由前五名合计虚构的 `report_local_aggregate` Relationship，并保留真实排名行为的 `report_local_anonymous` Relationship。
- 对原文行身份明确为合并抵消调整的 Segment/Measurement，执行既有 `consolidation_adjustment` 主体合同，消除 verify 的单一 `subject_unsupported` 误拦。
- 用一次三-scope 预检验证三个当前阻塞，再最多执行一次完整四报告新运行并生成真实 Benchmark。
- 在满足研究验收政策时登记 `research_slice_usable`，同时保持全部记录为研究态且生产未授权。

**Non-Goals:**

- 不放宽 Evidence、字段绑定、source-native、单位、期间、Activity actor、comparison basis 或 §14 冻结 blocker。
- 不把任意 blocked/unresolved 候选当成可忽略，也不让部分 accepted 行掩盖真实缺行或冲突。
- 不修改 Gold 的 source-native 期望值，不追求 Gold 24/24，不拼接历史或定向 run。
- 不增加 timeout、重试循环、LLM schema、PDF/OCR、数据平台或新的语义 owner。
- 不启动阶段 6、旧 backfill、approved 表、商品暴露、价值链、DCF、scheduler、API 或 Telegram。

## Decisions

### 1. Resolve supplemental rejection by a closed rule

在 workflow 汇总中，将候选分为 accepted、普通 unresolved/blocked，以及“已解决的补充拒绝”。只有同时满足以下条件的 disposition 才属于最后一类：同一 `field_id` 已有 accepted 记录；该候选状态为 blocked；唯一 reason 是 `object_not_allowed`。这类候选继续保留 blocked disposition 和人工审计材料，也继续从 projection 排除，但不覆盖 accepted 记录导出的 `observed` coverage，亦不单独阻止 task completion。

选择封闭规则而不是“有一条 accepted 就忽略其他失败”，是为了修复前五名聚合坏候选这一现实误拦，同时保持 Evidence mismatch、subject unsupported、冲突、provider failure、missing verify target 等阻塞语义。

### 2. Keep counterparty identity semantics unchanged

排名第一至第五名的来源身份继续使用 `report_local_anonymous`。前五名合计金额或比例只能形成 Measurement；从该合计生成 `前五名客户` Relationship 仍返回 `object_not_allowed`，不得进入 projection，也不得回填名称 coverage。coverage 修复只改变任务汇总，不改变非法候选的 disposition。

### 3. Normalize only the known consolidation-adjustment verifier false negative

Stage 5 provider 在独立 verify 返回后执行一个窄范围、确定性的响应归一化。只有当目标记录同时满足以下条件时，才可把单一 `subject_unsupported` block 归一化为 pass：

- 对象为 Segment 或 Measurement；
- `row_class=consolidation_adjustment`；
- `subject_scope=consolidated_group`；
- `subject_basis=direct_source_wording`；
- source-native 行标签通过现有明确调整行校验；
- verify 没有 value、Evidence、field、period、unit 或其他 reason。

同时在 verifier 分类指令中说明这一既有规则，但不写公司、产品、数值或预期答案。普通产品/分部行、裸“公司”表述及带其他错误的调整行仍按原结果处理。

### 4. Bound live validation to one preflight and one complete run

离线单测和 strict validation 通过后，只运行一次包含三个阻塞 scope 的新-ID 预检：宁德分部、宁德前五名、璞泰来分部及调整行。预检满足合同时，才运行一次另一新 ID 的完整四报告切片。两次运行都从现有 Evidence plan 和公共 LLM gateway 重新生成结果，不读取历史 accepted 记录补值。

现有 scope selector 仅允许单份 sample，不能生成上述单一预检 bundle。本 change 只把该 selector 收窄扩展为：operator 必须显式列出 approved sample 和唯一 scope ID；每个选中 sample 至少命中一个 scope；任何未知 scope 仍在 provider 调用前失败。它仍由同一个 Stage 5 service 和 bundle store 执行，不新增语义循环或编排 owner。

完整运行是唯一可成为新权威结果的候选。Benchmark 必须从其已提交 bundle 自动计算，保留 Gold failed/conflict 和未触发负例；只有四报告均达到 `usable` 或 `usable_with_caveats`，且无 required execution failure、§14 blocker 或已评估负例失败时，才登记 `research_slice_usable`。

## Risks / Trade-offs

- [补充拒绝规则掩盖真实错误] → 仅容忍同字段已有 accepted 且唯一 reason 为 `object_not_allowed` 的 blocked 候选；其他 reason 继续阻塞。
- [归一化绕过独立 verify] → 只归一化已经由本地模型和 source-label validator 证明的调整行主体判断，任何其他 verify reason 都保留；新增负向回归测试。
- [模型波动导致完整运行仍失败] → 不追加定向 run；保存新 bundle 的真实状态并报告具体执行或语义 blocker。
- [Gold 差异被误读为事实缺失] → Benchmark 保留分层结果，不把 Gold 全等作为切片可用门，也不修改 runtime 迁就 Gold。
- [研究可用被误认为生产批准] → bundle、projection、状态和审计持续写入 `production_authorization=not_authorized`。
