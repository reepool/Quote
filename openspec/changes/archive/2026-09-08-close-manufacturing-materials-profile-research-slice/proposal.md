## Why

政策生效后的权威四报告运行仍有三个可定位的阻塞：宁德时代分部 verify 单次执行失败、一个非法前五名聚合 Relationship 反向破坏已满足的 coverage，以及璞泰来“合并抵消项”被主体校验误拦。需要用最小局部修正确认四报告能否达到研究可用，而不是继续扩大 Gold、网关或语义框架。

## What Changes

- 使 coverage 由已独立验证的合法记录或合法空结果决定；同字段的额外坏候选仍被阻断，但不得把已经满足的 scope 降回 `unclear`。
- 对 `row_class=consolidation_adjustment` 且原文行身份明确包含“合并/抵消”语义的记录，允许该调整行使用 `subject_scope=consolidated_group` 与 `subject_basis=direct_source_wording`；该规则不得推广到普通产品、分部或仅写“公司”的事实。
- 保持前五名排名身份为 `report_local_anonymous`，继续阻断仅由合计生成的 `report_local_aggregate` Relationship。
- 对宁德时代失败的分部 scope 使用新 run ID 复验，不回写或拼接历史 bundle。
- 离线验证后最多执行一次三-scope 预检和一次完整四报告新运行，并从新 bundle 生成真实 Benchmark 和报告状态。
- Gold 只修正确认错误的评估元数据，不修改 source-native 期望值，不以 Gold 24/24 作为研究切片可用门。
- 保持 `accepted_for_review`、隔离 bundle 和 `production_authorization=not_authorized`；不启动阶段 6、旧 backfill、商品暴露、价值链或生产写入。

## Capabilities

### New Capabilities

无。

### Modified Capabilities

- `company-profile-bounded-semantic-workflow`: 已满足 coverage 不被额外坏候选覆盖，并为原文明示的合并抵消调整行定义窄范围主体校验。
- `manufacturing-materials-profile-isolated-slice`: 固定本次预检/完整运行预算、不可拼接要求和按研究验收政策计算最终状态的完成门。

## Impact

- 仅影响 `research/company_profile/` 的 coverage 汇总、verify 适配、少量 Gold 元数据及对应测试。
- 使用现有 `CompanyProfileSemanticService.run_task`、Stage 5 provider、Evidence plan、公共 LLM gateway 和隔离 bundle store。
- 不修改 PDF/OCR、LLM 网关、数据库 schema、scheduler、API、Telegram 或旧生产业务画像链。
