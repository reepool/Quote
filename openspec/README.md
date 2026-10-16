# OpenSpec 当前适用范围

其他领域继续按各自spec/change执行。本节仅明确公司画像2026-10-16方向接续，避免历史合同误作下一轮执行授权。

公司画像固定阅读顺序：[产品总需求](../docs/development/company_profile_product_and_industry_semantic_requirements.md) → [验收计划v2](../docs/development/company_profile_acceptance_program.md) → [顺序任务卡](../docs/development/company_profile_delivery_plan.md) → [A/B流程v2](../docs/development/ab_role_development_workflow.md)。唯一新实施change：[restore-company-profile-llm-led-generalization](changes/restore-company-profile-llm-led-generalization/proposal.md)。该change的delta是已批准规划的目标，代码尚待实现；主spec保留已归档实现基线，不能以旧基线否决新目标。

以下旧规则只适用于原冻结身份/来源/合同，不授权新的扩样或生产，不再作为新处理政策：

- `deliver-company-profile-m4-*`、`repair-company-profile-*`、`company-profile-m4-first-expansion`、`common-core-owned-page-gap-repair`：历史样本观察、修复和验收。
- `company-profile-common-core-owned-page-facts`中的无LLM核心完成、bounded workflow中的仅缺字段fallback、制造阶段的固定章节/Gold/逐字正文规则：仅旧版本行为；可靠结构化数字解析仍可复用。
- Stage4/Stage5固定样本、scope/reconcile/aggregate/restricted-promotion及制造shadow/OOS系列：仅原研究范围，不是日常全市场人工审批制度。
- `define-company-profile-staged-acceptance`、`prepare-company-profile-s1-s2-bounded-acceptance`和v59收口change的未执行步骤：由新计划接续，不追勾完成；原S1回执、S2源准备和冻结合同保留，旧v61执行绑定暂停。

主spec中不冲突的来源/金融时间/原数值/主体局部优先/身份/幂等/单owner等约束继续有效。新政策三视图、真实LLM及语义覆盖适用新delta。每个实现切片经A审核后同步实际已实现的要求，不能仅因本次改文档就声明代码能力上线；新change仅在对应必需任务完成后归档。

原始首发成绩、补救轨迹、归档合同和证据不改写。失效执行指令已从当前导航移除；代码和临时runner按交付计划的替代/调用/验证条件删除，正式PDF和受保护历史不当垃圾清理。生产仍`not_authorized`，规模声明false，另行授权才可切换。
