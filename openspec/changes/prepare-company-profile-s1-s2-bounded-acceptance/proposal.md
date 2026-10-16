> 2026-10-16接续说明：S1回归与S2来源准备及原结果保留，不追改成绩或勾选。后续执行以[新change](../restore-company-profile-llm-led-generalization/proposal.md)和[验收计划v2](../../../docs/development/company_profile_acceptance_program.md)为准；旧规则身份/预算不授权新LLM或S2运行，未执行任务由新卡承接，不按已完成归档。

## Why

逐对恢复全部附注到100%没有固定结束数量。A要求在S0消除四条确定服务错误后，以12家历史核心回归及20/100家固定数量验证可迁移性，首发与补救成绩分列。

## What Changes

- 冻结S1十二家公司、六类真实风险的核心正反断言，交付同一v61代码回归结果。
- 独立准备S2二十家公司、候补、官方来源、核心合同、切片及总预算；本change不运行S2/S3。
- 将固定数量门槛、抽查范围、补救次数与停止条件写为可执行要求；唯一进度表仍在现有验收计划。

## Capabilities

### New Capabilities
- `company-profile-bounded-acceptance`: 固定数量、核心合同和有上限补救的公司画像验收准备。

### Modified Capabilities

无。当前S0同源修复仍由原change交付，不在此复制实现。

## Impact

仅新增有界回归测试和验收准备材料，沿既有freeze、业务及读写owner。生产not_authorized、规模false，不新建执行框架，不重审全历史，不启动S2/S3。
