## Why

v27投入关系修复获A角声明来源范围有限通过，旧change已同步有效规格并归档。当前验证全行业common-core在下一对未见官方年报的新业务披露形态上能否完整且准确交付，不继承历史分母或扩大质量声明。

## What Changes

- 现有freeze owner显式接收完整28家delivered_ids（原26家加600026.SH、600063.SH），按原SSE/SZSE/BSE及所内代码升序选service/manufacturing各一家；标签只用于抽样。
- 固定完整v27身份、官方有效报告版本、cutoff2026-09-17、共享50000token和独立m4_v27_unseen_small_batch目录。重复读一致，冻结不enqueue，历史不变。
- 独立阅读新PDF，首次执行前固定三维实质正文、重要收入行、明示原生角色及动作/关系依据，召回分母由新来源建立。
- 沿现有owner依次首次运行→查询→导出，第二家使用剩余预算；完整计时包含重试，保存五阶段实际复用，首次失败/超限不替换。
- 全部接受事实及已答正文唯一复核主体、对象、动作/关系、栏目、数值/单位；Measurement只计准确率，同角色多源不重复召回，负例限检查页。双100%、数字错误0、≤300秒/50000token后提交有限验收，否则暂停并只为真实缺口立修复卡。

## Capabilities

### New Capabilities

- `deliver-company-profile-m4-v27-unseen-small-batch`: 28家排除后的新报告冻结、执行前来源合同、首次正式交付及独立动作语义复核。

### Modified Capabilities

无。

## Impact

复用CompanyProfileTaskService.freeze_m4_next_batch_plan、execute_published_task及record_published_source_review，保持单一写入owner与common-core三维合同。不改业务模板、生产入口/配置/身份默认值，不新增依赖或框架。量价、成本、行业增强、框架整理与无关旧失败后置。生产not_authorized，规模质量false。
