## Why

v17 正式轮对两份修复样本完成有限验收(17/17、28/28、关键数字错误 0、正式轮 173.3 秒、reused_scope=false),当前主要矛盾回到新年报上的业务泛化能力。历轮已观察公司达 16 家:`302132.SZ`、`600000.SH`、`600004.SH`、`600006.SH`、`600007.SH`、`600008.SH`、`600009.SH`、`600010.SH`、`600011.SH`、`600012.SH`、`600017.SH`、`600019.SH`、`600022.SH`、`600028.SH`、`600031.SH`、`600038.SH`;默认冻结只排除当前身份的已交付记录,盖不住这些公司。

## What Changes

- 复用现有 `CompanyProfileTaskService`,完整身份保持 `owned_page_facts=v8 + material_input_facts=v1 + revenue_sentence_repair=v17` 与现有 rules。冻结时通过 `delivered_ids` 排除上述全部 16 家已观察公司。
- 按 SSE→SZSE→BSE、所内代码升序各取一家服务和制造公司;冻结 cutoff `2026-09-17`、报告身份、文档版本及 PDF 绑定,两家公司共享 50000 token,使用独立目录 `reports/m4_v17_unseen_small_batch`。
- 服务先跑、制造后跑;接受→持久化→查询→导出使用同一 identity,两家进入同轮合并观察,第二家使用剩余预算。从首次真实执行到第二家导出返回实测整轮耗时;失败、重试及耗用如实保留。
- 观察前固定规则:独立阅读两份新年报重建来源清单,六维答案核正文,每条实际接受事实评价一次;重复披露不重复增加召回,无角色负例注明检查范围;新分母不固定为 17 或 28。分数不预填,不回写任何历史轮次。

## Capabilities

### New Capabilities

- `deliver-company-profile-m4-v17-unseen-small-batch`: 用 v17 身份交付一组尚未观察过的服务公司和制造公司并完成独立复核。

### Modified Capabilities

- 无。v17 修复验收与全部历史观察保留,不回写。

## Impact

- 选样仍走现有冻结入口,不改同身份默认排除。
- `production_authorization` 保持 `not_authorized`,`scale_quality_claim_allowed` 保持 false。
- 召回、准确率均 100%、关键数字错误 0、共享 token≤50000、整轮≤300 秒才通过;失败则保留观察、暂停后续批次并按真实漏项另立修复卡,通过仅允许下一次范围评审。
