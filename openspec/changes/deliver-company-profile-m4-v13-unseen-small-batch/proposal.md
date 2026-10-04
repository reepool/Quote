## Why

v13 正式轮有限通过(36/36、68/68、关键数字错误 0、正式轮 232.3 秒、reused_scope=false),当前主要矛盾是完整业务闭环对新年报的泛化能力。历轮已观察公司达 14 家:`600000.SH`、`600004.SH`、`600006.SH`、`600007.SH`、`600008.SH`、`600009.SH`、`600010.SH`、`600011.SH`、`600012.SH`、`600019.SH`、`600022.SH`、`600028.SH`、`600031.SH`、`302132.SZ`;默认冻结只排除当前身份的已交付记录,盖不住这些公司。

## What Changes

- 复用现有 `CompanyProfileTaskService`,完整身份保持 `owned_page_facts=v8 + material_input_facts=v1 + revenue_sentence_repair=v13` 与现有 rules。冻结时通过 `delivered_ids` 排除上述全部 14 家已观察公司,不能只查 v13 自身交付记录。
- 按 SSE→SZSE→BSE、所内代码升序各取一家服务和制造公司;冻结 cutoff `2026-09-17`、报告身份、文档版本及 PDF 绑定,两家公司共享 50000 token,使用独立目录 `reports/m4_v13_unseen_small_batch`。
- 服务先跑、制造后跑;自动选证据→接受→持久化→查询→导出使用同一 identity,两家进入同轮合并观察。从首次 execute 到第二家 export 返回实测整轮耗时;环境重试计入并保留,超时或失败如实记录,不删除失败后挑选更快结果。
- 从两份新年报重建分母,分别审核六维答案正文和全部实际交付事实(Activity、Overview、Segment、Measurement),核对主体、动作、对象、栏目、金额、单位;无商品角色须有明确检查范围。分数不预填,不回写任何历史轮次。

## Capabilities

### New Capabilities

- `deliver-company-profile-m4-v13-unseen-small-batch`: 用 v13 身份交付一组尚未观察过的服务公司和制造公司并完成独立复核。

### Modified Capabilities

- 无。v12 的 hold 与全部历史观察保留,不回写。

## Impact

- 选样仍走现有冻结入口,不改同身份默认排除。
- `production_authorization` 保持 `not_authorized`,`scale_quality_claim_allowed` 保持 false。
- 召回与准确均 100%、关键数字错误 0、token≤50000、整轮≤300 秒才通过;失败暂停下一批,通过仅允许后续范围评审。
