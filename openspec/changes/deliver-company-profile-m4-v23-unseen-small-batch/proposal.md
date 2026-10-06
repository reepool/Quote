## Why

v23已在声明来源范围内获A角有限验收，旧change同步规格后有限归档。当前目标是检验全行业通用核心对下一对未见年报及新披露形态的泛化，不扩大生产或规模质量声明。

## What Changes

- delivered_ids显式排除全部24家已观察公司，沿现有SSE/SZSE/BSE及所内代码升序规则各取一家service、manufacturing。保存完整排除名单，冻结不enqueue；抽样标签不决定业务模板。
- 固定完整v23身份（rules=company_profile_common_core.v1、owned_page_facts=v8、material_input_facts=v1、revenue_sentence_repair=v23）、官方有效版本、cutoff2026-09-17、共享50000 token及独立m4_v23_unseen_small_batch目录。重复读取一致、历史不变。
- 沿现有owner首次真实运行→查询→导出，第二家使用剩余预算，保存实际正文、表格、角色、缺项及runtime复用。整轮从首次execute到第二export返回，包含全部重试；失败和超限不替换。
- 从新冻结PDF独立重建来源清单，按正文实质核召回，不继承32/57分母。实际接受事实及已答正文逐对象唯一核准确率，Measurement仅准确率，角色绑定底层事实。
- 双100%、数字错误0、≤300秒、≤50000 token后提交有限验收；失败暂停后续，仅修真实业务缺口。无角色结论限实际检查页。

## Capabilities

### New Capabilities

- `deliver-company-profile-m4-v23-unseen-small-batch`: 排除24家后的未见冻结、首次受控交付及独立原文复核。

### Modified Capabilities

无。

## Impact

复用CompanyProfileTaskService.freeze_m4_next_batch_plan、execute_published_task及record_published_source_review现有owner，不新增执行、抽样、审核框架。数量细分、成本拆分、行业增强及既有测试问题后置。生产not_authorized，scale_quality_claim_allowed=false。

## 首次正式观察（失败）

74b8faf4…计划冻结华能水电600025.SH和华润双鹤600062.SH。首次正式194.75秒、0/50000 token、五阶段无复用，实质召回19/62、准确率48/57（53事实＋4已答正文）、数字错误0。华能水电12条收入行及电力角色未交付、产品/收入正文不完整；双鹤主营缺失、产品正文偏原料药，产销/销售矩阵角色漏失并误将客户医院名中的钢铁投影为销售。本轮执行失败分支，当前change不归档、后续批次暂停，原结果不替换；数量/成本/行业及既有测试后置。

## Authorized v24 closure scope

Repair the real missing business narratives, continued revenue tables and named sales objects on the two frozen reports through existing owners. Reject unstated commodities inferred from counterparty names. Register v24 and perform one independent first formal repair round only after full owner-page regressions pass. Fix the complete 62-item source scope before execute, retain original failed outputs and scope timing limitation, and submit finite acceptance or a truthful failure. Current change stays open pending A-role review; subsequent batches remain paused.

## v24首次正式结果（待A角有限验收）

完整62项在执行前固定。现有owner首次正式201.09549884870648秒、0/50000token、实际五阶段无复用；实质召回62/62、唯一准确率104/104（98事实＋6答案）、数字错误0。六维正文、29收入行及27原生销售角色交付，主体、API/制剂及客户边界核实。保护616文件含原593历史＋18首次失败不变。达到声明门槛，提交有限验收；保持未归档和下一批暂停，原失败/范围时序限制保留。
