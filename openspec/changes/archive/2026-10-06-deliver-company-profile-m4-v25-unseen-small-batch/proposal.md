## Why

v25已获A角声明来源范围有限验收，销售语义阻塞关闭；当前需要验证通用核心在下一对未见年报及新披露形态上的业务泛化。旧change已同步主规格并有限归档，历史失败与限制保持。

## What Changes

- 沿现有owner显式排除26家已观察公司（原24家加600025.SH、600062.SH），按SSE/SZSE/BSE及所内代码升序各取一家service、manufacturing；仅抽样标签，不选择画像模板。保存完整清单，冻结不enqueue，重复读取一致。
- 固定完整v25身份（rules=company_profile_common_core.v1、owned_page_facts=v8、material_input_facts=v1、revenue_sentence_repair=v25）、官方有效版本、cutoff2026-09-17、共享50000token及独立m4_v25_unseen_small_batch目录。
- 独立读取新PDF，在首次执行前固定三维实质正文、重要收入构成、明示商品角色来源条件；不继承62/104分母。
- 现有owner首次真实运行→查询→导出，同轮观察，第二家用剩余预算；计时从首次execute至第二export返回含重试，核五阶段runtime真实复用。失败或超限原样保留。
- 每条实际接受事实及已答正文唯一核准确率，Measurement仅准确率，角色绑定底层事实不重复；负例限实际检查页。双100%、数字错误0、≤300秒及50000token后提交有限验收，否则暂停后续、只记录真实业务缺口。

## Capabilities

### New Capabilities

- `deliver-company-profile-m4-v25-unseen-small-batch`: 排除26家后的未见冻结、执行前来源固定、首次受控交付及独立业务复核。

### Modified Capabilities

无。

## Impact

复用CompanyProfileTaskService.freeze_m4_next_batch_plan、execute_published_task、record_published_source_review；不新增抽样/执行/审核框架。服务/制造沿全行业通用三维合同。数量细分、成本拆分、行业增强及既有测试后置。生产not_authorized、scale_quality_claim_allowed=false。

## 首次正式观察：可信失败

新条件执行前固定56项。首次真实owner整轮203.40秒、0/50000token、五阶段无复用；实质召回19/56、唯一准确率47/65（61事实＋4答案），数字错误0但11条Segment缺千元单位均判错。主营/产品/收入及角色漏失和3个错误Activity成立；原654历史及18首次制品未变。三卡按失败分支完成，current change未归档、下一批暂停；未改核心算法或重跑替换。
