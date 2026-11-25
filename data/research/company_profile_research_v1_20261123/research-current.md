# 当前21股研究画像（2026-11-25，6.FM）

当前唯一研究读取根：`/home/python/Quote/data/research/company_profile_research_v1_20261123`。

[全21股人读版](exports-6fm-20261125/annual_profiles.md) · [当前API快照](current-api-6fm-20261125.json) · [完整年度JSON](exports-6fm-20261125/annual_profiles.json) · [原owner CSV](exports-6fm-20261125/profiles.csv)

当前可读682条内容、1115个可见指标。数量包含带范围限制或保留原始写法的指标，不代表这些对象已逐条独立确认正确。API同时交付缺项、原因、建议补读、来源、作者及审核状态；缺项可交付，不强求全部补满或供应链精细化。

## 本轮研究增量

海尔智家600690.SH是本轮新增公司，正式2025年报首读先交待核稿，再显式进行独立模型源复核。目前可读24条内容、39个可见指标，说明保鲜烹饪、洗护、空气、用水、商用制冷、全球品牌及变现方式。两项收购品牌主体范围已按真实复核意见修订；订单预测增幅的一个错误单位指标已隔离，未把推测值补回。分业务金额缺单位时仍保原文与限制。

王府井600859.SH由原29条/63指标增至37条/80指标，净增8个可读对象/17指标。补读关注主要业态经营网点、经营方式、期末规模及报告期后免税新店，明确不同段落和表格的统计范围不完全相同，不合成统一门店总数或纯建筑面积。A对本轮ops-010、ops-012、ops-013、ops-014另加严格hash绑定的source_only，保其余有效增量及所有原限制。自有/租赁托管数量候选因本地单位检查拒绝，专业店细分仍建议补充，没有为这些细目追加调用。

其他19家完整画像保持不变。原writer仅追加三份获准记录，原59份记录、旧快照、来源、模型、审核及独立使用限制不改。原query/导出、新实例读回、21股HTTP批量及两股详情完全一致；普通读取0模型请求。

## 质量与使用边界

当前整体准确率尚未充分评估。历史`512/523 = 97.8967%`仅是v12当次流程终稿的已审对象分数，不是跨轮累计准确率，也不能代表当前研究版；旧错误没有被反复加入当前分母。修订、隔离后的当前内容只继承精确内容hash匹配的审核，未独核部分保持未评估，不通过删难项或重算旧分宣称达标。

这是带来源、审核和缺项说明的研究交付，尚未取得生产、100家/全市场、DCF或交易使用授权。模型已核、A独核、可引用原文和已知错误隔离分开呈现；completed仅表示任务执行结束。

## 读取与按股补读

在API进程显式设置 `QUOTE_COMPANY_PROFILE_RESEARCH_ROOT=/home/python/Quote/data/research/company_profile_research_v1_20261123`。共享配置未改，本轮未启动生产服务。

读取接口：`GET /api/v1/research/company/{instrument_id}/annual-profile`、`POST /api/v1/research/company/annual-profiles/query`。查询和enrichment-plan只读，不隐式调用模型。

首读、后台复核及指定股票/具体问题补读通过显式HTTP任务入口执行，必须沿已安装的有限授权策略；不会自动循环补齐。用法见[研究任务API说明](../../../docs/research/company_profile_http_tasks.md)。

## 本轮来源与资源

三项真实任务共10次物理请求、321442供应商total/账本，0恢复；官方DeepSeek处理、既有Scorpio/Grok高阶源复核，约19分钟，费用unknown。执行、补读及逐对象源核结论见[6.FM交接](../../../openspec/changes/restore-company-profile-llm-led-generalization/workpaper-http-research-use-20261125/b-result.md)。

当前API SHA-256为`60bd91819b82f8332f69da79d5dff3a90cc2580661697b02023e76f61e6708b7`，年度JSON为`06198a04bb9edc3743d13be4633835e15de4545f7fd2d4c87a880970d61fe6d9`，年度MD为`1654a4f2c23f1b5709064d1bff6e4ac7c4395faed364f40f64f2ea680ea9392f`。

旧20股交付仅作冻结历史：[原20股API](current-api-20261125.json)、[原20股人读版](exports-current-20261125/annual_profiles.md)。`company_profile_http_research_20261125`隔离root仅保本轮真实HTTP历史运行依赖，不是当前安全研究版；当前消费以本页MAIN根及新命名API/年度导出为准。
