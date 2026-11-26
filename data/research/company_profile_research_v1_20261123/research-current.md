# 当前39股研究画像（2026-11-26，6.FO）

当前唯一研究读取根：`/home/python/Quote/data/research/company_profile_research_v1_20261123`。

[全39股人读版](exports-6fo-20261126/annual_profiles.md) · [当前API快照](current-api-6fo-20261126.json) · [完整年度JSON](exports-6fo-20261126/annual_profiles.json) · [原owner CSV](exports-6fo-20261126/profiles.csv)

当前可读1082条内容、1648个可见指标。数量包含带范围限制或保留原始写法的指标，不代表这些对象已逐条独立确认正确。API同时交付缺项、原因、建议补读、来源、作者及审核状态；缺项可交付，不强求全部补满或供应链精细化。

## 本轮统一交付

原21股682条/1115指标保持完整；6.FN的18股安全研究稿400条/533指标已沿原writer追加到同一root，共98份记录（原62份、新36份）。新36份为18份首读及18份精确hash绑定的源复核后继，逐字节等于原安全交付；原模型作者、来源、独立使用限制、source_only、指标位置限制、缺项和同报告谱系均保留。

API快照显式查询39股及尚无画像的光大证券601788.SH、国信证券002736.SZ：返回39份画像、两股列missing，state为partial。missing只表示本地没有可读画像，不推断未披露或模型失败；普通查询不重跑。

本轮0新增模型请求、供应商usage或付费，未发布6.FN组后恢复候选或原自动错误稿。原reader新实例与年度导出的39份完整profile均等于A冻结预期；HTTP实际依赖由A独立核验，B未重复执行。

## 质量与使用边界

当前整体准确率尚未充分评估。6.FN正式V1验收未通过，原首发/终稿、95%/99%门槛及失败结论不改；安全研究交付不回算自动成绩。历史`512/523 = 97.8967%`仅属v12原冻结批次，不能代表当前研究版。未独核内容保持原状态，不通过删难项或重算旧分宣称达标。

这是带来源、审核和缺项说明的研究交付，尚未取得生产、100家/全市场、DCF或交易使用授权。模型已核、A独核、可引用原文和已知错误隔离分开呈现；completed仅表示任务执行结束。

## 读取与按股补读

在API进程显式设置 `QUOTE_COMPANY_PROFILE_RESEARCH_ROOT=/home/python/Quote/data/research/company_profile_research_v1_20261123`。共享配置未改，本轮未启动生产服务。

读取目录：`GET /api/v1/research/company/annual-profiles`；单股：`GET /api/v1/research/company/600350.SH/annual-profile`；批量：`POST /api/v1/research/company/annual-profiles/query`，请求体例如：

```json
{"instrument_ids":["600690.SH","600350.SH","601788.SH","002736.SZ"]}
```

先查看返回的information_gaps和suggested_acquisitions，再针对具体缺项只读规划。例如山东高速sm-12原指标1的租赁收入主体限制：`POST /api/v1/research/company/600350.SH/annual-profile/enrichment-plan`，请求体为：

```json
{"request_id":"research-600350-rental-scope-1","targets":[{"element":"scale_and_mix","question":"核对2025年报其他业务租赁收入的主体与表列口径，对应现有sm-12指标1限制；保留其他业务合计与租赁收入的区别。"}]}
```

计划绑定当前work/report/hash，返回execution_started=false；计划不会解除现有限制，不隐式调用模型或执行补读。

首读、后台复核及指定股票/具体问题补读通过显式HTTP任务入口执行，必须沿已安装的有限授权策略；不会自动循环补齐。用法见[研究任务API说明](../../../docs/research/company_profile_http_tasks.md)。

## 交付核验与历史

本轮仅追加已有安全记录并沿原reader导出。[B结果与逐记录hash](../../../openspec/changes/restore-company-profile-llm-led-generalization/workpaper-unified-39-research-20261126/b-result.json)保留准确写集、保护结果与时间；[6.FN原结果](../../../openspec/changes/restore-company-profile-llm-led-generalization/workpaper-current-version-acceptance-20261125/result.md)保留真实84请求、原质量分及失败。未新增真实处理成绩。

当前API SHA-256为`5278df57188627da8f406c3895ab25a604d473abbbbbe032f0899a85a171a934`，年度JSON为`2a6e49968b47a92c72f6d322bcdf7eb54b287eef5f888a48043b398f28a8d19f`，年度MD为`558390b9a74090b1812c293ccd42b2d93a3f3ad27fe634cb51b86f01d7c2fc3b`。

旧交付保留冻结历史：[原21股API](current-api-6fm-20261125.json)、[原21股人读版](exports-6fm-20261125/annual_profiles.md)、[原20股API](current-api-20261125.json)、[6.FN安全18股](../company_profile_v1_acceptance_delivery_20261125/profiles.md)。`company_profile_http_research_20261125`隔离root只保历史HTTP运行依赖；当前消费使用本页唯一研究root及6.FO新命名交付。
