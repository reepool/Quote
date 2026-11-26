# 当前39股研究画像（2026-11-27，6.FR）

当前唯一研究读取根：`/home/python/Quote/data/research/company_profile_research_v1_20261123`。

[晟楠/雅戈尔/中集当前人读版](three-reviewed-current-6fr-20261127.md) · [三股当前JSON](three-reviewed-current-6fr-20261127.json) · [王府井当前人读版](600859-current-6fp-20261126.md) · [6.FO历史39股API](current-api-6fo-20261126.json)

当前可读1094条内容、1677个可见指标。数量包含重复读取、范围限制或保留原始写法的指标，不代表这些对象已逐条独立确认正确。API同时交付缺项、原因、建议补读、来源、作者及审核状态；缺项可交付，不强求全部补满或供应链精细化。完整39份以实际API为准；旧快照不覆盖，本轮仅新增三股当前投影，不重导39家公司。

## 本轮三类真实源核

晟楠、雅戈尔、中集三份保存稿在不重抽/选章的情况下，由Grok high独立源核、DeepSeek各一次合并修订、Grok终核。预设4个风险均被新初核指出，4项修订经A原PDF独核有源：晟楠主营收入按公司主营合计表达、军用客户和投标统计保局部限定及unclear；雅戈尔区分预售订单与交付结转营业收入；中集45.04%明确为交易完成后条件比例，不当2025年末已实现值。

另纠正一处服装业务研发/供应链范围；品牌定位措辞收紧有源但不计经济错误修复收益。两条新增财务候选在本地源检查拒绝且未送终核，保持未获，不追加调用追齐。模型并未自动解除旧A限制，A按当前事实hash独立核原文后才解除4处旧限制；没有新增加的安全隔离。晟楠旧模型限制误套新hash的消费问题在真实运行封存后零调用修复，原响应/归因/usage不变。

三股可读数由晟楠22/35→24/39、雅戈尔19/25→20/28、中集20/30→20/31（内容/指标）；合计恢复3条描述及8指标，另有既有描述修正，不把相同公司内容重复计为新知识。原writer只新增3安全后继及1条必要封存父链，主root103记录；其余36完整profile保持，旧403文件仅本页获准更新。

本轮实际9物理/283804供应商total及账本/0恢复/751.569组秒，费用unknown；当前8000端口39+2missing批量3.714秒，三详情0.739–1.293秒，实际API/原reader/fresh完整等预期。结论是对这三份已知问题稿确有帮助，不是新的未见验收/总体99%；完整边界见[6.FR结果](../../../openspec/changes/restore-company-profile-llm-led-generalization/workpaper-three-source-review-observation-20261126/result.md)。

## 前轮查询提速及王府井补读

原writer只追加1份王府井补读记录，主root共99份记录。原模型作者、来源、独立使用限制、source_only、指标位置限制、缺项和同报告谱系均保留；其余38家完整画像不变，王府井已有37条/80指标和全部事实行/限制也保持。

王府井现46条/101指标。新增读取9条/21指标中，实质新增主要是公司营收10,149,906,153.24元、上年11,371,675,304.54元和同比下降10.74%这一项规模说明；其余8条主要重复既有业态及客流内容，不当新增独立知识。未获同店收入下降3.75%：原PDF17/18页有披露，但对应片段未选入模型供源；返回的同店客流不能替代，不追加调用强求补齐。

实际8000端口冷单股23.604秒、热单股0.589–0.982秒、10股0.908秒、39股2.174秒、4并发2.917秒；原完整39内容等冻结预期，补读后其余38不变。原reader按文件内容hash复用严格校验，变稿仍重校，首次冷读仍慢。补读共3物理/90243账本、0恢复、费用unknown，初核review v14完成且A按原PDF独核9项有源，未新增人工限制；这一个有限任务不证明总体检错率提升或99%。

光大证券601788.SH、国信证券002736.SZ仍无画像，显式查询列missing，不推断未披露或模型失败；普通查询不重跑。

## 质量与使用边界

当前整体准确率尚未充分评估。6.FN正式V1验收未通过，原首发/终稿、95%/99%门槛及失败结论不改；安全研究交付不回算自动成绩。历史`512/523 = 97.8967%`仅属v12原冻结批次，不能代表当前研究版。未独核内容保持原状态，不通过删难项或重算旧分宣称达标。

这是带来源、审核和缺项说明的研究交付，尚未取得生产、100家/全市场、DCF或交易使用授权。模型已核、A独核、可引用原文和已知错误隔离分开呈现；completed仅表示任务执行结束。

## 读取与按股补读

实际API进程通过既有私有`.env`设置 `QUOTE_COMPANY_PROFILE_RESEARCH_ROOT=/home/python/Quote/data/research/company_profile_research_v1_20261123`。本轮已授权普通重启并安装单次研究任务窗口；共享模型/调度配置不改，不授权全市场、DCF或交易。

读取目录：`GET /api/v1/research/company/annual-profiles`；单股：`GET /api/v1/research/company/600350.SH/annual-profile`；批量：`POST /api/v1/research/company/annual-profiles/query`，请求体例如：

```json
{"instrument_ids":["600690.SH","600350.SH","601788.SH","002736.SZ"]}
```

先查看返回的information_gaps和suggested_acquisitions，再针对具体缺项只读规划。例如山东高速sm-12原指标1的租赁收入主体限制：`POST /api/v1/research/company/600350.SH/annual-profile/enrichment-plan`，请求体为：

```json
{"request_id":"research-600350-rental-scope-1","targets":[{"element":"scale_and_mix","question":"核对2025年报其他业务租赁收入的主体与表列口径，对应现有sm-12指标1限制；保留其他业务合计与租赁收入的区别。"}]}
```

计划绑定当前work/report/hash，返回execution_started=false；计划不会解除现有限制，不隐式调用模型或执行补读。

首读、后台复核及指定股票/具体问题补读通过显式HTTP任务入口执行，必须沿有限授权策略；不会自动循环补齐。本轮仅王府井一次enrich，已经用尽，原任务进度/结果仍可查，新请求429，同ID同内容复用。凭据和下一窗口要求见[研究任务API说明](../../../docs/research/company_profile_http_tasks.md)。

## 交付核验与历史

本轮读取、真实HTTP补读与源核实物见[6.FP结果](../../../openspec/changes/restore-company-profile-llm-led-generalization/workpaper-fast-read-live-enrich-20261126/result.md)。[6.FO原合并核验](../../../openspec/changes/restore-company-profile-llm-led-generalization/workpaper-unified-39-research-20261126/b-result.json)和[6.FN原结果](../../../openspec/changes/restore-company-profile-llm-led-generalization/workpaper-current-version-acceptance-20261125/result.md)保留首次结果/分数，不回算。

原6.FO的39股API/年度JSON/人读版及各hash继续冻结；本轮不重复导出旧39股。最新单股由原reader新实例与实际API完整比对，Markdown复用原annual renderer。

旧交付保留冻结历史：[原21股API](current-api-6fm-20261125.json)、[原21股人读版](exports-6fm-20261125/annual_profiles.md)、[原20股API](current-api-20261125.json)、[6.FN安全18股](../company_profile_v1_acceptance_delivery_20261125/profiles.md)。`company_profile_http_research_20261125`隔离root只保历史HTTP运行依赖；当前消费使用本页唯一研究root及6.FO新命名交付。
