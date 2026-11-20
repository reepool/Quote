# 6.ED：已有复核意见已落实到研究画像/API

本轮没有重新抽取或调用模型。沿原writer/read owner生成十五份画像的新研究快照，普通分级内容400条事实、648个指标；十四份画像完全继承，分众除两条重大变化补充对象外的内容保持。不是新增十五家公司，也不是S2通过或生产上线。

可看[分众画像](002027-profile.md)、[分众API实物](002027-annual-api.json)及[十五家公司目录](catalog.json)。八要素、可选供应链和模型自主特色继续按原范围交付；仍待核或有源不确定的事实保留其原标签，不为补充项追求全绿。

## 两条意见如何处理

| 对象 | 新研究读取 | 原因及剩余信息 |
| --- | --- | --- |
| 分众F-MC-01 | 仅源参考，普通fact为null | 原披露项目勾选“不适用”，不宜扩大为业务、产品或服务确定未发生变化；不认定实际发生业务变化。合并范围那半句不人工拆成新事实。 |
| 分众F-MC-02 | 整对象隔离，普通fact为null | 把2025年末媒体数量错配到2024基期，并配了另一比较期间的变动率。未人工改写日期、数值或拆取部分指标。 |

依据为[6.EC独立只读对源](../workpaper-review-channel-check-20261120/finding-spot-check.json)，不是把模型意见自动当真值。新[使用决定](use-decisions.json)保原七项分众独核决定和原依据，另加两个当前fact/hash绑定的限制；其他公司决定原样继承，没有伪称本轮全稿重审。

API的`facts`保状态、具体原因和证据编号，两个payload为null；`information_gaps`中的`material_changes`说明未可靠取得的结论及建议补读章节。不把局部问题变为整家公司失败，也不把“没获取”改成“确定没有”。京东方F-OI-04的跨页引用覆盖限制保持，0条新模型意见不能将其洗掉。

分众原初核`failed`、实际作者/复核模型及原调用记录保持；本轮不伪造高阶复核完成、修订采纳或新任务。原始对象仍在writer记录供审计，普通研究API/正文不再消费两个对象。未来需要时可沿原接口生成[单股票补读计划](optional-enrichment-plan.json)，本轮仅验证计划，不执行补读、不隐式联网。

## 如何读取

新根为`/home/python/Quote/data/research/company_profile_reviewed_20261120`。没有修改共享配置或服务当前根；使用时在独立研究服务进程显式设置`QUOTE_COMPANY_PROFILE_RESEARCH_ROOT`为此目录。现有API为：

```text
GET /api/v1/research/company/annual-profiles
GET /api/v1/research/company/002027.SZ/annual-profile
POST /api/v1/research/company/002027.SZ/annual-profile/enrichment-plan
```

该根仍是研究用途；读取及计划不调用模型。生产任务、DCF与交易未获授权。旧研究根及旧API快照未改，未启动公司仍不伪造画像。

## 验证与边界

[直接读回核查](verification.json)验证原query、export、新实例一致，全部原事实/指标/来源/生成及质量复核不变；十四份投影完全不变，分众其他对象完全不变。旧254份验收证据、15个writer文件、33份EC材料hash在前后均成立。完整临时导出只用于比对，不重复永久保存全套原稿。

局部现有单元测试10通过、16不相关项未运行（3.05秒），覆盖独核绑定、局部隔离覆盖模型completed、坏身份/hash/引用拒绝及限制显示。HTTP目录/分众详情/京东方详情/仅计划均200，未启动的南京银行404，均与原read owner数据一致，文件未被读取或计划改写；见[API核查](api-check.json)，不是新模型质量证明。首次本地测试使用未带交易所的股票ID得到404，已按既有规范改为`002027.SZ`，只修测试URL，没有扩大股票别名合同或修改业务代码。

验证命令（每次先`source scripts/quote_dev_env.sh`）：

```text
/home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_workpaper_delivery.py -q -k 'restriction_overrides or invalid_or_stale or independent_checked or source_checked_fact_with_explicit'
OPENSPEC_TELEMETRY=0 DO_NOT_TRACK=1 openspec validate restore-company-profile-llm-led-generalization --strict
git diff --check
```

本轮一次性投影/本地ASGI核验脚本在本人QuoteBak根运行，完成后清理；没有新增常驻operator或业务循环。API只在核验进程设置新根、禁外部socket，不修改共享服务配置。

S2确认未通过、当前方案结束的结论保持；不补未启动五家、不开第41家、不进S3或生产。本轮只是把已有纠错收益落实到可消费结果，不声称全稿99%或通道长期稳定。下一步优先用现有分级API做真实研究需求核对，按需选股精读；不为两个补充项再付费追完美。新的正式扩大方案仍需另行裁决。

本轮无生产代码改动。Review只核当前数据投影/绑定/读取，不扩修旧入口或并行代码；清理及验证命令随交接记录，必要证据仅此一套，正式源、有效缓存、旧失败/尝试及既有工作区改动保护。
