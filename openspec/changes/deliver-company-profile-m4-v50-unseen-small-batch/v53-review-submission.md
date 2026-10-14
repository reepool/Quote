# v53正文冲突补正后继正式交付：提交A角有限审核

- Change：`deliver-company-profile-m4-v50-unseen-small-batch`；沿A/B流程v1连续包。
- 范围：海泰发展600082.SH／开创国际600097.SH原官方2025年报、原计划、cutoff2026-09-17；原170正向和31负例不变，新增1正文冲突负例，共32。
- 原v51/v52授权已用2/2，本次另授权的唯一正式轮已用1/1；不追加运行。

**B正式自检达标，提交A有限审核：170/170召回、448/448动态唯一准确率＝442事实＋六正文、数字错误0、32/32负例；217.74秒、0/50000 token。** 两家五阶段success且无复用/前驱，重新查询、正式导出和磁盘一致。A尚未授予有限通过，当前不归档、不扩批；生产`not_authorized`，规模声明`false`。

## 本次P1与实际变化

v52三维答案曾直接引入开创p149“生产及销售应用软件”“生产及销售集成电路”。同页罐头鱼产品／渔货贸易的身份，与p8–9远洋渔业、加工、贸易、运输及p84鱼种／罐头／鱼柳等收入来源相冲突，不能以“官方原文连续匹配”判成确定经营事实。

现有owner保留该完整原始Evidence和source_text；在已有uncertainty中明确`official_business_description_conflict:unclear`，展示为低置信度并记录使用限制。核心答案及resolved-core复用排除该不确定记录，使用p8–9、p84及同页正确分部说明支撑原实质业务。没有编造更正后的原文，也没有删除全部软件／集成电路词：正常软件、集成电路业务的合成正例明确通过。

[逐对象差异](v53-formal-semantic-delta.json)使用多重集合，保留重复对象计数：海泰156、开创286条原生事实语义全部保留，只一条开创原文增加不确定性。海泰三个正文原字节相同；开创三个正文去除冲突段的确定引用及支持ID，正确产品、收入机制和分部边界保留。110收入、54动作、缓存合计行及原31项负例无回归。新增第32负例实际核原文仍保留、unclear存在、三个答案无冲突业务肯定及无冲突支持ID。

正文自检现在分别核原文是否连续、产品／业务／收入语义是否一致、冲突记录是否限定、是否进入确定答案或其依据。源quote匹配只能证明原文，不能代替语义准确性。本轮原生不确定来源记录仍作为带限定的实际输出纳入准确率，全部事实和六正文按实际ID唯一计分，未预填448，Measurement只计准确率。

## 首次记录与范围限制

|轮次|召回|B准确率|数字错误|负例|整轮|状态|
|---|---|---|---|---|---|---|
|原v50|66/160|193/262|14|27/31|202.63秒|原首次失败保留|
|v51原授权第1轮|168/170|444/444|0|31/31|212.74秒|失败保留|
|v52原授权第2轮|170/170|448/448|0|31/31|215.68秒|B原自检保留；A三正文冲突阻塞，不授予通过|
|v53额外唯一轮|170/170|448/448|0|32/32|217.74秒|达标待A有限审核|

A角v52阻塞结论及新授权已旁挂在[tasks第6节](tasks.md)，原合同、分数、首次制品、复核和执行代码全部保留原字节。此次不变更原170正向分母，新增负例独立计数；相同准确率分母不等于原正文正确。本轮耗时含PDF读取、布局和实际重试，限已有PDF解析缓存条件，不外推冷缓存或规模性能。

`source_review.expansion_gates_met=false`原样保留。A已核实既有merged复核把第二家公司剩余执行时间保存为扩批耗时阈值；本轮该字段上限为195.846秒，独立冻结的整轮300秒门槛已满足。详见[有限合同回执](v53-finite-contract-gates.json)。本次不修计时或框架，也不允许扩批。规则验收只覆盖这两份来源和已声明边界，不宣称全部官方冲突可自动裁决。

## 验证与证据入口

- [170正向＋32负例冻结](v53-source-scope.json)、[旧合同逐项映射](v53-contract-mapping.json)、[代码冻结](v53-source-freeze-receipt.json)、[38模块重建](v53-execution-code-reconstruction.json)。完整v53五开关、精确资产入队和主体展开接续，默认身份不变。
- [真实缓存page-source局部闭环](v53-local-cached-source-verification.json)、[独立p149官方原页夹具](../../../tests/fixtures/company_profile_v53_official_segment_conflict.json)、[来源一致核验](v53-independent-source-verification.json)。旧98页文本／29份布局快照复用同字节核验，p149已独立重读文本与布局。
- [全部实际对象及独立语义判据](v53-source-review-evidence.json)、[逐对象决定](v53-actual-object-decisions.json)、[原文引用库](v53-source-review-quote-bank.json)、[正文与冲突审核](v53-business-overview-source-audit.json)、[32负例](v53-negative-review-evidence.json)、[合同外输出](v53-contract-external-objects.json)。
- [唯一正式脚本](v53-first-execution-script.txt)、[19首次制品保护](v53-first-formal-delivery-preservation.json)、[8正式复核保护](v53-first-review-preservation.json)、[重读一致性](v53-delivery-read-consistency.json)。目录分别为`data/checkpoints/company_profile_common_core/reports/m4_v53_same_source_repair`、`data/research/company_profile_common_core/m4_v53_same_source_repair`和`data/exports/m4_v53_same_source_repair`。
- [只读机械入口](v53-readonly-verification-script.txt)、[同快照回执](v53-readonly-verification-receipt.json)。本入口不执行run／export、不写业务库或审核证据，A仍独立核原文语义。计分脚本写本轮材料并调用review owner，不能作为A只读命令直接重跑。
- [定向验证](v53-validation-receipt.json)：新旧直接组合**33 passed**；最终冲突／使用限制**5 passed**为重复专项复验；共享接受／核心投影**50 passed**。Ruff、相关格式、当前change及三主规格严格校验通过，不声明全套通过。reads.py仅核本次改动行，旧无关格式保持原样。
- [范围Review](v53-scope-review-receipt.json)无剩余已知P0/P1；行业增强、量价成本、性能、正文精简、框架和无关旧问题后置。[基线保护](v53-repair-baseline.json)与只读回执确认3340项历史／旧轮文件及19份既有工作区文件哈希不变，首次制品与正式复核均保持。

B活跃开发时间未知，未建立精确开发计时；子卡交接等待0，额外正式1次，正式用量0token仅为业务owner统计。Git闭环只提交本人改动，原三tracked脏文档及既有未跟踪内容不触碰；提交／推送结果见最终交付回复。5.1仍等待A有限通过。

提交前完整staged diff检查仅提示冻结执行patch的15个空上下文标记（每行一个统一diff必需空格）。执行patch原字节及重建哈希保留；排除此证据patch后的全部本人暂存改动检查通过，详见`v53-final-verification.json`。
