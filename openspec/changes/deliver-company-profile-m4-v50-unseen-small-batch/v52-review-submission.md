# v52后继正式复验提交A角有限审核

- Change：`deliver-company-profile-m4-v50-unseen-small-batch`
- 业务owner：`execute_published_task` / `record_published_source_review`；沿A/B流程v1连续同源修复包。
- 范围：原海泰发展600082.SH／开创国际600097.SH官方2025年报、原计划与cutoff2026-09-17；原160条件逐项保留，A角裁决新增十项，后继170正向＋31负例。

**B自检达标，提交A有限审核：170/170召回、448/448动态唯一准确率、数字错误0、31/31负例；整轮215.68秒、0/50000 token。** 实际442条事实＋6正文全量计分，Measurement仅计准确率。两家五阶段成功、无复用或前驱，重新查询与正式导出／磁盘一致。当前不归档、不扩批；生产not_authorized、规模质量false，等待A角独立原文审核。

## 结果与历史口径

|轮次|合同召回|唯一准确率|数字错误|负例|整轮|状态|
|---|---|---|---|---|---|---|
|原v50首次观察|66/160|193/262|14|27/31|202.63秒|失败原字节保留|
|v51授权第1轮|168/170|444/444|0|31/31|212.74秒|失败原字节保留|
|v52授权第2轮|170/170|448/448|0|31/31|215.68秒|达标待A有限审核|

两次后继正式授权已使用2/2，预算各轮独立，不互借。v51缺海泰p15分行业及分产品合计；实际缓存文本将“合计”放在前商品销售行末，pypdf夹具未覆盖。已在现有owner中拆回原行，v52新增四个配对事实，原正式对象与六正文语义保留。原160与后继170不可直接作可比首次成绩，不改写历史分数。

## 实现与直接验证

- 后继合同与原160逐项映射固定：海泰p23五项目结转均打印0万元，p24五租金464.97／1850.99／254.24／406.49／29.48万元，不能以合计替代项目或强行财表勾稽。
- 收入按原表物理列及单位保存，三笔仅上期收入拒绝本期借额；完整子公司与交易方、时点／时段确认时间保留；开创环太422469539.59元与ALBO818099682.13元分别绑定，国内贸易24593423.83元、空运输及正合计1028359860.15元成立。
- 六正文恢复服务、产品矩阵及真实交易确认机制；原代理净额、方信2025/9/30失控、合作捕捞、51%合并子公司及母公司不适用限定保留。
- 具名动作与实际投入分别按原句、原主体表达；混合伪销售删除，原合法父类保留，采购不推投入、空位不推动作。
- 完整v52接续五开关、精确资产入队及释义展开，默认身份不变。实际缓存page source完整局部owner通过；最终新v52＋v51及直接旧v50／v49组合**28 passed**。Ruff、格式、当前change＋三主规格严格校验、代码与文档范围diff检查通过；不声明全套测试通过。
- Review范围仅本轮P0/P1：修正跨页页脚主体串接、保留原合同栏目名称、拆分缓存同行末尾总计。旧测试预期未改，无范围内已知剩余阻塞；行业、量价、性能、正文精简、框架及无关旧问题后置。

## 证据入口

- [后继合同](v52-source-scope.json)、[原160＋十项映射](v52-contract-mapping.json)、[原十项漏冻证据](source-contract-omission-sidecar.json)。
- [正式来源及38模块冻结](v52-source-freeze-receipt.json)、[baseline73586fcc＋patch重建](v52-execution-code-reconstruction.json)、[执行patch](v52-executed-code.patch)。
- [正式执行脚本](v52-first-execution-script.txt)、[19份首次制品保护](v52-first-formal-delivery-preservation.json)、实际目录`data/checkpoints/company_profile_common_core/reports/m4_v52_same_source_repair`、`data/research/company_profile_common_core/m4_v52_same_source_repair`、`data/exports/m4_v52_same_source_repair`。
- [全部计分对象与来源](v52-source-review-evidence.json)、[原文引用库](v52-source-review-quote-bank.json)、[逐对象判断](v52-actual-object-decisions.json)、[31项负例](v52-negative-review-evidence.json)、[合同外对象](v52-contract-external-objects.json)、[正文原引用布局复核](v52-business-overview-source-audit.json)。
- [真实缓存路径局部核验](v52-local-cached-source-verification.json)、[v51/v52逐对象差异](v52-formal-semantic-delta.json)、[最终定向回归及静态回执](v52-validation-receipt.json)、[重读一致性](v52-delivery-read-consistency.json)。
- [同一快照只读机械回执](v52-readonly-verification-receipt.json)、[只读入口](v52-readonly-verification-script.txt)。此入口不执行run或改写业务库；计分脚本写当前复核材料并调用review owner，A审核不得直接作为只读命令重跑。A保留独立原文语义审核。
- [原失败保护](first-formal-delivery-preservation-completion.json)、[原复核保护](first-review-preservation.json)、[v51首次正式保护](v51-first-formal-delivery-preservation.json)、[v51失败复核保护](v51-first-review-preservation.json)。

## 限制与闭环

现有source_review的`expansion_gates_met=false`原样保留：第二家公司run传入整轮剩余192.367秒，该值被现有merged live-run保存在expansion阈值中，并与整轮215.681秒比较。它不等于本卡冻结的整轮300秒门槛，亦不构成扩批授权。只读回执独立核定本卡170/31、双100%、数字0、300秒/50000token条件全部成立；本次只提交A有限审核，不改变该字段或另开框架修复。

耗时含两家run→query→export、PDF读取、布局和本轮全部实际重试，结论限已有PDF解析缓存，不外推冷缓存、全报告或规模性能。开创p149原分部说明含应用软件／集成电路模板文字，仅保留原来源引用及原报告分部；未生成这些商品动作，未据此扩展合同。六份实质正文仍由真实本期渔业来源支持。

机械核验确认历史3182项、原首次18份、原复核6份、v51与v52各19份首次制品及19份既有工作区文件哈希不变。38执行模块可从基线与本轮patch重建。

B活跃开发时间未知（多次中断、未建精确计时）；正式运行时间如上。卡内子卡交接等待0；后继正式2次，重复缺口根因1次（首次局部未验证实际缓存行形态），已补真实入口回归。执行token0仅代表业务owner用量，不代表A/B开发审核消耗。

Git闭环：本人改动限定四业务模块、两新测试及夹具、当前change流程与后继材料；既有三tracked文档及未跟踪内容保护未触碰。提交／推送结果见最终交付回复；5.1仍等待A有限通过，不提前同步业务主规格或归档。

提交前范围diff核验：完整staged检查仅提示两份冻结执行patch的27个空上下文行标记（每行恰为一个空格，为统一diff语法）。这两份patch保留原字节及重建哈希；排除这两份证据patch后的全部staged文件检查通过，明细见`v52-final-verification.json`。
