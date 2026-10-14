# v55同源首次后继正式复验提交A有限审核

本次B正式自检达标，等待A独立有限验收。当前不归档、不启动下一对、不扩批；production=not_authorized、scale_quality_claim_allowed=false。

## 结果与有限范围

大名城600094.SH／青山纸业600103.SH，两份2025官方年报、cutoff2026-09-17、原307正向及44负例均未改变。**307/307召回；712/712动态唯一准确率＝706事实＋六正文；数字错误0；44/44实际负例通过。** 原合同分解为六正文＋134确定收入＋九冲突租金保留/unclear＋158具名动作，不把九冲突项说成确定收入交付。

| 公司 | 正文 | 确定收入 | 冲突行保留/unclear | 具名动作 | 实际事实 | 负例 |
| --- | --- | --- | --- | --- | --- | --- |
| 大名城 | 3/3 | 66/66 | 9/9 | 47/47 | 314 | 19/19 |
| 青山纸业 | 3/3 | 68/68 | 0 | 111/111 | 392 | 25/25 |

全部实际对象与checkpoint唯一对应，合同外收入、动作及原披露也计准确率；307条件与实际对象使用一对一绑定，Measurement仅计准确率，不重复召回。见[逐对象正式证据](v55-source-review-evidence.json)、[原合同](source-scope.json)、[负例](v55-negative-review-evidence.json)、[source_review owner回执](v55-source-review-owner-receipt.json)。这是B自检，不代替A独立原文审核。

## 原失败与来源冲突保留

原v54首次自检95/307、259/315、数字错误10、274.93秒均保留，未改分母或原成绩。[初版评分](first-source-review-evidence.json)、[首次18制品保护](first-formal-delivery-preservation-completion.json)、[首次38模块源码](first-executed-code-snapshot.json)仍为原字节。初版负例43/44的整页交易方误匹配已旁挂更正为42/44，两笔无具名供应方的仅上期煤炭采购误输出不判通过；见[旁挂更正](first-negative-review-correction.json)，不覆写原版。

用户指出大名城p23“万元”显著不合理、可能元，记录为来源判断，不冒充官方更正。p23九行原打印值、万元表头、Evidence及18个Segment/Measurement配对均保留并标unclear，退出确定三维答案支持，display允许但聚合、排序、跨公司比较禁止；p151189,019,138.54元独立成立。见[来源冲突证据](source-unit-conflict-evidence.json)及[保留的执行前提交](preexecution-source-conflict-submission.md)。

## 业务修复与正文核查

沿原选证据、接受、投影、查询／导出owner局部修复：毛利率说明停止；完整项目续表与单位；母公司先导；跨页收入成本与分部空位；原列合计；经营租赁可变收入及合计续页；关联本期、获批额度、上期及原交易方。真实负数抵销、打印零及合法合同外收入保留。

具名母子产品矩阵、木竹工艺投入、采运、维修、污水与原商品分别表达，不采购反推投入。参股、报告披露日、未并表、筹备、研发及原子公司范围保留。Review另修服务采购不能标商品、手工皂/洗衣液不能标服务，以及佰才邦成立说明默认集团与后续投资节污染；完整来源关系原句实际接入Evidence和正文，business_segment范围，不升格集团本期。

六正文已按独立原文实质核查，不把quoted或answered当内容合格。47条BusinessOverview逐条语义核绑定source_text/范围hash，未知文本不能自动通过；六正文同样绑定已审全文hash。正式对象与局部来源语义对应后逐对象判定，A可独立挑战正文与合同外输出。见[原文语义核查](v55-local-business-overview-semantic-audit.json)、[局部判定](v55-local-object-decisions.json)、[修复差异](v55-repair-difference.json)、[评分脚本](v55-first-review-script.txt)。

## 正式执行与机械核验

v55完整继承五开关、精确资产入队及主体展开，默认身份不变。新目录m4_v55_same_source_repair，原官方资产实际page source与完整局部输入一致。两家公司五阶段status=success，无复用、无前驱；只读重新查询＝正式query＝export＝磁盘。整轮从首次run入口到第二家公司export返回为**281.2912428788841秒、0/50000 token**，含PDF、布局及全部重试。耗时只代表已有PDF解析缓存；不外推冷缓存。

原expansion_gates_met=false保留：第二家公司请求的剩余171.08秒被现有owner用作其合并live-run复核阈值；本卡独立冻结门槛仍为整轮300秒，实测满足，不改原门槛字段或另建计时框架。生产及规模边界不变。

[正式完整执行](../../../data/checkpoints/company_profile_common_core/reports/m4_v55_same_source_repair/formal_execution.json)；[19制品即时保护](v55-formal-delivery-preservation-completion.json)；[只读核验](v55-readonly-verification-receipt.json)；[重新查询](v55-requery-receipt.json)。实际414来源页与本轮完整输入一致；运行实际layout17＋19＝36页，独立文本36＋37＝73页及独立布局73页与官方PDF重建一致，三个独立文本页按原pypdf回退核对。三组数字含义分开，不能称runtime有73layout。

3519项历史、18份原首次、6份原复核、19项既有改动及本次19制品保护通过，首次及v55各38模块源码可重建，执行码与冻结hash相同。见[v55代码冻结](v55-source-freeze-receipt.json)、[38模块源码](v55-executed-code-snapshot.json)、[正式复核保护](v55-review-preservation.json)。

## 验证、Review与停止点

最终组合：新样本完整owner/冲突用途/跨行贸易/服务采购、旧v54、旧v53 **16 passed**。直接旧v50/v54/v53组合 **11 passed、12 deselected**（与最终组合重叠六项，不能合称27独立测试）；旧v50完整owner及独立页与采购枚举在最终成立说明分支前通过，该分支不匹配旧来源，最终v54/v53已重验。Ruff、五文件格式、diff检查及当前change与三份直接主规格严格校验通过。先前失败及定位修正记录见[验证Review回执](v55-validation-review-receipt.json)，不声明全套pytest通过；旧无关失败继续后置。

首次观察1/1，后继正式1/2；不为用完次数再跑。4.1–4.4完成，5.1待A有限通过。行业增强、量价成本、性能、框架、正文精简和无关旧问题后置。

Git：本次改动完成正常提交与推送后记录于当前任务回复；仅本人业务代码、测试及当前change材料，19项既有改动未触碰。B活跃时间跨中断未完整计量，未知；本次正式1次281.29秒，无新增逐卡等待，来源裁决等待不混称开发时间。机械回执绑定同一快照供A复用，正文仍由A独立核原文。
