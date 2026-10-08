# v32同一对样本正式复验提交

B角结论：5.2、5.3已完成，本轮达到声明来源范围的有限提交门槛，提交A角审核。当前change保持未归档，扩批暂停；生产not_authorized，规模质量声明false。

首次正式结果：**51/51召回，159/159准确率，关键数字错误0；188.694143秒，0/50000token，五阶段实际无章节复用。** 召回按六实质正文＋38收入条件＋七角色；准确率按本轮实际153条接受事实＋六个已答正文动态唯一计数，未预填159。Measurement只计准确率，角色多证据和别名不重复召回。

## 主体、期间和状态正式贯通

凤凰主营、产品服务、收入三个正文均实际交付“锂电池业务本期仅统计一、二月”、原凤凰新能源后更名为凤锂新能源（惠州）有限公司，以及2025年2月21日丧失控制权、不再将其纳入合并范围的原句。这些限制来自已接受的p14–15原文和支持事实，不是标签或answered状态替代正文；三维查询、导出及磁盘一致。原有限期销售仍为named_subsidiary且保持原凤锂主体；未转为全年当前控制业务。

38项收入和七角色均按冻结原页复核，无本期／上期错列、截断地区、下游用途伪Activity或租赁误作设备销售。主营／其他business_type、revenue_timing、元／亿元、合并／母公司与父子口径保持；四项管理／出租收入仍正确。未知商品映射继续pending，量价后置。所有153事实与v31内容一致，浙江三个正文也一致；该比较仅用于回归检查，本轮准确率重新从原文逐对象生成。

## 首次执行与来源范围

沿原计划070fc963c73a23baf8977602992cb4d594db9460edf2f4162553d14e5d84590e，浙江新能600032.SH及凤凰光学600071.SH、原官方版本、cutoff2026-09-17与同一51项实质合同。完整身份rules=company_profile_common_core.v1、owned_page_facts=v8、material_input_facts=v1、revenue_sentence_repair=v32，五开关全开，默认身份不变。业务代码仅登记后继身份，沿用A角认可的状态投影，无重新开发或新owner。

完整页回归通过后，执行前重复核验63个官方页，合同主体／原文条件、收入／角色和PDF版本与v31一致，新冻结receipt及合同哈希早于首次execute，队列135不变，冻结没有enqueue。独立目录m4_v32_energy_optics_state_acceptance，现有owner先浙江再凤凰，分别run→query→export；第二家使用剩余50000token。计时首次execute至第二export返回，包含全部重试；实际transport_retries均0。五阶段runtime.reused_scope_ids及predecessor_lineage均空，不能以enqueue复用标记替代。

## 可审查材料

- v32-source-scope.json、v32-independent-frozen-source-pages.json、v32-source-freeze-receipt.json：同一51条件、官方63页及执行前冻结、五开关和38模块代码哈希。
- v32-actual-object-decisions.json、v32-actual-object-supplemental-source-pages.json：所有实际接受对象从原文重新核主体／动作／原生栏目／本期金额／单位和完整证据；三维另核完整期间／控制状态。
- v32-source-review-evidence.json：159个唯一对象、51项来源条件、runtime／缺项及实际资源；不存在补正分数或预填分母。
- v32-first-formal-delivery-preservation.json、v32-formal-result-preservation.json：18份首次交付＋1正式source-review原字节保护。
- v32-completion-receipt.json：三维状态限制、旧事实与浙江正文未回归、保护及测试／Review回执。

## 验证与保护

PYTHONPATH=. /home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_v31_energy_optics.py：**40 passed、7 warnings**，覆盖v31/v32身份、两公司完整owner及独立源页，包含原主体／计划／出租方向／下游边界及三维状态。Ruff、OpenSpec严格校验、diff检查通过。Review只检查本轮身份登记和当前业务验收，未发现新增阻塞。

957项历史文件（含原922及v31首次结果／复核／补正材料）哈希通过；v31原50/51、158/159失败、初版判断和更正、执行代码重建补丁均保留原字节。新18份首次交付和正式source-review保护通过，执行后38模块代码哈希未变。既有三份脏文档及未跟踪工作区内容未触碰，只提交本轮本人改动。

结论仅限51项声明来源及实际检查页，不推导全年报完整性。A角有限通过后才同步主规格／有限归档／安排下一对样本。量价、成本、行业增强、正文精简、去重框架、性能和五项既有失败继续后置。
