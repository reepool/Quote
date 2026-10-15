# v59同源修复正式交付：提交A有限审核

本轮B正式自检达标，申请A有限审核；当前change不归档、不扩批，5.1仍待A明确通过。范围仅同方股份600100.SH、美尔雅600107.SH两份冻结官方年报，cutoff2026-09-17。生产保持`not_authorized`，规模声明保持`false`。

## 结果与授权

- 原冻结合同618正向＝6正文＋328收入＋284具名动作，30负例，未缩减或以输出反填。美尔雅p23–24按A裁决以元解释，原万元表头及冲突说明保留；该处理仅适用于本次冻结官方document_version。
- 首次失败原样保留：39/618召回、145/189准确率（183事实＋六正文）、数字错误12、27/30负例；161.01秒、0token。原合同、首次结果和保护制品均未改写。
- v59正式：**618/618召回、1188/1188动态唯一准确率＝1182事实＋六正文、数字错误0、30/30负例**。同方897事实、美尔雅285事实；合同条件逐项绑定不同实际对象，合同外事实同样纳入逐对象复核。
- 正式进程启动至第二次导出178.452998秒；外层supervisor179.281626秒，0/50000token。计时包含registry、服务构造、PDF、布局和重试；结论限已有PDF解析缓存。两家五阶段成功、无复用或前驱；只读重新查询、正式query、export与磁盘一致。
- 首次1/1、必要后继1/2已用。已达标，不为消耗剩余次数重跑；剩余授权不转借后续样本。

## 实现差异及Review

沿现有选证据→接受→答案投影及唯一run/query/export owner局部修复，无第二套生产执行链。修复收入物理列与空格/打印零、跨行交易方和主体、原栏目、母公司及本期边界；恢复关联销售、出租、受托、终止经营、合营/联营、非全资主体。补完整产品矩阵、订单/开发/投运状态、PPP及销售/服务/租赁确认机制，保留失控和退出期间。

动作保持原主体和动词。安装、维护、技术支持分别保留source_verb并要求不同对象匹配；混合采购“采购商品/接受劳务”保留原栏目，不猜商品/服务或反推投入。实际业务投影同次收集已有别名展开结果，避免重复扫描，不增全局缓存或框架。完整v59身份接续五开关、精确资产入队及主体展开；默认身份不变。

范围Review：A类原收入身份/列/单位、主体物理行、实质正文、服务动词去重及评分漏检均已修复，未发现剩余范围内阻塞。B类正文精简、通用性能与框架治理后置；C类19项既有改动及无关旧失败未触碰。

## 正式评分绑定诊断与限制

原冻结评分脚本使用局部fixture的record_id连接合同外判定，正式Report/Evidence身份不同，最初172项未绑定。该诊断1016/1188完整保留，不作为业务语义错误或最终成绩，也未覆盖原脚本。新增正式评分脚本按精确原生语义字段和相同官方页重绑172项，不默认判准确，不改变事实、正文或合同。

全量1182条业务语义与已核局部结果一致，六正文原字节一致。另单列美尔雅p182合并表三个收入行的六个配对对象：subject_basis由局部direct_source_wording变为正式report_default_group_scope，subject_scope均consolidated_group，原合并表支持正式依据；栏目、主体、金额和期间不变。因此不宣称全部对象仅ID变化。

## 验证与保护

定向及直接旧样本回归实际完成28 passed、7 warnings（182.28秒），不声明全套测试通过：

```bash
PYTHONPATH=. /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_v59_technology_apparel.py tests/unit/test_research/test_company_profile_v58_energy_automotive.py tests/unit/test_research/test_company_profile_v55_property_paper.py --basetemp=/tmp/quote_v59_final_suite2 -q
```

Ruff、五文件格式、范围diff及四份OpenSpec严格校验通过（当前change与三份相关主规格）。评分反例8项通过，涵盖错误收入栏目/单位/金额/交易方、安装代替维护、错误万元、猜测商品及母公司空格借上期。

独立文本90页、布局91页；真实page source返回89页布局，其中60页与独立布局去空白完全一致。所有前置核验进程成功结束且检查后才冻结并启动。保护通过：3948项历史、19项既有改动、22份首次交付、11份首次复核、39模块首次代码、两份PDF、前轮归档161路径、39模块本轮冻结/重建、24项冻结验证输入及22份v59首次正式制品。最终复核材料另取新manifest，不覆盖冻结原件。

## 证据入口

- [冻结合同](source-scope.json)、[A单位裁决](source-unit-A-ruling.json)、[执行前裁决交接原件](preexecution-source-adjudication-submission.md)、[原始冲突](source-unit-conflict-evidence.json)
- [首次失败逐对象结果](first-source-review-evidence.json)、[首次制品保护](first-formal-delivery-preservation.json)、[首次复核保护](first-review-preservation.json)
- [v59正式逐对象及正文复核](v59-source-review-evidence.json)、[正式完整输入](v59-delivery-review-inputs/)、[正式评分脚本](v59-formal-business-audit-script.txt)
- [ID连接诊断原件](v59-first-audit-id-join-diagnostic.json)、[原生语义及subject_basis差异](v59-formal-native-equivalence-receipt.json)、[八项评分反例](v59-audit-counterexample-receipt.json)
- [只读重新查询与五阶段/三路核验](v59-readonly-verification-receipt.json)、[外层计时](v59-formal-supervisor-receipt.json)、[正式制品保护](v59-formal-delivery-preservation.json)
- [冻结代码与前置输入](v59-source-freeze-receipt.json)、[执行代码快照](v59-executed-code-snapshot/)、[范围Review](v59-preformal-review-receipt.json)、[来源一致性](v59-source-fidelity-receipt.json)、[最终保护核验](v59-postformal-protection-receipt.json)
- [28项测试日志](v59_final_tests3.log)、[Ruff](v59_ruff.log)、[格式](v59_format.log)、[范围diff](v59_scope_diff.log)、[复核材料保护](v59-review-preservation.json)

## 交接与效率

继续执行A/B流程v1，单一入口交接。请A独立审核原文语义与正式交付；B自检不等同A有限通过。5.1未勾选，不同步主规格、不归档、不启动下一对。提交前基线3072802a；仅提交本人可隔离改动并正常推送，最终提交号及推送结果由交接消息记录。19项既有改动保持原字节。

B活跃开发与交接等待时间无法准确还原，记为未知；不把正式0token或运行耗时充当开发/审核成本。首次观察1次、后继正式1次；评分ID连接诊断另列，未追加业务运行。当前无已知范围内P0/P1阻塞，待A有限审核。
