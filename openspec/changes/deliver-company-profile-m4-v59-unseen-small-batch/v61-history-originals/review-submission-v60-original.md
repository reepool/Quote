# v60同源正式复验交接（待A有限审核）

本次正式自检达到冻结门槛：**354/354、733/733、数字错误0、40/40负例**。请求A对明星电力600101.SH、亚盛集团600108.SH及本冻结来源/条件作有限审核；当前不归档、不扩批，5.1继续等待A通过。生产`not_authorized`，规模声明`false`。

## 正式结果与有限范围

原354＝六正文＋195收入＋153动作、40负例保持；仅按A本次裁决，对600108.SH-action-014后继采用`source_actor=亚美特公司`及仅原称别名，p9、产品、销售、named_subsidiary、本期均不变。原合同及未生效proposal保持原字节，[裁决映射](v60-actor-adjudication-mapping.json)与[后继合同](v60-source-scope.json)单列。未建立法律全称全局别名，也未合并财务主体。保存输出先验证354/354，原353/354局部回执保留。

正式实际727条事实（明星406、亚盛321）＋六正文＝733个唯一计分对象，逐对象计入合同内外输出；收入195/195、动作153/153、正文6/6，40负例实际评估通过。准确率分母由正式checkpoint重新计算。完整原生业务语义（包括subject_basis）多重集、六正文逐字和支持记录的语义多重集与已审核局部交付一致；报告包络/证据及记录身份差异另列，未对重复键强行配对。主体简称实际正式发布，非局部结果替代正式成绩。

完整v60身份接续五累积开关、精确资产入队、主体展开及入口预算语义，默认身份不变；原两PDF、cutoff2026-09-17、plan原字节不变，新目录`m4_v60_same_source_repair`。两家五阶段成功、无复用/前驱；独立重新查询与正式查询、导出、磁盘一致。

计时从进程启动至第二次导出返回**166.078238秒**，包含导入、registry、服务构造、PDF、布局及全部重试；外层至进程退出166.744665秒，返回0且未超时；共享0/50000token。两PDF日志均显示artifact cache hit，耗时仅限本次已有解析缓存条件，不推冷启动结论。原门槛字段不改写，未扩大质量声明。

## 历史与本次差异

旧v59归档964309b6有效。本change首次1/1已用：174.71秒，原匹配169/354回执保留；旁挂修复接受劳务/劳务等价匹配后190/354＝0正文＋135收入＋55动作。441/597准确率、数字0、36/40负例不变；591事实＋6正文首次原件未覆盖。首次日志有新解析，不能机械称为全缓存耗时。

业务修复已在46df55db：注册许可与本期真实经营分开，原Evidence/源段保留unclear且退出确定正文支持；原销售/出租动词、采购续行主体及空位恢复；稀疏/跨行收入成本列、分部、抵销、客户及合营关系分别绑定；恢复投运、种植、加工、工程和服务及六正文机制。局部原成绩353/354、733/733及A主体裁决轨迹保留。本次仅落实指定条件映射、正式复验及材料，未更改业务代码。

## 验证与Review

复用与当前41项代码/夹具SHA256一致的33 passed回执（新5项及同方/美尔雅、广州/上汽、大名城/青山直接回归28项），Ruff/格式回执有效。本次另验证保存输出全354项、主体原称正例和两个错误主体反例，并执行正式全量逐对象核查、六正文等价核对、40负例、独立只读重新查询及数据库五阶段核验。范围Review剩余P0/P1为0；未声称全套测试通过。

正式前4174项历史、19项既有改动、22份首次制品、6份首次复核、39模块首次快照、两PDF及41项测试快照全部核验成功结束，检查结果后才冻结39模块并启动。正式写入/日志writer结束后立即保护全部正式制品。后置保护与严格校验回执见下。

## 单一证据入口

- 原合同：[source-scope.json](source-scope.json)；[首次冻结](first-source-freeze-receipt.json)；[首次评分](first-source-review-evidence.json)；[首次旁挂更正](first-source-review-correction.json)；[首次保护](first-formal-delivery-preservation.json)。
- 主体：[原文冲突证据](source-actor-conflict-evidence.json)；[原proposal](proposed-actor-correction.json)；[A生效映射](v60-actor-adjudication-mapping.json)；[后继合同](v60-source-scope.json)。
- 局部：[原353回执](v60-local-source-review-evidence.json)；[裁决后保存输出354回执](v60-local-adjudicated-source-review-evidence.json)；[33测试及快照](v60-local-validation-receipt.json)。
- 正式：[执行代码冻结](v60-source-freeze-receipt.json)；[执行脚本](v60-formal-execute-script.txt)；[外层监督](v60-formal-supervisor-receipt.json)；[writer结束后立即保护](v60-formal-delivery-preservation.json)。
- 全量：[正式事实/六正文/40负例逐项](v60-formal-source-review-evidence.json)；[checkpoint真实提取](v60-formal-delivery-review-inputs/)；[局部/正式语义等价](v60-formal-native-equivalence-receipt.json)；[五阶段/独立重新查询/三路一致](v60-readonly-verification-receipt.json)。
- 保护：[前置成功核验](v60-preflight-protection-verification.json)；[正式后保护及严格校验](v60-final-verification-receipt.json)；[本次审核材料保护](v60-review-preservation.json)。
- 交付：[第二次导出](../../../data/exports/m4_v60_same_source_repair/)；[执行与观察](../../../data/checkpoints/company_profile_common_core/reports/m4_v60_same_source_repair/)。
- [任务表](tasks.md)；[A/B流程v1](/home/python/Quote/docs/development/ab_role_development_workflow.md)。

## 授权与Git边界

首次1/1、后继**1/2已用**；达标即交A，不使用剩余1次重复证明、不转借下一轮。当前不启动后续样本、规格同步或归档。行业增强、量价成本、性能、正文精简、框架及无关旧问题后置。

19项既有改动完整保留，不暂存、不提交、不触碰。本次仅提交可隔离裁决、正式证据及任务/交接文档；代码执行身份为46df55db中的冻结39模块，具体本次提交及正常推送结果见最终报告。

B活跃时间跨中断无法精确统计，本次续作约15分钟（估算），交接等待未混作开发时间。当前正式次数首次1＋后继1；未追加重复正式运行。同根因直接回归此前已通过，本次无新增代码修复。A/B流程v1：本次连续自主推进至正式达标，下一步等待A有限审核。
