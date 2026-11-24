# 6.FH 三份当前稿源复核：同原窗口完成，人工保护仍必需

原TaskService.review_batch已持久化三份真实审核后继，三份均`completed_with_open_findings`。本轮共5物理、0恢复、158,954个供应商total tokens，全部usage有据、费用unknown；没有重选章/抽取、追加阶段、源码/提示/共享配置改动或生产启用。原20股仍647可读/1050指标，原277业务文件不变。

首入口和接续分列：首次三家虽然exit0，但在本人source适配断言失败，均`incomplete/not_reviewed`，21.763181秒、0物理/0token。实际错误是typed ReportIdentity与JSON dict直接相等比较，不是模型问题。首入口provider已创建，但尚未发送。原marker/result/预算/终端checkpoint/错误日志已原字节保留于`b-first-entry/`。A通过`a-execution-ruling.json`明确允许唯一接续ID和分目录；修正使用原ReportIdentity做typed equality，三份正确身份接受/错误版本拒绝均有实际函数证明，原source及pages不变。接续418.604723秒，task seconds在启动前仅取首次deadline减当前时间再留50秒；每股及组均未重置窗口或增加累计额度。首失败不改判成功。

| 公司与真实后继 | 实际阶段/资源 | 新意见与继承意见 |
| --- | --- | --- |
| 华阳600348：`bp-work-52157e13cdbf80605eb26a27` | 初核1物理；41,033 total；Grok313.134秒 | 新2条组件限制；继承3条旧意见 |
| 江苏金租600901：`bp-work-47b8196e8bb0fe8fb4b1b4d0` | 初核1物理；47,985 total；Grok232.188秒 | 新0条；继承1条旧意见 |
| 学大000526：`bp-work-721e6851edbaa279f67917e5` | 初核+一次合并修订+终核3物理；69,936 total；287.055/4.515/94.415秒 | 新7条；继承3条旧意见；实际终核接受4候选 |

这些耗时为各物理请求elapsed，不冒称全流程耗时；供应商total包含其报告的推理token，不能改用可见completion之和。原任务账本的费用状态为unknown，未将供应商其他币种/内部ticks擅换人民币，也不承诺整组人民币总额。每股累计均在6物理/1m/3恢复内，每逻辑未发生恢复。

此次仅观察已见人工风险中的5对象：F007[0]、F009[4,5,6]、F023[0,1,2]、F22[1]和整条F12，共1整事实+8指标位置。按A本轮独审，新的WF001命中F009三位置；F007/F023/F22/F12仍miss，剩余5指标位置和1整事实继续依赖原A保护。不是对原7整事实+12指标位置的全部复验，也不是新样本准确率。

华阳WF002额外限制F014[4,5]，属于上述旧人工风险范围之外的新增2位置，不计作原8位置修复。WF001/002共5位置保原正文/安全指标，坏指标不进普通numeric payload。API的F009 metric_gaps[metric_index=4/5/6]同时标`independent_and_model_source_check`、`model_finding_ids=[WF001]`；F014[4/5]标`model_source_check/WF002`。每条gap均给原name、不带坏value；F12仍`fact=null/withheld`，未被本轮复核解禁。继承意见以`inherited:`保留来源，不能计为新检出。

学大4个实际接受候选为F01、F10、F11及新增`review-add:R6`，均经过自己的本次真实终核。R4/R5候选金额有源，但金额单元格断行导致本地metric source检查拒绝；它们未进入终核、未被采纳，不能写成高阶拒绝或原报告未披露。原raw/拒绝notes和wire保留。R7继续为补充遗漏建议，不追补、不追加付费。A语义结论与模型/本地消费状态分别成立，不以计数替代独审。

三份实际初核均逐字绑定原v7 packet、原当前draft与fact hashes；没有旧quality/UseReview/人工备注进入新请求，保原selection/extraction作者及两份回执。精确证明见`b-actual-initial-wire-verification.json`。源适配修正未推断或改数值/方向/主体，不修改原输入事实或人工决定。独立UseReview新增0不能被解释为无需人工；本轮A仍独核真检出、漏检及候选来源。

当前交付入口为`data/research/company_profile_automation_probe_20261124/current-draft-probe-api.json`，原owner导出在同根`exports-source-probe-20261124/annual_profiles.json`及各股Markdown。新实例API与年度导出三份完整profiles一致。probe根当前88可读/169指标，其中本次三审核record为80可读/127指标，另8条/42指标是原华阳6.FA补充reading，保原内容，不计本轮新增。此独立probe不替换原20根或旧API。

原字节保留的8父record与三新后继共11份writer。所有实际新/修订内容、完整当前fact/hash、物理页和新/继承finding分列于`b-content-source-index.json`；资源、5份原wire及账本索引见`b-resource-summary.json`和`b-runs/`。R4/R5等本地未采纳候选可在学大002原wire及writer质量notes核对，不能用其他final补称其已核。

验证：原writer→新ReadService→年度导出profiles相等；原277业务文件、9个冻结应用文件、3份原planned、8父record及首入口全部已记录文件hash保持。未重跑旧123/全仓测试，未加载HTTP/新服务。收口脚本曾因错误导入SectionPacket及误用source_refs字段退出，已仅修本人只读收口脚本，复用已生成API/导出完成索引；不影响冻结运行、模型请求或业务源码。所有首次执行失败和拒绝均未删除或改判。

结论：current-draft真实入口已形成有限业务证据，且明确仍存在语义漏检；不能据此称99%、自动化免人工、正式新20或扩100/生产。旧97.8967%和正式不通过不重算。后续规模与语义裁决归A，无后续补读/模型阶段。

清理完成：本人确切临时根`/home/python/Quote/data/QuoteBak/company-profile-6fh-b-n9gWAGfj`已删除（25文件、8,055,400逻辑bytes）；本轮独立工作checkpoint根`/home/python/Quote/data/checkpoints/company_profile_automation_probe_20261124`已删除（32文件、555,602逻辑bytes）。合计57文件、8,611,002逻辑bytes，两根均已确认不存在，清理前无活跃本人进程/无symlink。原wire、marker/账本/result、首失败全部JSON/终端checkpoint及每次真实运行日志已各保一套并逐hash核对；历史checkpoint路径现以本目录`b-first-entry/`、`b-runs/`为必要持久证据，不能据旧路径重开执行。

原PDF/cache、原20根、8父record/3后继、A文件均保留。末端见`b-terminal-and-cleanup.json`：API sha256=`9c9cfcba3255f8d1b8ccd1572dbebae18e89860829e1a013c653fe319c9a0c3b`，年度导出sha256=`46e1e746a84889de7f59e0b46df86d1b0ee612d4dabe6cea5eb9b06352f96271`（envelope不同，三份profiles相同）。`git diff --check`通过，暂存区空；未stage/commit/push，B写集释放，A负责末端独审及Git。

A末端有限通过：原PDF逐源核9本次意见及全部4实际采纳，精确新work_id/fact hash/final回执与当前API成立；5条真实wire/hash/usage和原deadline、277原文件/9源/3旧planned及49既有脏改保护已核，见`a-independent-review.json`、`a-wire-bindings.json`与`a-terminal.json`。这是有局部改善的研究评估交付，语义漏检及两跨行金额候选本地拒绝明确未解决；下一优先复用保存候选做通用消费修正，非再跑三股/20股或追供应链尾项。本轮应用源码未改，不扩跑旧测试；OpenSpec strict/diff及范围内Review通过。A已删除本人确切QuoteBak根（11文件、2,938,435逻辑bytes），A/B合计68文件、11,549,437逻辑bytes，全部相关本人根确认不存在，必要证据一套保留；A仅提交本卡相对开工基线可隔离增量，普通Git收口，不纳49既有脏改。
