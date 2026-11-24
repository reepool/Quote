# 6.FG：自动化能力与人工保护边界

当前20股研究交付保持647条可读、1050个指标。本轮未发现新的消费逻辑P1/P2：原ReadService和delivery投影正确执行现有独立限制；需要后续真实验证的是模型语义检错及current-draft新政策的实际表现。本轮0供应商请求、0联网、0付费、0业务root写入、0源码/测试改动，没有新增程序语义规则。三股计划和预算由A准备，B未另造plan、execution文件、任务或provider。

唯一机器审核产物为本目录`automation-audit.json`：冻结`reading_work_id + fact_id + 原fact hash + 原metric索引`，保留真实receipt绑定、独立来源及投影差异；不保存反事实正文或坏数值。依据原`CompanyProfileReadService.research_profiles/_validated_snapshot`及`project_workpaper_delivery`，仅在内存typed副本中移除UseReview作对照，真实记录未改。8份已有独立审核材料按hash复用，不由算法另判语义。

| 当前范围 | 实际结果与含义 |
| --- | --- |
| API对象 | 1394行；740条superseded另列，不计当前654对象 |
| 当前状态 | 579 reviewed、68 limited、3 withheld、4 source_only；后两类不可读 |
| review_basis | 637 model_source_check、17 independent_source_check；包含不可读项，不能当人工依赖数 |
| 模型回执绑定 | 647条可读对象均绑定保存的源核回执：594初核、53终核；绑定不等于正确 |
| 可精确复用的A正向源支持 | 541条可读对象；支持范围可能排除受限指标。其余106条仅为本组有限证据未匹配，不判错/未核 |
| 当前独立决定 | 10条正文正向支持且带指标限制的source_checked、3 withheld、4 source_only；不是10条完全正确独核 |
| 独立指标限制 | 10事实共19个位置；模型同时限制其中7个位置 |
| 保存的模型组件限制 | 7 findings、17位置；本轮新增检出0。不得将历史发现或继承意见冒称新检出 |
| 待核与缺项 | 当前pending对象0；active缺项124条（active缺省true）、失效历史缺项123条；数量不判断语义完整 |
| 新政策真实证据 | actual current-draft policy readings=0；当前活动reading25个、目录record52个 |

去掉人工限制后，7个整事实会重新可读，另6个可读事实会重新暴露12个受限指标位置。下面的指标索引均为原数组的零起始索引，完整work/hash绑定见JSON；这13对象是当前投影依赖范围，不是新增错误分母。

| 仅靠A保护的整事实 | 当前处置 |
| --- | --- |
| 华夏600015 F25、F37 | withheld；F37为依赖重复，不另计独立错误 |
| 学大000526 F12 | withheld |
| 兴证601377 F34 | source_only；排名本身有源，问题是绑定证据覆盖，不称错误排名 |
| 潍柴000338 F-OD-01 | source_only |
| 王府井600859 F18、F24 | source_only |

| 仅靠A保护的局部指标 | 原位置 |
| --- | --- |
| 华夏600015 F-WEALTH-FEE-INCOME | [0,1] |
| 华夏600015 F-CREDIT-CARD-REVENUE | [0,1] |
| 华阳600348 F007 | [0] |
| 华阳600348 F009 | [4,5,6] |
| 华阳600348 F023 | [0,1,2] |
| 江苏金租600901 F22 | [1] |

另7个位置由模型和A共同保护：潍柴F-SM-02[0]、中建F15[1]/F20[0,1,2,3]/F26[0]。模型独有10位置含原6.FB五位置和6.FE RM-007五位置；均为保存结果的检查，不是本轮新模型改善，也不能把回原seed后重新限用全称作相对已交稿的新收益。当前未出现`inherited:`组件finding；后续current复核继承的旧意见仍须保留来源，不能算新模型发现。

历史6.EY的523唯一对象/512支持/11错误、97.8967495%、53项修订源核及正式不通过结论原样引用，不回算。12个原受处置对象（含F37依赖重复）+2个6.FA对象+3个6.FB决定共17条冻结映射均有当前exact绑定；这与当前17独立决定、13个仅靠人工保护对象是不同集合。跨hash没有源/作者/谱系等值证明的情况应为unknown，不按同名字段推断。6.FD五候选只有typed/source-ready离线证据，没有自己的真实final或采纳，不计入当前成果。

结论支持继续使用现研究版及其明确限制，不支持据此宣称新鲜99%质量、解除人工保护、扩大100/生产或批准新20。可复用原`plan_review_recovery(..., source_recheck_policy="source_review_current_draft.v1")`准备有限验证；三股选择、合同和预算归A。本轮没有执行该阶段，也不借旧额度。华阳需显式选含F007/F009/F023的当前reading；三份原packet均为保存v7，不能冒称新v8源。

B2的HTTP诊断见本目录`b2-result.md`及`b2-checks.json`：沙箱socketpair.send的EPERM足以造成asyncio唤醒等待；允许本地IPC且禁外网后原router代表性单股200、5.797308秒clean exit0，无应用源码修复。B1未重复HTTP检查，原失败/超时记录不撤回。

A独立复验见`a-review.json`：使用原投影对全部17个独立决定逐项挑战，确认7整事实与6事实12指标位置的边际依赖；fresh20完整profile等current API，读取2.965827秒，277旧文件/5源文件及6本人管理文档之外43旧脏文件（含原缺失）保持。首次A检查误假设各期映射共用hash字段，已改用其实际字段及唯一reading/fact/hash身份；这是观察脚本修正，未改业务数据或预期。没有新增源码故未扩跑工程测试。

A扩量裁决：现有20家分级研究交付继续可用，全部人工保护保留；本轮不能证明无需人工或新99%，暂不扩100/生产。三份下一验证已具体化为`proposed-probe.json`与`proposed-probe-plans/`：华阳600348、江苏金租600901、学大000526的实际当前批次，8父record本地隔离计划等值，原v7公开源包和当前稿hash绑定，planned/未启动/新调用0。仅需验证current-draft真实源核能否自行识别主体、方向、明示零问题；不重选章/重抽，不送人工判断/旧审核。新有限外传/预算已一次合并询问、尚待答复，未批准不发送，不继承已结束旧窗口；具体预算、停止及隔离根见卡。计划不算真实模型改善或新的未见样本。

验证与保护：20整份profile与当前API/A冻结hash一致；647可读的源核回执无mismatch；277原业务文件、5个冻结源码文件和8份引用材料hash均未变。49既有脏文件中44个含缺失标记均保持，另5个为A并行管理文档变化；B没有修改任何既有脏文件。current API sha256仍为`1803aa6aeb1f5d97d3bcb2beeceae98a48fa052a9f824c0f04f5271b4eb3e0d4`。未跑旧测试/新模型质量测试，没有新工程通过数量可计。

审计过程曾将正向支持错误地按record关联，已修为逐fact ID+exact hash；整条隔离隐藏的指标也已从局部限制计数剔除，最终JSON采用541正向绑定/12个仅A指标位置，初版观察不作结论。两次收口工具启动因裸`python`及OpenSpec shebang找不到node报127，改用既有Quote Python和临时PATH；无业务请求或合同失败。`OPENSPEC_TELEMETRY=0 DO_NOT_TRACK=1 openspec validate restore-company-profile-llm-led-generalization --strict`与`git diff --check`最终通过，不以工具检查代替语义验收。

清理完成：本人确切根`/home/python/Quote/data/QuoteBak/company-profile-6fg-b1-JK1yW0HF`已删除并确认不存在；删除前无活跃脚本/无symlink，共1文件、释放24,208逻辑bytes。删除的是可复建的一次性审计脚本，hash已留JSON；必要结果仅本报告和审核JSON，不保留永久审计程序。A/B2临时物、原PDF/cache/writer/raw、首次业务失败均不动。未stage/commit/push，B1写集释放，Git及后续预算裁决交A。

A末端收口：有限Review未见本卡剩余阻塞；OpenSpec strict及diff检查通过。3份新调用授权仍待答复，因此本卡只完成零调用评估，计划保持planned。A/B本人四个确切临时根均已清并确认不存在，共30文件、4,694,002逻辑bytes；含8份可从原root重建的父record副本、一次性脚本和文档基线，不删除原业务/源资产。全部必要审计/失败摘要、最小HTTP复现及3份计划只留本目录一套，清理明细见cleanup.json；既有49脏文件内容未纳入本卡提交。
