# 6.EJ：保存复核计划已接原TaskService执行

本轮把6.EI的只读计划接到内部`CompanyProfileTaskService.execute_review_recovery`：同一报告/阅读/当前内容hash的计划，沿原队列、原seed/resume、原writer及read owner补未完成阶段，不重新选章或抽取。HTTP依旧只规划；真实调用须另获有限授权，本轮新增真实发送、公司数据写入和生产启用均0。

## 业务行为

- 保存原JSON/回执验证后重建原输入，缺失、损坏、错模型身份或原稿hash不符不盲重跑。分众、顺丰、国航三份保存真实失败稿均精确重建，原文件不改。
- 初核失败从初核开始；修订/终核失败复用前段成功回执。受控终核只发一个新逻辑请求，拒绝候选不采纳；通道再次失败后可对保存的后继生成新计划继续，不清旧失败或偷偷延续旧窗。
- 同一request已写结果返回`already_processed`，不再发送；同ID另一计划拒绝。旧计划被后继取代后，新的执行不得再用旧计划重做。
- 复核接续期间父稿继续可读，不先发布还原出的旧错项。结果作为独立work记录加入原报告/hash链；旧阅读及审核状态保留，旧事实`superseded/fact=null`，仅后继计入普通内容。历史缺项/建议`active=false`，当前缺项不清空；query单次兼容结果和原失败不改，`annual_profiles.json`与API同链。
- 独立source_checked只有内容hash不变才继承；已变/新增事实不冒充独核通过。source_only/withheld/duplicate限制保守继承并明确不是新独核，模型不能自行解除。继承决定的引用指向父阅读/hash，原独核记录仍在父稿。

每个执行使用独立TaskService实例，不在同一实例并发；它复用原_run，不是新调度器或业务循环。API字段/内部调用示例见[接口说明](../../../../docs/api/restful_api.md)。旧S2结束、95%/99%及S3/生产边界不变，不补旧五家或第41家。

## 验证

1. `pytest tests/unit/test_research/test_company_profile_review_execution.py -x -q --basetemp=<本轮根>/pytest-execution`：16 passed、0 skipped、226.50秒，覆盖实际TaskService成功/拒绝/初核接续、无提前曝光、重复请求、失败后二次新计划、原文件保护、API/导出/新实例、独立限制继承与坏原稿拒绝。
2. 原6.EI受影响五文件定向集（review_recovery、catalog、workpaper_continuation、annual_export、review_failures，`-k 'not test_task_service_stops_before_third_company'`）：77 passed、2 deselected、0 skipped、82.39秒。与新增16项文件不重叠；两个旧长用例不作为全套通过。两组并发运行，时间不相加当作墙钟。
3. 首次140秒探索运行在最后一个存储初始化用例处到限，6项完成、142.69秒；这次部分结果不累计，也不称全套通过。后续完整16项已完成。6个既有依赖警告未扩修。
4. OpenSpec严格校验、语法及diff检查通过。Review直接处理旧错稿提前公开、变化事实误继承独核和重复发送风险，无已知新增阻塞；后台调度/读性能/非当前复杂分支后置，既有核心/共享配置/市场并行不动。

精确命令、源码/test hash、验证边界及清理量见[验证记录](validation.json)。所有网关响应/重试均为受控传输，测试次数不是LLM质量或新公司成绩。

## 一份真实后段材料的零发送就绪核对

顺丰002352的原稿hash、正式PDF及14原文页/16证据片段已核，从原保存初核/修订抵达唯一终核发送接点；五候选本地源检查成立，请求内容体30817字节（不是完整wire/供应商token或费用）。当前研究根17个writer文件hash不变，原九个独立source_checked决定不变；详[source-boundary.json](source-boundary.json)。检查由无传输替身在发送前停止，没有模型接受或采纳。

下一[6.EK有限卡](../company-profile-sf-final-recovery-card.md)只拟这五候选一次真实Grok high终核，不重抽或扩公司；已询问新明确授权，未获批前0新发送/排队，不借旧余额。若批准，待本轮Review/代码冻结后直接整卡执行；不需要再准备一轮空卡。

## 清理与交付

开工确认前轮本人6.EI根不存在，本轮唯一`company-profile-6ej-KXt6aJ`。必要一套种子/source边界/验证证据持久后，删除本人脚本、测试库、中断残留和重复导出；具体文件数与文件字节见validation.json。原PDF、有效缓存、当前研究根、原失败/attempt、既有工作区改动及并行任务保护。没有修改共享LLM或API根配置，没有发布新真实画像。
