# 6.EB：复核暂不可用时保画像，连续同类执行失败自动停后续处理

本轮是离线缺陷修复，没有新增公司、真实模型调用、外传或复核结论。继续沿用6.EA的[15份画像及缺项](../workpaper-s2-confirmation-result-20261119/comparison.md)，不重抽、不追供应链细节；S2确认失败及本方案结束不变。

## 已实现的业务行为

- 原复核owner把实际失败阶段、错误分类写入可选`quality_review.execution_failure`；初核、局部修订、最终再核均可说明卡在哪里。`requested_profile`和实际`selected_profile`/模型/来源分别保留，实际身份未知不拿配置首选冒充。
- 年报画像API的`readings[].review_execution_failure`按需展示该次读取的执行失败。已核/待核/限用、信息缺口和建议补读仍分别呈现，不把复核通道故障写成“未披露”。没有该元数据的旧稿保持原输出形状，不回填历史。
- 原TaskService可显式使用`processing_identity.workpaper_batch_stop_policy="consecutive_same_execution_failure.v1"`。仅针对明确证券名单的review-enabled新`run`，收窄到本轮已入队工作集；按runtime实际语义处理结束顺序判断，而非假定股票顺序或数据库hash顺序。相邻不同公司在同一通道发生相同技术失败时，第二份结果先由原writer落盘，然后停止后续语义发送并返回`state=paused`及`stop_reason`。
- 成功、普通缺项/覆盖意见、不同技术原因、不同通道及没有结果的间隔不合并成同一连续失败；每轮重置，不把一家公司重复事件凑成两家。默认未启用的调用行为不变。无明确名单、无范围resume或未启源复核的任务不能假装该策略已生效。
- 不改网关原有有限重试、逻辑请求身份、总预算或未知usage计费。未完成最终再核的修订仍是候选，原错误事实不复活，无关主营内容继续可读。

## 直接验证与实际限制

复用EA保存的空正文/缺finish_reason内容形态，通过实际CommonGateway的**替换传输**重放，非原HTTP字节、非新模型回答。初核/修订/终核各自用尽允许的一次重试后，失败阶段和`response_parse_error/empty_response`准确保存；两笔未知usage预留照常留账。一次空返回后成功则完成复核，不留下失败标记。

原TaskService三公司受控整链：第一家终核失败、第二家初核同类失败；启用策略时10物理请求后停止，第三家没有模型发送，前两份主营及状态仍可query/export/新实例读回，未核修订未采纳。未启用时13物理请求处理三家，证明默认行为未改变。队列未承诺按输入股票顺序处理，测试按实际发送/结果身份核查，没有靠放宽非空或停止断言通过。

EA原254份权威证据和15份writer输出hash全部不变，当前reader目录与15家API内容等于原快照。旧稿缺少机器原因继续未知，不把本次解析能力倒灌成历史模型成功。

精确pytest命令、每次结果/范围及直接复验见[verification.json](verification.json)：直接修复/交付检查42通过，最终归因/作用域14通过（有重叠，不相加冒充56项）。包括早期测试夹具不匹配的失败：最初请求数超默认单窗cap、随后把三公司受控整链套在单公司120000预留上；已修测试准备，不改生产预算规则/断言。最终新增状态/归因与作用域检查只重跑直接相关部分，不反复全仓测试。OpenSpec严格校验、Python语法和diff检查均留同一份回执。

较广旧入口回归在75通过/3失败后主动停止，余9项未完成，不宣称全套通过。三项为`test_run_uses_new_writer_and_does_not_call_legacy_semantic_writer`、`test_pause_stops_further_drain_and_resume_skips_completed_scopes`、`test_one_unbound_company_does_not_block_the_other`；共用旧`_RequestBoundOverviewProvider`夹具。第一项用任务前`21ffe7a2`的五个修改模块在内存还原后独立复现`extract_calls=0`而非预期1，未改工作区/旧语义；后两项共享该抽取/暂停hook预期，但未逐项跑完整基线，不把相关性当成已完成证明。这些不是本轮新画像源复核的成功路径，也未为迁就它们改并行核心骨架或旧测试预期；本卡只对直接修复范围给出通过结论。

Review：停止判断漏掉终核失败、队列实际顺序和跨无结果范围误连属于本轮直接阻塞，已在原owner修复并验证；请求配置误作实际身份已分开。可选字段、供应链精度和通用平台均后置；既有核心骨架/配置/市场数据及混合文档段未纳入提交。

外部Scorpio为何返回空内容仍不能由这些回执确定，本轮**没有证明供应商通道恢复或真实模型质量提高**。旧冻结EA并发operator保持不改，也不再作为下一轮运行方案：独立并发多个TaskService的调度者仍需消费同一`consecutive_review_failure`判定，不能继续用其failed-only条件。本轮证明的是原TaskService单运行队列中的正确停派和可读降级，不声称已验证新的多进程并发调度。

## 清理与后续边界

开工已核EA本人临时根不存在；本轮只使用`data/QuoteBak/workpaper-eb-p8RgLe`。必要测试XML、旧API核对和结论持久化一套后，清测试数据库、导出、过程日志与脚本，详[cleanup.json](cleanup.json)。正式PDF/有效缓存、尝试标记、旧证据/API及并行改动保护。

下一步不应再花一整组公司证明复核通道可用。应先裁决新的通道验证/适用范围，再用极小范围真实验证；具体模型/公开材料/预算需要新的明确授权，本卡不借EA余额、不补五家/第41家、不进入S3或启生产。已交研究稿与API仍可直接阅读，重要缺项按需精读而不阻塞批量基础描述。
