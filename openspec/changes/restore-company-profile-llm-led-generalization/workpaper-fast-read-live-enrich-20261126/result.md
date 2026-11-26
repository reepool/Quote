# 6.FP：查询提速、实际HTTP补读及共性源核完成

本卡有限通过。实际8000端口热单股0.589–0.982秒、10股批量0.908秒、39股2.174秒、4并发墙钟2.917秒；首次冷读23.604秒，仍有初始化等待。原reader按精确root、原文件内容SHA256和校验身份复用严格验证，每次观察新文件字节、变稿重校、坏源隔离/导出漂移拒绝保留，没有新事实库或宽松源校验。39份完整内容在补读前等冻结预期，实际服务验收未override reader依赖。见[a-live-read-audit.json](a-live-read-audit.json)。

[当前研究使用页](../../../../data/research/company_profile_research_v1_20261123/research-current.md) · [王府井当前人读版](../../../../data/research/company_profile_research_v1_20261123/600859-current-6fp-20261126.md) · [当前单股JSON](../../../../data/research/company_profile_research_v1_20261123/600859-current-6fp-20261126.json) · [HTTP使用说明](../../../../docs/research/company_profile_http_tasks.md)

用户授权的王府井600859.SH一股一次真实HTTP enrich已完成，原入口派至TaskService及writer，专用凭据仅服务器私有.env，只有一个有限名额。任务ID `profile-http-f2f27d6402ad6806e257b3e1c99719e2`；实际202→queued/running→completed→结果200，同ID同内容复用。新增请求在额度用尽后429，无凭据401，已完成任务和画像继续200；此窗余0，不自动续窗或借余额。用了已有免密服务重启权限一次，API/调度器正常，无系统权限改造；共享模型/调度配置未改。见[a-http-final.json](a-http-final.json)、[额度独核](a-allowance-audit.json)。

补读输出9条/21指标，A按正式PDF第5、18、19页独立核验全部有源，原表未抵消口径和约数/超过写法保留，不额外强求统一。真正新增主要是公司合并营业收入10,149,906,153.24元、上年11,371,675,304.54元和同比下降10.74%这一项规模说明；其余8条主要重复既有业态收入和客流，不能算8项独立新增。王府井旧37条/80指标和全部原事实行/限制保持，现46条/101指标；其他38家完整profile不变，当前39家1091条/1669指标，原381文件仅后续获准导航更新。原reader新实例与实际HTTP当前单股完整相等，Markdown复用原annual renderer，只新增目标股一份当前JSON和人读版，不重导旧39。见[a-content-and-preservation-audit.json](a-content-and-preservation-audit.json)。

同店收入仍未补回：A预先阅读的PDF17/18页披露下降3.75%，但对应段落没有进入模型选择的源片段，实际抽取请求中不含3.75；所返同店客流是另一指标。模型供源/判断没有拿A答案补写，已保留缺项和读取范围，不再新增调用追齐。新输出有披露依据，不表示整份年报已覆盖；源核v14本次没有提出修订，也不证明相对v13的总体检错率提升或正式99%。

B2在原源核/一次修订/终核三阶段共同检查事实自身scope/period及各metric，metric-only限制不再仅以正文正确为提示依据；有源歧义可unclear+具体原因，已知错误不靠删metric或不确定标签洗成正确。旧v13反序列化/内容hash和原A限制保持，未改强schema或加逐公司规则。B1共有29个完成的独立直接用例，B2相关108项全部通过，A复用精确命令/源码hash/真实输入回执并独立Review和实际服务验收；没有重跑无变化的82数值用例或全仓。较早夹具/命令失败和B1一项额外长回归中断已在各自结果如实保留，不冒称全套通过。见[b1-result.json](b1-result.json)、[b2-result.json](b2-result.json)。

实际3次物理请求、90243供应商total暨账本、0恢复、费用unknown，全部finish_reason=stop；2次官方DeepSeek选章/抽取、1次既有Grok high源核。已受理任务墙钟220.976秒（约3分41秒），首次HTTP提交另23.711秒；供应商请求合计170.429秒，审核/开发时间不混入。原71源码发送前冻结且全过程未变，没有修订/终核追加或新公司/窗口。有限资源与原wire保在既有checkpoint一套，见[a-resource-result.json](a-resource-result.json)。

有限Review无本卡剩余P1/P2；OpenSpec strict、直接静态/隔离diff、原文件/凭据不泄露核对沿本卡收口。原有源人读导出的行尾空白按非阻塞保原字节，不扩格式整理。必要证据仅本目录及原writer/checkpoint，B各自已清12.20MB/3.16MB临时物；A全量期望、脚本和本人bytecode在必要摘要持久化后清理，最终数量见cleanup.json。49项既有/并行改动保护，futures配置的并行变化只记录不纳入提交；普通Git只提交本人隔离增量。6.FN正式失败、旧分/阈值和当前整体未充分评估保持，不据本卡开启新确认组、全市场或交易生产。
