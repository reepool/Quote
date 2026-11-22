# 6.EV 保存真实底稿接日常入口：零调用交付

本轮A规划和独立源核，B子代理落实原日常job/TaskService/runtime/writer。已复用圆通600233.SH旧真实响应取得待核画像；新业务模型请求0、无新供应商账本，不重选章或抽取、不改共享配置/默认服务/生产。源复核仍为planned，未获得本次限定外传的明确答复前不发送，不借已结束6.EU额度。

当前[20家画像人读版](../../../../data/research/company_profile_saved_draft_release_20261122/exports/annual_profiles.md)和[年度JSON](../../../../data/research/company_profile_saved_draft_release_20261122/exports/annual_profiles.json)可经原API/ReadService读取。原19家592可读记录完全保留；圆通新增27条待核/保真限用记录，整根619条可读记录不是619个独立正确事实。20份可读不等于20份已审或阶段通过；6.EU首发17/20、必要后继19份及原失败/95次调用成绩均不重算。

## 这次解决的业务问题

圆通原模型已写出内容，只因缺一个末尾对象括号和三类已知字段放错层级而整稿无法消费。现有严格local_format_only能处理该结构，本轮只把它接到原main job的显式本地执行文件，不新增JSON修补算法。新文件独立schema绑定一股/报告hash、原checkpoint/成功选章/拒绝正文/账本及序号，无extract/review配置或供应商budget；旧付费first_read/review_batch/enrich计划及指纹保持兼容。

本地分支在模型配置、凭据、LlmClient和费用账本构造前转原run，限定只drain本次入队scope。原raw、模型作者deepseek-flash、拒绝状态和原usage保留；另记归一化操作及原/规范化hash。除了校验文件hash，还逐字匹配旧实发source_text/reading_scope与原owner重建源包，避免同报告但锚点变化错绑引用。不能恢复的字符串/数组/要素/未知字段或冲突结构仍拒绝，不推断业务内容。

原30条作者事实经既有typed/source校验保留28：F15数字引用不匹配、F19指标限定词不匹配仍隔离。A核正式PDF物理12/13/14/16/29/30/31页及已发送20/33页源文，确认三基础问题及主要收入/毛利支持；另发现F23将“不适用”误成“未发生变化”，只在新交付副本通过现有UseReview精确hash绑定隔离该对象，27条普通内容继续可读。没有手填或改写模型答案，没有新增逐公司/勾选规则。

旧源索引祖先误标财务报告，当前副本的limitations明确以报告hash、物理页和原文小节定位，不改旧源包或祖先身份，不为此重做目录算法。API、人读版均可见这一定位限制。原文地区成本空白不是0；未知表格或未读范围保留。

## 实际交付与审核范围

原日常入口：`python main.py job --job-id company_profile_common_core --action run --execution-plan-file <local-plan.json>`。实际计划/入口结果、重复ID与错源/篡改/停写等回执见[b-development/status.md](b-development/status.md)和[使用说明](../../../../docs/development/company_profile_task_usage.md)。这是本地新授权，不授权模型发送。

单一研究根`data/research/company_profile_saved_draft_release_20261122`由原writer保存39个记录，38份旧副本原样、圆通一份附精确隔离与定位限制；原19根和本地raw首稿不变。圆通原模型抽取仍为rejected、quality_review为空；当前22 pending、5 limited、1 withheld。原API进程内ASGI新圆通详情及20批量200，JSON/人读版/新实例一致，默认共享API未切根。A独立对比原19年度完整投影一致、圆通28保留事实的原文字/主体/期间/引用/全部原有指标值未变、F23不回流普通正文。

本轮直接验证两组为54通过/2排除、66通过/4排除，有重叠不相加；覆盖本地0模型/0凭据/0新budget、显式付费计划兼容、源包精确匹配、停写/幂等与已有格式/复核owner。末端只对新增恢复限制问题跑6项正反，全部通过，未重复大套。语义准确由A原文检查说明，不由测试数或字段数量推算99%。精确命令/测试XML和源码身份见B回执。

A独立判定见[a-source-review.json](a-source-review.json)、[a-local-content-validation.json](a-local-content-validation.json)及不可变[a-candidate-use-decisions.json](a-candidate-use-decisions.json)。已核三问题和主要数，不声称独核所有补充事实、全报告完整或正式95%/99%。F15/F19以及具体原料、供应商等遗漏不追齐，供应链仍可选。

## 复核接续、当前边界与后续

原review owner此前要求抽取accepted，导致这类保持rejected的正常本地候选不能接续。现在只在有精确可重算归一化凭证时接受保存的拒绝回执作为复核seed，仍不把拒绝改成接受；原普通分支仍要求accepted，独立F23限制随原link继承，模型不能自动解除。实际prepare/seed供源离线就绪，新review_batch文件为planned。

A末端独立挑战另外发现：新API定位限制加入后，首次源核失败再接续时必须准确重建包含该限制的原seed，否则hash拒绝。已在local normalization分支保原有及额外显示限制，然后仍精确比原review-input hash；6项正反（含实际release、坏raw和篡改seed）通过，不跳哈希、不放宽普通accepted分支。原稿/实际研究交付未因此改写，见B追加回执recovery-limit-verification.json。

幂等证据的两种hash口径已澄清：磁盘result原始字节为e7909857…，解析后按ensure_ascii=False/sort_keys=True再序列化为61fb45…，同一当前文件同时复算成立；不是已证实覆盖或更换结果。末端重复同run_id的磁盘hash/mtime均未变，原计划指纹9e4c6102…保持。没有据未保存的历史快照补造时间点证据。

当前范围结论：本地恢复和研究API交付成立，真实源复核尚未执行，正式扩量失败及生产边界不变。后续若用户批准已提出的单稿限定源核，只复核此27条可读/原28条源稿及公开选定章节，至多一次合并修订/终核，不重跑前段或其他公司，不为供应链与补充项补齐再付费。本轮不等待授权而留下已获准的工程未完成，也不把计划写成执行成功。

## Review、清理与Git

本轮Review只处理实际阻塞：本地日常入口缺失、原源包/锚点绑定、拒绝回执复核接续以及额外显示限制下的恢复hash兼容。祖先标题误差仅作限用；新行业规则、全库质量平台、所有字段齐全及无关市场/骨架改动均后置。

B主开发临时根已清265文件/23,602,676字节；末端6项复验临时根再清2文件/10,314字节，共267文件23,612,990字节。A未创建临时脚本/库，前轮两个精确根和原19根已核保护。正式PDF/有效缓存、旧失败/账本/回执、raw候选/新研究根保留，既有脏改动不动。OpenSpec strict通过，A最终diff Review无剩余当前阻塞；本人隔离普通提交推送在会话收尾，提交hash及推送结果以最终回复为准，不将历史脏文档整份提交。A/B活跃时间未单独精确计量，不拿等待墙钟或模型token冒充开发成本。
