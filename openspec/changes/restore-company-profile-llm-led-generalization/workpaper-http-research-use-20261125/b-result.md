# B1：6.FM 最终交接

A已批准并完成主root发布，且独立release audit通过。当前唯一研究交付为 `data/research/company_profile_research_v1_20261123`：21股、682条可读内容、1115个可见指标。21份完整profile全部等于A预期，其余19家和原59份record保持不变；365保护文件中仅获准的research-current.md更新。没有等待发布或等待A清理的剩余工作，Git由A接管。

[最终发布核验](b-release-result.json) · [清理清单](b-cleanup.json) · [当前21股API](../../../../data/research/company_profile_research_v1_20261123/current-api-6fm-20261125.json) · [当前21股人读版](../../../../data/research/company_profile_research_v1_20261123/exports-6fm-20261125/annual_profiles.md) · [当前年度JSON](../../../../data/research/company_profile_research_v1_20261123/exports-6fm-20261125/annual_profiles.json)

## 实际内容与A限制后的交付

| 公司 | 原始真实模型观察 | A批准后当前可消费内容 |
|---|---|---|
| 海尔600690 | first_read 24条/40指标；review_batch保24条/39指标，10条reviewed、14条limited | 24条/39指标；两份原始记录与MAIN逐字节相等，没有新增人工限制 |
| 王府井600859 | 原29条/63指标；enrich后原始probe为41条/91指标，本轮12对象/28指标 | A对新ops-010/012/013/014加严格hash绑定source_only后为37条/80指标，相对原稿净增8条/17指标；所有旧限制保留 |

本轮读取对象数包含对已有业务描述的再次回答，不能当作完全去重的信息增量或独立正确事实数。原始probe与A限制后的MAIN不同，不得用原始41条/91指标冒充当前安全交付。

海尔形成保鲜烹饪、衣物洗护、空气、用水、商用制冷、全球品牌和变现方式等画像内容。独立模型源复核发现订单预测增幅将百分比标成百分点，仅隔离原错误metric；一次合并修订纠正Kwikot和PROFROID主体范围，经实际最终原文核验接受。缺单位或主体范围不明的数字保原文/限制，不强行标准化。A独立阅读确认核心及主要数字和两项修订有源。

王府井补读聚焦主要业态经营网点、经营模式、期末规模及免税前后口径，保留报告期后2026年2月机场门店属性。真实终核采纳期末78家、建筑面积/租赁面积567.2万平方米的混合栏目及表内业态范围限制。自有25家/租赁托管53家的修订候选因“万平米”单位触发本地 `source_unit cannot relabel a numeric magnitude` 而未进入最终再核；专业店细分仍建议补充。原抽取ops-008/009/011的源/数字消费拒绝原样保留，没有追加调用或热修。

A进一步将新ops-010的过强范围限制说明，以及ops-012/013/014的项目/持股企业主体歧义整条source_only；不修改probe原record，也不解除旧F18/F24等限制。依据见[a-content-review.json](a-content-review.json)与[a-approved-records.json](a-approved-records.json)。

当前整体准确率尚未充分评估；旧 `512/523=97.8967%` 仅为v12当次流程终稿已审对象分数，不是累计历史错误持续摊薄的当前准确率。本交接不重复B2的计算审查，不宣布新99%、规模或生产通过。

## 实际HTTP链及真实资源

采用原FastAPI router → CompanyProfileHttpTasks → execute_configured_task → execute_published_task → CompanyProfileTaskService/queue/runtime/writer/read。httpx ASGI连接实际router，仅显式注入真实隔离ResearchStorageManager和共享官方asset access；没有fake transport、controlled identity、旧语义/人工答案或绕HTTP执行文件。

三个POST均202、任务均completed，先有非空待核稿，再单独源复核和指定股票补读。状态/结果及普通详情读取新增模型请求0。真实状态见[http-receipts.json](http-receipts.json)，原始快照hash与实际差异见[b-resource-and-diff.json](b-resource-and-diff.json)；已核后的完整HTTP正文/before/current聚合快照按用户收尾要求删除，不保多套全量正文。

| HTTP动作 | task_id | HTTP工作秒 | 物理请求 | 供应商total/实际账本 | 恢复 | 费用 |
|---|---|---:|---:|---:|---:|---|
| 海尔first_read | profile-http-21166683a6f840a039916f799dea7d7d | 175.901 | 2 | 78241 | 0 | unknown |
| 海尔review_batch | profile-http-4a36ff2272b8790976109b0c2dac89d3 | 379.652 | 3 | 101976 | 0 | unknown |
| 王府井enrich | profile-http-8d3e856cf7ac87fe2fb1994d8692da57 | 581.592 | 5 | 141225 | 0 | unknown |
| 合计 | 三任务策略已占3/3 | — | 10 | 321442 | 0 | unknown |

整组HTTP工作段1141.496601秒，末段fresh-reader完成后1142.087009秒。全部finish_reason=stop，6笔官方DeepSeek `deepseek-flash`、4笔既有Scorpio `grok-4.7` high。Grok total包含其单列reasoning，使用供应商实际total计账，不是字符模拟账本；思考内容没有充作答案。DeepSeek输出token超过声明max_completion_tokens的warning没有导致截断或重试，实际供应商用量全部计入。未知单价如实unknown，未记0元或推算人民币费用。

## 主root发布及最终验证

A名单三份源文件沿原writer追加到MAIN，旧记录没有覆盖：

| 用途 | work_id | MAIN record SHA-256 |
|---|---|---|
| 海尔首读 | bp-work-e622431bbfb9b4320cab84f1 | 371e715d2c01e1c022e2019c7bb39879b08481aa04c2817b01d6835bbb02d416 |
| 海尔源复核 | bp-work-59deb464f2f91ec0bd579c0b | 08c5a51bfa1f3cd6e595110a51a0edbf4b3560dbd2fb2ab9130f0956322fb075 |
| 王府井A限制后补读 | bp-work-5189881c12bb58c6aa019be1 | 433951ba1a4d31317eed5d428029096b6b122ca12ce83ae3ba103ef30d8de73a |

新API、年度导出、fresh-reader与实际HTTP两详情/21股批量按股票对齐完整相等。API顺请求序、导出按股票排序，未为顺序差异修改应用。只读HTTP均200、0新增模型调用，原32个任务检查点文件逐hash未变。

本地准备脚本的清单字段/显式股票参数假设在写入前被拦住，只适配一次性脚本。实际追加和导出完成后，受限环境只读ASGI未返回，停止本人精确进程；获准环境的只读核对于21:33正常完成，未重复写入或提交模型任务。没有应用源码修改或新增测试范围。

| 最终文件 | SHA-256 |
|---|---|
| current-api-6fm-20261125.json | 60bd91819b82f8332f69da79d5dff3a90cc2580661697b02023e76f61e6708b7 |
| exports-6fm-20261125/annual_profiles.json | 06198a04bb9edc3743d13be4633835e15de4545f7fd2d4c87a880970d61fe6d9 |
| exports-6fm-20261125/annual_profiles.md | 1654a4f2c23f1b5709064d1bff6e4ac7c4395faed364f40f64f2ea680ea9392f |
| research-current.md | ef76b4d59cc59ea3ce73956e25bbf4765f02d7f5b3052bd021371a7999841348 |

## 清理与必要保留

A确认MAIN/hash后，共清理本人27文件、6,815,543逻辑字节，QuoteBak本人根已删除。精确路径、hash和保留用途见[b-cleanup.json](b-cleanup.json)。

保留 `data/checkpoints/company_profile_http_research_20261125` 的原32文件：任务账/policy/plans/started/预算/10笔wire/result/control，一套原始真实运行依据，不通过清账恢复额度。wire是解析后的供应商envelope并保content字符串，不冒充原始HTTP字节。

隔离output仅保6份runtime谱系和policy/锁元数据，供原三HTTP任务历史状态/结果重建；包括原王府井raw record和原三父链，海尔重复仅为原HTTP reader的直接依赖。probe明确是历史观察，不是当前安全版。重复完整导出、阶段工作checkpoint、完整before/current/三份HTTP正文快照、本人DB/脚本/缓存已删除。正式PDF、共享解析缓存、A本人根及他人内容未触碰。

B1实施与清理已结束，未提交或推送，A接管最终Git。
