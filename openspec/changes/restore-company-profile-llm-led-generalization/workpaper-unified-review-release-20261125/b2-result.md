# B2 · 6.FK 通用复核与三稿观察

v13通用提示、唯一同版三股真实观察及只读收口完成。A原PDF、7份真实wire、9组正控和两项修订独审通过，正式名单见[a-approved-successors.json](a-approved-successors.json)，语义结论见[a-observation-review.json](a-observation-review.json)。B2不stage/commit/push。

研究使用统一引用[MAIN20股API](../../../../data/research/company_profile_research_v1_20261123/current-api-20261125.json)及[MAIN人读版](../../../../data/research/company_profile_research_v1_20261123/exports-current-20261125/annual_profiles.md)，不保第二事实库或重复三股全量导出。[A最终发布审核](a-release-audit.json)确认3新record原字节、3新profile/17原profile、API/export/fresh20一致及310原资产保持；MAIN仍为650可读/1059指标。重复probe的12父+3新record已逐字节核MAIN后全部清理。

## 实际效果与限制

原8风险指标位置本轮命中4个：F009[4,5,6]再次检出，新增为金租F22[1]；华阳F007[0]/F023[0,1,2]仍漏4位置，原独立保护保持。学大F12整条本轮真正检出，按同范围邻表明示零修订并final；其旧独立withheld不自动解除。

六条意见不等于六个新错误：WF001/F009及WF002/F014[4,5]为重复保护；WF003/F015为有源范围澄清，不计确定原稿错误；WF004为缺项措辞建议，不计新事实错误。两个实际修订F015/F12均有原PDF支持，9组正确fact hash及可消费components保持。只证明已见三稿的有限改善，不是新99%、未见泛化或生产放行；不追加轮次/公司。

## 资源与验证

| 股票及MAIN后继 | 物理/恢复 | 供应商total及账本 | driver业务秒 |
| --- | ---: | ---: | ---: |
| 华阳600348 · ef3a65 | 3/0 | 67,014 | 456.487353 |
| 金租600901 · b4e5ee | 1/0 | 46,740 | 240.931746 |
| 学大000526 · 36c6f5 | 3/0 | 74,490 | 384.424897 |

合计7物理、0恢复、188,244 total，usage未知次数0，费用unknown，不宣称人民币费用达标。逐公司业务秒合计1081.843996，物理请求秒合计970.926405，组墙钟493.821583秒；原每股/整组时间、账本及请求限制内，无追加窗口或热修。

实际7份wire的source_text均等原v7包，三初核的当前draft/原fact_hashes及两个system内v13提示均匹配冻结；旧quality/UseReview/A答案未入请求。原writer/query/export/新实例及年度投影一致，三股共90可读/175指标，另F12一条仍隔离；不把累计数量当新增质量。36冻结输入和受保护原资产差异0，49旧脏的非A管理文档差异0，A管理文档更新仅列差异。B2只改指定提示/政策及一个双分支直接用例；8项去重直接验证通过，A审核后未再测试/调用。

必要失败保留：新用例初次origin夹具错误已修，非空断言/消费门槛保持；混合长回归60秒停止不计整组通过；收口统计字段错误只修报告、复用已完成导出。子代理两次自动审批均在创建进程前拒绝，0发送，不记模型失败；主任务携完整直接用户授权上下文获准后启动上述唯一真实首次。详情见[b2-execution-blocked.json](b2-execution-blocked.json)。

## 权威证据与清理

[紧凑索引](b2-live-audit.json)只保file/hash、ID/指标位置、处置/资源统计和A解释；已删除重复raw_findings、original/revised正文及大段quote。唯一模型原正文为`b2-runs/<code>/receipts/`的7份真wire；三股各一份budget/started/result，共16份执行文件，复制后逐字节核对且7 wire hash等A独核。原稿/修订事实以MAIN writer为权威，原期望/保护字典只引用A文件及hash，不复制。

已清三开发根，均确认不存在：个人QuoteBak根`/home/python/Quote/data/QuoteBak/b2-6fk-review-2am9JpuI`（79文件/15,110,532字节）、本次checkpoint根`/home/python/Quote/data/checkpoints/company_profile_review_strategy_probe_20261125`（22文件/618,867字节）、重复probe根`/home/python/Quote/data/research/company_profile_review_strategy_probe_20261125`（30文件/9,676,478字节）。合计**131文件、25,405,877逻辑字节**，含测试DB、日志、scripted副本、driver/过程脚本及重复15记录/三股导出；原开发路径仅保作历史身份说明。

另删6份重复driver回执和1份重复成功XML（未计入上述字节）；保原失败XML及8项有效用例的一套证据。清理前核16 authority文件逐字节、7 wire等A hash、MAIN全部15条等原字节、3新稿等A批准hash，A已确认三完整profile及最终20股发布。原310旧资产均在删除目标外。逻辑字节不是实测NFS空闲增量；可重建开发内容未留备份，业务事实和7 wire/账本/结果权威仍完整。源PDF、有效缓存、旧成绩、A/B1及既有改动未清理。无临时保留项，不据历史plan重启或新增请求；Git仍由A负责。
