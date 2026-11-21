# 6.ET B实施状态

**B本卡实施及收尾已完成。** 唯一release根为 `/home/python/Quote/data/research/company_profile_release_20261121`，15家公司、23writer、434可读条目/667指标。HTTP批量/两详情200，与JSON导出和新实例一致；银河F26仅源参考，京东方49条含完整精读父链。A的决定已按原owner消费，见release-verification.json；B未写准确率或扩量结论，未commit/push。

基线为 `f4e2d6bd` / master，开始时暂存区为空；既有脏改动保护。B实施仅涉及 `main.py`、`research/company_profile/task_entry.py`、对应测试、使用说明及本目录。未commit/push。

已核现有CLI→job→task_entry→原TaskService；两份公开源绑定：

| 股票 | 官方资产report_id | 文档版本 |
| --- | --- | --- |
| 京东方000725.SZ | asset_ee8381d6ef1a54776597e5dab8f426fa | 34ba47131763d6992d0997e714fa2e741e38abbfbcb7b6f537cfe2fdb97b1a80 |
| 中国银河601881.SH | asset_10336f11433796cd2e554ea5ba82ab23（实际截止日绑定） | 56394465e14ee2a657b8654147848bfa9242d16ecbd32b5b9b68380e2823a410 |

已修直接阻塞：`main.py`显式执行文件路径只复用研究存储初始化，直接转原ScheduledTasks方法（不传job_config，原报告owner默认禁发），不继承调度默认3600秒或初始化无关行情源。旧无文件任务不变；`task_entry`保存每个实际物理请求的公开正文/供应商响应对象（含失败正文字符串），不保存请求认证头或URL查询参数，不冒称原始HTTP字节。原逻辑2450秒/高阶1200秒/Flash600秒复制接线不变。

已启用profile及凭据存在性本地核对成立（不输出密钥）：官方 `semantic_extraction__deepseek` / deepseek-flash / api.deepseek.com；复核 `semantic_extraction__scorpio_grok` / grok-4.7 / scorpio.reepool.com / high。

6.ER/6.ES指定临时根均不存在。当前临时根为company-profile-6et-b-RPLyiJzh。完整36个直接用例通过（123.06秒，含受控首读→保存复核及精读两条实际CLI纵向链）；初次沙箱测试中断不算通过，以tests.xml这一次获准环境结果为准。A已明确启动整卡，授权引用human-20261121-integrated-1to4-A6ET；当前完成真实文件/同环境DNS和TLS冻结后直接启动，不再等待逐阶段指令。新结果仅两股独立研究根，后段按实际已用资源计算剩余额度。

已于22:26:46/47（本地时区）并行启动两家真实首读；当前均已完成真实目录选章，正在第二笔正文请求。实际网关日志确认provider逻辑2450秒、Flash物理600秒，只有官方DeepSeek；请求/响应保存到各checkpoint的executions/run_id/receipts。执行代码hash见execution-freeze.json，DNS/TLS1.3成立。后续保存计划复核和京东方精读同卡连续，不等额外指令。

首读已结束：京东方35可读对象，36.013秒，2物理/38507账本；银河30可读对象，35.623秒，2物理/49679账本；均0恢复/费用unknown。分别见000725-first-read-api.json、601881-first-read-api.json及first-read-results.json，A可并行审正文。两份review_batch已按实际扣减原链剩余额度启动，高阶实际1200/2450秒。银河按截止日的实际报告身份为asset_10336f11433796cd2e554ea5ba82ab23（与准备时未限定截止日的current asset不同，但文档哈希相同）；保实际源身份，不混旧链或静默替换。

京东方复核已完成，初核1意见，经一次修订和终核后仍35可读对象；3物理/58770账本/0恢复，原writer为bp-work-7f46c1fc1a2be83f1a52f6a2。模型自己把F31后半从“未发生重大变化”改为“有关情况勾选不适用”，A的同类发现未作为模型输入，也未改原首读。京东方一次产品用途/客户领域精读已于22:33:24启动，仍同一原CLI入口。银河高阶初核仍在原等待窗内；24旧writer当前hash均保持。收尾时删除重复的中间API完整副本，原writer/请求响应/最终000725-final-api.json保留此阶段及后继。

三链已全部结束：两份review_batch与京东方enrich均completed；京东方当前49可读条目/44指标（三轮含可能语义重叠），银河30/39（两轮）。原生真实物理共15、0恢复，保守账本和全部供应商已知total均332655，费用unknown；组业务墙钟631.198秒。源/代码冻结hash均保持。详execution-results.json和actual-model-revisions.json；两份最终API为000725-final-api.json和601881-final-api.json。当前没有新增代码问题或待重发模型阶段；接下来根据A的末端语义裁决通过原writer组装15家release研究根，独立API/export对照一次后清理，不自写准确率、不冒称S2通过。

release接续注意：京东方enrich的base_workpaper_sha256严格绑定原review父稿；不要把新的use_review附到这个父稿后又静默重写既有子链hash。两份新独立use_review宜只挂末端paper（京东方a4aa0e8f2b8e24be913e625a、银河c265f11a31d0e86f8f223804），京东方父稿F31已由真实模型修订/终核成立，A的独核可在审核材料引用。其他13家公司整条原链保持原内容/hash关系，不扩改公共read语义。

A独立决定已仅附到两个release末端副本，原运行5writer未动。单一release业务根已生成：data/research/company_profile_release_20261121，15家公司/23writer；根内release-manifest.json逐股记录原根/报告/工作ID/原与新hash、选用理由和本轮运行状态。京东方/银河用本轮完整链，宁波/万达用6.EP已核两阶段完整链（同为原15家公司），其余11家沿原batch_reviewed链；不跨report_id硬合并，没有新模型窗口。正在完成一次原HTTP/API/export/新实例对照。一次性参数准备脚本已删除，五份实际执行JSON和原clock/usage/请求响应保留，不另建legacy脚本区。

## 末端交接

- **代码与Review**：仅本人修改main.py（显式计划的局部初始化/原任务薄转发）、task_entry.py（逐物理公开正文及响应回执）、对应configured_task测试和使用说明。A已Review无新增阻塞，B无后续生产代码改动；执行前后的冻结源码hash相同。未编辑scheduler/tasks.py或其既有脏块，也未编辑A负责的spec/tasks/合同文件。
- **直接验证**：获准执行环境运行 `python -m pytest tests/unit/test_research/test_company_profile_configured_task.py -q`，36通过、0跳过，123.06秒；完整命令的basetemp为本轮QuoteBak、JUnit为本目录tests.xml。涵盖真实CLI控制流的替换传输首读→保存计划复核/精读，不当作模型质量。首轮沙箱执行中断不算通过，随后同一必要集在获准环境完成；未重跑无关大套。最终四个修改文件 `git diff --check` 通过。
- **真实运行**：五次原 `python main.py job --job-id company_profile_common_core --action <first_read|review_batch|enrich> --execution-plan-file <本目录plans/对应文件>`，原TaskService/网关/writer未替换。实际15物理、0恢复、332655全为供应商已知total及账本，费用unknown；所有响应stop、无额外窗口/公司。原研究链使用参数完整保留，回执索引指向持久checkpoint，不记录请求凭据，也不冒称原始HTTP字节。
- **资源边界**：京东方首读至复核5物理/97277、原链345.289秒；银河5物理/133758、629.840秒；京东方精读5物理/101620、228.328秒。全组业务墙钟631.198秒含各阶段准备/间隔，以并行壁钟计；不把它当总开发/人工独审时间。每链时间/账本/物理/恢复均在6.ET冻结额度内，后段仅分配实际剩余，不重写已开始run_id。精确阶段分额见execution-results.json；无可靠价仍unknown，不能承诺人民币总额。
- **实际研究交付**：release根内release-manifest.json是逐股选择权威；delivery/annual_profiles.json及annual_profiles.md是唯一最终完整导出。15股都取单一报告身份的完整链，非正式S2、非全稿99%或生产。API消费者可显式设置 `QUOTE_COMPANY_PROFILE_RESEARCH_ROOT=/home/python/Quote/data/research/company_profile_release_20261121`；共享配置和服务未切换。
- **保护**：旧20＋两阶段4＋本轮5，共29个原始writer哈希全部保持。新release只有两个末端副本附A的独立UseReview，京东方review父hash未改，因此14目标条目没有因人审元数据断链。原响应/原稿/失败和实际模型归因保留。
- **清理**：本轮确切QuoteBak根已删除，共122个临时文件、5640957文件字节；另删无后续依赖的持久参数准备脚本1个/4334字节。必要原请求/响应/账本和release业务根保留，用途见cleanup.json。没有清既有/并行内容、正式PDF或有效缓存，没有重复最终导出。
- **剩余与效率**：B范围没有待实现/待发送阶段或已知新增工程阻塞；正式质量与扩量只由A按本卡合同裁决。B活跃开发/交接等待未精确计时，不能用631.198秒代表开发总耗时；真实首读可见时间36.013/35.623秒、高阶与精读耗时单列。Git由A整卡审核后统一处理，B未暂存、commit或push，基线既有改动保护。
