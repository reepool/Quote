# 6.EU B开发/运行交接

## 最新状态

**B本卡实施/真实运行/交付/清理均已完成，待A末端收口；未暂存、提交或推送。**

- **业务交付**：`/home/python/Quote/data/research/company_profile_prospective20_release_20261122`为唯一当前整组研究根，原writer发布38个保存记录，19家可读592条内容/775指标；`delivery/annual_profiles.json`及`delivery/annual_profiles.md`是实际同一投影。原ReadService批量、query/export、新实例和实际FastAPI路由ASGI验证一致，19份200/圆通404。可复制只读代码及显式API root用法已同步`docs/development/company_profile_task_usage.md`，未切默认配置或启生产。见`delivery-verification.json`、`release-manifest.json`。
- **首次/后继分列**：完整20首发17有稿/3无稿、85物理/1恢复/2120968实际供应商total与账本，业务墙钟2391.015秒，原34writer及281首次冻结文件不改。000070/002500各唯一必要后继均完成，新增2可读/10物理/256642账本；原累计窗分别2849.333/2975.211秒，未重开6000秒或余额，未重跑17成功链或圆通。首发正式失败不改，19可读不等于正式通过，A裁决质量/扩量。
- **总资源**：95物理/94逻辑/1恢复、2377610供应商total=账本、未知usage预留0、费用unknown；到最后公司链3664.423秒，到统一研究交付核验4107.902秒，最大同时CLI4。每股/整组累计资源及绝对窗均核过，详`resource-validation.json`；有价部分为0不代表总费用0。
- **P2修复**：只改开工干净的`research/change_watermarks.py`、`research/announcement_assets/repository.py`和`service.py`。前者只在目标重复加列且重查已存在时容忍DDL竞争；后者固定cutoff只优先当时实际body，新空失败下载不遮旧正文，新坏字节/未来版本不借旧正文洗白，采集诊断默认保原行为。002500同cutoff/同批准hash298页原来源绑定0调用证明已存；同work ID恢复用现有force-replay owner，无新恢复框架、抽取循环或main/task_entry改动。三个新增对应测试和使用说明可隔离。
- **验证/Review**：owner修复7项通过、最终DDL竞争/其他错误4项通过；原force terminal用例本次通过，新失败acquire同ID恢复用例最终通过。首次新测试有一处reason字符串误写，按已有owner正确值修正并单测复验，原混合失败xml保留说明，不冒充整份全绿；重叠测试不累加质量分数。源码diff/直接回归和A有限Review无剩余新增阻塞，不为风格/全字段齐全扩范围。见`validation.json`及直接xml。
- **独立使用限制/质量边界**：仅在release副本通过原bind_workpaper_use_review+writer消费A原`independent-use-decisions.json`，000037/F14原值/metrics未改且limited，不能确定电价测算；其余正常投影与原股根一致。A另核8家公司/26实际采纳修订，其余未核不默认正确，不称全稿99%。完整facts/metrics/evidence及初核/修订/隔离机械包见`audit-bundle-index.json`、`actual-model-revisions.json`，三原失败和后继见`failure-index.json`，B未生成语义自评分。
- **清理/保护**：本人唯一QuoteBak根已删除88个常规文件/17485133文件字节，含过程日志、一次性参数/调度/验证脚本及测试库；另移除已无用途的dispatch.lock。必要初始化错误仅保尾段，原运行预算/结果/95份实际解析请求响应均持久保留。旧29writer、原15家release的23writer及本轮34首次writer/281文件hash核齐不变，正式源/有效缓存/原失败/attempt、A临时根及所有并行既有改动未动。详`cleanup.json`。B活跃开发时间未知，不把等待、业务墙钟或供应商token当开发耗时。

下列为本轮过程状态，不覆盖上述末端结果或原始首次回执。

首发已发生3个不同失败：600233.SH抽取两次同hash的未闭合JSON（3物理含1恢复、75782真实total），000070.SZ正式CLI研究存储初始化duplicate column name: row_version（0模型，execution owner前失败，无budget/result），002500.SZ原来源owner pages_not_bound（0模型，incomplete）。失败不换样/不重开额度，原日志暂保本人QuoteBak，原回执/队列checkpoint保留。当前首发可用理论上限17/20，18/20与95%已不可能达标；A据此决定后续派发/必要后继，不继续无意义400项全量审核。业务代码未热修，不能把执行completed/研究稿数量当阶段通过。

最新紧凑实际状态与逐股阶段/最终路径见execution-results.json；实际模型意见/修订见actual-model-revisions.json（无B语义自评分），原请求/响应索引见receipt-index.json。在途计量暂含未结供应商usage的保守预留，末端另核完成量，不把未知费用记0。

20家正式候选及固定顺序候补已完成元数据/全文PDF hash/页数/目录入口核验，见`metadata-shortlist.json`。历史303家保守排除及证据见`historical-exclusions.json`，包括旧两批S2及历史候补/人工参考提及。本轮仅核元数据与目录入口存在性，不读公司业务正文、不作模型预演，模型调用0。

分层制造6、服务6、金融4、其他4；沪9/深9/北2。A可直接按每条asset.archive_path读源冻结参考，不必等待运行准备。模型启动等待A冻结对应源参考及launch，非新增用户审批。

Git基线3f0b7003/master、暂存空；本人仅写b-execution和QuoteBak确切临时根`company-profile-6eu-b-3YUSDBLW`。6.ET旧B根不存在，旧A/B结束清理证据已核；当前6.EU A并行根保护。所有既有工作区改动、15家release、29原writer及正式源/有效缓存保护。

20份执行文件、14SW一级及沪9/深9/北2已冻结，见execution-freeze.json。相同获准联网环境DNS/TLS1.3通过；20份CompanyProfileTaskPlan及启用模型/profile/凭据接线已检查（仅记存在，不记录密钥）。代码没有改动，复用6.ET36项同代码直接CLI证明，不重跑大套。

A明确冻结source-reference-batch1.json后，首4（600176/600115/601288/600163）已从获准环境实际启动，唯一CLI first_read→review_batch、每股独立研究/checkpoint根、首阶段3000秒/500000/4物理/1恢复；复核只给原6000秒绝对窗、1m/10/3及有价10元的实际余额。20名单及交叉顺序固定，不读A参考正文，不通知、不改共享配置。A可及时读取api/<股票代码>-first-read.json与最终final.json。原CLI自动保留checkpoint的每次实际请求/响应及预算，过程日志只在本人QuoteBak。

本轮不提交或推送，末端语义/正式验收结论由A裁决。
