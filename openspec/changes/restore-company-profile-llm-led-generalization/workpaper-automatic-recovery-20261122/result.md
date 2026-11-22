# 6.EX 正常入口自动纯格式恢复：离线交付

已在原workpaper provider/runtime链接通新版正常`first_read/run`的自动纯格式恢复，不再要求先手工另开local_format_only计划。**本卡真实模型、联网业务请求及新增供应商usage均为0**；现20股620/834研究成果未重抽、未改写、未重新导出。下一前瞻范围及来源仅由A另行planned，未获得真实调用授权，不送本轮fixture或模型。

## 实现与边界

基线fa7d1fd6/master，暂存空。只修改三个直接owner、两个测试文件及唯一用法文档；未改全局网关、共享配置、公开动作、原read/唯一writer或A管理文档，无commit/push。

- `workpaper_llm.py`：新身份`workpaper_format_recovery=local_shape_before_retry.v3`。复用原normalize_retained_workpaper_format，原严格JSON读取只提取为私有共用函数，不改变允许的修补集合；在一次当前拒绝响应的原raw/hash/finish=stop匹配后尝试原算法。
- `stage5_provider.py`：仅新版workpaper抽取使用网关既有text模式，原wire schema仍作为指令提供；业务适配器先严格解析，避免网关在业务拿到原始正文前付费重试。其他purpose和旧身份继续原schema/重试逻辑，不改全局client/config。
- `runtime.py`：阅读身份比较纳入格式消费版本，防新旧处理语义混用；原TaskService、deferred/inline及review_batch循环不复制、不重建。
- 测试新增自动入口用例；旧正常网关测试只更新明确的新默认身份断言。唯一用法`docs/development/company_profile_task_usage.md`说明自动行为、旧身份兼容及限制；磁盘日常计划字段/既有完成ID指纹不改。

先消费合法结果；仅实际finish=stop的拒绝原文，允许既有“一次末尾对象brace＋无冲突已知3字段归根”。不补字符串/数组/要素/业务值，不允许重复键或非有限数；length不作为格式恢复。归一化仍经原typed/source/数字校验，局部坏对象隔离、其他内容保留。保原raw、实际作者/usage、rejected及前后hash/操作，不伪装模型原生accepted，不等同源复核。

新版不可恢复的非空坏JSON直接结束该股本次抽取，不重复付费发送同一坏JSON；旧v2保原重试与validation feedback。网络/空响应等网关已有恢复没有被取消，仍受原累计预算约束。显式local_format_only仍0模型；正常deferred先交待核、inline和新实例review_batch仍沿原源核owner。

## 实际工程路径与调用序列

所有新模型响应均由ScriptedTransport离线提供，测试禁止DNS/socket；下面次数是**受控传输次数，不是新供应商调用**。

| 验证 | 实际入口及非空结果 | 受控调用 |
| --- | --- | --- |
| 混合批量 | 磁盘v1日常计划→execute_configured_task→原TaskService first_read：正常/可恢复/不可恢复三输入同批收尾，前两股主营/产品/变现各3条非空，坏股保missing/失败不洗白 | 三股各选章＋抽取，合计6；格式恢复本身0额外 |
| 新实例保存计划复核 | 原ReadService计划→新review_batch服务：正常稿完成源核，可恢复稿复核失败仍可读待核内容，原首稿保留，API/导出/新实例一致 | 1次正常初核＋2次受控空响应/恢复，累计9；非重抽 |
| 新版inline | 原runtime run阶段链，自动恢复后源核完成，generation.extraction仍rejected | 选章→抽取→源核，3次 |
| 真正暂停/新实例resume | 第一股抽取后暂停，已交1股非空稿；新服务只续剩余阶段及第二股，最终2股；完成后再次resume不重发 | 2→4→4 |
| 保存真实圆通raw | 原正常run_workpaper抽取分支，无extraction_resume/local_format_only身份；真实选章001/坏正文002在受控网关重放，走同一自动策略 | 2次：原选章＋原抽取，无原003或格式付费重试 |

混合批量还严格保存已完成first_read同run_id的result字节/mtime、账本字节/mtime及首稿字节/mtime，无新增配置/客户端或发送。新实例review_batch完成重复ID同样不重发。暂停发生在semantic已持久、verify/publish尚未收尾时，顶层provider_calls阶段摘要从原两项变空；权威workpaper、generation、原回执/来源/作者/供应商usage逐字段不变，没有再抽第一股。按A已澄清合同分列此阶段收尾，不为摘要改runtime；所有已结束record在再次resume后字节不变。

紧凑证据：`mixed-batch.json`、`pause-resume.json`。fixture的600000/600001/600002是受控合成报告ID，不是本轮新候选、未见成绩或真实公司业务评估。

## 保存真实源与内容保真

`saved-real-response-replay.json`绑定原001/002文件及正式PDF hash d14fd26f…，原raw hash29fad702…、归一化hash74107510…与A独立计算一致。原30作者事实经原typed/source约束保28，仍隔离F15/F19；28条文字、主体、期间、引用编号和原始指标值不变。F23原错误经济断言在此历史raw中仍是待核内容，不能因此称这28条全面正确；当前daily研究根的新修订/独立限用成果不受影响。

本轮extract transport **实际发送**的source_text及reading_scope与旧002实发请求逐项完全相同，二者canonical包hash均为：

`bbd9b9c8ada541b703a3a99dddbe8f70471a7f8f7fbd5504c47e818f36abe0ea`

没有换源、重选章节、改锚点或把A正确答案输入模型；新版本只改响应消费策略/请求协议，不改该源包。回放里的模型名及供应商usage来自保存响应/受控接线，是历史归属，不计本卡新增耗费。A独立原文/容器核查见`a-independent-review.json`，不由B同一归一化函数自证语义。

## 验证、失败与复用

按XML实际时间对(classname,name)去重，**37个不同用例最终全部通过、0skip**。不把重跑相加为不同覆盖，也不追改中间失败：

- `automatic-tests.xml`：初轮15项中14通过/1暂停字节断言失败。
- `corrected-tests.xml`：仅源包、legacy反馈及暂停边界后继；2通过/1顶层摘要字段断言失败。
- `final-entry-tests.xml`：按A合同澄清后，仅复验混合入口及真正暂停续跑，2通过；增强已结束first_read字节/mtime/账本严格幂等，不删非空或调用序列断言。
- `compatibility-tests.xml`：原严格拒绝边界、local_format_only等18项通过。
- `gateway-tests.xml`：合法网关/不确定字段/容器消费4项通过；默认新身份断言与实际入口相符。

### 实际已运行的pytest命令

执行工作目录均为`/home/python/Quote`，每条独立命令先运行`source /home/python/Quote/scripts/quote_dev_env.sh`。以下保留实际解释器、筛选式、basetemp、XML与日志路径；只补历史交接，**本次没有重跑**。这些QuoteBak路径已按cleanup清理，不是新增执行授权。

1. 初轮新增自动路径整文件：未使用`-k`，本文件15项全部执行，无skip；不包含仓库其他测试。

```bash
QUOTE_AUTOMATIC_RECOVERY_EVIDENCE_DIR=/home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-automatic-recovery-20261122 /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_automatic_format_recovery.py -q --basetemp=/home/python/Quote/data/QuoteBak/company-profile-6ex-b-rAsXp5Ju/pytest-final --junitxml=/home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-automatic-recovery-20261122/automatic-tests.xml > /home/python/Quote/data/QuoteBak/company-profile-6ex-b-rAsXp5Ju/final-tests.log 2>&1
```

2. 源包精确比较、旧身份反馈和暂停后继：选择3项，另12项deselected，不是skip。XML保留当时2通过/1摘要断言失败。

```bash
QUOTE_AUTOMATIC_RECOVERY_EVIDENCE_DIR=/home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-automatic-recovery-20261122 /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_automatic_format_recovery.py -q -k 'new_instance_resume or actual_saved or legacy' --basetemp=/home/python/Quote/data/QuoteBak/company-profile-6ex-b-rAsXp5Ju/pytest-corrected --junitxml=/home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-automatic-recovery-20261122/corrected-tests.xml > /home/python/Quote/data/QuoteBak/company-profile-6ex-b-rAsXp5Ju/corrected.log 2>&1
```

3. 最终实际入口两项：只复验磁盘混合批量与真正暂停/新实例resume，2通过、另13项deselected；前组源包/legacy及其他已通过项复用，未再整文件重跑。

```bash
QUOTE_AUTOMATIC_RECOVERY_EVIDENCE_DIR=/home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-automatic-recovery-20261122 /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_automatic_format_recovery.py -q -k 'new_instance_resume or disk_job' --basetemp=/home/python/Quote/data/QuoteBak/company-profile-6ex-b-rAsXp5Ju/pytest-finish --junitxml=/home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-automatic-recovery-20261122/final-entry-tests.xml > /home/python/Quote/data/QuoteBak/company-profile-6ex-b-rAsXp5Ju/finish.log 2>&1
```

4. 既有严格格式/local-only兼容：两文件按下列原筛选式收集，实际18项通过、51项deselected。`independent_assertions`未匹配第二文件的测试名，因此18项均来自format_consumption文件；未把该未命中分支算作覆盖，网关正常路径另由下一组4项验证。其余长集成/保存源用例未执行。

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_workpaper_format_consumption.py tests/unit/test_research/test_company_profile_workpaper.py -q -k 'only_container or ambiguous_or_semantically or local_binding or independent_assertions' --basetemp=/home/python/Quote/data/QuoteBak/company-profile-6ex-b-rAsXp5Ju/pytest-compat --junitxml=/home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-automatic-recovery-20261122/compatibility-tests.xml > /home/python/Quote/data/QuoteBak/company-profile-6ex-b-rAsXp5Ju/compat.log 2>&1
```

5. 正常网关消费兼容：只选该参数化测试的4种合法/不确定/容器组合，4通过、37项deselected，无真实网络或供应商调用。

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_workpaper.py -q -k contract_passes_real_gateway --basetemp=/home/python/Quote/data/QuoteBak/company-profile-6ex-b-rAsXp5Ju/pytest-gateway --junitxml=/home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-automatic-recovery-20261122/gateway-tests.xml > /home/python/Quote/data/QuoteBak/company-profile-6ex-b-rAsXp5Ju/gateway.log 2>&1
```

原初轮中11项自动严格边界拒绝测试明确覆盖：length、字符串/数组截断、缺要素、根字段冲突、重复键、NaN/溢出、坏数字源隔离及legacy原付费反馈。最终有效计数和各用例末次归属见`final-freeze.json`。更早开发夹具的必填构造参数、预算授权、反馈消息定位及供应链计数接线调整不作为业务失败或真实供应商尝试；未为绿色结果放宽归一化/数值/非空条件。

并发初始化和截止源问题沿已验证owner不再重做，复用6.EU：`b-execution/row-final-tests.xml`（4项）、`owner-fixes-tests.xml`（7项，含历史截止有效正文/未来及坏字节拒绝）、`source-replay-final-tests.xml`（同截止来源接续，1项）。完整路径/hash与未重跑声明在final-freeze，不冒称本轮新验证。

本轮无全套pytest、无真实LLM或HTTP联网测试；`git diff --check`及OpenSpec strict通过，日志为`strict-validation.log`。Review仅修阻塞主线的响应消费问题；暂停摘要属有据阶段收尾，不扩历史修复、框架或平台。不存在当前已知业务阻塞。

## 保护、资源与清理

旧当前20股daily根及更早研究根、旧raw/请求/预算/失败和年度JSON/人读稿按冻结hash验证不变，未为本卡新建20股版本。来源及原供应商预算保留；没有新供应商usage或费用，受控/历史usage不可当本轮花费。没有可发送的新真实计划；A的下一计划与元数据保持planned，真实样本和累计资源待另一次合并授权。

当前六个本人文件hash、旧文件保护索引、去重测试结论和有效旧证明链接均在`final-freeze.json`。此前6.EW及6.EV三个确切临时根复核不存在。本轮唯一`/home/python/Quote/data/QuoteBak/company-profile-6ex-b-rAsXp5Ju`中的测试库、脚本、日志及重复导出在证据持久后删除，释放字节/范围见`cleanup.json`；正式PDF/有效缓存、旧失败/原根、必要证据和A/并行内容保留。

B不暂存/commit/push，交A有限末审及可隔离Git收口。当前成果是自动恢复的工程就绪，不是新真实模型成功、正式95%/99%通过、扩20/100或生产授权。
