# 6.EV B 本地闭环交接

已沿原日常 `main.py job --job-id company_profile_common_core --action run --execution-plan-file` 完成圆通保存真实响应的零调用恢复。未重抽、未构造模型客户端或新供应商预算；源复核未获明确外传授权，保持0新发送。

## 可直接消费的交付

- 单一20家研究根：`/home/python/Quote/data/research/company_profile_saved_draft_release_20261122`。
- 当前整组JSON：该根 `exports/annual_profiles.json`；当前人读版：`exports/annual_profiles.md`。均由原writer/ReadService导出，未手工拼事实，未切默认配置/启生产。
- 原圆通待核稿：`/home/python/Quote/data/research/company_profile_saved_draft_20261122/600233/company_profile_common_core.v1/bp-work-f5e16b21c76fb7851c247c21.json`；该首稿不改。
- 原30作者事实经既有typed/source约束留28；F15数字引文校验、F19指标限定词校验隔离。新release另消费A精确F23 UseReview为withheld，当前22 pending、5 limited、1 withheld，即27条可读，不代表27条准确或已审。
- 旧章节祖先标签仅属未经核准的定位元数据；圆通新release通过现有limitations明示以报告hash、物理页及原文小节为准，不补A正确章号或业务参考，不改旧源包。

复制只读使用（不排队、不调用模型）：

```python
from research.company_profile.reads import CompanyProfileReadService
reader = CompanyProfileReadService('/home/python/Quote/data/research/company_profile_saved_draft_release_20261122')
stocks = [p['instrument_id'] for p in reader.research_profile_catalog()['profiles']]
batch = reader.research_profiles(stocks)
yuantong = reader.research_profile('600233.SH')
```

原HTTP使用独立进程显式 `QUOTE_COMPANY_PROFILE_RESEARCH_ROOT` 指向上述根，`POST /api/v1/research/company/annual-profiles/query`；未改共享服务默认根。

## 直接验证与证据

- `local-plan.json`：实际原CLI本地文件；实际result在 `/home/python/Quote/data/checkpoints/company_profile_saved_draft_20261122/600233/executions/6ev-600233-local-saved/result.json`，state=completed。必要日志只留 `local-cli-tail.log`。
- `input-verification.json`：旧实发source_text+reading_scope与原runtime重建包精确匹配；分别改source_text或scope后，即使重绑resume hash也0发送拒绝。重复同run_id不重写，无新增budget/receipts；原拒绝/作者/usage保留。
- `release-index.json`：38个原19 writer与当前新副本hash索引，不复制旧19全文/API。原19内容投影不变，新20批量/导出/新实例一致；HTTP回执为 `http-verification.json`。
- `local-tests.xml`：54 passed，2 deselected（本地入口/错绑定、停写、幂等及旧付费三类计划/指纹/CLI兼容直接检查）；命令为新saved_draft_job及configured_task两文件，排除重复完整付费传输fixture纵向用例。
- `review-owner-tests.xml`：66 passed，4 deselected，259.12秒；新saved_draft_job、旧format_consumption、review_execution直接回归。包括原正常初核/修订/终核接续、失败/幂等及限制继承；不算真实模型质量成绩。
- `implementation-freeze.json`：8个本人文件hash、旧圆通首次result/budget/001-003回执hash及Review分类；`git diff --check`、定向compileall通过。未暂存/提交/推送，既有脏配置/scheduler/骨架/市场和A管理文档未触碰。

## 原源核owner已就绪，但未发送

接续检查发现原owner仅接受accepted抽取且要求完整回执；此稿必须保持rejected。已在直接owner最小修复：local-only保留真实拒绝原文回执，prepare_review_recovery复算既有归一化凭证并精确比对；不会把拒绝变接受。旧普通付费分支仍要求accepted。

实际本稿 `prepare_review_recovery` 和 `seed_material` 离线通过，原源包不变；新release仅增加拒绝回执元数据及上述通用定位限制，原raw稿不改。F23限制按现有link继承，模型不能自动解除。见 `review-owner-preparation.json`。

`review-plan.json`为原ReadService生成；`source-review-task-plan.json`为strict校验过的原review_batch文件，**planned/未启动**。待A传达明确限定外传批准后，才可沿原入口执行：仅圆通，6000秒/1m保守账本/6物理含3恢复，每逻辑至多1恢复；DeepSeek flash必要至多一次合并修订、既有Scorpio/Grok grok-4.7 high初核/必要终核，高阶1200物理/2450逻辑、Flash600。未知价费用unknown，有价10元；不借旧余额、追加公司/窗口或启生产。

原6.EU首发/后继/失败和95物理、2377610供应商total均不改；本卡新增模型请求/供应商usage为0。A独核范围见同目录上层a-source-review/a-local-content-validation文件，不能用本工程测试或可读内容计数替代99%质量结论。

## 收尾

本人唯一临时根为`/home/python/Quote/data/QuoteBak/company-profile-6ev-b-XmezIFaO`；所有验证进程结束后清理其测试库、脚本、过程日志和被替代的本卡旧导出。清理回执及释放字节见cleanup.json。保留正式PDF/有效缓存、旧失败/预算、原19根、新待核稿及单一20交付根；不清他人临时根。

## A末端唯一阻塞修复及哈希口径澄清

实际release在抽取后增加的定位limitations，原先仅在首次源核种子中保留；若源核失败再从raw重建，因丢失该限制而与original_draft_hash不符。仅在local_format_normalization分支，现同时保留raw限制和当前paper附加显示限制，然后仍精确匹配original_draft_hash；不跳过哈希，不改普通accepted分支、旧raw或实际研究交付。

新增单项参数化回归先复现2失败/4通过，修复后6通过、无skip（fixture与实际release各含正例、raw篡改及seed限制篡改拒绝）。只跑该新测试，原54/66结果复用，见recovery-limit-tests.xml；本次未新增模型调用。当前release/原raw/API导出及未启动源核计划均未重写。

input-verification的repeated_result_sha256=61fb45…使用解析后JSON的ensure_ascii=False、sort_keys=True序列化口径，implementation-freeze的e7909857…使用磁盘result原始字节口径；同一当前文件同时复算得到两值，不是CLI补元数据或已证实的文件覆写。磁盘result mtime为2026-11-22T11:04:15.339771558+08:00，原input-verification生成mtime为11:09:29.267468786+08:00（后者因本次字段说明而更新，原时间已记回执）。本次再次同run_id仅返回原result，并同时锁定当前文件hash/mtime；见recovery-limit-verification.json。未声称获得早先未保存的时间点快照。

本次另建确切临时根company-profile-6ev-b-recovery-UcB1AL4D，已验证结束后删除，释放字节及受保护对象见cleanup.json追加记录。没有提交/推送；源核仍planned/0新发送，交A有限收口。
