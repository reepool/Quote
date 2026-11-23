# 6.FA B执行交接（末端交付，2026-11-23）

三家原enrich入口**首次3/3失败**；A显式裁决后，以保存的原raw/原v8包零模型消费，仅接原窗内未用源核。后继交付29条可读记录、61个可消费新指标；整组仍20股，628条可读记录/993指标。以上是研究消费记录数，不是独立正确事实率；原97.8967%及首发失败不改，不扩100或生产。末端实际计数及定向复验以`final-consumption-verification.json`为准。

| 公司 | 后继实际源核状态 | 新正文记录 | 可消费新指标 | 保留问题 |
| --- | --- | ---: | ---: | --- |
| 华夏600015 | completed_with_open_findings | 10 | 19 | 5条原候选被既有typed/source规则隔离；3个合并修订候选未获本地采纳，未发终核；4个业务收入scope指标依A决定隔离 |
| 华阳600348 | completed | 8 | 42 | 原历史指标限制不解除；本次源核0意见 |
| 兴证601377 | completed_with_open_findings | 11 | 0 | 1项未补充排名建议；部分研究奖项叙述有重叠，不当作独立经济发现 |

三份实际新内容由A逐对象独立核源，见`a-independent-review.json`；不是重新审核全部20股或重新计算99%准确率。新增值和原历史值不自动对账、覆盖或解除原限制，active gaps仍机器可读；供应链等可选缺项不补追。

## 当前研究交付及可运行读取

唯一新根：`/home/python/Quote/data/research/company_profile_targeted_gap_release_20261123`。批量API快照`annual-api.json`；原owner导出`exports/annual_profiles.json`、`exports/annual_profiles.md`及同目录CSV/单股稿。显式root，未切共享默认配置。其他17整份画像、三股旧fact entries及旧根87文件均保持。

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
QMT_BUSINESS_API_SANDBOX=1 /home/python/miniconda3/envs/Quote/bin/python - <<'PY'
import json
from research.company_profile.reads import CompanyProfileReadService
reader = CompanyProfileReadService('/home/python/Quote/data/research/company_profile_targeted_gap_release_20261123')
print(json.dumps(reader.research_profiles(['600015.SH', '600348.SH', '601377.SH']), ensure_ascii=False, indent=2))
PY
```

既有HTTP入口仍为`POST /api/v1/research/company/annual-profiles/query`及单股`GET /api/v1/research/company/{instrument_id}/annual-profile`；部署进程如需读取本根，显式`QUOTE_COMPANY_PROFILE_RESEARCH_ROOT`指定，不改共享配置。本轮只在本地ASGI调用既有路由，无启动生产服务。

`plans/*-enrich.json`是已消费的首次入口；`plans/*-local-consume.json`是0模型本地消费；`plans/*-unused-review.json`仅执行未用review_batch。每份实际CLI命令在`*-process.json`和`*-review-process.json`，不是新的抽取授权。原writer/runtime/checkpoint/raw/预算保在业务根；`new-content-index.json`只给实际内容、修订和来源索引，不重复完整原文。

## 首断点与保真材料

输入PDF/cache/目录选章正常，三家均完成真实选章1次、抽取1次。抽取返回的是完整、可解析JSON（finish=stop）；共同失败为非目标要素status=`not_read_for_this_task`不属于schema枚举，相关facts均空且有明确未读limitations。华夏另有两项指标限定符/raw_value约束错误，原有逐事实指标格式隔离可处理；不修写其业务值。

`run_workpaper`捕获原SemanticProviderError后无条件走`normalize_retained_workpaper_format(raw)`，旧严格算法只尝试末尾补`}`，在已闭合JSON上产生`JSONDecodeError Extra data`并遮盖原schema错误。已开始最窄离线修复：完整合法JSON保留原schema错误；不改严格格式修复算法、不增加付费重试。

runtime checkpoint的`workpaper=null/workpaper_receipts=[]`属实，但本次原task_entry的实际wire回执完整保留。权威根：`/home/python/Quote/data/checkpoints/company_profile_targeted_gap_closure_20261123/`。每股路径为`<股票>/executions/6fa-<股票>-enrich/receipts/001.json`（选章）和`002.json`（抽取）；含实际request、response、原usage，无鉴权header。正式checkpoint位于新研究根`company_profile_common_core.v1/checkpoints/`，不能将wire与runtime回执混称。

| 股票 | 物理请求 | 保守账本tokens | 抽取原正文sha256 | 业务结果 |
| --- | ---: | ---: | --- | --- |
| 600015 | 2 | 39970 | 5787af354cc3f43848c41181697ba517bf86333d8df87d63b7c3171c52836165 | schema拒绝 |
| 600348 | 2 | 40495 | 22fe13f27e6ea2cb090e896254b2b5e13fe85322e84e55884489f24383b42e35 | schema拒绝 |
| 601377 | 2 | 37640 | ea046805ef8336a55ce4fed72a23388f4f7dfc7dd32f237effcc192f0976bf28 | schema拒绝 |

上述为首次阶段：6物理、0恢复、保守账本118105，全部DeepSeek，0Grok。全卡最终合计**10物理、0恢复、218324供应商total tokens**；本批保守账本也为218324，但两种口径分别保留。华夏4次/78379，华阳3次/65945，兴证3次/74000；7次DeepSeek、3次Grok，费用均unknown。明细`resources.json`，原供应商usage/失败不抹去。

## 下一动作及边界

**A开发接续裁决已按卡完成**：最窄非目标空状态消费、原raw精确同源复建后接未用源核，全程受原绝对6000秒/原累计余额；未重选章/重抽/新增公司或追加窗。三份实际磁盘本地消费completed、0新模型，原失败分列不改。普通新enrich接`enrichment_unread_recovery=empty_non_target_unread_status.v1`，历史身份不启用；完整直接CLI自动消费/源核/保存回读用例已通过。修复另冻后完成原窗内后段。

开发接续冻结后实际完成原review_batch未用阶段。`continuation-freeze.json`、`review-continuation-binding.json`记录86文件快照、每股原截止、已用2物理/账本与剩余额度；三份实际packet（source_text和reading_scope）逐内容相等。本地10/8/11待核稿及路径见`local-consumption-verification.json`，原作者仍rejected。普通新版enrich经实际磁盘CLI、原TaskService、受控传输、自动规范、源核、writer、新实例回读/导出/幂等，共9项通过；历史无policy身份拒绝2项通过。旧40项的1失败是测试误解包prepare_review_recovery的(seed,resume,receipts)顺序，原失败XML保留，局部最终通过见automatic-enrich-tests.xml，不改历史为40全绿。

A末审发现华夏F-WEALTH-FEE-INCOME/F-CREDIT-CARD-REVENUE的4个metric错标集团scope。按`a-source-use-decisions.json`的原fact hash及原位置，经原UseReview/writer仅添加使用决定，不改文字、值、scope、generation或quality。两个正文limited可读，metric numeric payload不含这4项，`metric_gaps[].name`及active `information_gaps`公开原因/精读建议；精确样例见最终验证。A决定前模型原件另存原review执行根`model-runtime.json`，与派生投影分列。997指标是A决定前导出，最终刷新为993；没有声称历史从未变化。

准备及旧根保护：`preparation.json`、`original-protection.json`（87文件）、三份`plans/*-enrich.json`；当前owner源身份见`current-owner-source.json`。网络预检`network-preflight.json`为require_escalated同环境DNS/TLS，无模型。发送前冻结`execution-freeze.json`，三个真实进程原绝对deadline在`*-process.json`及`group-started.json`，首次结果/预算保原路径不改。

已通过原磁盘CLI→TaskService enrich→复核→writer→新ReadService/export/幂等受控纵向用例（1项，0真实模型）；命令：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
QMT_BUSINESS_API_SANDBOX=1 timeout 600 /home/python/miniconda3/envs/Quote/bin/python -m pytest 'tests/unit/test_research/test_company_profile_configured_task.py::test_explicit_plan_uses_official_source_gateway_first_read_and_saved_review_on_same_owner[enrich]' -q -o faulthandler_timeout=120 --basetemp=data/QuoteBak/company-profile-6fa-b-FhRR3FCV/pytest-entry-approved --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-targeted-gap-closure-20261123/entry-tests.xml
```

## 最小改动、最终验证和清理

改动仅在原workpaper/schema/本地输入/TaskService接线和对应测试：普通新enrich新增policy身份；仅明确非目标、空facts、非空limitations及精确非法未读status可规范；保原rejected、raw hash、作者/usage和显式规范记录。完整JSON保原schema错误，旧严格brace算法不改。显式local consumption复用原saved_draft→runtime/writer，0凭据/模型预算；prepare_review_recovery复验新规范记录及旧拒绝回执。没有新执行平台、API、供应商层或公司特例。

新增9项自动路径/边界、末次2项历史拒绝边界以及旧39项有效验证均复用；A按最新报告去重核为56不同用例最终全通过、0skip。初版local-schema-tests.xml的40项中1失败如实保留；修的是测试解包观察，不是生产返回顺序。`git diff --check`通过。以下为实际已执行命令（都先source统一开发环境，Quote解释器；获准环境中的controlled transport也没有真实供应商发送）：

```bash
QMT_BUSINESS_API_SANDBOX=1 timeout 600 /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_automatic_format_recovery.py -k 'complete_json_schema_rejection or live_raw_recovery_strictness' -q --basetemp=data/QuoteBak/company-profile-6fa-b-FhRR3FCV/pytest-schema --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-targeted-gap-closure-20261123/schema-error-tests.xml
QMT_BUSINESS_API_SANDBOX=1 timeout 600 /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_enrichment_schema_recovery.py tests/unit/test_research/test_company_profile_saved_draft_job.py -q --basetemp=data/QuoteBak/company-profile-6fa-b-FhRR3FCV/pytest-local-schema --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-targeted-gap-closure-20261123/local-schema-tests.xml
QMT_BUSINESS_API_SANDBOX=1 timeout 600 /home/python/miniconda3/envs/Quote/bin/python -m pytest 'tests/unit/test_research/test_company_profile_configured_task.py::test_explicit_plan_uses_official_source_gateway_first_read_and_saved_review_on_same_owner[enrich_unread]' tests/unit/test_research/test_company_profile_enrichment_schema_recovery.py -q -o faulthandler_timeout=120 --basetemp=data/QuoteBak/company-profile-6fa-b-FhRR3FCV/pytest-auto-enrich --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-targeted-gap-closure-20261123/automatic-enrich-tests.xml
QMT_BUSINESS_API_SANDBOX=1 timeout 600 /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_automatic_format_recovery.py -k 'complete_json_schema_rejection' tests/unit/test_research/test_company_profile_enrichment_schema_recovery.py -q --basetemp=data/QuoteBak/company-profile-6fa-b-FhRR3FCV/pytest-final-schema --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-targeted-gap-closure-20261123/final-schema-tests.xml
```

整组ReadService/HTTP batch20、变更三股HTTP、JSON/Markdown/CSV、新实例一致性已实证；A四指标决定后只重核华夏变化的ReadService/单股HTTP/新实例，原export owner重新生成同一最终出口，不重复其他17或整套pytest。既有export拒绝覆盖目录，因此A决定前导出移至本人临时根，最终owner重新生成后删除重复物；原20根不受影响。`final-consumption-verification.json`明确997是A决定前数量、993为当前，不声称历史未变化。

原87文件及冻结业务代码hash末核一致；首次result/budget/6个wire/失败checkpoint、后继回执、正式PDF/有效cache均保。模型阶段/独立显示限制分列，B只生成机械索引，不自评准确率。唯一持久证据本目录及原业务runtime/checkpoints；没有在OpenSpec复制整份20股API或重复全文。

本人确切临时根`data/QuoteBak/company-profile-6fa-b-FhRR3FCV`已删除，清190文件/22758995字节；移入的A决定前重复导出、测试库、过程日志/脚本一并清。2份未发布离线候选重复稿、2个一次性编排脚本亦清，必要脚本hash与实际运行命令保在cleanup.json和原process回执。execution/continuation冻结中的这些脚本路径表示执行时身份，末端删除是明确清理例外；业务源码hash仍可完整复算。必要首失败栈仅保first-failure-traces.txt，不复制重复日志。A根`company-profile-6fa-a-DvSxOcWf`、正式源/原成果/并行产物未动。

基线8682e3cc/master，B未stage/commit/push；A负责最终独审/OpenSpec及本人可隔离提交推送。既有脏配置、骨架、scheduler、市场代码和A文档未被B改写。后续没有自动付费阶段，本卡已收口。
