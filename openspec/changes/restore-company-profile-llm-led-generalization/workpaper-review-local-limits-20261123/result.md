# 6.FB B交接：四股真实源复核与当前研究V1已完成

## 最新授权接续（2026-11-23，已完成）

用户已明确批准原四股/原有限预算，A已在卡顶部登记；前文工程阶段“待批”仅为历史，不再表示当前阻塞。基线de3168dc，四份task plan仅authorization_status/approval_reference改为本次明确批准，源/问题/预算不改。原102和V1基线writer/raw/独立限制已保护；7业务代码及123工程回归复用，运行中不热修。

获准联网环境DNS/TLS/profile及2450逻辑/1200高阶/600Flash接线预检通过，见live-network-preflight.json；冻结见live-execution-freeze.json。正式执行沿4个原CLI review_batch，不重选章/抽取、不传旧quality/UseReview/A答案；只追加同源审核后继。执行checkpoint根`/home/python/Quote/data/checkpoints/company_profile_review_local_limits_20261123/{000338,600859,600015,601668}`，请求/响应在各自`executions/6fb-股票-source-review/receipts/`。每股最初process.started_epoch+6000秒，组12600秒，累计额度不另开后继。

本人新临时根`data/QuoteBak/company-profile-6fb-live-b-Ozzq0sZX`；A根company-profile-6fb-live-a-82jwV1Mu保护。模型是否实际使用metric_limit、命中旧错误/仍靠原人工保护、必要修订及本轮新增人工决定，末端分列，不以0新A决定冒称自动收益。

四股均交新的同源审核后继，quality状态都是`completed_with_open_findings`，不把开放缺项等同整家公司拒绝，也不当作正式质量通过。当前V1为**20股633条可读记录/1003个可消费指标**；相对运行前628/993，净变化是投影记录+5/指标+10，不是5条全新经济发现或准确率改善。其他16股完整API不变，原46个writer与原102保护根全部hash相同。

| 股票 | 新后继可读记录/指标 | 实际模型局部意见/位置 | 实际采纳修订 | 物理/供应商total | 从入口到结束秒 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 潍柴000338 | 33/50 | 2/5 | F-MC-02、F-OD-02 | 3/86865 | 439.25 |
| 王府井600859 | 29/63 | 1/1 | F30 | 3/111250 | 467.75 |
| 华夏600015 | 10/19 | 0/0 | 0 | 2/43634 | 253.28 |
| 中建601668 | 29/56 | 3/6 | F18、F27 | 3/88010 | 746.43 |

合计**11物理、0恢复、329759保守账本tokens/329759已知供应商total tokens**（本批恰好相等，两种口径仍分开）；7次Grok、4次DeepSeek。全组墙钟746.51秒，每股/组绝对截止及原额度均未突破；费用unknown，无可靠单价不转换wire tick为人民币。没有重新选章/抽取、第五股、额外窗口或生产。原预算/初核/修订/终核全部保在业务checkpoint，资源详live-resource-summary.json，实际原CLI命令详live-股票-process.json。

### 实际检出与人工依赖，不以零新增A伪称自动化

模型有效`metrics_limited`共6意见/12原位置；原A0明确的11位置命中7、漏识别4（均为华夏），另有5个超出旧A位置列表的模型限制。A独立读源确认这些局部限制及5项实际采纳修订。限制保正确正文和未受限指标，模型gap带model basis/finding ID；同位置旧A约束与模型约束取保守并集，不伪造UseReview。

“额外5位置”不是当前数据5项新质量提升：潍柴F-BS-03在运行前base已修为business_segment，本次回原seed后再次限用，是旧原稿检错/保守收紧，反而减少当前可计算字段；另外位置也须结合实际before/after，不能直接折成总体收益。华夏旧4metric错误本轮完全未被模型识别，仍靠旧A限制保护。旧银行v7的F25/F37不属于本次10条v8 seed，原whole限制继续保留，不冒充本次识别。

更重要的是重核原seed重新暴露了旧修订/隔离内容，本轮仍需A新增**3个source_only决定**：潍柴F-OD-01（全球行业环境误作集团自身事实）、王府井F18（业态/面积范围错误）、F24（4/5家原文冲突未明确限定）。这三项是实际人工介入，最终普通API fact=null、缺项/来源理由可见；没有靠删metric救错误正文。原模型质量/原raw不改，明确分列模型与独立消费，不宣称全链免A或新全稿99%。

两股UseReview均引用最终冻结`a-live-independent-review.json`的hash **20475f3b8caf30dd4ffaeb2d32dfa3b604ca3873d2a0e9a28fc27053fe2eac41**，对应3个原fact hash未变；既有独立决定保留。`live-independent-use-application.json`给模型原runtime存档、只增加UseReview及受影响要素append-only缺项说明的证明；未将模型stage改写为A自动解禁。A独核/末端机械核验分别见a-live-independent-review.json和a-live-terminal-verification.json。

### 首断点、重暴露与剩余缺项

华夏4个合并修订候选及王府井R1收入候选，均因比较指标raw_value与当前规范不兼容被typed消费隔离，未进入终核。实际receipts/002中的value是字符串（如“-0.71”），不是数字JSON类型；raw_value如“下降0.71%”“同比下降10.74%”带中文方向，当前workpaper.py去单位后不能直接Decimal解析。standardization_notes虽显示“metric values must be decimal strings”，不能据此误归因为value类型。先前B对此归因不准确，现按A实际回执核查更正；5候选拒绝/未终核、原作者/usage均不改，不修代码或重跑。详细拒绝点见live-rejected-candidate-diagnostics.json。王府井门店4/5和开业日期2022/2023冲突保不确定，不选定一个答案；可选成本/排名/尾项不补追。

模型阶段新可读：王府井F18/F24/F30、中建F04/F05/F06/F07/F27；其他两股无新增fact ID。A已读中建原完整表头/期间和附注，接受4条重新可读的有源经营记录以及F27限定修订，不把旧“未核列”永久当作错误。A三项处置后的当前可读变化是：潍柴34→33（F-OD-01仅源参考），王府井28→29（仅F30新可读，F18/F24仅源参考），华夏10→10，中建24→29。原文限定/中海地产不含宏洋的口径不推断扩为金融合并。

逐事实新增/撤回/变化和原旧finding索引见live-model-before-after-deltas.json（A处置前）与live-before-after-deltas.json（最终）；不能只看本轮findings而漏掉重新暴露。live-model-content-index.json只保原模型runtime路径/hash与意见索引，完整事实/原响应留原owner；live-content-index.json为当前机械来源索引，不自评语义准确率。

### 最终消费、保护与清理

当前唯一根仍为`/home/python/Quote/data/research/company_profile_research_v1_20261123`。`annual-api.json`及原export owner的`exports/annual_profiles.json`、`exports/annual_profiles.md`、CSV和单股稿均已刷新；新ReadService和既有HTTP本地ASGI批量20返回200并逐结构相等，annual API与annual JSON的profiles相等（两者envelope按各自合同不同，不声称整个envelope相同）。新模型6个组件的正确正文保留，12个坏metric不在numeric payload，3个A source_only不回放错误正文；仍有的缺项/保真不确定随API交付。详live-final-consumption-verification.json。未切默认根/共享配置或启动服务。

de3168dc及20个执行冻结文件全程未改；旧V1基线46个writer、原102文件、旧独立限制/失败/97.8967%成绩保护。原93-file V1快照中的导出按本轮授权更新，只保一套当前业务导出；旧基线内容在完全不改的102保护根及原writer，A另留了独立快照。先前末端投影因A第三决定到达而停止，不算模型/业务失败；重绑最终A hash后才生成本次最终导出。

本人确切临时根`data/QuoteBak/company-profile-6fb-live-b-Ozzq0sZX`已清**55文件/18199734字节**，含pre-live重复导出、一次性脚本与日志；脚本/日志hash和运行命令指针保live-cleanup.json，原raw/receipts/budget/checkpoint、两份必要模型阶段存档、有效PDF/cache及A临时根均保留。早期潍柴gap重复示例已由最终全组例证替代删除；模型原稿未删除。live-final-freeze.json给最终代码/计划/交付hash。未重跑123既有工程测试；B未stage/commit/push，既有脏改与A管理文件未覆盖。

结论：本轮真实路径证明模型能触发并交付安全局部指标限用，但仍漏识别银行4位置并因回原seed产生3个需A处理的正文问题；只作有源、分级、缺项可见的研究版。原正式质量分数不回算，不扩100/生产或额外付费。A可按本轮独审收口。运行等待约12.44分钟，B活跃准备/汇总及末端导出约15分钟（估算）；A审核并行，不以等待或工具数代替业务收益。

## 前序工程/V1交付（原阶段记录）

基线fd2012de/master，暂存空；6.FA相关临时根已不存在，无垃圾盲删。既有config/骨架/选源/scheduler/市场脏改保护，B不stage/commit/push，不改A文档。0新真实模型/联网发送，四家真实源复核仍待另行批准。

当前业务闭环已实现：正常新版源复核能显式支持正文、只隔离原metric索引，原writer/ReadService/JSON/Markdown保正文和安全指标、显示limited及指标名/缺口；模型意见在quality_review，独立UseReview始终分开且约束不解除。**没有新真实模型质量结果**；四股计划仍authorization_status=planned，无正式入口/新usage。

V1根`/home/python/Quote/data/research/company_profile_research_v1_20261123`，批量`annual-api.json`与原20股逐结构相同；628条可读记录/993个指标是已有研究消费数量，不是本轮新增正确事实。原owner导出`exports/annual_profiles.json`、`exports/annual_profiles.md`及CSV/单股稿，末端见`v1-consumption-verification.json`。未切默认配置或生产。直接用法：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
QMT_BUSINESS_API_SANDBOX=1 /home/python/miniconda3/envs/Quote/bin/python - <<'PY'
import json
from research.company_profile.reads import CompanyProfileReadService
reader = CompanyProfileReadService('/home/python/Quote/data/research/company_profile_research_v1_20261123')
print(json.dumps(reader.research_profiles(['000338.SZ','600859.SH','600015.SH','601668.SH']), ensure_ascii=False, indent=2))
PY
```

四份未启动磁盘接点见`plans/000338-review.json`、`600859-review.json`、`600015-review.json`、`601668-review.json`。批准前不能执行：

```bash
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action review_batch --execution-plan-file /home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-review-local-limits-20261123/plans/000338-review.json
```

`source-package-verification.json`给四份原真实extract wire、原PDF/cache路径及hash；原source_text与reading_scope逐内容一致，原quote验证通过。银行明确用6FA原v8的10条稿，另3份用原EY各自真实extract seed；旧review opinions、UseReview、A后加要素说明均不进入新draft。实际绑定/原作者、原rejected及seed哈希见`preparation.json`，不把旧completed/not_needed当新成功。四份检查只读、0模型，A源期待文件未用于请求构造。

最小协议：沿原quality_review finding增加显式`limit_metrics`动作及可选窄组件信息（原fact exact hash、正文有源支持、原metrics整数索引）；只在新处理policy中接受。原review handler核事实/要素/证据/hash及唯一索引后保存`metrics_limited`意见，不伪造独立UseReview。delivery对实际新初核回执及当前fact hash绑定成立的意见保原正文和安全指标、标limited及metric_gaps（含原name、不含坏value），原独立决定/正文或证据错误优先收紧。无新字段的旧JSON/hash不变，旧policy不接新动作；失败或过期绑定不能成为全通过。

普通review→原writer→新ReadService/API/export受控非空闭环已完成。当前无已知工程P0–P2阻塞；四家真实预算尚未答复，按卡交工程/V1完成及planned四股，不空等或以新增测试续工。

当前实现`workpaper_metric_limit_policy=source_review_exact_metric_limits.v1`，quality finding单独保存metrics_limited+exact hash/索引及模型来源；非独立UseReview。新policy并存限制取保守并集，已补旧source_checked/partial不能掩盖新whole defect、旧whole不可由模型解除。首轮17项15通过/2失败：单稿API缺项的active缺省是测试观察错误；未知模型fact ID使其他known IDs获reviewed是实际P1，已改为该不明target不授予当前核验覆盖，最终直接复验通过。原失败XML保留。

并存来源可机器区分：fact.review_basis仍可为independent_source_check；模型形成的metric_gaps单独带review_basis=model_source_check、model_finding_ids和model_reasons，与旧纯独立gap的原形状分开。同位置两种来源为independent_and_model_source_check；其他位置取并集。正文/证据问题与无效limit已知目标仍整条隔离，索引/metric数组变化的hash绑定过期拒绝。其他事实修订失败及保留初核后接续也直接验证原局部限制不丢。

现原owner对completed稿返回not_needed；四家新同源审核需要最窄显式`source_recheck_policy`计划字段，沿原plan/prepare_review_recovery重建精确原extract seed及原sourcepacket，不输入旧quality/意见，再走原review_batch/writer。保旧记录和继承独立限制，不开通用自动重核或新入口。

此最窄字段已由A明确裁决并实现；默认absent时旧plan/hash和completed不重开行为不变，未知policy拒绝。实际磁盘CLI证明已完成稿的显式recheck发出1个新初核、不重选抽取，前后source_text/draft一致；后加A-only说明不进入请求、原writer字节不改。新quality没有沿用旧review回执冒充本轮来源。

末端人工介入对照分列：模型实际有效metric_limit意见/位置；与A0已知限制的命中/未识别；最终仍依赖旧A保护的范围；A新增语义判断/消费决定数。对照只在末端离线，不送A答案/旧意见给模型，不因未触发再付费；无UseReview投影仅隔离诊断、不发布，也不算新的全稿分数。

本轮该对照尚**未评估**（4家真实尚未发送），不能以A新增决定0推断人工介入减少，也不把受控夹具触发的新动作记作真实模型命中。现V1仍完全保留原独立限制。

## 验证范围与实际命令

最新报告按测试身份去重**123项通过、0skip**（120项既有并集＋A末审要求的3类型身份，不按XML相加）。77项组在A要求收口前已在途，验证原review修订/原拒绝/恢复受影响分支。初版17中的2失败保留；最后3类型初验中的2失败也保留：bool/false在原wire Schema层触发一次已有恢复，夹具只有一响应用尽，修夹具响应与调用数后3项全通过，生产validator未放宽。负index、bool index、narrative_supported=false均明确拒绝。`test-summary.json`给7份实际报告时点、去重数和0真实模型。

```bash
QMT_BUSINESS_API_SANDBOX=1 timeout 600 /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_review_metric_limits.py -q --basetemp=data/QuoteBak/company-profile-6fb-b-33QWe8J0/pytest-slice --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-review-local-limits-20261123/slice-tests.xml
QMT_BUSINESS_API_SANDBOX=1 timeout 600 /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_review_metric_limits.py tests/unit/test_research/test_company_profile_workpaper_delivery.py tests/unit/test_research/test_company_profile_metric_use.py -q --basetemp=data/QuoteBak/company-profile-6fb-b-33QWe8J0/pytest-direct --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-review-local-limits-20261123/direct-tests.xml
QMT_BUSINESS_API_SANDBOX=1 timeout 600 /home/python/miniconda3/envs/Quote/bin/python -m pytest 'tests/unit/test_research/test_company_profile_configured_task.py::test_explicit_plan_uses_official_source_gateway_first_read_and_saved_review_on_same_owner[source_recheck]' tests/unit/test_research/test_company_profile_review_metric_limits.py -q --basetemp=data/QuoteBak/company-profile-6fb-b-33QWe8J0/pytest-recheck --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-review-local-limits-20261123/recheck-tests.xml
QMT_BUSINESS_API_SANDBOX=1 timeout 600 /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_review_metric_limits.py tests/unit/test_research/test_company_profile_workpaper_review.py -q --basetemp=data/QuoteBak/company-profile-6fb-b-33QWe8J0/pytest-review-regression --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-review-local-limits-20261123/review-regression-tests.xml
QMT_BUSINESS_API_SANDBOX=1 timeout 600 /home/python/miniconda3/envs/Quote/bin/python -m pytest 'tests/unit/test_research/test_company_profile_configured_task.py::test_explicit_plan_uses_official_source_gateway_first_read_and_saved_review_on_same_owner[source_recheck]' -q --basetemp=data/QuoteBak/company-profile-6fb-b-33QWe8J0/pytest-recheck-clean --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-review-local-limits-20261123/recheck-clean-tests.xml
QMT_BUSINESS_API_SANDBOX=1 timeout 600 /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_review_metric_limits.py -k 'bool_index or negative_index or narrative_false' -q --basetemp=data/QuoteBak/company-profile-6fb-b-33QWe8J0/pytest-final-types --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-review-local-limits-20261123/final-type-tests.xml
QMT_BUSINESS_API_SANDBOX=1 timeout 600 /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_review_metric_limits.py -k 'bool_index or negative_index or narrative_false' -q --basetemp=data/QuoteBak/company-profile-6fb-b-33QWe8J0/pytest-final-types-recheck --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-review-local-limits-20261123/final-type-recheck-tests.xml
```

这些均为本地/受控验证，未开展新公司语义试抽或真实网络模型请求。diff检查通过；OpenSpec strict/规格同步由A负责，B不修改A管理文件。

## 最终冻结、保护与交接

V1批量HTTP200与本根ReadService/API/annual JSON完全一致，原owner Markdown/CSV/单股出口齐备，原缺项保留。新模型gap的basis/finding信息已由直接用例验证；当前V1没有新模型意见，不能声称实际触发新动作或减少人工。4份真实源复核均planned/0启动，后续须收到该预算批准并做获准联网环境preflight后方可发送，不借旧余额。当前工程与V1已完整收口，无需因预算答复未到继续开发。

`implementation-freeze.json`冻结7个直接业务owner＋2个测试文件、8份磁盘计划和V1交付hash；旧102文件全部hash匹配。原extract、review、UseReview、已知错误与97.8967%成绩未改；A源参考未送模型。Review已修本卡未知limit target误获已核、独立旧checked覆盖新whole defect两个现实阻塞；无新增平台/registry/股票规则，无剩余已知业务阻塞。非阻塞通用重构未做。

本人确切临时根`/home/python/Quote/data/QuoteBak/company-profile-6fb-b-33QWe8J0`已清121文件、3585926字节，临时库/脚本/日志未永久复制；必要脚本hash记cleanup.json，原PDF/cache、旧102、新V1、planned计划及失败XML全部保留。一项清理预检查曾将saved-review文件误当task-plan而中止，按schema_version区分后才清理，未影响原件。6FA两确切根不存在，A并行`company-profile-6fb-a-VJyULsd0`未动。持久证据只有本目录和原owner业务交付，preparation的长heading清单已收为6.9KB必要身份/模型/版本摘要。

B未暂存/提交/推送；A管理docs/spec/tasks等及既有脏配置、骨架、选源、scheduler、市场改动未被B覆盖。A可按有限审核收口工程/V1及普通Git；本轮四家真实结果仍未评估。活跃实现约半小时（估算），主要运行等待为直接CLI和原owner导出/相关回归，无付费调用等待。
