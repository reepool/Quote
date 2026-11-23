# 6.FB B实施：工程与V1交付，四家真实源复核待批

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
