# 6.FD B实施完成：当前稿重核与局部格式限用

基线 `cae1b92a`。业务代码已冻结，未stage/commit/push，未修改A管理文档或真实研究writer。新供应商调用、真实研究写入均0；本轮含B2的OpenSpec CLI遥测DNS失败尝试，不能声称所有网络尝试为0。该尝试未向供应商发送年报；后续OpenSpec命令使用 `OPENSPEC_TELEMETRY=0 DO_NOT_TRACK=1`。

## 实际行为及源码候选

- `workpaper_review.py`：新显式 `source_review_current_draft.v1` 使用当前typed稿，保已经接受的修订、已移除事实及缺项；不做语义merge。计划严绑base workpaper、current seed、实际请求draft和source packet hash，逐fact保exact hash。新请求排除旧quality/UseReview及自由文本limitations，避免猜哪些是A追加；这些本地缺项仍随后继保留。独立限制保守继承；旧模型未解限制另行继承，改动后的metric位置不能猜映射，收紧为whole quarantine。`inherited:`明确不是本次模型发现，reason含父work_id/原finding，metric gap带 `model_review_origin=inherited_prior_review`。
- `reads.py / operations.py / runtime.py`：原plan→TaskService review_batch→provider→writer→新ReadService/export接线，错base/report/source/draft绑定0发送。current失败续审从严绑父稿恢复，不能回最早extract；同一runtime重入不重复追加继承意见。旧显式 `source_review_exact_metric_limits.v1`、默认completed/not_needed及无新增字段旧序列化保持；旧policy仍严格核其original_draft_hash，不能将不同current input冒称同一旧input。
- `workpaper_metric_format.py / workpaper.py / workpaper_llm.py`：B2的窄helper仅新 `workpaper_metric_format_policy=source_metric_unparsed_value_null.v1` 接受已见方向百分比格式，原raw/单位/主体/期间不改，normalized value=null并明确本地格式原因。其他坏字段按原typed/source规则拒绝或局部隔离。新builder已自动带flag，保存旧identity缺flag仍旧合同；普通首读/enrich复用原抽取owner。repair在validate前用同helper，notes进原standardization_notes，新flag实际系统消息附同条保raw/null指令，旧identity不附。
- `workpaper_delivery.py`：继承的未解除模型缺项在原API information_gaps可见；原独立/模型依据分开，局部坏指标不作为确定数值输出。未改旧真实稿/旧API。

本轮8个源码候选及3个直接test文件的精确hash见 `final-freeze.json`。B2释放写集后B1只接identity默认启用和相关legacy fixture；无共享配置/骨架/scheduler变更。

## 业务入口与验证结果

普通用法仍从 `CompanyProfileReadService.plan_review_recovery(..., source_recheck_policy="source_review_current_draft.v1")` 保存计划，再由原TaskService `review_batch`消费；没有新增业务action或执行循环。本轮纵向受控路径证明：已有修订+隔离的current稿新核无意见时，正确正文/scope不回退、已删除事实不复活、缺项和独立限制仍在；JSON/Markdown/CSV由原export输出，新实例API一致，原writer字节不改，重复同plan不重发。

新格式路径证明：raw带方向字样可保正文及null指标，但不存在自己的final时不采纳；其原repair receipt/raw保持；初核失败和verify_only续审按record原flag重建精确seed/候选，hash不丢。错误正文、证据、单位和任意坏字段不能借null放行。受控final仅工程fixture，不是供应商真实核源。

最终接受组去重 **72个不同测试身份passed、0fail、0skip**：B2 `b2-metric-tests-final.xml` 55项，B1 final-integration18项、current-continuation-compat2项、freeze-final6项存在交叉，不能相加。builder启用flag后已重测gateway False/True，False显式移除flag，和new-default断言均通过。全部尝试的历史身份另见 `verification-summary.json`，不把已合并/移除的旧测试身份重复计为当前覆盖。

保留的失败证据及处理：

- `current-recheck-tests.xml`：首版2失败是测试错误假设CSV文件名及writer属性；实际正常export已完成，只修观察。
- `repair-format-tests.xml`：首版1失败是unknown-field Schema拒绝触发原一次恢复、夹具少一响应；补原恢复响应/次数观察，生产拒绝未放宽。
- `freeze-targeted-tests.xml`：1个新增测试误期待旧original-draft合同接受不同current seed；保生产精确hash拒绝，修为拒绝断言。`freeze-final-tests.xml`最终6项通过。
- B2 `b2-metric-tests-legacy-version-failures.xml` 4失败：HEAD源码原已v8而旧test仍断言v7，A已独立确认基线问题，未为本卡修改旧expectations；不称全仓全绿。

B1实际命令（cwd `/home/python/Quote`；每次先 `source /home/python/Quote/scripts/quote_dev_env.sh`，解释器固定完整环境）：

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_current_recheck.py tests/unit/test_research/test_company_profile_repair_format_integration.py tests/unit/test_research/test_company_profile_metric_format.py::test_new_draft_policy_is_opt_in_and_the_wire_contract_already_supports_null tests/unit/test_research/test_company_profile_metric_format.py::test_normal_gateway_extraction_preserves_original_receipt_with_transport_only_replaced -q -o faulthandler_timeout=120 --basetemp=/home/python/Quote/data/QuoteBak/company-profile-6fd-b-BlzNXwcp/pytest-final --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-stable-review-closure-20261124/final-integration-tests.xml

/home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_current_recheck.py::test_failed_current_recheck_resumes_exact_parent_not_earliest_extract 'tests/unit/test_research/test_company_profile_configured_task.py::test_explicit_plan_uses_official_source_gateway_first_read_and_saved_review_on_same_owner[source_recheck]' -q --basetemp=/home/python/Quote/data/QuoteBak/company-profile-6fd-b-BlzNXwcp/pytest-compat --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-stable-review-closure-20261124/current-continuation-compat-tests.xml

/home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_current_recheck.py::test_existing_model_metric_guards_survive_new_no_findings_and_changed_fact_fails_closed tests/unit/test_research/test_company_profile_current_recheck.py::test_failed_current_recheck_resumes_exact_parent_not_earliest_extract tests/unit/test_research/test_company_profile_repair_format_integration.py::test_repair_needs_own_final_and_preserves_original_receipt -q --basetemp=/home/python/Quote/data/QuoteBak/company-profile-6fd-b-BlzNXwcp/pytest-freeze-final --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-stable-review-closure-20261124/freeze-final-tests.xml

git diff --check
```

没有跑全仓、旧123或四家真实调用。B2有效55项与A源核按对应XML/回执复用。

## 保存真实材料的本地回放与限度

`saved-replay.json`索引两个真实current base及两份原002 repair wire，未复制完整API/PDF/正文。潍柴原34事实中BS03 business_segment、OD01 unclear保持；王府井原28事实无F18/F24，受控无意见后仍保持。原raw/作者/usage/源包不变，模型旧意见只在本地继承、不入新wire。

A `a-source-probe.json`已独立证明真实5候选、13metrics的typed/source readiness（7方向格式降null、6普通数值保持），B复用不重复同一自证。B的原repair owner回放进一步证明5目标进入各自final请求，实发source_text逐字同原wire；模拟final空响应仍不采纳5目标、保原拒绝/receipt。真实供应商final不存在，未写V1，也没有借其他候选final。信用卡交易额原候选日期字段与期间流量语义仍需它自己的源核，不能因typed-ready称全部正确。

实包回放首次被单测合成120k context上限挡在transport前；只在一次性离线fixture调context至1m后完成，未改业务预算/共享配置，也不构成真实供应商尝试。

## 保护、清理与交接

`final-freeze.json`复算现V1 **104/104**、旧根 **102/102** 文件hash全同A基线；633/1003、两份FC planned及旧成绩不改。A已启动一次fresh20独立末验，结果由A写 `a-terminal.json`，B不重复长查询。

开工核6.FC两根absent。B1已核无活跃本人Python进程，删除确切 `/home/python/Quote/data/QuoteBak/company-profile-6fd-b-BlzNXwcp`：**123文件、15,205,078逻辑bytes**，含本地库/脚本/导出/日志，根已不存在。B2清理10文件/62,176bytes按其交接记录分列，不冒称B1重算；A根未触碰。必要持久B证据仅本result、3小JSON和直接XML，原失败轨迹保留。

本卡工程闭环完成，真实质量/新供应商成功未评；无新研究版或生产启用。既有脏改保持，Git交A。

A末端独核已完成：`a-terminal.json`确认新实例20API等于原快照、104现/102旧逐hash保持；两真实current稿修订/撤回未回退。8源/3测试与freeze hash/AST一致，源码Review未剩当前阻塞，OpenSpec strict/diff通过。A另清13文件3,091,280逻辑字节；三方共18,358,534逻辑字节，详`cleanup.json`。本人代码/测试/规范增量可隔离提交，既有脏段落和并行不纳入。
