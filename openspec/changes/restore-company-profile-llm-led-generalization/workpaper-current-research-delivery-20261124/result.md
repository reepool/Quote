# 6.FF 当前20股统一研究交付完成

同一个原API/ReadService根现交 **20股、647可读记录、1050指标**；124个active缺项/123个失效历史记录（缺省active=true），不是必补任务或质量分母。A `a-terminal.json`已通过完整20profile、API/annual JSON等值及227旧文件保护。基线`8d8cbeb5`，0新联网/模型/供应商token/收费；未stage/commit/push，写集已释放。

## 当前入口与内容边界

唯一显式root：`/home/python/Quote/data/research/company_profile_research_v1_20261123`。本轮新入口为 `research-current.md`、`current-api-20261124.json`、`exports-current-20261124/annual_profiles.json`、`annual_profiles.md`及`profiles.csv`，原owner共导出46文件；20导航锚点和26个本地链接有效。旧annual-api/research-use/exports及6.FE独立增量根不覆盖。

仅追加6.FE已核两份writer：`bp-work-75219f282a5b80afada802ac.json`、`bp-work-5470620ee9834702d5a05c32.json`；写前确认不存在，原字节/hash等于冻结源。两股完整profile等于6.FE，余18等于原V1，20个compact canonical hash匹配A独立预期。新当前输出和使用页hash、两个追加hash及保护结果见`delivery-verification.json`。

兴证/海直14个可读对象/47指标是6.FE已核内容接入，未新抽取。RM007五个错误scope指标仍limited并公开缺口；海直跨行通航运输金额候选仍repair_rejected、未终核不采纳，非目标99.13%不算新API内容。兴证分部金额表本轮未读到，不等于原报告未披露。两份审核仍completed_with_open_findings，旧UseReview和缺项不自动清除；6.FE的7物理/143802 supplier total/293.958秒/unknown费用只作历史说明，本轮不重新调用或收费。旧97.8967%及正式质量/规模/生产边界不改。

API进程只需显式 `QUOTE_COMPANY_PROFILE_RESEARCH_ROOT` 指向上述root，共享配置未改。使用页已带实际挂载前缀：`GET /api/v1/research/company/{instrument_id}/annual-profile`、`POST /api/v1/research/company/annual-profiles/query`、`POST /api/v1/research/company/{instrument_id}/annual-profile/enrichment-plan`；计划仍planned、未执行/无预算，不创建provider或任务。本卡未启动生产服务。

## A授权P2修复及最终验证

初版发布没有源码变更；随后A明确裁决重复source_integrity是实际API/导出P2，仅改`research/company_profile/reads.py`和新`tests/unit/test_research/test_company_profile_read_call_snapshot.py`。每次调用严格validate每个record一次，选择/谱系共用同批typed对象；不跨请求缓存，不跳source/hash/UseReview，不改公开签名/处理身份或业务值。导出前廉价重读文件hash，拒绝并发改稿、新增/删除/可读性变化；稳定不可读无关文件继续局部忽略。

最终 **17个不同用例通过（12新+5原兼容）**，初9项是12项子集不重复计；覆盖两股多谱系/限制等值、每文件只validate一次、源错/独立hash错/祖先hash错仍隔离、后续请求看见变动/新record、导出并发拒绝和稳定不可读兼容。精确冻结hash见`read-fix-freeze.json`；`git diff --check`通过。未重跑旧600秒CLI或全仓套件。

冻结后现20股原owner读取 **2.720秒**、临时20导出 **6.369秒**，完整profiles等于现快照；A独立fresh20 **2.332958秒**。见`fixed-owner-verification.json`。正式current API和已完成46份导出未因修复重写；临时导出核后清理。

`http-verification.json`证明原router的20股batch、两股single及两股planned/hash绑定共5笔响应/断言通过，局部挂载`/api/v1`；进程级async依赖仅返回原root-bound ReadService，handler仍走原asyncio.to_thread，socket联网禁用。20股batch没有重复。首次两股40秒未得响应不计通过；P2后整组45秒已完成20batch+兴证两笔，未完海直两笔只作短接续。最后两笔响应/断言完成并持久回执后，进程在事件循环关闭阶段仍由外层50秒到限退出124，**不称ASGI整进程clean exit或全程全绿**；实际5笔响应证明与生命周期限度分列，A已按实际范围接受，不继续扩HTTP/平台治理。

A首轮误把research_profiles.json当annual聚合造成的观察false保在A文件；正确比较为annual_profiles.json，本卡未为观察误差改数据。初版90秒导出/40秒HTTP及本地spaced-vs-compact hash观察修正保如下历史，不追改首次结果。

最终定向命令（cwd`/home/python/Quote`，每次先source项目统一环境）：

```bash
QMT_BUSINESS_API_SANDBOX=1 timeout 90 /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_read_call_snapshot.py -q --basetemp=/home/python/Quote/data/QuoteBak/company-profile-6ff-b-MmcRLSKs/pytest-read-final --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-current-research-delivery-20261124/read-snapshot-final-tests.xml
QMT_BUSINESS_API_SANDBOX=1 timeout 90 /home/python/miniconda3/envs/Quote/bin/python -m pytest tests/unit/test_research/test_company_profile_annual_export.py::test_combined_export_matches_api_after_two_readings_with_local_restriction tests/unit/test_research/test_company_profile_annual_export.py::test_export_uses_selected_base_not_another_latest_report tests/unit/test_research/test_company_profile_annual_export.py::test_changed_snapshot_is_rejected_before_writing_another_base -q --basetemp=/home/python/Quote/data/QuoteBak/company-profile-6ff-b-MmcRLSKs/pytest-export-compat --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-current-research-delivery-20261124/export-compat-tests.xml
```

已执行薄脚本命令：`timeout 65 <Quote-python> <Btmp>/verify_fixed_owner.py owner`和同脚本`http`；余下两笔为`timeout 50 <Quote-python> <Btmp>/finish_http_remaining.py`，均带`QMT_BUSINESS_API_SANDBOX=1 PYTHONPATH=/home/python/Quote`。脚本hash保留，临时脚本已清；源码test可按上述命令复验，不把会生成临时导出的命令称为纯只读。

## 保护与清理

原V1104文件+6.FE21文件+旧102=227文件不变；A已核其他既有脏改保持。B不碰配置/骨架/scheduler或A管理文档。6.FE有效语义审核复用，不新造质量成绩。代码和test按A最终Review无剩余本卡阻塞后冻结释放。

本人`/home/python/Quote/data/QuoteBak/company-profile-6ff-b-MmcRLSKs`已核本地进程全部退出并删除：**214文件、20,811,752逻辑bytes**，含脚本、fixture writer、临时导出/测试物；没有正式PDF/cache/旧writer或A根被删，必要证据和当前业务输出保留。开工已核6.FE本人根不存在。Git及最终规范交A；不再改reads/test、添加HTTP、尾项或新调用。

## A末端审核

A独立新实例读取完整20股，全部canonical hash等于发送前独立预期；两股内容/状态等6.FE，其余18股等原快照，227旧文件及无关既有脏改保持。正确年度JSON与current API相等，源/模型/使用限制没有借读取优化改变，原5指标限用与跨行收入未采纳保持。A先前误选raw导出投影导致的一项观察false已修，首次记录保留，不改业务内容或期望。

A有限源码Review发现并修正一个兼容性问题：稳定不可读无关文件须继续局部忽略，而可读性变化须拒绝变稿导出；最终实现/12新用例及5既有用例已核，代码/test冻结hash和AST成立。其余非阻塞优化后置，没有本卡剩余业务阻塞。HTTP只接受5笔实际响应和计划绑定范围，关闭到限继续明示，不将它写成整进程通过或生产服务已启用。

OpenSpec strict及diff检查通过。B已清214文件/20,811,752逻辑bytes；A清理与被最终12覆盖的早9成功XML移除见`a-cleanup.json`，首次失败轨迹保留。Git只提交本卡本人/受委托B的可隔离代码、测试及文档增量，不纳原有脏改；活跃开发/审核未精确计时，不将2.333秒读取当全部开发用时。旧97.8967%及正式规模/生产边界保持，本卡研究交付收口。

## 首次发布与短限轨迹（历史记录，最终状态以上文为准）

基线`8d8cbeb5`，0联网/模型/新供应商token/收费，不stage/commit/push。只将6.FE两个已核writer原字节追加到原V1 namespace，源hash吻合，原125业务文件与旧102保护。追加/原对照阶段0源码变更；A随后明确裁决6.FF内P2，现仅修改`reads.py`和一个直接新test，在单次调用内复用严格验证typed records，不跨请求缓存，不放宽source/hash/UseReview。

当前API已在原20根生成：`data/research/company_profile_research_v1_20261123/current-api-20261124.json`。实际20股647可读记录/1050指标，124个active缺项记录、123个失效历史记录（缺省active=true），不是必补任务或质量分母。两股完整profile等于6.FE快照，其他18完整profile等于原V1；20项compact canonical hash全部等于A独立预期。原104文件、旧快照/旧exports、6.FE根保持。新增研究入口页`research-current.md`已给同根20导航与限用；原owner新exports已完成46文件，annual JSON的20份profiles与当前API逐内容相等。

首次本地hash比较用了带空格JSON，结构逐内容已相等；按A compact序列化重算后20匹配，没改事实/预期。首次整段发布90秒在原ReadService.export重复typed/source验证处到限，栈为`workpaper._wrapped_table_cells → _check_fact_source → source_integrity → reads._research_profile_from_record → export`，并非模型/联网或全系统启动；当前快照及两个新增record保留。仅接续一次原bulk export，独立180秒上限，不改源码/源校验。

HTTP首次仅局部挂载原router、进程级显式root、阻断socket联网，尝试两股batch；40秒到限未取得响应，trace在`asyncio.runners.close → loop.run_until_complete → selectors.select`。未得single/plan结果，**不计HTTP通过、不称20HTTP**。按A指示不多轮重跑，等待A是否需要一次20读取或依赖接点裁决；原ReadService及年度JSON对照不等于此HTTP检查通过。未启动服务、未写共享root配置、未创建provider/checkpoint/预算。

精确本地命令（均先source项目统一环境，`QMT_BUSINESS_API_SANDBOX=1 PYTHONPATH=/home/python/Quote`）：

```bash
timeout 100 /home/python/miniconda3/envs/Quote/bin/python data/QuoteBak/company-profile-6ff-b-MmcRLSKs/publish_current.py
timeout 185 /home/python/miniconda3/envs/Quote/bin/python data/QuoteBak/company-profile-6ff-b-MmcRLSKs/finish_export.py
timeout 60 /home/python/miniconda3/envs/Quote/bin/python data/QuoteBak/company-profile-6ff-b-MmcRLSKs/check_http.py
```

开工已核6.FE本人根不存在。本人当前临时根`/home/python/Quote/data/QuoteBak/company-profile-6ff-b-MmcRLSKs`，只含脚本/诊断，无数据库/PDF/20股副本；必要保护及汇总见`delivery-verification.json`。末端清理待运行退出后执行，A根不碰。
