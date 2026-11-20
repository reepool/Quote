# 6.EI：画像API可规划复核失败接续

本轮交付的是只读接续能力，不是新增真实模型复核：原年报API的`readings[].review_follow_up`区分补初核、补修订/再核、只补终核、执行已完成及需先检查合同/历史；新增`POST /api/v1/research/company/{instrument_id}/annual-profile/review-plan`绑定具体阅读的报告、工作ID和当前内容hash。原首稿、缺项、实际模型状态及局部限制保留，不重抽、不自动排队或调用，不创建新事实库/调度器。

## 用户可用行为

| 情况 | 接续计划 | 现有内容 |
| --- | --- | --- |
| 尚未初核或初核通道失败 | `review_saved_draft`，从初核起 | 首稿继续按原状态可读 |
| 一次合并修订执行失败 | `repair_and_verify`，复用初核 | 错项仍局部隔离 |
| 已有修订候选但终核通道失败 | `verify_only`，复用初核与修订 | 候选不因本地合法就采纳 |
| 审核执行完成但留补充意见 | `not_needed`，不自动重跑 | 缺项/限用照常显示 |
| 回执损坏、阶段未知或合同/源问题 | `blocked`，先检查 | 仅阻止盲目接续，不拒整家公司 |

回执复用检查原JSON/hash、purpose/request ID、完成状态、类型及已记录的模型/通道身份；原文、原稿、独立使用限制和有限授权仍须由既有TaskService/seed/resume路径在执行前验证。`max_new_logical_requests`只是最多剩余语义阶段数，不含物理重试，不是已获预算。HTTP计划不输出原始响应、原错项或未采纳候选正文。

请求示例：

```json
{"request_id": "saved-final-follow-up", "reading_work_id": "已有阅读工作ID"}
```

省略`reading_work_id`则选择当前同报告读取链的末端；跨报告/链外工作ID拒绝，投影到绑定之间内容改变也明确拒绝。具体字段和调用边界见[API说明](../../../../docs/api/restful_api.md)。本地ASGI成功不表示共享服务已经部署；没有改变共享配置或生产授权。

## 现有真实材料的零调用核对

沿`company_profile_galaxy_v7_20261120`现有研究根读取十五家公司，426个可读对象/684指标保持，包含跨轮重复，不是新增或去重经济事实。19份受保护文件（原失败记录及当前writer文件）的hash前后相同；详[保存材料核对](saved-plan-check.json)。

末端阅读的计划为11 `not_needed`、3 `planned`、1 `blocked`：分众/国航补初核，顺丰原5候选只补终核，通威历史阶段/合同问题先检查。11仅指审核执行状态，不能推算全稿准确率；3份计划未真实执行，通威已有内容未被拒绝。旧S2失败/未启动五家/原质量阈值/生产边界不改，没有恢复旧窗口或增加公司。

原顺丰终核失败及分众初核失败的保存真实回执各自给出正确阶段，不改历史缺失元数据。另以受控网关产生初核/修订/终核失败，保存后重读，并将plan选出的原成功回执接原终核消费者；接续只发一次受控终核，原主营保留、原错利润仍隔离，只有终核接受的营业收入候选才进入结果。它证明工程消费路径，不是供应商恢复或真实语义准确性证明。

## 验证与Review

最终命令：

```text
python -m pytest tests/unit/test_research/test_company_profile_review_recovery.py tests/unit/test_research/test_company_profile_catalog.py tests/unit/test_research/test_company_profile_workpaper_continuation.py tests/unit/test_research/test_company_profile_annual_export.py tests/unit/test_research/test_company_profile_review_failures.py -k 'not test_task_service_stops_before_third_company' -q --basetemp=<本轮临时根>/pytest-targeted
```

实际77 passed、2 deselected、0 skipped，43.30秒，6个既有依赖警告；覆盖新计划/HTTP/原接续、目录、合并导出及原始序列化保护。测试集合有重叠，不累计成更大测试数。OpenSpec strict及diff检查通过。

如实保留验证过程：首次沙箱ASGI挂起后主动中止；获准环境首跑16通过/1失败，是新测试误把现有导出JSON包当列表，纠正本轮断言后53项通过。一次扩大验证在第二个长TaskService队列用例处达到120秒，72项已完成、剩余未完成，不称全套绿；最终定向集合显式排除这两个未修改长用例，不为其扩改队列或变更正确预期。

Review：本轮直接风险为阅读版本变化、坏/重复/错身份回执导致重复成功阶段或错误归因，均已明确拒绝并覆盖；无已知新增阻塞。B类自动后台调度/性能优化后置，C类既有核心、共享配置与市场并行改动不动。原workpaper/源hash输入和执行消费者未变，本轮只新增安全投影与计划入口。

## 资源及清理

新真实模型、探针、物理请求、外传、公司writer记录均0；受控传输不记为供应商费用或新公司成绩。当前研究根/共享配置未改，不启生产、S3或旧S2补窗。必要一套保存核对/验证后，清本轮唯一QuoteBak临时根内脚本、重复导出、测试数据库和中断测试残留；具体范围与文件字节见[验证及清理记录](validation.json)，正式源、有效缓存、原失败/attempt及既有并行改动保留。

本轮接续计划能力到此完成。后续应在明确的新有限授权下选择实际失败阶段补核，不重抽整个批次、不把重要补充缺项升级为门槛；本轮不承诺或触发新的付费运行。
