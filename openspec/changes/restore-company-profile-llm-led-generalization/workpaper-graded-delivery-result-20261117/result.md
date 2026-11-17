# 6.CZ 原查询/导出已接通研究分级

现在不是只在文档里屏蔽：原CompanyProfileReadService的query、JSON、CSV和Markdown都消费同一事实级分级投影。查看[十二家实际读回总览](profiles.md)、[可程序读取的分级JSON](research-delivery.json)及[实际导出CSV](profiles.csv)。每家公司Markdown是原读取入口的导出摘录，只省略长复核日志和引用全文，章页位置与原query链接保留。

## 实际交付

原371对象保留全部原值、主体、期间、Evidence、模型和原复核记录；新开发副本经原writer持久，再query/export/新实例读回，12/12一致。

- 普通已核327条，分别标明独立源核或模型源核；“已核”是研究核对范围，不是正式公司可用等级、准确率或生产接受。
- 保真限用34条，其中6条仍待核，均为上汽旧核验缺少可绑定当前内容的依据。已核的限用和待核的限用分别展示，不通过清状态掩盖。
- 4条仅源参考/解释、5条隔离、1条重复不进入普通正文或交付指标；它们的原文/原指标仍在原底稿和审计中，没有删除或改数。

“待核合计（含限用）6”是交叉口径，不能再加到上述371个对象上。初次工程读回已三读一致，但只列互斥分级时容易把“待核0”误读成全已核；小回执留在initial-readback.json，随后补齐交叉计数及逐条限用原因，最终以validation.json为准。没有新模型窗口或抹掉真实失败。

## 原入口怎样消费

`profile.workpaper_delivery.facts`是普通研究消费入口。每项有`state`、`review_basis`、原因和证据ID；`fact`非空才有可展示的原事实。隔离/源参考/重复项的`fact=null`，其值和指标不会混进普通正文。未核内容仍可以作为明确标识的待核草稿阅读。

`profile.workpaper`明确标为`workpaper_raw_for_audit=true`，保存原始底稿及新使用审核元数据，仅供追溯；不要绕过分级把它当已接受事实。既有`workpaper_status`仍是模型流程状态，`accepted_facts`、维度/`core_complete`和生产权限保持原义，当前仍为空/false/not_authorized。CSV新增分级计数和“含限用的待核合计”，业务摘要已去掉不允许普通展示的对象；仓库交付CSV仅将行尾转为LF，解析后的全部字段与实际导出一致。

新独立使用审核`use_review`绑定完整报告身份及逐fact文本/指标/引用hash，并记录审核人、依据包、证据及限制。抽取DraftResponse不能写此字段。未知/重复/错版/陈旧决定拒绝；来源不确定可以限用保留，不强求统一精确值。生产代码不读取OpenSpec或公司清单，操作适配器才将原CT/CV/CX已有审核绑定到新的开发副本。

模型核对也需要当前内容绑定。今后的新初核本地记录实际输入fact hash；旧未改稿可按其原输入整稿hash核对。保存的真实初核输入能精确匹配时，可本地补回逐条绑定，未变内容才继承；广州本次复用CW真实输入完成此接线，不新增核验。复用旧初核回复的接续不能把新seed重新写成“模型已读过”，旧编号/公司completed本身都不足以放行。旧use_review缺省和旧checked_fact_hashes缺省均不序列化，旧稿/hash保持兼容。

## 验证与边界

直接新分级测试、既有原writer三读、错误优先级、近似限用、陈旧/未知/重复/错版、模型无真实回执、修订缺最终核验及原输入/保留回复区分均覆盖。六文件180项55.72秒通过，排除3个未受影响的TaskService长测；末端新分级＋旧格式兼容48项17.20秒通过，排除1个无变化的长入口，两组重叠不加总。命令均由`scripts/quote_dev_env.sh`和Quote Python执行：

```bash
python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_delivery.py tests/unit/test_research/test_company_profile_workpaper_review.py tests/unit/test_research/test_company_profile_workpaper_continuation.py tests/unit/test_research/test_company_profile_workpaper_local_consumption.py tests/unit/test_research/test_company_profile_workpaper.py tests/unit/test_research/test_company_profile_workpaper_features.py -k 'not task_service'
python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_delivery.py tests/unit/test_research/test_company_profile_workpaper_format_consumption.py -m 'not integration'
```

12份真实保存结果经原writer/ReadService持久读回，普通正文/指标不含已知禁用对象，原值及原文件hash未变；当前代码重建的投影与保存结果、CSV的待核合计再次核对一致。新TaskService语义运行、新数据库运行、新模型/联网/外传/usage/费用均0，不称新抽取、同版首发或S1成功。原调用及unknown费用不改，研究分级不自动启用生产/DCF/交易。

有限Review修了A类“限用里的待核被总计隐藏”和旧保留初核套新seed的问题；已知错误进入隔离、范围限制可读、其他原有内容保留。B类写作/章名/细项、C类既有并行不动。不补满供应链、不追所有状态全绿，不增加通用质量平台。

本轮三个离线开发读回根均finally清理，合计17,883,216字节；最终只留一套分级结果、必要初次小回执和脚本快照，无十二份重复原query/PDF库。临时脚本11,944字节及无依赖计数副本12,287字节在hash核齐后清，合计17,907,447字节；其他直接测试finally清而未量，不虚加。正式源/有效缓存/旧回执/attempt和既有改动保留。OpenSpec严格校验、diff及原件/hash/CSV/当前投影一致性核查通过，Git只隔离本人代码/测试及CZ新增文档/结果，正常提交推送，不纳入旧混合改动。

本卡的分级消费功能已完成；正式接受、最终同版质量/资源及生产授权仍是独立验收，不能直接改core_complete制造通过。下一应按已明确的研究消费范围制定有限正式验收，不再追加展示层或单家公司细节修补。
