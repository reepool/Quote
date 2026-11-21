# 6.ER 日常命令执行单股精读

现在可以把API生成的具体问题计划放进显式执行文件，通过原`company_profile_common_core --action enrich`完成同报告补读及源复核。无需手写Python操作脚本；原稿保留，补充内容、未获取项和实际审核状态由原API/导出逐轮交付。用法见[日常命令](../../../../docs/development/company_profile_task_usage.md)。

基线`397d5d6e`。同一task_entry增加可选`enrichment_plan`，一股/报告版本与动作必须相符，转原TaskService.enrich核父链/原稿hash并执行目录选章、抽取及复核。CLI和scheduler只扩动作转发，未新增抽取/复核循环、写入owner或共享配置。旧first_read/review_batch文件不含新字段时保持原参数指纹，已有检查点不失效；新补读仍受原run_id/预算规则约束，不自动追满。

受控纵向验证使用生成的两页PDF、正式源适配器、真实CLI解析/main/run_job/目标job、原应用/网关/队列/writer，只替换通用初始化/分发、外部传输及通知。先首读，再经原API计划处理函数生成问题，落盘文件后由CLI精读：原主营事实保留，新增非空产品服务事实，经源复核完成；新实例与annual_profiles.json一致，缺项仍见，重复文件不创建新client或重发，过期原稿hash拒绝且原画像不变。HTTP API及原owner两次补读/失败保原稿由相关旧回归覆盖。受控结果不代表新真实模型质量。

验证共有42个不同用例，最终全部通过、0跳过：首次获准环境运行41通过/1失败（188.58秒），失败为测试复用旧request_id、先触发请求冲突而未命中其预期hash断言；仅给该测试的过期计划换独立request_id后，定向1项通过（64.35秒），其余41项复用相同代码的有效回执，未重复跑整套。更早沙箱运行在API线程调用未返回后中断，没有完整结果，不计通过。原始两份JUnit及[验证摘要](validation.json)保留，不把首次失败改成全绿。

两研究根17份保存稿计划（15家唯一公司）本地无调用地生成、完整序列化并消费检查通过，24个真实writer hash未变，见[saved-compatibility.json](saved-compatibility.json)。这只证明计划格式兼容，未重新核其内容或假称补缺成功。Review修正新增字段会改变旧v1指纹的兼容问题；无剩余新增A类阻塞。B类自动补满/批量精读及非息收入尾项后置，C类市场/配置/骨架和旧S2改动未动。

本轮真实调用/外传均为0，未修改真实画像、默认研究根、模型共享配置或生产调度，不重开S2/扩样。OpenSpec strict、目标语法与diff通过。本人QuoteBak根收尾清理，释放量及保留范围记在review-submission.md §365；正式源/有效缓存、原稿/失败/attempt及既有并行改动保护。下一步使用这条入口按业务需要提出新问题；不以补齐每个字段作为批量画像前提。
