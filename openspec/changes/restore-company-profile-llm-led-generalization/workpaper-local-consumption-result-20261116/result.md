# 6.CU：保主要业务内容的零调用消费修正

本轮完成原消费链三个实际阻塞的局部修正。[可读改进](profiles.md)直接展示上汽主要收入构成；不扩字段/行业规则/平台，不为可选供应链或尾项付费重抽。原八要素、可选特色、公共网关、TaskService和唯一writer保持；零新模型/探针/可发送绑定，S1仍0/12、生产未启，旧CT结果和50次/1238665账本不改。

## 业务结果

- 上汽原模型的分产品/分行业主要收入金额不再因正文“亿元”展示被整条误删。只对同一事实已核原始数值/明确源单位、同主体/期间的指标做十进制倍率/舍入核对；精确raw/value/unit/evidence不改，正文展示附程序限用说明。无指标、错数/符号/量级、单位未知/冲突或主体期间不符仍不接受。F19没有合计指标，仍隔离，不程序补数。
- 上汽复核ID名单与当前事实集不一致时保留已知唯一检查范围和三项合法意见，隔离F23/F30并可到原一次修订边界；缺失/未知ID不当作已检查，整体仍incomplete。合法意见不会再因另一项错误或覆盖不全丢失。未知证据/目标、重复finding ID均局部拒绝且留原因，原raw不改。
- 海泰minor/other的possible_omission挂F24时，作为关联补充保留，不撤正确原事实；两项原意见都保留，只有material补充进入修订。两家没有原修订响应，所以用明确受控失败停止，不伪造成候选/再核成功。
- 广州保存的23448字符坏JSON经实际CommonGateway两次替传输仍失败，第二次附JSON错误位置及原响应。只在原已许可重试内反馈，不增加尝试或改共享配置；其他调用不启用此元数据时行为不变。不本地补JSON或改判旧失败。

执行身份为review v9/source-binding v5/format-recovery v1，抽取prompt仍v18、guide v4/预处理v4及投影schema不变。旧读取不运行新源绑定，不改旧workpaper形状/hash。舍入说明只用于展示，不代替科目/主体的模型源核，也不宣称输出全对。

## 验证与已知限制

统一命令前缀：`bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q`。

| 范围（tests/unit下） | 结果 |
| --- | --- |
| `test_research/test_company_profile_workpaper_source_cells.py`与`test_company_profile_workpaper_review.py`，`--confcutdir=tests/unit/test_research -k 'not task_service and not gateway_owner'` | 末版127 passed/3 deselected，51.07秒；前版同范围127/29.20秒不相加 |
| workpaper、features、local_consumption三文件，`--confcutdir=tests/unit/test_research -k 'not task_service'` | 74 passed，57.80秒；末版显示源绑定另以两份原raw重新核对，38/27绑定对象不变 |
| 新`test_company_profile_workpaper_ct_consumption.py`两条原TaskService | 2 passed，125.52秒；实际业务调用30.230/22.584秒，三读一致、原件hash不变，controlled_fixture而非新真实LLM |
| 同新文件`-k guangzhou` | 1 passed，5.21秒；原JSON两次失败、反馈载荷/次数/原件验证 |
| `test_utils/test_llm_gateway.py`、physical_budget、stream_diagnostics三文件，`--confcutdir=tests/unit/test_utils -k 'not initial_queue_wait_preserves_full_execution_budget'` | 88 passed/1 deselected，56.24秒；含新恢复正反、物理预留/失败账本/流式与截断 |

扩展到routed_client的第一次111项观察18失败/93通过、101.90秒，单独重验仍15失败/96通过、62.49秒，多数为0.04–0.5秒时序窗口；没有说扩展套件全过。两项代表性失败（initial_queue_wait及rate_limit_failover）在未修改HEAD客户端代码内存装载后同环境复现，2失败/3.26秒；获准执行环境对应观察1失败/1通过4.10秒。只能确认这两项并非本轮引入，其他扩展失败未逐项归因，未改时限/断言/生产调度。其余直接相关网关、账本和业务回归通过，不为既有时序问题扩成调度/日志重构。

广州测试构造先缺开发预算授权、后缺provider必填参数，修正夹具后按原常规预算/原输出约束通过，未放宽业务合同。OpenSpec初次因把场景插到requirement正文前未通过，改文档位置后strict通过。requests既有依赖warning如实保留，不升级依赖。

有限Review：A类为局部消费不得自动全绿、无源单位不得作展示换算、已核错误不得复活，直接正反及保存响应已覆盖；B类显示说明精简/特色呈现后置；C类既有时序回归、配置/骨架/混合段与其他并行不修改。没有把有源数值存在等同语义正确；没有真实修订/再核仍限用。

## 清理与下一步

开工本人CT/CU无依赖残留0、释放0。两条原入口测试TemporaryDirectory/finally清库、导出及checkpoint，清前已量2194841＋2085375＝4280216字节；其他受控测试根退出清理、体积未量不虚加，不新增临时operator/整套PDF副本。持久材料只留三份结果摘要、本说明及可读改进；正式PDF/有效缓存、原CT回执/attempt和既有并行保护，未按日期盲删共享工具缓存。

下一最短业务工作应是上汽/海泰保存真实初核后的必要修订/再核，或广州唯一格式问题的一次有界恢复；须另明确新调用范围，不借CT余额、不重跑原十二家。当前不启动新外呼，不把本卡工程成功或新增可读内容计正式S1/生产可用。活跃开发/审核工时未精确计量unknown，测试秒数不作人工工时。
