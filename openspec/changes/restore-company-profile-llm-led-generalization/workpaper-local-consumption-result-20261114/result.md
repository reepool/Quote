# 6.BW：保存真实内容的局部消费收口

本轮交付不是再跑一个模型，而是把已有内容安全地交到阅读入口：[七稿索引](profiles.md)。大名城45、同方29、美尔雅33事实经原CompanyProfileResearchWriter保存、query/export/新读取实例一致。全部保留对象与各自旧稿相同，旧query/原响应/binding的SHA256未变；没有合成模型pass或采纳无final候选。

## 解决的实际问题与未解决项

- 合法`2.19个百分点`按完整后缀解析，不留“个”；改符号、改值、错单位仍拒绝，原文不改。
- 已有唯一fact ID按其真实要素定位，错误但已知的要素提示只作本地规范并留说明；已有事实的重要遗漏用原ID纠正，仍须真实final；次要遗漏只留说明，不撤回事实或新增修订调用。未知ID/要素/证据仍拒绝。
- 只在新绑定/修订上核所引金额附近明确货币单位，冲突隔离所属事实，不猜单位/换算；明确段落后缀优先于前表头。默认旧JSON读取不追改。

大名城仅隔离F015（租金原表万元/返回元）。同方F029与review-add:Q002已能typed解析，但整组源核仍报“metric number is absent from cited source: 主营业务收入同比减少额”；未到final、未采纳，原29条保留。到此停止，不为这一句继续放宽数值定位或追符号。

美尔雅原3条review现在能消费，F039按实际material_changes定位、重要遗漏按原编号待修；F036处置时点、F039原待补事实、F041无据未披露断言从可读正文隔离，不造修订。新绑定另隔离F016/F017/F018：正式PDF物理23–24页的品牌、线上和华中收入表头万元，原返回为元。即使按量级看原表可能有问题，也不擅自选择一个确定单位。合计39→33条，其余33对象不变。

七份原保存稿作新绑定回归：大名城仅F015、美尔雅仅F016/F017/F018因单位冲突退出；青山、同方、广州、中视、林海无额外事实丢失。没有执行七家公司或新增复核，详见[回归明细](seven-draft-binding-regression.json)。这只是绑定回归，不是全稿准确率/全报告召回。

## 来源与运行语义

重消费只取6.BV真实响应、原稿与正式PDF。原raw字节hash/JSON一致、PDF版本及全部保留Evidence坐标核过；origin和原generation保持llm_generated及原模型身份。新execution为offline_local_projection且新模型调用/账本/探针/可发送binding均0；所有review用量均为继承历史，不能算本轮新结果。同方原repair rejected、美尔雅原review拒绝记录不改。

本轮证明为原写入器及原read owner的三读，不是TaskService全链新跑。三份业务处理分别2.796/2.498/1.523秒，只是本地消费耗时，不是模型全流程时间。局部operator全局拒绝联网，未构造替代模型答案：[复现快照](operator-snapshot.txt)；若复现，脚本及输出须放QuoteBak，不能覆盖本次权威交付。

逐公司[大名城验证](600094/validation.json)、[同方验证](600100/validation.json)、[美尔雅验证](600107/validation.json)列明原hash、继承usage、保留/隔离对象与三读。core_complete仍false、生产not_authorized；旧6.BV8启动/7可读/5模型闭环、旧四类2/4及S1 0/12不改。

## 验证与有限Review

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_local_consumption.py tests/unit/test_research/test_company_profile_workpaper.py tests/unit/test_research/test_company_profile_workpaper_review.py -k 'local_consumption or metric or literal or qualifier or unknown_section or invalid_initial or invalid_or_unconfirmed or retained_review or revalidated_repair'
```

44 passed、40 deselected，10.67秒。没有重跑未改入口的149/169秒长链。正反包括旧JSON兼容、单位一致/不确定、金额段落后缀、未知目标/证据、改数与符号、必须final、次要遗漏不触发修订。旧wrong_element反例改为真正未知要素，已知错要素另有明确新合同正向测试。

初次测试抓到单位核误入默认旧JSON，已改为仅新绑定/修订；第一次回放抓到表后段落金额被前表单位误套，已修明确后缀优先并回归。同方格式通过后发现真实数值源核残余，保留限用；美尔雅新增3项冲突经正式PDF核实，不改原表或按公司加特例。上述不是新模型失败。

Review A类：三处真实消费/可靠性断点已作最小修正，未知引用/改数/无final不放行，七稿保留对象及旧hash核过。B类：同方单句进一步修正、供应链精度、全量表头审计/更多标准化均后置。C类：开工配置、骨架、规划与约1005行原审核等既有/并行改动保护，不纳本轮提交。OpenSpec strict通过，Git差异检查在提交前执行；CSV保标准CRLF和原hash，使用core.whitespace=cr-at-eol检查，不为风格改字节。

## 清理与下一步

开工未见本人前序无依赖根/脚本，释放0、不重复计算。三份最终临时根177461+127147+154636=459244字节退出即清；较早美尔雅36条中间根164609字节已清，旧本轮中间导出521516字节和临时operator11821字节收尾清除，可测合计1157190字节。中途失败TemporaryDirectory已finally清但未量测，不计空间；正式源、有效缓存、旧结果/尝试标记、既有及并行改动不动。本轮只保一套必要交付和复现快照，不留工作库/重复开发根。

主线下一是6.BX：海泰、开创、黄山、浦发四份上轮未启动报告各一次当前同版完整流程，直接交画像/复核及资源对照，不重跑已有七稿、不追供应链细度。新窗口已另询明确批准，批准前不绑定/调用，不借旧额度，不启生产。
