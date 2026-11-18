# 6.DT 批量供源与格式恢复：离线共性修正

本卡新增真实模型调用、外传及公司业务入口均为0。6.DS的20家首次结果和未通过结论不改；没有新增完整公司或准确率成绩。沿原来源、章节、网关和writer/read owner修复，不增加平台或逐股票规则。

## 现在已经能做什么

| 原阻塞 | 本轮行为 | 验证范围 |
| --- | --- | --- |
| 官方年报“（全文）”误拒绝 | 全文后缀正常识别，摘要/说明/相关公告仍排除 | 浙江东日原正式标题及通用年度/半年度正反例；未重新登记或启动该公司 |
| 无编号业务标题遗漏 | 恢复独立成行的常见经营标题，排除数字续行和重复页脚 | 中信原PDF的“报告期内公司从事的业务情况”变为可选源节，实际包349字符，含投资银行/财富管理/资管/托管等正文；未请模型重写画像 |
| 一页layout失败拖垮整包 | 用已经提供的共享原生文本局部降级；原生也不可读则明示该页未读，其他有效节保留 | 申万原导航选中的完整14页包可建立，7592字符；原生p29和layout丢标题页均有明确布局限制，不把它们当已恢复的对齐表格 |
| JSON恢复原样返回坏格式 | 同一有界恢复中加入具体错误位置、是否在EOF及可验证的闭合括号语法提示，要求返回完整修正对象 | 特力/平安银行两份原坏响应直接验证，旧raw/hash不动；未自动补括号采纳，未增加请求或恢复限额。真实模型是否会正确修复尚待新授权观察 |

新预处理为`toc_sections_native_layout.v5.plain-headings-local-layout`，格式反馈为`validation_feedback.v2.syntax-diagnosis`。旧保存导航按原编号策略重建，不能套用新索引后冒领原引文；广州旧候选原TaskService接续及三读复验通过，实际作者/旧rejected与原事实不变。

降级页使用`shared-pdf-layout-with-native-fallback.v2`，逐页说明随Evidence、reading_scope、模型源包和JSON保留；原生文本不证明表格列关系或数字归属。正常页仍用既有等宽提取，不全报告OCR、不猜列。当前run→原writer/query/export/新实例的非空受控路径及后续源复核seed都可保留降级来源；这是工程回放，不是真实LLM复核通过。

## 验证及限制

[精简证据](verification.json)含两份真实源包的标题/原页/长度/hash、两份实际失败的恢复诊断、旧16家原runtime/API不变及各次测试结果。原API仍16家/473可读记录/672指标，未覆盖旧研究根或改绿模型状态。没有额外持久测试库或复制完整PDF/画像。

最终定向回归146通过、1排除，47.40秒；另补原生不可读页无layout元数据的直接检查。146覆盖标题、章节、共享PDF/格式反馈、物理预算和原writer读回。此前大回归199通过、1失败、5未选；该失败为`test_initial_queue_wait_preserves_full_execution_budget`中的40ms真实时序，单独复验及测试进程回绑HEAD唯一改动函数后也失败，未改原时限/队列逻辑，不冒称全套绿色。广州原TaskService慢入口复验47.41秒已通过，复用该有效回执不反复跑。

命令均先`source scripts/quote_dev_env.sh`，使用`/home/python/miniconda3/envs/Quote/bin/python -m pytest`。最终范围为：

```text
tests/unit/test_research/test_business_profile_documents.py
tests/unit/test_research/test_company_profile_workpaper_source_recovery.py
tests/unit/test_research/test_company_profile_workpaper.py
tests/unit/test_utils/test_llm_gateway.py
tests/unit/test_utils/test_llm_physical_budget.py
-k 'not initial_queue_wait_preserves_full_execution_budget' -q --tb=short
```

首次新增测试误把旧索引判断成“无标题”，实际是只剩数字伪标题；已改为明确核原业务标题缺失/数字伪标题存在。兼容回归暴露的广州旧导航重建已显式使用原编号策略；不是删除非空/三读断言或放宽业务门槛。完整申万源包又暴露“文本可读但标题丢失”的边界，已同卡处理并复验完整原选章，不只测试p29函数退出。

Review：A类入口/供源及旧导航错接按本卡修复；B类非典型标题、精细表格恢复和可选供应链不追满；C类40ms基线时序失败及原工作区并行不动。OpenSpec strict、diff/来源/旧读取hash核对及本人改动隔离后收口。开发/人工核查未精确计时，不报零成本。

## 下一步

工程阻断已收窄，但本卡不能证明新模型召回率或真实JSON恢复成功。下一次真实观察应只检验这些共性修正与基础业务内容，不重抽原12家或为供应链精细化加窗；新运行需新的冻结版本/有界调用授权，本批20家不再是未见样本，不自动启动确认组、S3或生产。

开工已核上一轮本人临时根无残留；结束核必要证据及退出进程后清本卡QuoteBak测试/脚本/过程XML，仅保本结论、精简verification和必要复现脚本快照。具体文件字节与保留项见cleanup.json。正式PDF/共享有效缓存/旧raw与API/attempt/既有并行保护，删除的派生物可据源码/来源和快照重建。
