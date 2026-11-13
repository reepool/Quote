# 已有公司画像进入查询／导出总览

本轮新增的是可消费的读取结果，不是再次采集或复核公司。原CompanyProfileReadService现在同时返回独立workpaper_summary；CSV保留原可信核心列，追加底稿内容和状态列，有底稿时另导出profiles.md。研究者可以区分“没有内容”和“已有内容但尚未可信发布”。

直接看[三家公司总览](profiles.md)或[CSV](profiles.csv)。总览里的业务描述直接取原business_structure事实，未调用模型另写摘要；完整内容链接复用原权威JSON/Markdown，不保留重复全文。

| 公司 | 可读底稿事实 | 结构化指标 | 原复核状态 | 未决问题 |
| --- | ---: | ---: | --- | ---: |
| 同方600100 | 38 | 36 | completed | 0 |
| 林海600099 | 47 | 59 | completed_with_open_findings | 1 |
| 黄山600054 | 59 | 39 | completed_with_open_findings | 1 |

计数按当前事实/指标对象统计，包括保原文、未标准化的指标，不是独立准确率、召回率或公司评分。三份仍为llm_generated且pending可信验证；原accepted_fact_count=0、core_complete=false和生产not_authorized未变。复核完成、无未决问题也不自动等于生产发布。

## 实现与兼容

沿原read owner/原writer，不新增接口服务、数据库、导入平台或生产开关。query的workpaper_summary包含来源类型、原业务描述、当前事实/指标数量、各要素数量、复核状态、未决问题数，以及原回执中实际抽取/初核模型。缺历史归因保持未知，未复核的未决数为null，不造“零问题”。人工作品和受控夹具保各自origin。

CSV旧列保持原顺序与原义，新增workpaper_*列；旧无底稿记录这些列为空，纯旧路径导出不强加profiles.md。新增Markdown总览来自同份投影，原文中的竖线/换行和HTML标记被安全转义，不另生成未经支持的公司描述。正文、引文、金额、模型调用历史、范围限制与可信状态均不修改。

CSV中新导出的外部业务描述若以公式字符开头，按文本前缀输出，避免打开表格时被当作公式；JSON中的原值不变。该直接安全分支含四种开头的1项定向复验3.78秒通过，三家实际CSV再与最终版本导出逐字核对一致。

## 零调用实物验证

仅从三份既有query取原workpaper/receipts/可信字段，在QuoteBak内由原writer保存只读重展现用的临时封装，再经原query/export和新实例读回。未运行语义阶段、provider或模型，临时封装明确记录零新工作；历史模型作者与用量保在原对象中，不冒充新真实处理。原文件hash、全部原字段值和原Markdown均保持。

第一次本地检查因同方旧JSON缺后来新增的可空元数据而拒绝逐字对象相等；没有改业务代码或旧结果。核明只新增null/空数组/exact等已有兼容默认值后，检查改为要求所有旧字段值不变、只允许列明的空默认值。失败原因和第一次清理仍留first-local-check.json，不伪造首次通过。

第二次三读一致，已有三家一次读取/导出耗时1.037秒；不含原采集、LLM复核、准备和开发时间，不能当作生成一家公司画像的耗时。全部原JSON/Markdown哈希未变，实际计数与CSV一致。必要结果见[validation.json](validation.json)，新模型调用/新处理公司均0，未将材料导入生产默认目录。

验证命令：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper.py tests/unit/test_research/test_company_profile_reads.py -k 'workpaper or export_writes_structured_json'
```

37项通过、7项未选，169.67秒；其中旧可信导出兼容用例149.60秒，不再重复。随后仅为新用例退出清理和缺历史模型断言复验5项，9.63秒通过。覆盖有内容但可信0、无/有/失败/未决复核、旧路径空列、混合路径、未知归因、特殊字符、原writer/三读及无新语义请求。OpenSpec strict及差异检查通过；有限Review未发现需要扩范围的新阻塞，未补可选字段或放宽任何质量底线。

## 清理与边界

两份本地重展现临时根各1,696,548常规文件字节均finally删除；本轮结束的6个测试目录共4,912,375字节核归属后清理，新增测试改用TemporaryDirectory保证以后退出清理；operator7,894字节交接前删除。已测合计8,313,365字节，不重报历史或未测测试空间。都是可由代码/原材料重建的临时产物，原权威画像、正式PDF、有效解析和并行改动未动。

当前没有新增付费、外传、公司运行或生产授权，S1仍0/12。下一应以可见内容判断当前版本的小组连续验证／必要消费接入，不再为黄山或林海的可选细节另派局部修补；新的真实运行仍需另冻结范围与授权，不能拿本轮零调用展示计数充成绩。
