# 6.CW 广州发展：零新调用交出待核画像

广州600098此前无稿，本轮复用保存的真实模型正文，原TaskService/唯一writer交出结构化候选：[原查询JSON](query.json)、[同源完整候选](profile.md)、[入口及原件保护核查](verification.json)。**28条候选不代表28条已准确或复核通过。** 本页是有限核源后的阅读摘要，不是另一次模型作答；下面不采用已发现的F30问题。

## 先看公司是什么

广州发展是一家综合能源企业，经营电力、燃气、能源物流、新能源及储能，并开展能源管理服务和配套金融业务。公司形成产、购、销、储、运、用的能源产业链：电力、蒸汽由所属发电设备生产；天然气、煤炭、成品油等外购。燃气集团经营城市高压管网和天然气购销、输配及分销，能源物流集团经营煤炭及油品物流仓储。来源：第三节《管理层讨论与分析》→《一、报告期内公司从事的业务情况》，物理页9—10，source-0001/0002；对应F01、F03—F10。

销售产品包括电力、煤炭、天然气、蒸汽、成品油；也提供装卸、运输、储存和综合能源服务。电力主要通过国家电网、南方电网销售，其他能源产品通过自有管网、销售平台及一体化物流销售链面向终端用户。公司主要服务华南与粤港澳大湾区，同时经营范围延伸到国内其他地区及海外。以上仍是原模型的有源归纳，不推断未披露的具名交易关系。

2025年**营业总收入508.83亿元**，报告叙述同时列利润总额30.51亿元、归母净利润23.19亿元。这里不将“营业总收入”改称“营业收入”：物理页17利润表另列营业收入50,841,626,989.35元，两个科目不混用。508.83亿元的来源是第三节经营讨论物理页12，source-0005，F11。装机容量1,097万千瓦、火电可控484万千瓦、风光可控613万千瓦，来自物理页9的F15；不据此自行重算报告所称绿色低碳能源比例。

供应链已能作粗粒度描述：公司既生产电热，也采购、输配、贸易和储运能源，并服务终端客户。相关内容在基础栏目内，模型没有另填可选supply_chain容器；不能把空可选容器误报成完全没有供应链信息，也不要求再补物料明细。

## 什么仍不能采用或尚未核完

- 程序保留28/34对象（26基础＋2自主特色），F12/F13/F14/F18/F24/F32六对象局部隔离，具体原因见verification。部分原文确有金额，但正文换算与事实/指标的主体范围不能直接关联；这不等于六条都是模型编造。原34对象/金额/引用仍完整保存在CV坏正文，不本地补写、拆句或改scope去放行。
- 因此主要分行业/分产品收入构成尚未形成当前可采用的结构化答案，不说“未披露”。原收入表在物理页18—19已经供源，不能归因于没读到；也不为本轮追齐全部构成再调用。
- F30仍在完整待核候选内供审计，**本页明确不作为结论采用**：物理页18“本期公司业务类型、利润构成或利润来源发生重大变动的详细说明”及页22相关项目只勾“不适用”，不单独证明经济意义上“没有重大变动”。不得复制为可靠发布事实。原query不改写，后续正式复核应处置该对象。
- 此次只有限独立核主营/产品/渠道/采购、营业总收入与装机原句，检查F30及被隔离的主要收入上下文；其余候选和特色未做逐条真值审核，quality_review为空，不报准确率、全报告召回或发布通过。
- 原章节索引保留了“3.3 倍”“3.8 亿立方米，同比增长33%”两个伪标题，物理页11—15仍归经营讨论正文。原页坐标和引文已核，当前不改历史导航或扩成标题解析专项。

## 做了什么，没做什么

只补一个缺失末尾对象闭括号，再将误嵌elements的company_features、limitations归回根层；事实文本/指标原值/证据ID均未增删改写。标准化对象hash为`426d294ff7156c46cf3611beee4d4ba5a3b9f202e0c2a6b5403fd46eabe661bb`，原正文hash仍为`6465c28c656dcc7c592b9f3b6b5a13971d8cf48748f7095f2d707c461f4ca8bf`。随后才运行现有结构、来源/数值/单位检查和局部隔离，不能把修格式解释为修语义。

显式hash-bound local_format_only模式不修改公共网关默认解析；拒绝重复键、截断字符串/数组、冲突/未知字段、错版与缺基础要素。原模型是实际物理回执记录的官方deepseek-flash，原抽取contract_status仍rejected，原用量留在CV，不造成功抽取回执。标准化记录随原JSON/Markdown交付，旧无此字段的序列化/hash形状不变。

本轮0新模型、0网络探针、0外传、0新供应商usage、0新增模型费用；旧费用unknown和旧失败不改。成功本地TaskService阶段调用21.316秒，完整集成用例55.84秒/pytest59.80秒，包括建库、源包重建、读取等，不能用作真实模型全流程耗时。查询、导出及新实例profiles一致，原页坐标有效、可信accepted_facts为空、core_complete=false、未二次审核。S1仍0/12，生产not_authorized。

## 验证、Review与清理

- 新本地模式21项直接7.40秒通过；追加已成稿checkpoint篡改保护后，新文件＋原接续最终53项20.66秒通过（与234项有重叠，不加总）。广州真实保存内容原入口1项59.80秒通过，无模型替身作答或新传输。第一次集成因测试读取不存在的real_semantic_complete字段和错误调用storage.close失败，已按实际接口纠正；不是一次真实运行失败，也不隐去这次复验。
- 既有workpaper/特色/局部消费/复核/源单元/接续六文件234项213.37秒通过，证明旧元数据与受影响读写兼容；包含三项原TaskService长用例，不再重复。不是全仓套件全绿声明。
- Review仅处理当前阻塞：缓存已有画像时同样校验接续payload/hash，防止磁盘恢复跳过格式输入绑定；新增已标准化/未标准化checkpoint的正反证明。非阻塞章名/写作取舍后置，无公司规则或共享配置改动。
- 开工未见本人CU/CV/CW无依赖临时残留；测试TemporaryDirectory退出清理。成功入口可量清理2,190,520字节；失败入口及其余直接测试也由上下文清理，但未量体积，不虚加。未建临时operator脚本，权威候选/原件索引只保本目录一套；正式PDF/有效缓存/CT-CV原件/attempt与既有及并行改动保护。

本卡可读候选交付结束，不再整批重抽或追可选细度。下一若继续，优先在此候选与原章节上作一次有界高阶源复核、必要一次合并修订及最终再核；真实范围须另有新明确授权，不能借CV余额。正式S1、盲测和生产准入仍按验收计划，不能将本地恢复计为真实抽取成功。

验证命令（均经`scripts/quote_dev_env.sh`、Quote环境Python、`--confcutdir=tests/unit/test_research`）：

```bash
python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_format_consumption.py tests/unit/test_research/test_company_profile_workpaper_continuation.py -m 'not integration'
python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_format_consumption.py -m integration
python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_continuation.py tests/unit/test_research/test_company_profile_workpaper.py tests/unit/test_research/test_company_profile_workpaper_features.py tests/unit/test_research/test_company_profile_workpaper_local_consumption.py tests/unit/test_research/test_company_profile_workpaper_review.py tests/unit/test_research/test_company_profile_workpaper_source_cells.py
```

OpenSpec `validate restore-company-profile-llm-led-generalization --strict`及`git diff --check`通过。设置`QUOTE_WORKPAPER_FORMAT_EVIDENCE`会在指定全新change子目录保存本轮三份生成材料；不设置仅本地临时读回，不覆写既有证据。代码/新增测试及本卡文档正常提交推送，既有混合文档只隔离本人CW新增段，不捎带原改动；最终hash/推送见会话交付。
