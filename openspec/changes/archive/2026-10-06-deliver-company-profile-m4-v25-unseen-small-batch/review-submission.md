# v25下一对未见年报：首次正式失败提交

三张卡已按失败分支完成，业务验收未通过；当前change保持未归档，后续批次暂停。未修算法、未重跑或替换首次观察。生产not_authorized、scale_quality_claim_allowed=false。

## 已完成有限归档

A角已通过的旧v25修复change同步主规格并归档至openspec/changes/archive/2026-10-06-deliver-company-profile-m4-v23-unseen-small-batch。29个原文件逐字节保持，新增独立A角有限结论；v23失败、五药来源时序限制、v24审核限制及各轮制品保持。参见prior-change-archive-receipt.json及归档目录v25-a-role-finite-acceptance.json；有限结论不代表全年报/规模质量。

## 冻结与首次观察

完整delivered_ids显式排除26家，既定SSE/SZSE/BSE＋代码升序规则冻结中远海能600026.SH(service)、皖维高新600063.SH(manufacturing)，计划2d759ea87f4b3b6ce95685799bdaf2959ccf852154365a1829247bac14d5de0c。抽样标签不决定业务模板。完整v25身份、官方有效2025报告、cutoff2026-09-17、独立m4_v25_unseen_small_batch、共享50000token。重复读一致，冻结未enqueue，队列115未变。

独立读取官方PDF后，执行前固定56项实质来源条件：6正文＋30非汇总收入行＋20明示角色；不继承62/104，执行后未补入条件。source-freeze-receipt.json记录时间、完整原页和清单哈希，早于首次execute。中远海能小计/合计作源上下文和勾稽，不额外计召回或累计；皖维PVB胶片/中间膜、PVA光学薄膜/膜别名不重复。承运货物不是自有商品销售，数量/成本后置。

首次owner运行→查询→导出：203.39592756秒、0/50000token，第二家使用剩余50000；计时覆盖首次execute至第二export返回及重试，五阶段实际reused_scope_ids与predecessor_lineage为空。两家query/export/磁盘JSON相等。654历史数据文件与本轮18首次制品哈希不变。

## 实质计分

- 召回19/56＝0/6正文＋15/30重要收入行＋4/20原生角色。
- 准确率47/65＝实际61接受事实＋4已答正文的唯一评价。Measurement仅准确率；角色绑定底层事实，exposure不额外计分。
- 错误或不完整answered正文均不算召回，缺失的两个答案只算未召回、不增加准确率对象。
- 数字错误0的具体含义：没有报错数值或明确错误单位；11个中远海能Segment单位为空全部判不准确。其中9条非汇总源行不算召回，另外2条小计不额外计召回。原千元不补值、不换算。单位缺失仍是本轮业务阻塞，并非正确数字交付。

## 真实业务缺口

1. 中远海能：主营/产品正文缩到LPG子公司段，缺两大油轮/LNG主业、化学品及完整物流服务；收入未答，真实即期/期租/COA/POOL/LNG长期租约机制未交付。LNG/LPG收入行漏失、千元单位遗失且无对应收入Measurement；燃油耗用角色缺失。承运原油/成品油/LNG/LPG/化学品未误报销售，负例限已读页。
2. 皖维：主营未答；产品正文缺已售偏光片；高强高模PVA纤维、VAE乳液、PVA光学膜、PVB中间膜四收入行漏失，收入正文为不完整子集。14目标销售只交付聚乙烯醇/水泥/熟料/电石四个；煤/醋酸/甲醇/乙烯投入、电力耗用缺失。
3. 错误对象：利用被截为业务对象；产销续行把高强高模PVA纤维与下一行醋酸乙烯拼成一个虚构商品；贸易业务收入表的泛称贸易被当作商品销售。3条实际错误Activity各评价一次，派生exposure不重复计分。

事实证据中的source_pages为owner证据入口页，independent_verification_pages为独立PDF实际页，跨页表行在原续页核验；两者并存，不改首次页证据。所有条件与逐对象结论见source-scope.json、independent-frozen-source-pages.json、source-review-evidence.json；首次与历史保护见first-formal-delivery-preservation.json、freeze-receipt.json。

仅围绕上述实际主路径缺口提出后续修复，不启动新批次、不扩展行业/数量/成本/框架。修复应另用新身份和独立目录，经完整实际owner源页接受→查询→导出回归后再正式验收，不替换本次失败。

## 验证与Review

定向命令：`/home/python/miniconda3/envs/Quote/bin/python -m pytest -q tests/unit/test_research/test_company_profile_m4_next_batch.py tests/unit/test_research/test_company_profile_v20_identity.py tests/unit/test_research/test_company_profile_source_review.py`，35 passed；Ruff通过。新change和两份同步主规格OpenSpec严格校验、diff检查通过。人工Review核当前diff、原文与逐对象制品：本轮实施/规格/归档未发现新增阻塞；上述业务缺口是本次未见观察确认的核心失败，待后续修复卡，不以工程测试通过冒充业务通过。原工作区改动未触碰/暂存。
