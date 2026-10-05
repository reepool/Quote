# B角提交：v21正式修复轮，待A角有限验收

任务5.1–5.3已完成。上海电力p19燃料销售进入接受事实、查询及导出；首次v21正式轮在声明范围内达到召回38/38、准确率66/66、关键数字错误0、180.27秒、0/50000 token。此处是B角验证结果，A角验收仍待确认；当前change不归档、下一批暂停，生产not_authorized、规模质量声明false。

## 修复及前置验证

沿现有商品投影路径，仅从“报告期内公司存在贸易业务收入”、勾选适用的公司自有贸易收入表捕获燃料/product_sales。绑定2025-12-31及p19原页；11,193.86万元保留为本期营业收入证据。Activity的value/unit为空，不生成实物量、采购价或煤种；无精确商品映射保持pending、无市场价格关联。完整p19反例覆盖子公司、第三方、拟开展、否定及表格主体/计划，均不升格。

完整正式身份为rules=company_profile_common_core.v1、owned_page_facts=v8、material_input_facts=v1、revenue_sentence_repair=v21。五类累积修复开关全部接通，默认身份不变。完整两报告冻结页接受→查询→导出回归保持原七个角色、三维正文及14＋10条主营收入行，并交付第八个角色燃料销售，见[v21-prerequisite-validation.json](v21-prerequisite-validation.json)。

## 首次真实正式轮

沿用计划bcff2a299d9c7bd0197638f27aec1a1880c56ce68b2df7c3456c351626f3252f、官方冻结报告版本和cutoff2026-09-17；新目录plan.json与原计划字节一致。前置来源范围在真实执行前保存并由正式回执记录其SHA256，见[v21-source-scope.json](v21-source-scope.json)。

现有execute_published_task owner先上海电力600021.SH、后古越龙山600059.SH，完成run→query→export。第二家使用剩余50000 token，两家均completed/found/completed。计时从首次execute开始至第二家export返回，整轮180.27239676192403秒；没有删除产物后重跑或替换正式结果。provider_calls为空，实际共享用量0/50000；0 token不作为无复用证据。

上海work_id为bp-work-8a3339009b2e2c911eb710b9，古越为bp-work-3dfb326dd0add7e64d4b2cb4。逐项读取实际runtime/工作项记录，acquire、parse、publish、semantic、verify的reused_scope_ids均为空，predecessor_lineage也为空。实际查询与JSON导出逐对象相同；正式运行、查询、导出和来源范围哈希保存在data/checkpoints/company_profile_common_core/reports/m4_v21_power_wine_repair/formal_execution.json。

## 独立原文复核

重新读取官方冻结PDF并核document SHA256，逐项匹配原文标签、金额、货币单位及期间，没有沿用旧分母或预填分数。逐对象材料见[v21-review-evidence.json](v21-review-evidence.json)。

| 项目 | 实际结果 | 唯一计分范围 |
| --- | --- | --- |
| 召回 | 38/38，100% | 六维正文＋24条主营收入行＋八原生角色 |
| 准确率 | 66/66，100% | 60条实际接受事实＋6个答案 |
| 关键数字错误 | 0 | 原文收入金额、单位、报告期逐项核对 |
| 整轮耗时 | 180.27239676192403秒 | 首次execute至第二export返回 |
| token | 0/50000 | 同轮共享实际用量 |
| 实际章节复用 | 无 | 五阶段reused_scope_ids及前序血缘为空 |

60条接受事实为24 Segment、24 Measurement、6 Activity、3 BusinessOverview及3 Relationship。每个instrument_id/object_id只评价一次准确率；Measurement副本仅计准确率，原生角色在其底层接受事实上计召回，没有额外重复准确率对象。

八个角色为上海电力/热力/燃料销售、标煤投入，古越黄酒/玻璃瓶销售、糯米/小麦投入。原生商品映射均pending且not_linked。六维正文及接受的Overview保持连续来源，古越完整产品矩阵、99.98%黄酒收入与玻璃瓶少量外销，以及上海电力/热力/其他收入、直销均核对；14＋10条主营收入行的维度、完整跨页标签、金额、元和报告期一致。

复核限上海p10、11、16–19及古越p7、8、13、14、27的声明来源。p19燃料贸易收入金额保留在证据，不另称主营收入行；产销量细分数量、成本拆分和行业增强继续后置。不宣称全年报完整性或全文无其他商品角色。

## 验证及Review

使用/home/python/miniconda3/envs/Quote/bin/python运行以下定向命令，165 passed、1 deselected、7 warnings，31.51秒：

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_v21_trade_sales.py \
  tests/unit/test_research/test_company_profile_v20_unseen_repair.py \
  tests/unit/test_research/test_company_profile_v19_closure.py \
  tests/unit/test_research/test_company_profile_v19_unseen_misses.py \
  tests/unit/test_research/test_company_profile_v20_identity.py \
  tests/unit/test_research/test_company_profile_material_input_procurement.py \
  tests/unit/test_research/test_company_profile_material_input_role.py \
  tests/unit/test_research/test_company_profile_core_evidence_selection.py \
  tests/unit/test_research/test_company_profile_core_assessment_projection.py \
  tests/unit/test_research/test_company_profile_core_skeleton.py \
  tests/unit/test_research/test_company_profile_m4_next_batch.py \
  tests/unit/test_research/test_company_profile_source_review.py \
  --deselect=tests/unit/test_research/test_company_profile_core_evidence_selection.py::test_reads_through_section_and_stops_at_next_heading
```

排除节点属于已复现的既有失败（C类），未修改预期或默认路径；已报告的runtime基线失败继续后置。本人差异Review聚焦公司主体、成立语义、期间、证据、金额不误作量价、累积身份和唯一计分：燃料漏项已修复并通过完整页反例与正式交付验证，未发现新增阻塞问题（A类）。未因非阻塞建议扩大范围。OpenSpec严格校验、git diff --check通过。

## 历史保留与提交边界

494份历史文件未变。原19份首次失败制品中，其余18份路径未变；source-review路径此前已补正为v2，原v1字节由initial副本完整保留。本轮513份当前保护文件均未变；原29/36、补正29/37及相关失败制品保留。不能将此表述为原19份路径全部未改。

任务前已有三份修改及未跟踪目录/文档未触碰、不纳入提交。仅提交本人代码、测试及本change配套材料，不提交data目录研究制品；这些制品由逐对象材料中的哈希引用。待A角有限验收后再决定归档与下一批，不将B角门槛通过自动等同于生产授权或规模质量通过。
