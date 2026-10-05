# v22 首次失败观察及剩余漏行局部修复复核提交

B角结论：v22 首次真实正式轮未达有限验收，当前 change 不归档，下一批继续暂停。正式产物不删除、不替换；剩余漏行已局部修复，仅用冻结源页临时端到端回归验证，未再次执行真实正式轮。

## 首次正式轮的实际结果

沿原计划 `07734df3db25a8aadbdca7ebecad897da3a81a4fe1f96a05738f7c48872b66ba`、原两份官方报告、cutoff 2026-09-17，完整身份为 rules=company_profile_common_core.v1、owned_page_facts=v8、material_input_facts=v1、revenue_sentence_repair=v22。使用独立 m4_v22_energy_display_repair 目录，沿 execute_published_task 的既有 owner 依次 run→query→export。

| 项目 | 正式结果 | 依据 |
|---|---|---|
| 实质召回 | 31/32，96.875% | 六维正确正文＋15/16收入行＋十个原生角色 |
| 准确率 | 55/55，100% | 49条实际接受事实＋6个已答正文，逐对象唯一 |
| 关键数字错误 | 0 | 原文金额、单位、期间及收入语义核对 |
| 整轮耗时 | 194.66197881195694秒 | 首次execute至第二家公司export返回，首次正式执行 index=1 |
| 共享token | 0/50000 | 第二家收到剩余50000，实际同轮用量0 |
| 章节scope复用 | 无 | 五阶段runtime reused_scope_ids及前序血缘均为空 |

49条事实包含15 Segment、15 Measurement、14 Activity、4 BusinessOverview和1 Relationship。Measurement副本只计准确率；十角色在对应底层事实计召回，不另增加角色准确率对象。未继承旧分数：来源清单于执行前独立读取冻结PDF并固定，正文召回要求满足实质条件，不能以 answered 状态替代。

六维正文保留浙能五类主营、中来股份主语和三类光伏业务，海信愿景/战略定位、当前五类业务及后续布局原句状态。浙能收入从原文客户及市场化交易披露补足机制；在某一时点确认保留为收入确认，不称销售渠道。十角色是浙能电力、热力、背板、电池、组件、市场煤销售及煤炭投入，海信显示产品、集团芯片、AI耳机销售。芯片保留公司战略控股信芯微、乾照光电的集团范围；市场煤286,336.52万元只保留贸易收入证据，不变成实物量、采购价或煤种。

## 正式漏项及局部补齐

唯一漏项为浙能行业行“电力、热力生产及供应”72,870,145,425.91元。pypdf完整页夹具将金额置于第二行标签后；正式owner的冻结PDF派生解析将标签拆成两行，金额单列第三行。软折行已将两段标签合并，但现有“标签→下一金额行”承接正则仍未允许“、”，因此真实交付漏掉Segment和Measurement两条副本。

已局部在该承接正则补入“、”。完整冻结页夹具新增同一PDF实际owner解析页及原派生制品哈希，不改变既有原页内容；两种完整源页均贯通接受→查询→导出，8＋8收入行恢复，金额/单位/栏目及Segment/Measurement一致。正式失败仍是31/32、55/55，不将临时回归称为第二正式轮或有限通过。执行时代码哈希和失败产物保全见 v22-failure-preservation.json；当前最终代码包含该次失败后的局部修复。

## 验证与Review

最终累计定向验证205 passed、1 deselected、7 warnings，38.29秒；完整页和主体/计划边界单独28 passed。排除既有 test_reads_through_section_and_stops_at_next_heading，未改其预期。主要命令：

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_v22_energy_display.py \
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
  tests/unit/test_research/test_company_profile_v3_named_role_repair.py \
  tests/unit/test_research/test_company_profile_v6_core_answers.py \
  --deselect=tests/unit/test_research/test_company_profile_core_evidence_selection.py::test_reads_through_section_and_stops_at_next_heading
```

OpenSpec严格校验与git diff --check通过。人工差异Review关注当前成立语义、主体边界、实际解析列和单位、证据期间、收入不误作量价、五开关及唯一计分：A类真实三行承接漏项已局部修复，并完成正式owner源页回归；正式有限验收仍未通过，需A角复核后安排后续修复验证。C类既有失败和既有脏文件未处理；未因B类非阻塞建议扩展框架或行业包。

## 保留与限制

559份正式执行前保护文件未变（534历史文件＋19份上一首次失败当前制品＋6份原冻结/复核材料）。本轮失败后582份保护文件均未变（再加20份v22正式制品及3份v22来源/冻结/逐对象材料），包含18 JSON及2 CSV正式制品。旧39/45、补正38/45及原始副本保留；旧21/32为答案存在口径，三个错误或不完整正文被计命中，该限制旁挂，不修改旧证据字节。

复核范围限于 v22-source-scope.json 的检查页、正文、16条主营收入行和十个明示角色，不宣称全年报完整性。商品映射维持pending/ambiguous及not_linked；显示父角色不重复，泛称原材料不推面板，计划机器人不生成角色。产销量细分数量、成本拆分、行业增强、框架整理和既有测试治理后置。生产not_authorized，规模质量声明false。

任务前已有三份修改、未跟踪目录及development文档未触碰，不纳入提交。仅提交本人代码、测试、夹具和当前change配套材料；data目录正式制品通过哈希引用保留。当前change不归档，下一批暂停。
