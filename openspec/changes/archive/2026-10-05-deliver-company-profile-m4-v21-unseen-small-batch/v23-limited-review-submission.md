# v23 首次正式交付有限验收提交

B角结论：首次真实v23正式轮满足声明来源范围的业务门槛，提交A角有限验收。当前change保持未归档，下一批暂停等待A角决定；生产not_authorized、规模质量声明false。本轮不宣称全年报完整性。

## 实际交付及唯一评分

沿原计划 `07734df3db25a8aadbdca7ebecad897da3a81a4fe1f96a05738f7c48872b66ba`、原浙能电力600023.SH与海信视像600060.SH两份官方报告、cutoff2026-09-17，使用独立m4_v23_compound_revenue_acceptance目录。完整身份rules=company_profile_common_core.v1、owned_page_facts=v8、material_input_facts=v1、revenue_sentence_repair=v23；默认身份不变。沿现有execute_published_task owner依次完成两家run→query→export，首次真实执行即formal_execution_index=1，无删除产物重跑替换。

| 项目 | 实测结果 | 核对范围 |
|---|---|---|
| 实质召回 | 32/32，100% | 六维正文＋16收入行＋十角色 |
| 准确率 | 57/57，100% | 51实际接受事实＋6已答正文 |
| 关键数字错误 | 0 | 标签、金额、原生栏目、元/万元和报告期 |
| 整轮时间 | 186.53159573860466秒 | 首次execute至第二export返回，包含全部重试 |
| 共享token | 0/50000 | 第二家使用剩余50000，实际整轮0 |
| 章节scope复用 | 无 | 两家五阶段reused_scope_ids及predecessor_lineage均为空 |

51条事实为16 Segment、16 Measurement、14 Activity、4 BusinessOverview及1 Relationship。每个instrument_id/object_id准确率仅判定一次；Measurement副本只计准确率，角色召回绑定对应底层接受事实，无额外重复角色准确率对象。六维召回须满足来源实质条件，不能只看answered。沿A角批准的既定32项条件观察前重新核对冻结PDF；准确率分母根据本轮实际交付重建，未预填57或新分数。query与JSON export完全一致，逐对象材料绑定原文、实际事实、答案、工作身份及runtime。

## 业务收口

浙能最后漏行“电力、热力生产及供应”72,870,145,425.91元实际进入Segment与Measurement；两家各八行均保留原金额、栏目、单位及报告期，海信万元口径不变。浙能收入正文包含国家电网销售客户及市场化交易；原生“在某一时点确认”仍按源表sales_mode栏目保留，在正文称收入确认，不解释为销售渠道。

浙能主营/产品保留煤电、气电、核电、热电联产、综合能源，以及取得中来股份控制权和中来三类光伏业务的连续原句，保留中来主语。地域管理说明未成为Activity对象。海信正文保留愿景、战略定位、当前坚持/持续深耕的智慧显示终端、激光显示、商用显示、芯片、云服务及后续布局原句状态，不将愿景或计划升格为已成立销售。

十个角色为浙能电力、热力、背板、电池、组件、市场煤销售及煤炭投入；海信显示产品、芯片、AI耳机销售。保持合并集团范围，芯片保留公司战略控股信芯微、乾照光电的主体上下文。市场煤286,336.52万元仅为贸易收入证据，不改成数量、采购价或煤种；煤炭投入绑定公司煤电装机和成本风险上下文。煤炭映射ambiguous，其余pending，全部not_linked；产销单位不并入产品名。显示父角色不重复，泛称原材料不推面板，计划机器人不生成角色。

## 验证与Review

本轮功能代码仅增加v23常量及五类累计开关集合，共6行，沿用A角已复验的承接修复；未调整抽取算法、默认身份或owner。两种完整源页和当前主体/计划边界纳入v22/v23对照测试，并明确断言query/export完全一致。

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_v20_identity.py \
  tests/unit/test_research/test_company_profile_v22_energy_display.py
```

48 passed、7 warnings，11.03秒。验证范围覆盖当前6行身份改动及全部业务验收要求；A角上轮205 passed、1 deselected基线仍保留，既有失败治理后置。本次OpenSpec严格校验及git diff --check通过。人工差异Review核对五开关接通、默认兼容、完整页/主体/单位/计划语义、正式第一次执行、唯一计分、历史字节及提交边界：未发现当前范围A类新增阻塞，C类既有测试和工作区改动未处理，未因B类建议扩展行业或框架。

## 保留及有限范围

587份执行前保护文件哈希未变，包含原582份以及v22剩余审核材料、冻结页夹具；v22正式31/32、55/55失败及其执行代码/事后局部修复区分保持原字节。更早39/45、补正38/45及旧21/32答案存在口径限制保留，不将旧数字改写成实质召回。

检查页和32项实质条件见v23-source-scope.json；与A角批准的v22条件一致。无新增全年报或全文无其他角色结论。数量细分、成本拆分、行业增强及既有测试问题后置。A角验收未完成，不自行归档、不放行下一批、不改变生产与规模授权。

既有三份修改及未跟踪目录/development文档未触碰、未纳入提交。仅提交本轮可隔离代码、测试和change配套材料；data正式制品通过v23-source-review-evidence.json的哈希引用保留。
