# v25有限验收提交

5.1、5.2已完成，声明来源范围达到申报门槛，供A角独立审核。当前change保持未归档，下一批继续暂停。生产not_authorized，scale_quality_claim_allowed=false。

## 实际交付与计分

- 原计划74b8faf4…、600025.SH与600062.SH官方冻结报告、cutoff2026-09-17；身份rules=company_profile_common_core.v1/owned_page_facts=v8/material_input_facts=v1/revenue_sentence_repair=v25，五类累积开关开启，默认身份未变。
- 执行前同一62条件逐项/摘要对比固定：6实质正文、29原生收入行、27销售角色（电力1＋药品26），未改条件或补入新分母。
- 实质召回62/62；准确率104/104，实际98接受事实＋6正文唯一计分。Measurement只准确率，角色绑定底层事实，别名不重复。数字错误0。
- 首次真实owner运行→查询→导出，整轮196.66800413秒，计时从首次execute至第二export返回含全部重试，0/50000token，第二家用剩余50000。五阶段实际reused_scope_ids和predecessor_lineage为空；query/export/磁盘JSON一致。
- 642保护文件哈希保持：635历史数据文件（含v23/v24首次制品及原失败）＋7原v24材料；本轮18首次制品复核前后字节一致。v24原62/62、104/104、201.10秒不替换，审核限制见v24-a-role-audit-limitations.json；v23五药在首次执行期间补入的时序限制保留。

## 修复与验证

仅收紧现有具名销售清单的引导语义，尚未/将/未/计划对应清单不生成当前sells。完整owner p39及独立PDF p39原反例4项复现，修后8项负例接受→查询→导出贯通；五个仅受该清单支持的药品销售消失，同页14个另有肯定披露的角色仍保留。复方杜仲健骨颗粒有独立终端销售支持，不误删；华润紫竹原主体保留。

两公司两种完整源页在v25正式身份下保留全部正文、29行及27角色。26药品、括号品牌、原料药与制剂区别保持。定向累积回归215 passed、1项既有失败deselected（test_reads_through_section_and_stops_at_next_heading）；Ruff、OpenSpec严格与diff通过。人工Review修复一项P1具名清单语义错误，未发现新增本轮阻塞；无关既有测试/改动未治理。

## 证据与边界

- v25-source-scope.json、v25-freeze-receipt.json：执行前冻结、完整身份、官方版本、相同条件摘要和未enqueue。
- v25-source-review-evidence.json：逐对象事实/正文、原文条件、角色绑定、实际五阶段复用、动态分母。
- v25-first-formal-preservation.json、v25-protection-baseline.json：首次制品及历史字节保护。
- v25-validation-receipt.json：定向验证、语义反例、Review和正式门槛。

验收限于固定62项及实际核对接受事实/正文；不声明全年报完整性。数量细分、成本拆分、行业增强及既有测试治理后置。未启动下一样本；后续评审须显式排除26家已观察公司。等待A角有限结论后再按授权同步主规格、有限归档与下一对样本评审。
