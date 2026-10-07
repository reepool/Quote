# v29 有限验收提交

5.2、5.3已完成，声明来源范围达到申报门槛，供A角独立审核。当前change未归档，下一批暂停；生产not_authorized，scale_quality_claim_allowed=false。

## 实际业务交付

- 原计划e18e6b4f…、华电国际600027.SH/宇通客车600066.SH两官方冻结报告、cutoff2026-09-17；完整身份rules=company_profile_common_core.v1/owned_page_facts=v8/material_input_facts=v1/revenue_sentence_repair=v29。五类累积修复开启，默认不变。
- 同一49来源合同在首次execute前固定并逐项核对v28：6实质正文、28原生收入单元、15原生角色。独立重新读取两PDF核原文及版本哈希，无预填分数/准确率分母。
- **召回49/49，准确率94/94＝88接受事实＋6已答正文，关键数字错误0。** 每个对象唯一核主体、动作/关系、栏目、金额和单位；Measurement仅准确率，角色不重复底层事实。
- 集团煤炭销售接受Activity为sells/销售、本集团/consolidated_group/direct_source_wording；同页母公司说明不擦除当前集团主体。实际采购及material_input能源耗用分别保留，内部耗用不推外购。原六维、28收入单元、15角色成立。
- 查询、导出、磁盘JSON一致。首次真实owner运行至第二export返回**180.92秒**，包含全部重试；共享0/50000token，第二家使用剩余50000。两家实际五阶段reused_scope_ids和predecessor_lineage均为空。执行代码与当前代码哈希一致。

## 动作修复与验证

沿现有集团绑定检查煤炭销售所在枚举分支的否定/未来限定，集团定义只证明主体，不代替动作语义。完整owner及独立PDF p203的“不从事煤炭销售/拟开展煤炭销售”原代码4项失败已复现；修复后含尚未/将开展的16个v28/v29动作反例在接受原始事实→查询→导出中均拒绝。真实肯定销售、原主体、同页其他成立销售/采购/耗用保留，其他分支计划不删除成立煤炭销售。

累积定向**288 passed、1 deselected**（按授权后置test_reads_through_section_and_stops_at_next_heading），Ruff、OpenSpec严格及diff通过。人工Review修复本轮P1动作语义，未发现本次声明范围新增阻塞；行业模板/框架/风格及无关问题不扩入。

## 复核补正与保护

首版复核39/49、76/94由复核条件错误造成：要求表格物理页等于证据锚点起始页，忽略完整bounded_quote和continuation_pages。8收入单元的原生行及单位原本均在跨页证据内；更正为完整锚点核验，并逐对象核合并主体。初版逐对象材料及owner复核原字节副本、补正说明均保留。业务交付、首次时间、代码和49合同未改，未重跑正式业务。

817历史/审核制品、19份首次交付制品及3份既有脏文档哈希通过。原818项扫描基线保持，其中实时task_control.json由既有owner正常更新，独立保护范围说明将其与不可变历史制品区分；历史制品无例外。v27原40项失败、v28正式失败与初版/补正轨迹、v28原提交和本次A角动作审核限制全部保持。

## 复核入口及范围

- v29-source-scope.json、v29-source-freeze-receipt.json：执行前49合同、相同条件、报告版本、身份/开关、共享预算。
- v29-source-review-evidence.v2.json：94唯一对象、49来源条件、主体动作/关系、跨页证据、实际runtime。
- v29-action-regression-receipt.json：原反例复现及修后完整源页回归。
- v29-source-review-evidence.v1.json、v29-review-correction.json：复核初版与补正；原owner复核副本留在正式目录。
- v29-protection-baseline.json、v29-protection-scope.json、v29-first-formal-delivery-preservation.json：历史、实时owner状态与首次制品的明确边界。
- v29-validation-receipt.json：相关测试、静态检查及有限申报门槛。
- data/checkpoints/company_profile_common_core/reports/m4_v29_group_sales_action_acceptance/formal_execution.json：首次实际运行、查询、导出和计时。

结论限于冻结来源及实际检查页，不声明全年报完整性。量价、成本、行业增强、框架整理与无关既有问题后置。待A角有限验收通过后再同步主规格、归档及规划下一对未见样本。
