# v27 投入关系有限验收提交

B角已完成5.1–5.2，提交A角有限复核。当前change未归档，后续批次暂停。

- p27主要上游原材料的24条原生名称全部由现有材料章节接受为 `Relationship(relation_type=material_input)`，保留主体、期间、栏目及原页证据；该页不再衍生 `purchases`。p33四条外购仍为采购，电仍耗用，船用燃油仍消耗。PVA、PVB树脂、VAE乳液等当前销售与内部投入分别保留。没有数量、价格或成本推断。
- 原计划 `2d759ea87f4b3b6ce95685799bdaf2959ccf852154365a1829247bac14d5de0c`，600026.SH中远海能／600063.SH皖维高新，官方版本和cutoff `2026-09-17` 不变。完整v27身份、独立 `m4_v27_input_relation_acceptance` 目录，首次正式owner运行→查询→导出，计时 190.03 秒包含全部重试；0/50000 token。五阶段runtime实际无复用；查询／导出／磁盘JSON一致。
- 执行前固定与v26相同77来源条件，正文/行/角色条件逐字一致，另固定动作/关系依据。实质召回 **77/77**＝6正文＋30收入行＋41原生角色；准确率 **145/145**＝139实际接受事实＋6已答正文。每条实际事实唯一核验原文动作，material_input与外购采购分开，角色匹配不代替事实准确；Measurement只准确率、别名和重复源不重复召回。数字错误0、缺单位0。
- 完整owner页及独立PDF页回归，定向 **244 passed、1 deselected**；Ruff、OpenSpec严格、diff通过。Manual Review未发现未解决本卡阻塞；既有排除失败和数量/成本/行业/框架工作不扩入本轮。
- 保护 **713项**＝692历史数据＋21旧change JSON均未变，v26的18份与v27的19份首次制品保持。v26原145/145、初版与更正轨迹不改， `v26-a-role-acceptance-limitations.json` 明确该准确率不可采纳，未以重跑或覆盖替换旧观察。既有三个脏文档哈希保持，未触碰原工作区改动。

主要材料：`v27-source-review-evidence.json`（逐事实action/relation_type及来源、全部答案和实际runtime）、`v27-source-scope.json`（固定77合同及动作语义）、`v27-source-freeze-receipt.json`、`v27-independent-frozen-source-pages.json`、`v27-validation-receipt.json`、`v27-first-formal-delivery-preservation.json`、`v27-protection-baseline.json`。

结论限于冻结声明来源及实际检查页，不声明全年报完整性。未知映射仍pending/ambiguous；生产 `not_authorized`，规模质量声明false。等待A角验收后决定规格同步与有限归档，不自行放行下一批。
