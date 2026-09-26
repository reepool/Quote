## Context

阶段 4 最小竖切已归档在 `openspec/changes/archive/2026-09-26-scope-manufacturing-materials-stage4-minimum-slice/`。权威复核是 recall 19/23、accuracy 19/19、critical numeric errors 0、`expansion_gates_met=false`。19 条已交付 `raw_material_input` Relationship 和宁德时代正极材料一条独立 `product_sales` Activity 的准确性没有被这次失败否定。

漏召的四项是两种正文形态，不是目录或模型问题：

- 宁德时代第 73 页关联采购表写明公司采购原材料，括号中具名为磷酸铁锂、锂盐、氢氧化锂。
- 锦华新材第 51 页正式“主要原材料及能源”表把丁酮肟列为原材料耗用行。同报告第 12 页和第 50 页的委外加工安排仍然不是投入事实。该表中的蒸汽、电是能源行。

这些页码和名称只说明缺口从哪里观察到。实现规则不得把证券、页码或材料名写死。

## Goals / Non-Goals

**Goals:**

- 把范围审清楚并停在 1.1。
- 只增加两个 Evidence 入口：公司自身采购或关联采购行；正式“主要原材料及能源”表中的原材料行。
- 入口仍走现有 `extract_material_inputs`、Evidence、Stage 5 和研究隔离 bundle，输出只到 `accepted_for_review`。

**Non-Goals:**

- 1.1 通过前不改 Python，不改 Evidence plan，不入队，不 replay，不写 source-review。
- 不覆盖或改写 2026-09-26 归档观察、三份 dossier、enqueue、run、result 或 source-review。
- 不改 common-core identity、publication、closure、completed mode 或生产授权。
- 不处理沥青目录别名，不启用六章包、产销量或客户供应商。
- 不预设修复后的 recall 为 23/23，也不预设 gate 通过。
- 不授权下一轮扩大、规模质量或生产。

## Decisions

1. **两个入口是本 change 的全部范围。**
   采购行必须由表头、交易方向和行内容共同证明报告主体在采购原材料。原材料单元格或括号中的具名材料可以形成投入关系。销售、提供服务、只有泛称采购金额、以及买方主体无法确定的行继续拒绝。
   正式“主要原材料及能源”表只接收明确属于原材料的具名行。独立的采购或耗用行可以形成投入事实，包括丁酮肟这一名称。蒸汽、电等能源行继续排除。委外加工安排本身仍不能生成投入角色。

2. **宁德时代第 73 页只是验收 fixture。**
   它用来检查关联采购行是否被读成公司原材料投入。规则必须能用于其他报告的同类表格，不得硬编码 `300750.SZ`、第 73 页或磷酸铁锂、锂盐、氢氧化锂。

3. **已归档 19/23 保持为历史观察。**
   新的研究运行如果发生，必须写到新的研究隔离目录，并单独重算 recall、accuracy、critical numeric errors 和 gate。不得把 19 条或 23 个名称预设成新的分母或通过结论。

4. **研究隔离和现有边界不变。**
   销售证据单独不生成投入角色。直接材料成本和原材料存货金额继续拒绝。legal-empty、unclear 和 extraction failure 继续分开，并且不得改写成零、猜测的 commodity id 或成功事实。同一 source-native 名称可以同时保留销售和投入角色，不净额化。pending 或 ambiguous 映射仍不得丢掉已成立的投入关系，也不得携带 commodity id。

## Risks / Trade-offs

- [把关联采购里的销售或服务行收成投入] → 交易方向必须是报告主体采购原材料；销售、提供服务和主体不清的行拒绝。
- [把能源行收成原材料] → “主要原材料及能源”表按行类型过滤，蒸汽和电排除。
- [把委外加工安排改写成丁酮肟投入] → 加工安排本身仍拒绝；只有独立采购或耗用证据可以形成投入事实。
- [为了补这四项而改写归档 19/23] → 归档制品只读；新结果另记。
- [把 fixture 页写成规则] → 证券、页码和材料名不得进入实现条件。

## Migration Plan

1. 1.1 只审范围。通过前不实现。
2. 1.1 通过后才把两个入口接到现有材料章节，并补 provider-free 测试。
3. 实现审核通过后才可以做受控研究 replay，并单独重算指标。
4. 回滚本 change 只删除这份范围文档；不删除 2026-09-26 归档。

## Open Questions

- 无。1.1 审核前不另选章节，也不预设修复后的召回分数。
