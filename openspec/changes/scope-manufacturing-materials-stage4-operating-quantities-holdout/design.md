## Context

材料投入的局部观察已经归档：三份冻结年报、计划 `.2026-09-26.2`、只跑 `extract_material_inputs`，recall 23/23、accuracy 23/23、critical numeric errors 0、`expansion_gates_met=true`。那个 true 只覆盖那一个切片，不授权扩大、规模质量或生产。阶段 4 的 19/23 和固定两家公司的 9/9 仍然独立。

现有章节 `extract_operating_quantities` 已经能区分产能、在建产能、产能利用率、生产量、销量和库存量，但还没有在跨交易所、含 holdout 的最小竖切上单独验收。已批准研究清单里的相关报告只有 `300750.SZ`、`603659.SH`、`920015.BJ` 和 `302132.SZ`。前三份参加过材料投入 replay；`302132.SZ` 是阶段 3 的 regime 基线，也是这份产销量竖切的 holdout 候选。

## Goals / Non-Goals

**Goals:**

- 把第二个最小竖切的范围写成可审核合同，并停在 1.1。
- 只使用现有 `extract_operating_quantities`。
- 样本合同要求至少 3 份、覆盖 SZSE/SSE/BSE，并且至少 1 份是材料投入 replay 的 holdout。
- 同一个最小入口必须能覆盖至少两种真实数量披露形态。
- 合法空值、明确不适用、未披露、主体不清和 extraction failure 保持可区分。

**Non-Goals:**

- 1.1 通过前不改 Python，不改 Evidence plan，不改 identity、publication、closure 或 completed mode，不入队，不 replay。
- 不新增未经 dossier 审核的发行人，不把 fresh 或 expanded cohort 写进定义样本。
- 不从产能或利用率倒算产量，不把销售金额当销量，不把存货金额当库存量，不用行业常识补数量。
- 不启用完整六章包、客户供应商、regime、DCF、交易或价格敏感性。
- 不预设 recall、accuracy、critical numeric errors 或 gate。
- 不授权下一轮扩大、规模质量声明或生产。
- 不把材料投入 23/23 当成产销量已经通过，也不重新验证 regime。

## Decisions

1. **只打开现有产销量章节。**
   `extract_operating_quantities` 已经是制造/材料研究里的章节任务。本 change 不新建章节，不把产销量、材料投入、客户供应商和 regime 捆成六章包。材料投入切片保持已归档状态。

2. **定义样本来自已批准清单，并必须包含 holdout。**
   可进入 dossier 的报告只有 `300750.SZ`（SZSE）、`603659.SH`（SSE）、`920015.BJ`（BSE）和 `302132.SZ`（SZSE，材料投入 replay 未使用）。定义样本至少 3 份且三个交易所都要出现，所以三份已用报告不能单独构成这份竖切；至少要有一份 holdout。`302132.SZ` 是唯一已批准的 holdout 候选。它的 dossier 必须先确认产销量章节适合这家重组发行人；不适合就停止，不从清单外补报告。它进入本样本也不等于重新验证 regime。

3. **dossier 先于字段义务。**
   每份候选报告先写独立的产销量 dossier，再标注 required、conditional、optional、legal empty、unclear 或 extraction failure。旧 gold 只说明这些报告已经在研究清单里，并且提示可能存在不同披露形态；gold 不回填运行时，也不代替本 change 的 dossier。

4. **至少两种披露形态共用一个入口。**
   第一种是分类实物量表，生产量、销量和库存量按来源标签分开。第二种是产能、产能利用率和在建产能章节；报告只给产能和利用率、没有产量时，产量保持合法空值，不得倒算。`302132.SZ` 若 dossier 确认报告明确写了分类实物量不适用，那是第三种合法空值，不是第二种形态的替代品，也不能单独满足“两种形态”。页码和产品名只作为 dossier 要核对的 fixture，不得写进抽取规则。

5. **数量、金额、产能和利用率保持不同语义。**
   产量、销量和库存量必须带来源单位、期间和物理页锚点。销售金额、营业收入和订单金额不是销量。存货账面金额不是库存量。产能必须保留来源里的产能类型，在建产能不并入当期产能。产能利用率是报告值，不得用来反推产量。跨页表格按延续页锚定，不把多页拼成一个不存在的单元格。主体无法从正文确定时记 unclear，不默认成合并口径。

6. **三种空结果分开落盘。**
   合法空值是报告可读，但没有该数量，或明确勾选不适用。unclear 是证据存在，但数量、单位、期间、主体或表头不能唯一判断。extraction failure 是页面、表头、单位或跨页上下文无法绑定。三者都不得改写成零、猜测单位或成功事实。

7. **研究隔离，1.1 通过前不实现。**
   通过后的实现复用 Evidence、Stage 5 extract/repair/verify 和研究隔离 bundle，输出只到 `accepted_for_review`。identity 保持 `owned_page_facts=v8` 加 `material_input_facts=v1`。publication、closure、completed mode 和生产授权不改。`scale_quality_claim_allowed` 保持 false。

## Risks / Trade-offs

- [只用三份材料投入报告，看不出跨样本] → 定义样本必须包含至少一份 holdout，并且覆盖三个交易所。
- [302132 的重组披露不适合产销量] → dossier 不确认就不纳入；不另选清单外发行人，本卡也不改成 regime 竖切。
- [两种形态被收成一张表的特例] → 分类实物量和产能章节必须都能走同一个 `extract_operating_quantities` 入口；规则不得绑定某一家或某一页。
- [用利用率补出产量] → 利用率只作为报告值；没有产量就是合法空值。
- [把金额列读成数量] → 销售金额和存货金额拒绝；只有带实物单位的来源数量可以成为销量或库存量。
- [合法空值、unclear 和抽取失败混成成功或零] → 三种结果分开落盘，都不能提高 recall。
- [局部材料 gate 被当成产销量或生产授权] → 材料投入 23/23 保持为另一切片的历史观察；本 change 不预填指标。

## Migration Plan

1. 1.1 只审范围。通过前不写 dossier，不实现。
2. 1.1 通过后才为候选报告写产销量 dossier，并确认 holdout 与两种披露形态。
3. dossier 审核通过后才做最小研究实现和 provider-free 测试。
4. 实现审核通过后才做受控 replay，并单独重算指标。
5. 回滚本 change 只删除这份范围文档；不删除材料投入归档，不改控制面。

## Open Questions

- 无。1.1 审核前不把 `302132.SZ` 预定为已经适合，也不预设产销量召回或 gate。
