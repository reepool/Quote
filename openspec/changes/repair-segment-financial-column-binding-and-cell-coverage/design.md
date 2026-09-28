## Context

`scope-manufacturing-materials-stage4-segment-financials` 的 `replay/20260928` 已通过控制面审核，但 source-review 的 gate 为 false。四条关键错误都在 `300750.SZ` 物理页 25：收入构成续表的上年收入和上年占比被写成当期营业成本和毛利率。同一页 10% 以上表的正确成本和报告毛利率仍然存在。另外十个当期单元格没有交付。

原 replay 的 enqueue、run、result、source-review 是这次失败观察，字节必须保持不变。

## Goals / Non-Goals

**Goals:**

- 把列角色修复和十个单元格补齐写成可审核合同，并停在未勾选的 1.1。
- 只复用 `extract_segment_financials` 和原四份报告。
- successor 使用新计划版本和新隔离目录。
- 已正确的毛利率、空值和抵消标记保持原语义。

**Non-Goals:**

- 1.1 通过前不改 Python，不入队，不 replay，不写 source-review。
- 不新增发行人，不重选样，不复核 regime。
- 不把十四项补齐写成 gate 必然通过。
- 不修改 `replay/20260928`，不改产销量或材料投入历史制品。
- 不启用客户供应商、六章包、DCF、交易、价格敏感性或旧 writer。
- 不进入 common-core production，不改 identity、publication、closure、mode 或 checkpoint。

## Decisions

1. **失败观察只读。**
   原计划 `manufacturing_materials_stage4_segment_financials.2026-09-28.1` 和 `replay/20260928` 保持字节不变。四条错误记录只在 successor 中不再出现，不通过改名、替换 Evidence 或改写旧 result 消除。

2. **列角色跟完整表头走。**
   同一物理页可以先后出现收入构成续表和 10% 以上表。上年收入、上年占比和增减列只有在表头明确是当期营业成本或报告毛利率时才进入这两个字段。比较列和增减列留在原列，不提升为当期成本或毛利率。

3. **十个单元格是 source-review fixture。**
   `300750.SZ` 六个、`920015.BJ` 两个、`302132.SZ` 两个。每个正例在 successor 中形成对应 Measurement，并带自己的页、单位、来源维度、行名和 quote。证券代码、页码、公司名和产品名只出现在 fixture、dossier 和测试，不写进抽取规则。

4. **已通过的边界继续有效。**
   `118.30` 与 `104.19` 仍是增减列。合计行“-”和空抵消金额仍不是 0。公司整体 `29.98%` 仍不是分部毛利率。抵消行和抵消列继续使用 `consolidation_adjustment`。`not_applicable`、`not_disclosed`、`unclear` 不互相改写。

5. **successor 计划单独编号。**
   计划版本固定为 `manufacturing_materials_stage4_segment_financials.2026-09-29.2`。实现通过后的 replay 写入本 change 下的新隔离目录，disposition 为 `accepted_for_review`，provider 调用保持由实现卡约束。`production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。

## Risks / Trade-offs

- [用改名掩盖页 25 的四条错误] → successor 删除这些当期错绑，旧 result 保持原字节。
- [修表边界时丢掉境内、境外的正确成本和毛利率] → 这两行的当期成本和报告毛利率继续保留。
- [补单元格时把比较列再读成当期值] → 列角色必须来自完整表头。
- [把十个单元格补齐当成 gate 通过] → 新的 source-review 重新数分母，不预填 132/132。
- [抽取规则写死页码或产品名] → 名称只留在 fixture。

## Migration Plan

1. 1.1 只审范围。通过前不实现。
2. 1.1 通过后做最小实现和定向测试，覆盖四条错绑消失、十个单元格出现，以及已有边界不回退。
3. 实现审核通过后，用 `.2026-09-29.2` 做一次新目录 replay。
4. 再做独立 source-review。分母重新阅读四份年报，不从 bundle 条数倒推。
5. 回滚本 change 只删除这份范围文档和以后的 successor 目录；不删除 `replay/20260928`。

## Open Questions

- 无。1.1 审核前不把十四项补齐预定为 gate 通过。
