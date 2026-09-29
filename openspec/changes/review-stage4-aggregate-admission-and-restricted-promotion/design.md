## Context

阶段 4 已归档的观察彼此独立。材料投入 `.2` 的 23/23 只属于该计划、该样本和 `extract_material_inputs`。产销量原始 26/36 与 Putailai `.2` 的 36/38 仍是失败观察；CATL `.3` 的 38/38 只属于 `extract_operating_quantities`。分部财务原始 `.1` 与 column-binding `.2` 的失败仍保留；footnote `.3` 的 132/132 只属于该计划和 `extract_segment_financials`。客户供应商 45/45 只属于 `extract_counterparties_and_concentration`。两家公司 9/9 和 common-core `owned_page_facts=v8` 的 8/9 不属于这四条竖切，不进入本账本，也不参与 aggregate。

当前没有一份书面的 Stage 4 aggregate quality gate 合同。

## Goals / Non-Goals

**Goals:**

- 规定三层只读账本必须绑定哪些已归档观察。
- 规定 aggregate gate 的必要条件和 hold 规则。
- 规定本 change 只产生准入结论，不产生 promotion 实现。

**Non-Goals:**

- 1.1 通过前不读档落账、不重算指标、不改历史制品。
- 不把局部 true 回填到更早的计划。
- 不新增报告，不重选样本。
- 不入队，不 replay，不为失败观察另建 successor replay。
- 不改 publication、closure、mode、identity 或控制面。
- 不启动六章包、规模质量或生产。

## Decisions

1. 账本分三层，三层都只复制归档制品中已经写明的内容。
   - 观察层：章节、计划版本、报告集、recall、accuracy、critical numeric errors、gate 及其适用范围。
   - 身份层：该行冻结的 instrument、report_id、document_version、report period 和 processing identity。
   - 制品层：归档路径，以及 enqueue、run、result、source-review 的路径和 SHA-256。
2. 必须逐行纳入的归档目录是：
   - `2026-09-26-scope-manufacturing-materials-stage4-minimum-slice`
   - `2026-09-27-repair-material-input-procurement-and-materials-table-coverage`
   - `2026-09-29-scope-manufacturing-materials-stage4-operating-quantities-holdout`
   - `2026-09-28-repair-operating-quantity-603659-coverage-and-evidence-binding`
   - `2026-09-28-repair-operating-quantity-catl-segment-sales-coverage`
   - `2026-09-29-scope-manufacturing-materials-stage4-segment-financials`
   - `2026-09-29-repair-segment-financial-column-binding-and-cell-coverage`
   - `2026-09-29-repair-segment-financial-footnote-dimension-binding`
   - `2026-09-29-scope-manufacturing-materials-stage4-counterparties-and-concentration`
3. aggregate gate 同时满足以下条件才可能成立：有一份书面合同点名纳入的章节；这些章节的 source-review 都绑定到自己的计划和制品；报告集一致，并且该合同要求的交易所覆盖由同一报告集满足；失败观察和 critical numeric error 仍计入，不被后来的局部 true 擦除；没有任何局部 true 被回填或跨章节相加。缺一则 hold。
4. 材料投入 `.2` 的通过样本与后来四份报告的切片不是同一报告集。这个差异由 2.2 按归档制品核对；1.1 不把它预写成一个新的分数。
5. 六章包仍关闭。业务概览和经营体制没有进入上述竖切账本，不能用四条竖切代替完整章节集合。
6. 2.2 只记录是否允许另开 restricted-promotion 设计卡。允许的前提是 aggregate 准入合同已经独立审核通过。本 change 预期在现有材料下得到 hold，因此不授权那张设计卡；2.2 仍须先读档，不得在 1.1 里把 hold 写成已经完成的复述。

## Ledger

2.1 的三层账本写在 `ledger.md`。它只复制九行归档观察、各自冻结的报告身份，以及 enqueue、run、result、source-review 的路径和 SHA-256。材料投入两行没有 `processing_identity` 字段，账本保持空白，不用后一行的 identity 补上。材料投入 `.2` 的三报告集与四报告切片不同，只作为后续准入审核的事实。`ledger.md` 没有 aggregate 行，没有跨章节汇总，也没有 hold 结论。

## Risks / Trade-offs

- [把 23/23、38/38、132/132、45/45 加总成整体通过] → 账本按计划、报告集和章节分行，并禁止回填。
- [用失败观察缺失来制造 true] → 原始 26/36、36/38、122/132、102/132 必须留在账本里。
- [hold 被写成新的 recall 分数] → 2.2 只写 hold 或不允许 promotion 设计，不发明跨章节指标。
- [审核滑进实现] → 1.1 通过前不落账；全程不改 Python、不 replay、不改控制面。
