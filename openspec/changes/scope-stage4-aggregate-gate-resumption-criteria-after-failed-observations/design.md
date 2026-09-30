## Context

最近一次 aggregate admission 已归档为 hold。四个局部通过仍然有效，但只属于各自计划、报告集和章节：材料投入 `.2026-09-30.3` 为 23/23，产销量 `.2026-09-28.3` 为 38/38，分部财务 `.2026-09-29.3` 为 132/132、accuracy 144/144，客户供应商 `.2026-09-29.1` 为 45/45。MI-2 `.2026-09-26.2` 仍是三报告 23/23。产销量和分部财务的失败观察仍在已审核账本中。旧 hold-only 合同 `openspec/changes/archive/2026-09-29-scope-stage4-aggregate-gate-contract-and-resumption-criteria/` 不能被改写。

## Goals / Non-Goals

**Goals:**

- 冻结当前可引用的四个局部通过计划和统一四报告集。
- 规定失败观察，尤其是 critical numeric errors 1 和 4，如何继续保留。
- 规定将来重新判断 aggregate gate 时，新 successor 必须满足的条件。

**Non-Goals:**

- 不预填 aggregate `expansion_gates_met=true`。
- 不创建跨章节分数或 aggregate 行。
- 不修改历史 archive、ledger、replay 或 source-review。
- 不创建 replay，不改 Python。
- 不在本卡启动 OQ 或 SF repair successor。
- 不授权 restricted-promotion、六章包、规模质量或生产。
- 不改 publication、closure、mode、identity、checkpoint。

## Decisions

1. 当前可引用的局部通过只有四行，各自绑定自己的 source-review：
   - `extract_material_inputs`，`manufacturing_materials_stage4_material_inputs.2026-09-30.3`
   - `extract_operating_quantities`，`manufacturing_materials_stage4_operating_quantities.2026-09-28.3`
   - `extract_segment_financials`，`manufacturing_materials_stage4_segment_financials.2026-09-29.3`
   - `extract_counterparties_and_concentration`，`manufacturing_materials_stage4_counterparties.2026-09-29.1`
   统一报告集是 `300750.SZ`、`603659.SH`、`920015.BJ`、`302132.SZ`。
2. 必须继续作为历史行保留的失败观察：
   - 产销量 26/36，accuracy 26/27，critical numeric errors 1
   - 产销量 36/38，accuracy 36/36，critical numeric errors 0
   - 分部财务 122/132，accuracy 129/133，critical numeric errors 4
   - 分部财务 recall 102/132，accuracy 114/142，critical numeric errors 0
   后来的局部 true 不能擦除、替换或回填这些行。critical numeric errors 1 和 4 不能被记成 0。
3. 恢复准入不能只引用上面四个局部通过。只要产销量 critical numeric errors 1 或分部财务 critical numeric errors 4 仍是该章节被纳入判断的失败行，aggregate 就不能通过。若要让该章节重新进入判断，必须另立该章节自己的四报告 successor：同一报告集、新计划、独立 source-review，以及 enqueue、run、result、source-review 的哈希绑定。旧失败行仍保留。本卡不创建这个 successor。
4. 将来的 aggregate 判断只读取新观察的制品哈希。它不修改旧 replay、source-review、ledger 或已归档 hold-only 合同，也不把 MI-2 改成四报告结果。
5. 在本合同通过独立审核并且恢复条件被后续独立观察满足之前，`stage4_aggregate_expansion_gates_met` 保持 false。不生成跨章节分数，不授权 restricted-promotion、六章包、规模质量或生产。
6. `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。publication、closure、mode、identity、checkpoint 不改。

## Risks / Trade-offs

- [用四个局部通过直接恢复准入] → 失败观察仍在时，本合同不允许 aggregate true。
- [为了消掉 critical errors 而改历史 source-review] → 历史行只读。新证据只能来自新的 successor。
- [1.1 尚未通过就开 OQ 或 SF repair] → 本卡停在未勾选的 1.1，不创建 successor。
- [修改已归档 hold-only 合同] → 恢复条件只写在本 change。

## Migration Plan

无部署。1.1 通过前只有范围文档。通过后若要做章节修复，必须另立该章节的范围卡，不能从本文件直接启动 replay。

## Open Questions

产销量和分部财务的 successor 是否会被另立，留到本合通过独立审核之后。本设计不预填 aggregate true，也不创建那些 successor。
