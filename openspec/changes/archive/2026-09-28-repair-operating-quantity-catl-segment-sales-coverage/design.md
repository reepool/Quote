## Context

璞泰来 repair 的 successor replay 在 `replay/20260928.2`，计划版本是 `manufacturing_materials_stage4_operating_quantities.2026-09-28.2`。独立 source-review 重读四份年报后，36 条已交付事实全部准确，13 条 coverage 状态正确，critical numeric errors 为 0。Gate 仍为 false，因为 `300750.SZ` 第 21 页写明动力电池销量 541 GWh，第 22 页写明储能电池销量 121 GWh。两条都没有进入 bundle。

541 加 121 等于 662，与产销表电池系统 661 GWh 相差 1 GWh。它们对象不同、页面不同，不能并进 661，也不能互相合并。

当前璞泰来 change 的范围只修 `603659.SH`。这两条宁德时代缺口不放回那个 change，也不改它的制品。旧 `replay/20260928` 同样保持不动。

## Goals / Non-Goals

**Goals:**

- 把宁德时代两条分段销量收成可审核合同，并停在未勾选的 1.1。
- 只复用 `extract_operating_quantities`。
- 两条销量各自成为独立事实，带页面、对象、quote、Evidence、报告身份和页面文本 hash。
- successor 仍跑原来的四份报告，用来确认璞泰来修复结果和其他 coverage 没有回退。
- 新计划版本和新隔离目录承接后续 replay。

**Non-Goals:**

- 1.1 通过前不改 Python，不入队，不 replay，不写 source-review，不预填指标。
- 不新增报告，不重选样，不复核 regime。
- 不把 541 与 121 加成 662，不替换产销表 661 GWh。
- 不改璞泰来 repair 的 36 条事实、13 条 coverage 或 `replay/20260928.2`。
- 不改 `replay/20260928`。
- 不把合法空值改成数量，不从产能或利用率倒算产量。
- 不改 common-core、publication、closure、mode、identity 或生产授权。
- 不授权扩大、规模质量声明或生产。
- 新的 source-review 通过前，不进入本 change 的收口，也不归档。

## Decisions

1. **修复仍落在现有章节。**
   `extract_operating_quantities` 是唯一章节。四份定义样本保持不变。验收增量只看宁德时代第 21 页和第 22 页的两条销量；另外三份报告只证明没有回退。

2. **一条来源句子一个销量事实。**
   动力电池 541 GWh 和储能电池 121 GWh 是两句、两页、两个对象。运行时不得硬编码证券代码、页码或产品名；这些只出现在验收 fixture 和测试夹具里。对象必须来自该句，quote 必须是该句的连续片段。

3. **661 GWh 保持为产销表事实。**
   第 26 页电池系统销售量 661 GWh 继续交付，不改成 662，也不被两条分段销量替换。分段销量不从 661 减出来。

4. **successor 计划是 `.2026-09-28.3`。**
   后续正式入口使用 `manufacturing_materials_stage4_operating_quantities.2026-09-28.3`。replay 写入本 change 的新隔离目录，例如 `replay/20260928.3`。`.2026-09-28.2` 和 `.2026-09-27.1` 只在显式传入时使用，不能成为这条 successor 的默认版本。identity 保持 `owned_page_facts=v8` 加 `material_input_facts=v1`。输出只到 `accepted_for_review`，provider calls 保持 0。

5. **指标只由新的独立 source-review 重算。**
   本设计不把补上两条之后写成 38/38，也不把 gate 预设为 true。36/38 只描述 20260928.2，不作为本 change 的结果。

## Risks / Trade-offs

- [用页码或产品名写死规则] → 规则只描述「业务分段销量句子里的对象和数值」。宁德时代的页码和产品名留在 fixture。
- [把 541 和 121 加成 662 去对 661] → 禁止相加，也禁止覆盖 661。
- [补分段销量时改坏璞泰来 23 条或 13 条 coverage] → successor 仍跑四份报告，回退算修复失败。
- [写进 20260928.2 或旧 20260928] → 新目录、新计划版本；旧哈希不变。
- [提前写成 gate 通过] → 1.1 和实现卡都不填指标；只有新的 source-review 可以填写。

## Migration Plan

1. 1.1 只审范围。通过前不实现。
2. 通过后才补最小实现和测试。
3. 再在新隔离目录做 successor replay。
4. 再独立重读，重算 recall、accuracy、critical numeric errors 和 gate。
5. 只有这次新的 source-review 通过，才讨论本 change 的收口和归档。
6. 回滚本 change 只删除这份范围文档；不删除已有 replay，不改控制面。

## Closed observation

独立 source-review 绑定 `replay/20260928.3`，run id `stage4-operating-quantities-20260928.3`，计划 `manufacturing_materials_stage4_operating_quantities.2026-09-28.3`。enqueue `30315acceaed18e870e3ded36ca5f5beb58c54e919d667b100bef8c81b9b2eb2`，run `f14af3b32355551805e5dca0c9d6e732dc06c8bed0a76c2d83fa0f28232588c5`，result `4a1f9823922881a9e210b9889067afeb0063b6106c7f5669791e6199c830d420`，source_review `f1e041ecfc6a7d35ad041fcaef753adce13e51e21f871acf79a4052663a06345`。

重读结果是 recall 38/38、accuracy 38/38、critical numeric errors 0、coverage 13/13、`expansion_gates_met=true`。动力电池 541 GWh、储能电池 121 GWh 和电池系统 661 GWh 保持三条独立销量。第 20 页与第 26 页的 661 GWh 只计一次。

这个 true 只覆盖上述四份报告、该计划和 `extract_operating_quantities`。`scale_quality_claim_allowed` 仍为 false。`production_authorization` 仍为 `not_authorized`。common-core identity 仍是 `owned_page_facts=v8` 加 `material_input_facts=v1`。publication、closure、completed mode 和 checkpoint 没有因本次收口改写。`.2` 与旧 `.1` replay 未改。璞泰来 repair 仍按它自己的 3.2/3.3 处理。

## Open Questions

- 无。收口不授权下一轮扩大。
