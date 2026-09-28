## Context

2026-09-28 的受控 replay 只跑了 `extract_operating_quantities`，四份报告都交付，disposition 为 `accepted_for_review`，provider calls 为 0，生产未授权。独立 source-review 重读四份年报后，权威结果是 source recall 26/36、source accuracy 26/27、critical numeric errors 1、coverage 13/13、`expansion_gates_met=false`。

26 条已交付事实和 13 条 coverage 状态是对的。`302132.SZ` 的分类数量 `not_applicable` 和 `920015.BJ` 的产量 `not_disclosed` 不是阻塞点。阻塞点全部在 `603659.SH`：10 个已披露数量没有正确交付，另有 1 条第 15 页记录把 PVDF 的 Evidence 绑到了勃姆石和氧化铝，并把「超过 3 万吨」写成精确值 3。

旧 replay 目录 `openspec/changes/scope-manufacturing-materials-stage4-operating-quantities-holdout/replay/20260928` 保持不动。enqueue、run、result、source_review 的既有哈希是这张 repair 的对照基线，不是可覆盖的工作文件。

## Goals / Non-Goals

**Goals:**

- 把修复范围收成可审核合同，并停在未勾选的 1.1。
- 只复用 `extract_operating_quantities`，只修 `603659.SH` 2025 年报暴露出的漏交付和错绑。
- 10 个缺口各自成为带对象、指标、数值、单位、比较限定词、物理页和 bounded quote 的事实。
- 第 15 页两条产能句子各自保留 Evidence，不共用一条 quote。
- 基膜 20 亿平方米只计一次。第 14 页加工量和第 19 页正式销量继续分开。
- 26 条正确事实和 13 条 coverage 状态在后续 successor replay 中保持。
- 新计划版本和新隔离目录承接后续 replay。旧 `.2026-09-27.1` / `20260928` 制品不改。

**Non-Goals:**

- 1.1 通过前不改 Python，不入队，不 replay，不写 source-review，不预填指标。
- 不新增报告，不重选样，不复核 regime，不把修复扩成六章包。
- 不把合法空值改成数量，不用产能或利用率倒算产量，不把金额当实物量。
- 不把设备单线铭牌、行业统计、2026 年目标、二期择机投产或三期后续建设写进这 10 个缺口。
- 不改 common-core、publication、closure、mode、identity 或生产授权。
- 不授权扩大、规模质量声明或生产。
- 新的 source-review 通过前，不进入原 change 的 3.3，也不归档。

## Decisions

1. **修复落在现有章节，不新开入口。**
   `extract_operating_quantities` 继续是唯一章节。四份定义样本保持不变，successor replay 仍跑这四份，用来证明另外三份和璞泰来已正确的 13 条事实没有回退。验收增量只看璞泰来的 10 个缺口和 1 条错绑。

2. **一句话一个事实，对象必须来自这句话。**
   比较限定词和对象都属于这句话，不能从同页另一句借来。第 15 页「PVDF 有效产能超过 3 万吨」和「勃姆石和氧化铝的有效产能已达 3 万吨」是两条事实。旧记录把前一句的 quote 绑给后一个对象，因此整条记录是错误，不能靠改名或静默更换 Evidence 变成正确。运行时不得硬编码证券代码、页码或产品名；这些名字只出现在验收 fixture 和测试夹具里。

3. **比较限定词留在事实旁边，不另建比较运算平台。**
   「超过」表示该数值是下限，不能落成没有限定词的精确值。「已达」表示已经达到的来源表述，不能改写成「超过」，也不能把两个限定词合成一种精确相等。实现只需要在现有事实旁保留来源限定词。不新增通用不等式引擎、单位换算器或阈值评分。

4. **只合并被明确重复的同一披露。**
   第 15 页和第 27 页的基膜新增 20 亿平方米是同一在建产能，只交付一次。数值相同但对象、指标、单位标签或限定词不同的句子不合并。第 14 页「涂覆加工量（销量）」和第 19 页涂覆隔膜销售量即使数量级接近，仍然是两个锚点。页 15 的四舍五入出货量如果只是页 19 正式表的同一行，不另算一个缺口。

5. **successor 计划和新目录，不覆盖 20260928。**
   后续计划版本使用 `manufacturing_materials_stage4_operating_quantities.2026-09-28.2`。replay 写入新的研究隔离目录。`replay/20260928` 的 enqueue、run、result、source_review 不得 force replay 或改写。identity 保持 `owned_page_facts=v8` 加 `material_input_facts=v1`。输出只到 `accepted_for_review`，provider calls 保持 0。

6. **指标只由新的独立 source-review 重算。**
   本设计不把 10 个缺口补上之后写成 36/36，也不把 gate 预设为 true。coverage 13/13 只说明合法空值这次没有错，不证明数量召回已经通过。

## Risks / Trade-offs

- [用页码或产品名写死规则] → 规则只描述「句子中的对象和限定词」。603659 的页码和产品名留在 fixture。
- [把接近的加工量和销量合并] → 来源标签和单位不同就保持两条，即使换算后接近。
- [20 亿平方米交付两次] → 同一对象、同一在建产能、同一数值的跨页重复只留一条。
- [把「超过」和「已达」都存成 3] → 限定词必须随事实留下；超过不是精确值。
- [补缺口时改坏另外 26 条或 13 条 coverage] → successor replay 仍包含四份报告，回退算修复失败。
- [在旧目录上重跑] → 新目录、新计划版本；旧哈希不变。
- [提前写成 gate 通过] → 1.1 和实现卡都不填指标；只有新的 source-review 可以填写。

## Migration Plan

1. 1.1 只审范围。通过前不实现。
2. 通过后才补最小实现和针对对象、限定词、Evidence、页码、页面 hash 的测试。
3. 再在新隔离目录做 successor replay。
4. 再独立重读，重算 recall、accuracy、critical numeric errors 和 gate。
5. 只有这次新的 source-review 通过，才考虑原 change 的 3.3 和归档。
6. 回滚本 change 只删除这份范围文档；不删除 20260928 replay，不改控制面。

## Closed observation

目标修复的 23 条璞泰来事实已正确交付。四报告回归仍是 recall 36/38、accuracy 36/36、critical numeric errors 0、coverage 13/13、`expansion_gates_met=false`。失败原因仍是该 bundle 没有宁德时代第 21 页动力电池 541 GWh 和第 22 页储能电池 121 GWh。

这两条已由独立且已归档的 CATL `.3` change 修复。它的 38/38 不回填 `.2`，也不是 `.2` 的重算。阶段 4 原始 `replay/20260928` 的 26/36、26/27、critical numeric errors 1 仍是第三个独立观察。

`.2` 四份制品保持原字节。3.2 不标记为通过。3.3 只表示保留这次失败观察后归档。不新建 successor replay，不改生产授权。

## Open Questions

- 无。收口不把失败回归改写成通过。
