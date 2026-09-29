## Context

分部财务有三层互不替代的观察：

- `scope-manufacturing-materials-stage4-segment-financials/replay/20260928`，计划 `manufacturing_materials_stage4_segment_financials.2026-09-28.1`，run id `stage4-segment-financials-20260928`。source recall 122/132，source accuracy 129/133，critical numeric errors 4，`expansion_gates_met=false`。失败原因是页 25 列角色串线和十个当期单元格未交付。task 3.3 未勾选。
- `repair-segment-financial-column-binding-and-cell-coverage/replay/20260929.2`，计划 `.2026-09-29.2`，run id `stage4-segment-financials-20260929.2`。source recall 102/132，source accuracy 114/142，critical numeric errors 0，`expansion_gates_met=false`。失败原因是页 139 与页 178 的来源维度没有绑到印刷栏目。task 3.2 未勾选。
- 已归档 `2026-09-29-repair-segment-financial-footnote-dimension-binding/replay/20260929.3`，计划 `.2026-09-29.3`。source recall 132/132，source accuracy 144/144，critical numeric errors 0，`expansion_gates_met=true`。这个 true 只覆盖四份冻结 2025 年报、该计划和 `extract_segment_financials`。

仓库已有把 gate false 作为历史观察归档的先例，包括首次扩大、common-core owned-page repair，以及 Putailai 产销量 36/38。operating-quantity 的 26/36 不在本 change 内。

## Goals / Non-Goals

**Goals:**

- 冻结 `.1` 与 `.2` 的四份制品字节和哈希。
- 把三层观察写成互相独立的账本，失败层继续显示 false。
- 1.1 通过后，只更新两本失败 change 的 design/tasks，再按日期目录归档，并核对活动目录已删除。

**Non-Goals:**

- 不把 132/132 或 144/144 回填到 `.1` 或 `.2`。
- 不把 task 3.3 或 column-binding task 3.2 勾成通过。
- 不新建 replay，不改 Python。
- 不处理 operating-quantity 26/36、36/38 或 38/38。
- 不启动客户供应商、六章包、扩样、规模质量或生产。
- 不改 identity、publication、closure、mode、checkpoint。

## Decisions

1. 三层观察按计划版本分开保存。`.3` 解释的是 footnote-dimension repair 之后的读法，不是对 `.1` 或 `.2` 的重算。
2. 归档时失败任务保持未勾选。design/tasks 可以补上已经完成的失败数字和“本观察保持 false”的句子，但不能把 gate 写成 true。
3. 归档使用 `git mv` 到日期目录。`.1` 建议目录 `2026-09-29-scope-manufacturing-materials-stage4-segment-financials`，`.2` 建议目录 `2026-09-29-repair-segment-financial-column-binding-and-cell-coverage`。若目标已存在，停止而不是覆盖。
4. 归档前后逐字节核对两组四份制品。任一哈希变化即停止。
5. 1.1 通过前不执行归档。若 1.1 拒绝把失败 gate 归档，两本 change 继续留在活动目录，后续 successor 必须是新 change。

## Risks / Trade-offs

- [归档被读成失败已经通过] → 账本、spec 和未勾选任务同时保留 false。
- [`.3` 的 132/132 覆盖旧分母] → 合同写明它不回填、不重算 `.1` 或 `.2`。
- [顺手收口产销量 26/36] → 明确排除，另立独立 reconciliation。
- [归档移动改变制品字节] → 只使用 `git mv`，并在移动后重算哈希。
