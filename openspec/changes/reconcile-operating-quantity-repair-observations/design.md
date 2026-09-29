## Context

产销量有三层互不替代的观察：

- 活动 change `scope-manufacturing-materials-stage4-operating-quantities-holdout/replay/20260928`，计划 `manufacturing_materials_stage4_operating_quantities.2026-09-27.1`，run id `stage4-operating-quantities-20260928`。source recall 26/36，source accuracy 26/27，critical numeric errors 1，`expansion_gates_met=false`。失败点是第 15 页 PVDF 与「勃姆石和氧化铝」的 Evidence/对象绑定。task 3.2 与 task 3.3 未勾选。
- 已归档 Putailai repair `2026-09-28-repair-operating-quantity-603659-coverage-and-evidence-binding/replay/20260928.2`，计划 `.2026-09-28.2`。source recall 36/38，source accuracy 36/36，critical numeric errors 0，`expansion_gates_met=false`。23 条璞泰来目标事实正确交付。task 3.2 未勾选。
- 已归档 CATL repair `2026-09-28-repair-operating-quantity-catl-segment-sales-coverage/replay/20260928.3`，计划 `.2026-09-28.3`。source recall 38/38，source accuracy 38/38，critical numeric errors 0，`expansion_gates_met=true`。这个 true 只覆盖四份冻结 2025 年报、该计划和 `extract_operating_quantities`。

分部财务三层观察已经另行收口，不在本 change 内。

## Goals / Non-Goals

**Goals:**

- 冻结原始 `.1` 四份制品的字节和哈希。
- 把三层产销量观察写成互相独立的账本，失败层继续显示 false。
- 1.1 通过后，只更新原始 holdout 的 design/tasks，再按日期目录归档，并核对活动目录已删除。
- 原始观察归档核对通过后，再归档本 reconciliation 自身。

**Non-Goals:**

- 不把 36/38 或 38/38 回填到原始 `.1`。
- 不把 task 3.2 或 task 3.3 勾成通过。
- 不新建 replay，不改 Python。
- 不改已归档的 `.2` 或 `.3` 制品。
- 不处理分部财务的 122/132、102/132 或 132/132。
- 不启动六章包、客户供应商、扩样、规模质量或生产。
- 不改 identity、publication、closure、mode、checkpoint。

## Decisions

1. 三层观察按计划版本分开保存。`.2` 和 `.3` 是后续独立 repair，不是对 `.1` 的重算。
2. 归档时 task 3.2 与 task 3.3 保持未勾选。design/tasks 可以补上已经完成的失败数字和“本观察保持 false”的句子，但不能把 gate 写成 true。
3. 归档使用 `git mv`。建议目录 `2026-09-29-scope-manufacturing-materials-stage4-operating-quantities-holdout`。目标已存在则停止，不覆盖。
4. 归档前后逐字节核对原始四份制品。任一哈希变化即停止。
5. 不为这次失败再建 successor replay。1.1 通过前不执行归档。若 1.1 拒绝把失败 gate 归档，原始 change 继续留在活动目录。

## Risks / Trade-offs

- [归档被读成 26/36 已经通过] → 账本、spec 和未勾选的 3.2、3.3 同时保留 false。
- [38/38 覆盖原始分母] → 合同写明它不回填、不重算 `.1` 或 `.2`。
- [顺手改分部财务或已归档 `.2`、`.3`] → 明确排除，只移动原始 holdout 和本 reconciliation。
- [归档移动改变制品字节] → 只使用 `git mv`，并在移动后重算哈希。
