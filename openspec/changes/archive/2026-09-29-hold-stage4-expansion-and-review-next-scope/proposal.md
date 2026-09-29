## Why

阶段 4 的材料投入、产销量和分部财务三条竖切及其失败观察 reconciliation 都已归档。各切片的通过或失败只属于自己的计划、报告集和章节。当前没有一份可以把这些局部结果合成 `expansion_gates_met=true` 的整体质量门，因此不能据此启动生产、规模质量声明、六章包或下一轮 replay。

## What Changes

- 新建一份只读的阶段 4 观察索引。1.1 通过前只写本 change 的范围文档，不改 Python，不入队，不 replay，不重算、不回填任何历史观察。
- 索引逐项绑定已归档结果和路径。局部 true 不得合并成 Stage 4 整体通过。
- 明确当前没有 aggregate `expansion_gates_met=true`。`scale_quality_claim_allowed` 保持 false，`production_authorization` 保持 `not_authorized`。publication、closure、mode、identity 保持不变。
- 下一项业务能力只允许做 dossier-based scope 评估。候选是现有章节 `extract_counterparties_and_concentration`。不得直接启用完整六章包。没有新的范围审核通过前，不得实现或 replay。
- 若该候选无法在已批准清单内同时满足至少三份报告、SZSE/SSE/BSE 和至少两种披露形态，则停止，不从清单外补报告。
- 固定两家公司 9/9、common-core v8 的 8/9，以及材料投入、产销量、分部财务的各层观察，继续互相独立。

## Capabilities

### New Capabilities

- `hold-stage4-expansion-and-review-next-scope`: 建立阶段 4 只读观察索引，并规定下一项章节只能先做 dossier 范围评估。1.1 通过前不授权实现。

### Modified Capabilities

- 无。不改写任何已归档 replay 的指标或 gate。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的下一步只是候选章节的 dossier 评估；实现和 replay 必须另立 change。
- 不改 common-core identity、publication、closure、mode、checkpoint 或生产授权。
