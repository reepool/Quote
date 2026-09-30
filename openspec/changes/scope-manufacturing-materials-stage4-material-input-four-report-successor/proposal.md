## Why

材料投入的 coverage-only suitable 只说明 `302132.SZ` 可以在完整审核后以合法 coverage 进入该章节，不生成虚构的 `material_input` fact。它没有把这份报告补进 MI-2，也没有让 Stage 4 aggregate 通过。缺的是一份独立的四报告 successor 范围：同一 `extract_material_inputs` 章节、同一已批准四报告集、新的计划版本。产销量和分部财务的失败观察仍然保留，这张卡不能恢复 aggregate 准入。

## What Changes

- 新建一份只读范围卡。1.1 通过前只写 proposal、design、spec、tasks，不改 Python，不入队，不 replay，不预填 recall、accuracy、critical numeric errors 或 gate。
- 只使用现有 `extract_material_inputs`。不新建第二套抽取器，不启用六章包。
- 报告集冻结为已批准的四份 2025 年报：`300750.SZ`、`603659.SH`、`920015.BJ`、`302132.SZ`。每份保留原有 report_id、document_version、报告期、PDF hash 和交易所。
- `302132.SZ` 完整审核但没有具名材料时，允许记录有证据的 `not_disclosed` 或 `not_applicable`，不生成虚构的 material_input fact。具名材料、能源、泛称、存货金额、供应商合计和会计政策继续按现有材料投入合同区分。
- 冻结新计划 `manufacturing_materials_stage4_material_inputs.2026-09-30.3`、隔离目录 `replay/20260930/`，以及后续 source-review 必须只绑定这次 enqueue、run、result。MI-1 和 MI-2 的计划与制品保持原样。
- 这张 successor 只补材料投入的报告集缺口。它不回填 MI-2，不消除产销量或分部财务的失败观察和 critical numeric errors，也不产生 aggregate `expansion_gates_met=true`。

## Capabilities

### New Capabilities

- `scope-manufacturing-materials-stage4-material-input-four-report-successor`: 定义四报告材料投入 successor 的报告集、计划版本和 coverage 边界。1.1 通过前不实现、不 replay。

### Modified Capabilities

- 无。不改 MI-1、MI-2、已归档 assessment、dossier、aggregate 合同或其他章节规格。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的实现、受控 replay 和独立 source-review 是后续任务，本卡不预填指标。
- 不改 publication、closure、mode、identity、checkpoint。`production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。
