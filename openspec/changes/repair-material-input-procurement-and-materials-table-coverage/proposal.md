## Why

已归档的阶段 4 观察里，19 条已交付投入事实的准确率是 19/19。漏召的 4 项来自两个已经出现在正文里的 Evidence 入口没有进入 `extract_material_inputs`：公司自身的采购或关联采购行，以及正式“主要原材料及能源”表中的原材料行。当前瓶颈不是模型、目录映射或生产控制。

## What Changes

- 新建一份最小 repair 范围审核。1.1 独立范围审核通过前，不改 Python，不改 Evidence plan，不入队，不 replay，不写 source-review，不授权扩大或生产。
- 只覆盖两种披露形态。第一种是公司自身的采购或关联采购行：表头、交易方向和行内容共同证明报告主体采购原材料，单元格或括号中的具名材料可以形成投入关系。宁德时代第 73 页只作为验收 fixture，规则不得硬编码证券、页码或材料名。销售、提供服务、泛称采购金额，以及无法确定买方主体的行继续拒绝。
- 第二种是正式“主要原材料及能源”表。只接收明确属于原材料的具名行。丁酮肟的独立采购或耗用行可以形成投入事实。蒸汽、电等能源行继续排除。委外加工安排本身仍不能生成投入角色。
- 复用现有 `extract_material_inputs`、Evidence、Stage 5 extract/repair/verify 和研究隔离 bundle。输出只到 `accepted_for_review`。销售、直接材料成本、存货金额、legal-empty、unclear 和 extraction failure 的既有边界保持不变。
- 2026-09-26 已归档的 19/23 观察及其 dossier、enqueue、run、result、source-review 不得覆盖或改写。common-core identity、publication、closure、completed mode 和生产授权保持不变。
- 不处理沥青目录别名，不启用完整制造业六章包、产销量或客户供应商。不预设修复后为 23/23，也不预设 gate 通过。

## Capabilities

### New Capabilities

- `repair-material-input-procurement-and-materials-table-coverage`: 把公司采购行和正式原材料表行接入现有 `extract_material_inputs`，并保持已归档 19/23 观察不被改写。1.1 通过前不授权实现。

### Modified Capabilities

- 无。已归档的 `manufacturing-materials-stage4-minimum-slice` 观察保持为历史结果，本 change 不改写它的 19/23、19/19、0 或 `expansion_gates_met=false`。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的实现只复用现有材料章节和研究隔离 bundle，不写 common-core checkpoint、work、publication、closure、mode 或生产 identity。
- `scale_quality_claim_allowed` 保持 false，`production_authorization` 保持 `not_authorized`。
- 固定两家公司的历史观察和 2026-09-26 三报告观察都保持原样。
