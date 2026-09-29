## Why

阶段 4 扩张保持已经归档观察，并且 `extract_counterparties_and_concentration` 的 dossier 已在已批准四份 2025 年报上达到样本门槛。下一步只审核这一章的实现范围。局部通过的材料投入、产销量和分部财务不能合成整体质量门，也不能据此启动六章包或生产。

## What Changes

- 新建客户与供应商集中度的阶段 4 实现范围。1.1 通过前不改 Python，不入队，不 replay，不写 source-review，不预填 recall、accuracy、critical numeric errors 或 gate。
- 只复用现有 `extract_counterparties_and_concentration`。样本固定为已批准清单 `manufacturing_materials.2026-09-03.4` 的四份 2025 年报：`300750.SZ`（SZSE）、`603659.SH`（SSE）、`920015.BJ`（BSE）、`302132.SZ`（SZSE）。不新增发行人。
- 保留 dossier 中的三种披露形态：匿名排名身份；只披露前五名合计且名称覆盖为 `not_disclosed`；具名或报告内聚合身份与「是否存在关联关系」混排。
- 客户 Relationship、供应商 Relationship 和集中度 Measurement 分开。customer 与 supplier 的同名匿名身份不合并。`客户 A(1)` 与客户「第一名」即使金额相同也保持独立。关联方比例和合计行只作为集中度。相关交易、其他应收款和信用风险表不回填本章名称。
- `not_applicable`、`not_disclosed`、`unclear`、`extraction_failed` 保持独立语义。`legal_empty` 只能包裹其中一个 coverage 状态。章节主语只有「公司」时，`subject_scope` 保持 `unclear`。
- 研究输出只到本 change 的隔离目录，disposition 为 `accepted_for_review`。不写 production 或 common-core。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。identity、publication、closure、mode 和 checkpoint 不改。既有切片观察不回填。

## Capabilities

### New Capabilities

- `scope-manufacturing-materials-stage4-counterparties-and-concentration`: 在四份已批准年报上审核现有客户与供应商集中度章节，分开关系、集中度和合法空值。1.1 通过前不授权实现。

### Modified Capabilities

- 无。不改写材料投入、产销量或分部财务的已归档观察。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的实现只复用现有章节和研究隔离 bundle。证券代码、页码、客户标签和供应商标签只作为 fixture，不得写进抽取规则。
- 不授权六章包、规模质量声明或生产。
