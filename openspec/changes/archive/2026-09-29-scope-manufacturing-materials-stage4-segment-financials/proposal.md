## Why

产销量和材料投入两条阶段 4 竖切都已归档。分部财务是通用制造画像的核心 required 章节，优先于仍属 conditional 增强的客户供应商。下一步应审核现有 `extract_segment_financials`，而不是启动客户供应商、六章包或生产。

## What Changes

- 新建一份阶段 4 分部财务最小竖切的范围审核。1.1 独立范围审核通过前，不改 Python，不写 dossier，不入队，不 replay，不写 source-review，不预填 recall、accuracy、critical numeric errors 或 gate。
- 只复用现有 `extract_segment_financials`。同一张正式表里必须分开：分部、产品、行业、地区或销售模式维度；营业收入；营业成本；报告直接披露的毛利率；合并抵消项。不自行用收入减成本重算毛利率，不把金额表改成 Activity，不用摘要叙述替代正式表格证据。
- 样本只能来自已批准清单 `manufacturing_materials.2026-09-03.4`：`300750.SZ`（SZSE）、`603659.SH`（SSE）、`920015.BJ`（BSE）、`302132.SZ`（SZSE）。定义样本至少 3 份，并覆盖三个交易所。`302132.SZ` 只有在独立 dossier 确认其分部收入、成本、毛利章节适合后才能进入；不适合就停止，不从清单外补报告。纳入它也不等于复核 regime。
- 1.1 通过后先为每份候选报告写独立 dossier，再标注 required、conditional、optional，以及 `not_disclosed`、`not_applicable`、`unclear`、`extraction_failed`。`legal_empty` 只能包裹其中一个具体 coverage 状态，不能代替它。
- 研究输出只到新的隔离目录，disposition 为 `accepted_for_review`。不进入 common-core production，不启用客户供应商、regime、六章包、DCF、交易、价格敏感性或旧 writer。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。common-core identity、publication、closure、mode、checkpoint 和既有 replay 不改。产销量 26/36、36/38、38/38 与材料投入 19/23、23/23、9/9 继续作为独立历史观察。

## Capabilities

### New Capabilities

- `scope-manufacturing-materials-stage4-segment-financials`: 用已批准清单上的跨交易所样本，审核现有 `extract_segment_financials` 是否能在同一正式表内分开维度、收入、成本、报告毛利率和合并抵消。1.1 通过前不授权实现。

### Modified Capabilities

- 无。不改写已归档的产销量或材料投入观察。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的实现只复用现有分部财务章节和研究隔离 bundle。证券代码、页码和分部名称只作为 dossier fixture，不得写进抽取规则。
- 不授权下一轮扩大、规模质量声明或生产。
