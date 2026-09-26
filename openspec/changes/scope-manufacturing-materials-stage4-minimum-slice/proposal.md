## Why

固定两家公司的 `material_input_facts=v1` 局部验收不能证明制造/材料规则跨公司成立。阶段 3 研究合同已经要求至少三份正式年报、至少两家公司、至少两份挑战报告；下一张业务卡应先把跨样本最小竖切的范围审清楚，再实现。

## What Changes

- 新建一份范围审核 change。1.1 独立范围审核通过前，不改 Python，不改当前 `owned_page_facts=v8` + `material_input_facts=v1` identity，不入队、不 replay、不写 checkpoint。
- 样本以 `manufacturing-materials-profile-research-contract` 已点名的三份本地有效正式年报为下限：`300750.SZ` 2025、`603659.SH` 2025、`920015.BJ` 2025。其中至少两份用于挑战单一焦点报告。不得把范围收成只围绕 `600004.SH` / `600006.SH`。
- 每份报告先形成独立 dossier，再把字段标成 required、conditional、optional 或合法空值，并区分合法未披露与 extraction failure。
- dossier 完成后只选择一个已有章节任务作为最小 manufacturing/materials vertical slice。不一次启用完整六章行业包。
- 后续实现必须复用现有 Evidence、Stage 5 `extract` / `repair` / `verify` 和研究隔离 bundle。输出只保持 research-only 的 `accepted_for_review`，不进入 common-core production。
- 不启动下一轮扩大，不改 publication、closure、completed mode，不启用 DCF、交易、旧 writer 或完整制造业包。不预设 recall、accuracy 或扩大门槛通过。

## Capabilities

### New Capabilities

- `manufacturing-materials-stage4-minimum-slice`: 跨样本 dossier、单一最小竖切、字段义务和研究隔离的范围合同。1.1 通过前不授权实现。

### Modified Capabilities

- 无。阶段 3 研究合同、历史四报告隔离切片和当前 common-core identity 的要求都不在本 change 中改写。

## Impact

- 本卡只增加 OpenSpec 范围文档。
- 不修改 Python、checkpoint、work、source-review、publication、closure 或 mode。
- `scale_quality_claim_allowed` 保持 false，`production_authorization` 保持 `not_authorized`。
- 固定两家公司的 9/9 仍只覆盖该 identity 的局部验收，不构成全市场或规模质量结论。
