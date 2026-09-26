## Why

固定两家公司的 `material_input_facts=v1` 局部验收不能证明制造/材料规则跨公司成立。阶段 3 研究合同已经要求至少三份正式年报、至少两家公司、至少两份挑战报告；下一张业务卡应先把跨样本最小竖切的范围审清楚，再实现。

## What Changes

- 新建一份范围审核 change。1.1 独立范围审核通过前，不改 Python，不改当前 `owned_page_facts=v8` + `material_input_facts=v1` identity，不入队、不 replay、不写 checkpoint。
- 本次最小竖切的样本下限是 `300750.SZ` 2025、`603659.SH` 2025、`920015.BJ` 2025，覆盖 SZSE、SSE、BSE，其中至少两份用于挑战单一焦点报告。这三份不是阶段 3 的全部样本。阶段 3 的 regime 覆盖已经完成：已核验的 `302132.SZ` 2025 是历史 regime 基线，状态为 covered，不纳入本卡定义样本。本卡不选择 `extract_business_regime`，也不把这三份报告的结果说成 regime 能力已被重新验证。不得把范围收成只围绕 `600004.SH` / `600006.SH`。
- 每份报告先形成独立 dossier，再把字段标成 required、conditional、optional 或合法空值，并区分合法未披露与 extraction failure。
- 三份独立 dossier 完成后，最小竖切正式选定为现有章节任务 `extract_material_inputs`。三家公司都有形态不同的具名材料投入证据。产销量、客户供应商、`extract_business_regime` 和其余章节保持关闭。不一次启用完整六章行业包。
- 后续实现必须复用现有 Evidence、Stage 5 `extract` / `repair` / `verify` 和研究隔离 bundle。输出只保持 research-only 的 `accepted_for_review`，不进入 common-core production。
- 不启动下一轮扩大，不改 publication、closure、completed mode，不启用 DCF、交易、旧 writer 或完整制造业包。不预设 recall、accuracy 或扩大门槛通过。

## Capabilities

### New Capabilities

- `manufacturing-materials-stage4-minimum-slice`: 已完成的阶段 4 最小竖切观察。它包含三份 dossier、研究 Python 与测试、隔离 replay 和 source review。观察结果是 recall 19/23、accuracy 19/19、critical numeric errors 0、`expansion_gates_met=false`。它不授权扩大、规模质量或生产。

### Modified Capabilities

- 无。阶段 3 研究合同、历史四报告隔离切片和当前 common-core identity 的要求都不在本 change 中改写。

## Impact

- 本 change 实际包含研究 Python、测试、三份 dossier、隔离 replay 和 source review。
- 没有修改 common-core checkpoint、work、publication、closure、completed mode 或生产 identity。当前 identity 仍是 `owned_page_facts=v8` + `material_input_facts=v1`。
- `scale_quality_claim_allowed` 保持 false，`production_authorization` 保持 `not_authorized`。
- 固定两家公司的 9/9 仍只覆盖该 identity 的局部验收，不构成全市场或规模质量结论，也不被这次 19/23 覆盖。
