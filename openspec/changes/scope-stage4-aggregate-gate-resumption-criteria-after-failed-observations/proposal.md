## Why

Stage 4 已停在只读 hold。四个章节各有自己的局部通过，但产销量仍保留 26/36、critical numeric errors 1 和 36/38，分部财务仍保留 122/132、critical numeric errors 4，以及 recall 102/132、accuracy 114/142。后来的局部通过不能覆盖这些失败观察。旧 hold-only aggregate 合同已经归档，不能直接修改来恢复准入。缺的是一份新的、可独立审核的恢复条件合同。

## What Changes

- 新建一份只读 aggregate gate 合同。1.1 通过前只写 proposal、design、spec、tasks，不创建 replay，不改 Python，不预填 aggregate true。
- 读取已归档的 Stage 4 ledger、各章节局部 source-review 和历史失败观察。不改这些文件。
- 冻结当前四报告集 `300750.SZ`、`603659.SH`、`920015.BJ`、`302132.SZ`，以及四个当前局部通过计划：材料投入 `.2026-09-30.3`、产销量 `.2026-09-28.3`、分部财务 `.2026-09-29.3`、客户供应商 `.2026-09-29.1`。
- 规定失败观察和 critical numeric errors 继续作为历史行保留。产销量 critical numeric errors 1 和分部财务 critical numeric errors 4 不能被后续局部 true 擦除。
- 定义恢复准入的条件：若要把某一失败章节重新纳入判断，必须另立该章节自己的四报告 successor，并完成独立 source-review 和制品哈希绑定。旧失败行保留。aggregate gate 只读取新观察，不回填历史制品。本卡不创建这些 successor。
- 新合同通过前，不授权 restricted-promotion、六章包、规模质量或生产。`production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。

## Capabilities

### New Capabilities

- `scope-stage4-aggregate-gate-resumption-criteria-after-failed-observations`: 定义在保留历史失败观察的前提下，Stage 4 aggregate gate 将来如何重新判断。1.1 通过前不恢复准入。

### Modified Capabilities

- 无。不改已归档 hold、ledger、局部 source-review 或 replay。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 不改 publication、closure、mode、identity、checkpoint。
- 不启动 repair successor、replay、规模质量或生产准入。
