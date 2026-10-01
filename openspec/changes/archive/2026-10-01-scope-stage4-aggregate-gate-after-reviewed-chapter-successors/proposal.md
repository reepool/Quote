## Why

四个章节各自已有当前局部观察，但还没有一份合同把它们收成可独立审核的 aggregate 判断集合。已归档的恢复合同允许独立 successor 重新进入判断，历史失败行继续保留。缺的是这份新的只读范围：冻结四行、统一四报告身份，并预先写明 pass 与 hold 条件。1.1 通过前不应用该条件，也不记录 aggregate pass。

## What Changes

- 新建只读范围。1.1 通过前只写 proposal、design、spec、tasks。
- 判断集合只含四行当前观察：材料投入 `.2026-09-30.3` 为 23/23、23/23；产销量 `.2026-10-01.4` 为 38/38、38/38；分部财务 `.2026-10-01.4` 为 137/137、144/144；客户供应商 `.2026-09-29.1` 为 45/45、45/45。四行 critical numeric errors 都是 0，且只属于各自计划。
- 冻结统一四报告身份 `300750.SZ`、`603659.SH`、`920015.BJ`、`302132.SZ`，并逐章绑定 enqueue、run、result、source-review 哈希。
- MI-1、MI-2、OQ-1、OQ-2、OQ-3、SF-1、SF-2、SF-3 保留为历史观察。原指标、critical errors 和旧 hold 结论不改。
- 预先写明：报告身份一致、四行业务通过、制品绑定正确、既定语义与 coverage 边界成立，四项同时成立才允许以后记录本新合同的研究范围 aggregate pass。任一不满足即 hold。各章分数不跨章节相加。
- 本卡不记录该 pass。当前 aggregate 继续 hold。未来通过最多另立 restricted-promotion 设计卡。生产和规模质量仍未授权。

## Capabilities

### New Capabilities

- `scope-stage4-aggregate-gate-after-reviewed-chapter-successors`: 定义四个已审核章节观察的 aggregate 判断集合和 pass／hold 条件。1.1 通过前不应用该条件。

### Modified Capabilities

- 无。不改恢复合同、ledger、历史 replay 或 source-review。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 不改 Python、publication、closure、mode、identity、checkpoint。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。`stage4_aggregate_expansion_gates_met` 保持 false。
