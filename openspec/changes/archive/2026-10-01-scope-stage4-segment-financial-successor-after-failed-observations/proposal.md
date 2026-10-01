## Why

分部财务要重新进入 aggregate 判断，必须另立一份新的独立观察。SF-3 `.2026-09-29.3` 的局部通过不能回填 SF-1 或 SF-2，也不能抹掉 SF-1 的 critical numeric errors 4。缺的是一份只覆盖 `extract_segment_financials` 的 successor 范围：新计划、同一四报告身份、以及本 change 自己的隔离制品。1.1 通过前不启动 replay。

## What Changes

- 新建 successor 范围。1.1 通过前只写 proposal、design、spec、tasks。
- 只覆盖 `extract_segment_financials`。计划固定为 `manufacturing_materials_stage4_segment_financials.2026-10-01.4`，不得复用 `.2026-09-28.1`、`.2026-09-29.2` 或 `.2026-09-29.3`。
- 使用与 SF-1、SF-2、SF-3 相同的四份 2025 年报及其已冻结身份。
- 将来的 enqueue、run、result、source-review 只写入本 change 的 `replay/20261001/`。1.1 通过前不创建这些制品，也不预填 SHA-256、recall、accuracy 或 critical numeric errors。
- 将来的 source-review 必须写明 SF-1 与 SF-2 仍是历史行。不得修改旧 ledger、旧 replay 或旧 source-review，也不得把 SF-3 的 132/132、144/144 改写成旧观察结果。
- 即使该 successor 以后局部通过，也不能单独恢复 Stage 4 aggregate。产销量 successor、restricted-promotion、六章包、规模质量和生产都不在本卡启动。

## Capabilities

### New Capabilities

- `scope-stage4-segment-financial-successor-after-failed-observations`: 定义分部财务 successor 的新计划、四报告身份和隔离制品。1.1 通过前不创建 replay。

### Modified Capabilities

- 无。不改已归档的 re-entry 合同、ledger、SF-1、SF-2、SF-3 或它们的 replay。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 不改 Python、publication、closure、mode、identity、checkpoint。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。
