## Why

Stage 4 的 aggregate 仍是只读 hold。分部财务的后续局部通过不能自动清除历史失败行；只要原始失败观察的 critical numeric errors 4 和 column-binding 失败观察仍纳入 aggregate 判断，整个 Stage 4 就必须保持 hold。缺的是一份只针对 `extract_segment_financials` 的规则：将来的 successor 如何证明旧失败行不再是当前纳入行，同时旧行仍作为历史记录保留。

## What Changes

- 新建一份只读范围。1.1 通过前只写 proposal、design、spec、tasks，不实现 successor，不创建 replay。
- 范围只覆盖 `extract_segment_financials`。不处理产销量，不创建六章包。
- 读取并保留三行已有观察，不重算、不改写：
  - `.2026-09-28.1`：recall 122/132，accuracy 129/133，critical numeric errors 4
  - `.2026-09-29.2`：recall 102/132，accuracy 114/142
  - `.2026-09-29.3`：recall 132/132，accuracy 144/144
- 冻结这三行共用的四报告集及其报告身份，并分别绑定各自的计划、source-review、enqueue、run、result 哈希。
- 规定 successor 的重新纳入资格只写在新的独立观察中。旧失败行保持原指标和原哈希。`.3` 不回填到 `.1` 或 `.2`。
- 1.1 通过前不预填 aggregate `expansion_gates_met=true`，不创建跨章节分数，不授权 restricted-promotion、六章包、规模质量或生产。

## Capabilities

### New Capabilities

- `scope-stage4-segment-financial-successor-reentry-after-failed-observations`: 定义分部财务 successor 在保留历史失败行的同时，如何重新取得 aggregate 纳入资格。1.1 通过前不创建该 successor。

### Modified Capabilities

- 无。不改已归档 hold、ledger、分部财务 replay 或 source-review。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 不改 Python、入队、replay、source-review、publication、closure、mode、identity、checkpoint。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。
