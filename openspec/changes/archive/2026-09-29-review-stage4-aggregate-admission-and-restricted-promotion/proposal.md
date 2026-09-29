## Why

阶段 4 的材料投入、产销量、分部财务和客户供应商都已按自己的计划、报告集和章节归档。局部 true 不能合成整体通过。当前缺少一份经过合同定义和独立审核的 Stage 4 aggregate quality gate，因此不能判断是否具备下一阶段准入条件。

## What Changes

- 新建一次只读准入审核。1.1 通过前只写本 change 的范围文档，不改 Python，不入队，不 replay，不重算、不回填，不改任何已归档制品。
- 1.1 通过后，只读取已归档的 dossier、tasks、replay 和 source-review，建立三层不可变观察账本。账本按章节、计划版本、报告集、identity、指标、gate、制品哈希和归档路径分行，不把多行合成一个新指标。
- 账本必须分开保留：材料投入原始 19/23 与 `.2` 的 23/23；产销量原始 26/36、Putailai `.2` 的 36/38 和 CATL `.3` 的 38/38；分部财务原始 122/132、column-binding `.2` 的 102/132 和 footnote `.3` 的 132/132；客户供应商 45/45。每一行的 true 只属于自己的计划、报告集和章节。
- 规定 aggregate gate 的必要条件。没有书面 aggregate 合同，或任一必要条件不满足时，结论必须是 hold。不得臆造 aggregate `expansion_gates_met=true`，也不得新写一个跨章节的 recall 分数。
- 本 change 只回答是否允许进入后续 restricted-promotion 设计。它不实现 promotion，不启动六章包、规模质量或生产。
- `scale_quality_claim_allowed` 保持 false，`production_authorization` 保持 `not_authorized`。publication、closure、mode 和 identity 保持不变。

## Capabilities

### New Capabilities

- `review-stage4-aggregate-admission-and-restricted-promotion`: 只读审核 Stage 4 能否形成 aggregate gate，并决定是否允许后续 restricted-promotion 设计。1.1 通过前不授权账本落账或实现。

### Modified Capabilities

- 无。不改写任何已归档观察。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的账本和准入结论仍是只读记录。没有独立审核通过的 aggregate 准入合同之前，不另开 restricted-promotion 设计卡。
- 不新增报告，不重选样本，不新建 successor replay。
