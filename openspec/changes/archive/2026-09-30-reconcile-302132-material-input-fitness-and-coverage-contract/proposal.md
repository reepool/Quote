## Why

已归档评估把 `302132.SZ` 判为 unsuitable，依据是它没有 source-native 具名材料事实。上位制造/材料合同同时规定：`extract_material_inputs` 对未具名材料允许 `not_disclosed`；完整读取后没有具名投入时，应记录 legal-empty 或 `not_disclosed`，而不是抽取失败。`not_disclosed` 不生成事实，但可以是合法 coverage。当前不能把“没有可交付的 material_input fact”直接当成“不能作为该章节的可审核样本”，也不能在这个冲突未裁决前创建 successor 或把 Stage 4 当成终局锁定。

## What Changes

- 新建一份只读 reconciliation。1.1 通过前只写 proposal、design、spec、tasks，不改历史归档，不选择保留 unsuitable 或改为 coverage-only suitable。
- 对照材料只包括上位制造/材料需求、`manufacturing-materials-stage4-minimum-slice` 当前 spec、已归档的 `302132.SZ` 材料投入 dossier，以及已归档 assessment 的 proposal、design、spec、tasks。
- 2.1 只并列两种 suitable 定义：每份报告至少产生一个具名 material_input fact；或者报告被完整审核，并对无具名材料给出有证据的 `not_disclosed` 或 `not_applicable` coverage。必须把“章节不可评估”和“该章节合法未披露”分开，但不在 2.1 里选择。
- 2.2 只允许二选一。保留 unsuitable 时，必须说明 aggregate 样本门槛为何要求每份报告都有具名材料事实，以及该要求为何不违反上位 `not_disclosed` 合同；Stage 4 继续 hold，不开 successor。纠正为 coverage-only suitable 时，不得改写旧 dossier 或历史 replay，只记录旧评估把合法 coverage 误当成样本不适合，并授权以后另立独立的四报告材料投入 successor 范围卡。本卡仍不实现、不 replay。
- 3.1 才保留结论并归档。只有 2.2 明确确认 coverage-only 样本合法时，下一张才可以是 successor scope。否则阶段 4 继续 hold。
- 不改 MI-1、MI-2、aggregate ledger、历史 replay 或 source-review。不把 `302132.SZ` 补入 MI-2。不重开 aggregate gate。`production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。

## Capabilities

### New Capabilities

- `reconcile-302132-material-input-fitness-and-coverage-contract`: 只对照 `302132.SZ` 材料投入的样本适合性与合法 `not_disclosed` coverage。1.1 通过前不选择哪一种定义生效。

### Modified Capabilities

- 无。不改阶段 4 最小切片规格、已归档评估或历史观察。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 通过后的 2.1 只写两种定义的对照，2.2 才二选一。本卡不预填该选择。
- 不改 Python，不入队，不 replay，不创建 successor scope，不启动 restricted-promotion、六章包、规模质量或生产。
