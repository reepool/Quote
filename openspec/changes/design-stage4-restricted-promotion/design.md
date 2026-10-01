## Context

研究范围 aggregate 合同已经归档。四章当前观察是业务通过，但查询入口读不到它们。

权威入口是 `company_profile_common_core`。CLI、Scheduler 和 Telegram 只转发到 `CompanyProfileTaskService`。`query` 和 `export` 再转到 `CompanyProfileReadService`。common-core 运行记录的写入 owner 是 `CompanyProfileResearchWriter`。读服务现在只取 `company_profile_common_core.v1` 下每个证券最新的运行记录和 checkpoint，投影三维骨架、已接受事实、缺项和商品暴露。

四章已验收结果不在那套运行记录里。它们在四份隔离 `result.json` 中，并由 enqueue、run、source-review 的哈希绑定。本卡只设计如何把这些已验收 bundle 接到现有读模型，不实施。

## Goals / Non-Goals

**Goals:**

- 研究用户按公司和报告查询材料投入、产销量、分部财务、客户供应商。
- 导出事实、来源证据和 coverage。页码、期间、单位和章节语义保持 bundle 原值。
- 四家公司都能得到部分研究视图。`302132.SZ` 材料投入显示合法空值，事实数为 0。
- 重复查询不把事实再写入研究存储，重复导出不覆盖 replay、运行记录或历史观察。
- 缺项照实显示。四章通过不表示三维核心画像完整。

**Non-Goals:**

- 不改 Python，不 replay，不改控制面、旧 ledger 或 16 份制品。
- 不补 `extract_business_overview`、`extract_business_regime` 或六章包。
- 不把四章分数相加，不把历史失败行改成当前行。
- 不启用生产发布，不授予规模质量、DCF、交易或价格敏感性。
- 不做通用读模型重构，也不做界面。

## Decisions

1. 以后的实施只扩展现有 query/export。应用 owner 仍是 `CompanyProfileTaskService`，读 owner 仍是 `CompanyProfileReadService`。新代码不得在 CLI、Scheduler、Telegram 或脚本里复制查询循环，也不得让 `CompanyProfileResearchWriter` 写入这四章结果。
2. 最小适配点是读服务内部的只读投影。它按证券和冻结报告身份读取四份 `result.json`，核对应的 enqueue、run、result、source-review 哈希，然后把章节事实、证据和 coverage 放进查询结果的独立章节节。它不把这些行写入 `accepted_facts`，也不生成新的运行记录。
3. 冻结输入只有这四行：

| 章节 | 计划 | 归档 replay |
|---|---|---|
| `extract_material_inputs` | `manufacturing_materials_stage4_material_inputs.2026-09-30.3` | `openspec/changes/archive/2026-09-30-scope-manufacturing-materials-stage4-material-input-four-report-successor/replay/20260930/` |
| `extract_operating_quantities` | `manufacturing_materials_stage4_operating_quantities.2026-10-01.4` | `openspec/changes/archive/2026-10-01-scope-stage4-operating-quantity-successor-after-failed-observations/replay/20261001/` |
| `extract_segment_financials` | `manufacturing_materials_stage4_segment_financials.2026-10-01.4` | `openspec/changes/archive/2026-10-01-scope-stage4-segment-financial-successor-after-failed-observations/replay/20261001/` |
| `extract_counterparties_and_concentration` | `manufacturing_materials_stage4_counterparties.2026-09-29.1` | `openspec/changes/archive/2026-09-29-scope-manufacturing-materials-stage4-counterparties-and-concentration/replay/20260929/` |

4. 四份报告身份相同，报告期都是 2025-12-31：`300750.SZ` `asset_3b09f6c831975c7177b6bb3287cab781` `ver_09c0e677ec8192dc4fc12cb620069f29` PDF `c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9`；`603659.SH` `asset_50c70429093f66b34fc57ad8f896fcee` `ver_c867a6a692048e88fd9cb80473fbf908` PDF `4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6`；`920015.BJ` `asset_b87f1d1a48e662dae376c540cd021f69` `ver_cfdbd2d058af825b1fc39f494d7a9bd3` PDF `4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a`；`302132.SZ` `asset_0a488da55636b09107be6d719c9ebf39` `ver_2d20ba3aebc5fac6c562cd619695995a` PDF `605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020`。
5. 16 份制品哈希保持 aggregate 合同原值。材料投入 enqueue `09edffece890b126e33679a70b19800c8ad6b4fe094efe2aebf1793b168d15f8`，run `00aa0f4d5bacbf0ff914b9cc7fc3a9740e6b60fa08a5c7c693699f5b77d1714e`，result `2e4b795206b308caa5df6be9ff03c5dd8b8b864b734414faffe6e1df35ad0ed4`，source-review `b1346b1547b91f34c48aab1d4060062904596f534786b276cef59686bb9c6585`。产销量 enqueue `6f2363a463627a5e233a5f76e094577294b7276de90bd51db61598967e706e52`，run `977939ba5ceabe8da0238f594a284cdc6352edca5bafd9cfae8e4021d11a8788`，result `6ccd40a07803ac7411631729a1abac69debd3d10119f80260a5731e73f747add`，source-review `1a5c4753d0a3b407c84a188d68b98258e764e9ef463293ed5bdac0d0fcff8cdf`。分部财务 enqueue `8c2c40e06848563723e208ca4d60d0310804ff986fa7ae86b50aecc7200d3bfa`，run `8f007a464e89adfd9bfcf80d810c27d47ce321e885d207e5faedfe6ecf897b90`，result `6c5188ef7dc43c45bb69c23478d2dbc3bd19e9e657ca3afb674ad2494c9acc59`，source-review `0a38ac4c78d65dacd756cf310ea1033e9cd1e40e793a6e0a5a5ac256030dcfbd`。客户供应商 enqueue `7e9c1a72f11723f2d8508d751c27f8ea3f96cae048eb0ab6edc6224eb301bf7a`，run `8d3c7bd7a6c1f81a2e2b76064eb3d7fe9685d7570442a1e4d113b786a4cf0d7f`，result `b0344d3d577f423a1716b6dacc7c46b100ef3fe9ddddb5d37ef0160c2458a57a`，source-review `b2c38f9fa1cf947ab69e542606332689ac25ea1e6df63facc50ac6a1fa56baf4`。哈希不符时该章不交付，不得换一份历史结果顶上。
6. 投影保留章节原记录。材料投入的空值来自 `scope_outcomes` 的 `legal_empty`；产销量、分部财务和客户供应商的空值来自各自 `coverage`。`302132.SZ` 材料投入的五条 scope outcome 都是 `legal_empty`，事实列表为空。合法空值不得变成具名事实。
7. 查询不写文件。导出只写调用方给出的导出目录，沿用现有 export 的画像 JSON、CSV 和 manifest，并带上章节节。导出不得写入 replay 目录、common-core 命名空间、checkpoint 或发布控制文件。再查一次得到同一批事实，不追加第二份。
8. 章节节单独标明 `core_profile_complete=false`。overview 和 regime 作为未纳入本交付的缺项显示。已有 common-core 画像若存在，保持原样，不被这四章覆盖或标成完整。
9. 研究消费者只有 query/export。`production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。DCF、交易和价格敏感性保持未授权。本路径不调用发布开关的 enable、pause、resume 或 rollback。发布已暂停或回滚时，新的画像写入继续拒绝；这次只读交付仍只读冻结制品，不把开关拨回 enabled。

## Risks / Trade-offs

- [把四章投影写进运行记录] → 投影只存在于查询和导出结果，写入 owner 不接收这些行。
- [用另一章或历史 replay 填哈希缺口] → 哈希不符就缺该章，历史观察原值不动。
- [把合法空值显示成零条披露从而像未检查] → `302132.SZ` 材料投入必须显式列出 `legal_empty`。
- [查询成功被当成生产发布] → 响应保持 `not_authorized`，且不改发布控制文件。

## Migration Plan

无部署。1.1 通过前只有设计文档。通过后的实施才打通“读取已验收结果 → 研究查询 → 导出”。失败时不改冻结制品，查询继续只读。不能从本文件直接 replay 或启用发布。

## Open Questions

独立设计审核尚未接受本卡。审核前不预填实施结论，也不进入代码。
