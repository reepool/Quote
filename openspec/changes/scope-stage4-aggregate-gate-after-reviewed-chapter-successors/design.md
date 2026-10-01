## Context

恢复合同规定：历史失败行留在原处，章节只能通过新的同集 successor 重新进入判断。产销量和分部财务的 `.2026-10-01.4` 已分别成为当前章节纳入观察。材料投入 `.2026-09-30.3` 和客户供应商 `.2026-09-29.1` 是各自计划上的四报告局部通过。这四行还没有被收成一个 aggregate 集合。本卡只冻结集合和判定条件。1.1 通过前不给出 pass 或 hold 的应用结论。

当前 aggregate 继续 hold。`stage4_aggregate_expansion_gates_met` 保持 false。

## Goals / Non-Goals

**Goals:**

- 冻结四行当前观察、统一四报告身份，以及每章自己的四份制品哈希。
- 把历史失败行和旧局部通过留在判断集合之外。
- 预先写明全部条件成立才可记录研究范围 aggregate pass，任一不成立即 hold。

**Non-Goals:**

- 1.1 通过前不应用 pass／hold，不记录 aggregate `expansion_gates_met=true`。
- 不把四章分数相加，不计算跨章节 recall、accuracy 或 critical-error。
- 不修改历史 archive、ledger、replay 或 source-review。
- 不创建 replay，不改 Python。
- 不启动 restricted-promotion、六章包、规模质量或生产。
- 不改 publication、closure、mode、identity、checkpoint。

## Decisions

1. 本次判断集合只有四行。它们的 critical numeric errors 都是 0，分数只属于各自计划：

| 章节 | 计划 | Recall | Accuracy | Critical numeric errors |
|---|---|---:|---:|---:|
| `extract_material_inputs` | `manufacturing_materials_stage4_material_inputs.2026-09-30.3` | 23/23 | 23/23 | 0 |
| `extract_operating_quantities` | `manufacturing_materials_stage4_operating_quantities.2026-10-01.4` | 38/38 | 38/38 | 0 |
| `extract_segment_financials` | `manufacturing_materials_stage4_segment_financials.2026-10-01.4` | 137/137 | 144/144 | 0 |
| `extract_counterparties_and_concentration` | `manufacturing_materials_stage4_counterparties.2026-09-29.1` | 45/45 | 45/45 | 0 |

2. 四行使用同一报告身份，报告期都是 2025-12-31：

| Instrument | Exchange | report_id | document_version | PDF SHA-256 |
|---|---|---|---|---|
| 300750.SZ | SZSE | `asset_3b09f6c831975c7177b6bb3287cab781` | `ver_09c0e677ec8192dc4fc12cb620069f29` | `c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9` |
| 603659.SH | SSE | `asset_50c70429093f66b34fc57ad8f896fcee` | `ver_c867a6a692048e88fd9cb80473fbf908` | `4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6` |
| 920015.BJ | BSE | `asset_b87f1d1a48e662dae376c540cd021f69` | `ver_cfdbd2d058af825b1fc39f494d7a9bd3` | `4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a` |
| 302132.SZ | SZSE | `asset_0a488da55636b09107be6d719c9ebf39` | `ver_2d20ba3aebc5fac6c562cd619695995a` | `605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020` |

3. 每章只绑定自己的制品。哈希不得互相替换：

| 行 | enqueue SHA-256 | run SHA-256 | result SHA-256 | source-review SHA-256 |
|---|---|---|---|---|
| 材料投入 `.3` | `09edffece890b126e33679a70b19800c8ad6b4fe094efe2aebf1793b168d15f8` | `00aa0f4d5bacbf0ff914b9cc7fc3a9740e6b60fa08a5c7c693699f5b77d1714e` | `2e4b795206b308caa5df6be9ff03c5dd8b8b864b734414faffe6e1df35ad0ed4` | `b1346b1547b91f34c48aab1d4060062904596f534786b276cef59686bb9c6585` |
| 产销量 `.4` | `6f2363a463627a5e233a5f76e094577294b7276de90bd51db61598967e706e52` | `977939ba5ceabe8da0238f594a284cdc6352edca5bafd9cfae8e4021d11a8788` | `6ccd40a07803ac7411631729a1abac69debd3d10119f80260a5731e73f747add` | `1a5c4753d0a3b407c84a188d68b98258e764e9ef463293ed5bdac0d0fcff8cdf` |
| 分部财务 `.4` | `8c2c40e06848563723e208ca4d60d0310804ff986fa7ae86b50aecc7200d3bfa` | `8f007a464e89adfd9bfcf80d810c27d47ce321e885d207e5faedfe6ecf897b90` | `6c5188ef7dc43c45bb69c23478d2dbc3bd19e9e657ca3afb674ad2494c9acc59` | `0a38ac4c78d65dacd756cf310ea1033e9cd1e40e793a6e0a5a5ac256030dcfbd` |
| 客户供应商 `.1` | `7e9c1a72f11723f2d8508d751c27f8ea3f96cae048eb0ab6edc6224eb301bf7a` | `8d3c7bd7a6c1f81a2e2b76064eb3d7fe9685d7570442a1e4d113b786a4cf0d7f` | `b0344d3d577f423a1716b6dacc7c46b100ef3fe9ddddb5d37ef0160c2458a57a` | `b2c38f9fa1cf947ab69e542606332689ac25ea1e6df63facc50ac6a1fa56baf4` |

4. 历史观察不进入本次判断集合，原值保留：MI-1 19/23、19/19、critical numeric errors 0；MI-2 `.2026-09-26.2` 仍是三报告 23/23；OQ-1 26/36、26/27、critical numeric errors 1；OQ-2 36/38、36/36、critical numeric errors 0；OQ-3 `.2026-09-28.3` 的 38/38 只属于旧计划；SF-1 122/132、129/133、critical numeric errors 4；SF-2 102/132、114/142、critical numeric errors 0；SF-3 `.2026-09-29.3` 的 132/132、144/144 只属于旧计划。旧 hold 结论不改。critical numeric errors 1 和 4 不能改成 0。
5. 以后应用本合同时，四项必须同时成立才可记录研究范围 aggregate pass：四行报告身份与上表一致；四行各自的 source-review 仍是业务通过，且 critical numeric errors 为 0；四份制品哈希与上表一致；各章 source-review 已记录的语义和 coverage 边界仍然成立。任一不成立，结论是 hold。不得把 23、38、137、144、45 相加。本卡不应用这四项，因此不记录 pass。
6. 若以后的应用记录了该 pass，它只属于本新合同的研究范围。它最多允许另立一张 restricted-promotion 设计卡。它不授权六章包、规模质量或生产。`production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。在该应用完成前，`stage4_aggregate_expansion_gates_met` 保持 false。

## Risks / Trade-offs

- [把四个局部 true 当成 aggregate pass] → 本卡只写条件，不记录 pass。
- [用新的 `.4` 擦掉 OQ-1 或 SF-1] → 历史行留在判断集合之外，原 critical errors 不变。
- [把 MI-2 的三报告 23/23 并进材料投入 `.3`] → `.3` 是独立四报告观察。MI-2 继续是历史三报告行。

## Migration Plan

无部署。1.1 通过前只有范围文档。通过后才核对绑定并应用准入规则，然后归档。不能从本文件直接启动 replay 或生产准入。

## Open Questions

四项条件是否同时成立，留到 1.1 通过之后的应用任务。本卡不预填 pass 或 hold 的应用结论。
