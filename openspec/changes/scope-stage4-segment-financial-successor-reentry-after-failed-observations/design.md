## Context

Stage 4 aggregate 已归档为只读 hold。分部财务有三行独立观察，共用同一四报告集，但计划、指标和制品哈希各不相同。`.2026-09-29.3` 的局部通过没有改写 `.2026-09-28.1` 或 `.2026-09-29.2`。只要这两条失败行仍是当前 aggregate 纳入行，Stage 4 就保持 hold。本 change 只定义 `extract_segment_financials` 的重新纳入规则，不创建 successor。

已审核账本 `openspec/changes/archive/2026-09-29-review-stage4-aggregate-admission-and-restricted-promotion/ledger.md` 和旧 hold-only 合同只读。本卡不修改它们。

## Goals / Non-Goals

**Goals:**

- 冻结分部财务三行观察的计划、指标、报告身份和制品哈希。
- 规定将来的 successor 怎样取得当前 aggregate 纳入资格，同时旧失败行仍以原值保留。
- 把 `.3` 保持为独立局部通过，不把它回填到 `.1` 或 `.2`。

**Non-Goals:**

- 不实现 successor，不创建 replay，不改 Python 或 source-review。
- 不重算 recall、accuracy 或 critical numeric errors，不预填新的章节指标。
- 不产生 aggregate `expansion_gates_met=true` 或跨章节分数。
- 不处理产销量，不启动 restricted-promotion、六章包、规模质量或生产。
- 不改 publication、closure、mode、identity、checkpoint。

## Decisions

1. 范围只有 `extract_segment_financials`。三行观察分开绑定：

| 行 | 计划 | Recall | Accuracy | Critical numeric errors | 局部 gate | Archive | Replay |
|---|---|---:|---:|---:|---|---|---|
| SF-1 | `manufacturing_materials_stage4_segment_financials.2026-09-28.1` | 122/132 | 129/133 | 4 | false | `2026-09-29-scope-manufacturing-materials-stage4-segment-financials` | `replay/20260928` |
| SF-2 | `manufacturing_materials_stage4_segment_financials.2026-09-29.2` | 102/132 | 114/142 | 0 | false | `2026-09-29-repair-segment-financial-column-binding-and-cell-coverage` | `replay/20260929.2` |
| SF-3 | `manufacturing_materials_stage4_segment_financials.2026-09-29.3` | 132/132 | 144/144 | 0 | 仅该计划与该章节为 true | `2026-09-29-repair-segment-financial-footnote-dimension-binding` | `replay/20260929.3` |

SF-3 的局部 true 不是 aggregate true。critical numeric errors 4 仍属于 SF-1，不能改记为 0。

2. 三行 enqueue 使用同一四报告身份。报告期都是 2025-12-31：

| Instrument | Exchange | report_id | document_version | sample_id |
|---|---|---|---|---|
| 300750.SZ | SZSE | `asset_3b09f6c831975c7177b6bb3287cab781` | `ver_09c0e677ec8192dc4fc12cb620069f29` | `manufacturing-materials-300750-2025` |
| 603659.SH | SSE | `asset_50c70429093f66b34fc57ad8f896fcee` | `ver_c867a6a692048e88fd9cb80473fbf908` | `manufacturing-materials-603659-2025` |
| 920015.BJ | BSE | `asset_b87f1d1a48e662dae376c540cd021f69` | `ver_cfdbd2d058af825b1fc39f494d7a9bd3` | `manufacturing-materials-920015-2025` |
| 302132.SZ | SZSE | `asset_0a488da55636b09107be6d719c9ebf39` | `ver_2d20ba3aebc5fac6c562cd619695995a` | `manufacturing-materials-302132-2025-regime` |

`manufacturing-materials-302132-2025-regime` 只是这三份 enqueue 里的 sample_id，不是新的适用性结论。

3. 制品哈希按行分开，不能互相替换：

| 行 | enqueue SHA-256 | run SHA-256 | result SHA-256 | source-review SHA-256 |
|---|---|---|---|---|
| SF-1 | `935578cc64a58a84d84224443354c8a6e5f0d9a6167fd1753cbf156e7079ffe3` | `974dbbfda8608cd1de535215390a9b1accf67b8e7810a9770966620e2e45634b` | `7c2d5ea6e6433586b46545ad2c597a3c7cc60016ca1023559e7a137c8117d859` | `ff2f01fd4365cc621dcfeac2b2778d97b446bcf3d73c1d7f3bb06cb7b9035c2d` |
| SF-2 | `2a4c778790678197eda9472a907b8fc1c132be7f08fdbd5d95a3a0cc86c70f19` | `b5aad9e75ea4200a385ad107f6c044d7d14787f231156b077888018a68708b5e` | `6ca3e2960e11f90a13aaaa75e71dc43452344864b78d10db51566f9bff6912b1` | `6851d7d6b80274a03a14b04f7fe200278a7fff53b5ea4ed734c3470bb015a0ff` |
| SF-3 | `0f2ebfa087f8aa4327d15d4e0366af568bcd06a41ab923ba595899ec4e3cdd9b` | `2ab053d1b3e80b52351ea7963e45f323b97a2706971514c99486eaefd3234d1f` | `92ec1e2a8be5d4a6ee8bc5b492d7dc09a26dbc84bfbd0ad592ca5aab6cdc1eeb` | `4446fb7619f39b49e865bc563cf7673b98d2288d481c0b229de67394ee9c3c74` |

4. 重新纳入资格不靠修改旧行。将来的 successor 只有在同时满足以下条件时，才是 `extract_segment_financials` 的当前 aggregate 纳入观察：
   - 它是新的计划，不是 `.2026-09-28.1`、`.2026-09-29.2` 或 `.2026-09-29.3`。
   - 它使用上表同一四报告身份。
   - 它有自己的 source-review，以及自己的 enqueue、run、result、source-review 哈希。
   - 纳入语句只写在这份新观察中：SF-1 与 SF-2 保留为历史行，不再作为当前纳入行。
   - 该语句不编辑旧 replay、旧 source-review 或 ledger，不把 SF-3 回填到 SF-1 或 SF-2，也不把三行指标相加。
   - 本卡不创建这份 successor，也不填写它的 recall、accuracy 或 critical numeric errors。
5. 在该 successor 被独立审核之前，SF-1 与 SF-2 仍是当前 aggregate 判断中的失败行。SF-3 继续只是局部通过。`stage4_aggregate_expansion_gates_met` 保持 false。分部财务单独完成纳入资格，仍不能恢复 aggregate；产销量还需要自己的后续章节卡。
6. `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。

## Risks / Trade-offs

- [用 SF-3 的 132/132 清除 critical numeric errors 4] → SF-3 不回填 SF-1。4 仍留在 SF-1。
- [把三行哈希改成同一份制品] → 每行保持自己的 enqueue、run、result、source-review 哈希。
- [在本卡直接实现 repair 或 replay] → 1.1 通过前只有范围文档。successor 是否另立，留到本卡审核之后。
- [把分部财务纳入资格写成 aggregate true] → 本卡不产生 aggregate 行，也不计算跨章节分数。

## Migration Plan

无部署。1.1 通过前不改历史制品。通过后若要做分部财务 successor，必须另立该章节的实现卡，不能从本文件直接启动 replay。

## Open Questions

分部财务 successor 是否需要新的 replay，留到本卡独立审核之后再决定。本卡不创建该 successor，也不为它预填指标。
