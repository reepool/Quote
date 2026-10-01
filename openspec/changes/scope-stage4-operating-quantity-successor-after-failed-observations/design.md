## Context

Stage 4 aggregate 仍是只读 hold。分部财务 `.2026-10-01.4` 已取得当前章节纳入资格，但不能代替产销量。OQ-1 `.2026-09-27.1` 仍是 recall 26/36、accuracy 26/27、critical numeric errors 1。OQ-2 `.2026-09-28.2` 仍是 recall 36/38、accuracy 36/36、critical numeric errors 0。OQ-3 `.2026-09-28.3` 的 recall 38/38、accuracy 38/38、critical numeric errors 0 只属于该计划。本 change 是产销量自己的 successor 范围。1.1 通过前不实现、不 replay。

现有入口 `replay_operating_quantity_research` 已接受显式 `plan_version`，默认是 `OPERATING_QUANTITY_PLAN_VERSION`，即 `.2026-09-28.3`。enqueue 和 result 记录传入的计划。本卡不改 Python，因此不改这条默认路径。

## Goals / Non-Goals

**Goals:**

- 冻结新计划、run id、同一四报告身份，以及本 change 内的隔离目录。
- 规定后续只修正 dossier 路径，并复用现有 replay 入口。
- 规定继承的业务边界，以及将来 source-review 的通过条件。

**Non-Goals:**

- 1.1 通过前不改 Python，不创建 enqueue、replay 或 source-review。
- 不预填 recall、accuracy、critical numeric errors，也不把 OQ-3 的 38/38 抄成 `.4` 的结果。
- 不回写 OQ-1 的 critical numeric error 1，不把 OQ-3 回填到 OQ-1 或 OQ-2。
- 不创建 aggregate 行、跨章节分数或 aggregate `expansion_gates_met=true`。
- 不启动 restricted-promotion、六章包、规模质量或生产。
- 不改 publication、closure、mode、identity、checkpoint。

## Decisions

1. 范围只有 `extract_operating_quantities`。新计划是 `manufacturing_materials_stage4_operating_quantities.2026-10-01.4`。它不是 `.2026-09-27.1`、`.2026-09-28.2` 或 `.2026-09-28.3`。2.1 必须调用 `replay_operating_quantity_research`，显式传入该计划和 run id `stage4-operating-quantities-20261001`。默认计划保持 `.2026-09-28.3`。现有 `run.json` 不新增计划字段，通过 enqueue 哈希绑定计划。不新增抽取器，不改抽取规则。
2. 四报告身份及 PDF 哈希与 OQ-1、OQ-2、OQ-3 相同，报告期都是 2025-12-31。来源页范围继承 OQ-3：OQ-2 才加入 `603659.SH` 第 27 页，OQ-3 才加入 `300750.SZ` 第 21–22 页。历史 enqueue 不改。本轮来源页是：

| Instrument | Exchange | report_id | document_version | PDF SHA-256 | 物理页范围 |
|---|---|---|---|---|---|
| 300750.SZ | SZSE | `asset_3b09f6c831975c7177b6bb3287cab781` | `ver_09c0e677ec8192dc4fc12cb620069f29` | `c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9` | 26–27、21–22 |
| 603659.SH | SSE | `asset_50c70429093f66b34fc57ad8f896fcee` | `ver_c867a6a692048e88fd9cb80473fbf908` | `4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6` | 14、15、19、27 |
| 920015.BJ | BSE | `asset_b87f1d1a48e662dae376c540cd021f69` | `ver_cfdbd2d058af825b1fc39f494d7a9bd3` | `4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a` | 49–50 |
| 302132.SZ | SZSE | `asset_0a488da55636b09107be6d719c9ebf39` | `ver_2d20ba3aebc5fac6c562cd619695995a` | `605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020` | 15 |

`manufacturing-materials-302132-2025-regime` 仍只是历史 sample_id，不是新的适用性结论。

3. 当前 binding 的 dossier 路径指向 `openspec/changes/scope-manufacturing-materials-stage4-operating-quantities-holdout/dossiers/`，该活动目录已不存在。同一字节在 `openspec/changes/archive/2026-09-29-scope-manufacturing-materials-stage4-operating-quantities-holdout/dossiers/`，哈希与 OQ-3 enqueue 一致：

| Dossier | SHA-256 |
|---|---|
| `300750-sz-2025.md` | `180cbb1d5e3bcae46e4dcc78047b13b25bf0602204c9250fa5ff02c552a1b0a7` |
| `603659-sh-2025.md` | `4458a489585741d36fe3bd44156b06067e6194bccd9adab2cbda7b51a18ebaee` |
| `920015-bj-2025.md` | `b49b61490d9740b0218af424ee10674f8bbf5f519136edb37db53ef22905a6b8` |
| `302132-sz-2025.md` | `f0b8123f3f6abe9a769a0b918398808dd8060a2c1326749d9343ac4cb9aa2e36` |

2.1 只把路径指到这个归档目录。不改 dossier 字节、PDF 哈希或来源页。新 enqueue 记录实际可读路径。

4. 隔离目录是本 change 下的 `replay/20261001/`。1.1 通过前该目录不存在，SHA-256 不预填。完成后的 enqueue、run、result、source-review 哈希必须不同于下列历史哈希：

| 行 | enqueue SHA-256 | run SHA-256 | result SHA-256 | source-review SHA-256 |
|---|---|---|---|---|
| OQ-1 | `5fffde878890fb43c136c4e0082f4eba32fc9e65402e166a96cca164396fab7a` | `3fa107b4bf914cf7603ee1e2e73937392a04ad95bf52df4e68d5bff304a5e9cf` | `67e37d98ed0d0dc57f9672f6ef224482db52fc3e9ce96ece8f366866a1e87302` | `5568d4ee73cbc332c63cb935fba42a13ea99e4dfe0344f6b700ad4fcc212ab0b` |
| OQ-2 | `b38b3fb11ea3f3a59b21f3072ac719ed7c7fbe4bc1063ea39294b9ca8afa79bc` | `387340ccfaa95237010b4fe7fa2cffd4a8467a11cc3b4b5679260ac97b246319` | `17c990a55ce099459fdf8c24683fbc34499c5819fd987e6483d93c8846ece571` | `81ed17c742fc6f64e1e2aa88b5e5ac8068c73c036e03374b1757b3d7c45575ea` |
| OQ-3 | `30315acceaed18e870e3ded36ca5f5beb58c54e919d667b100bef8c81b9b2eb2` | `f14af3b32355551805e5dca0c9d6e732dc06c8bed0a76c2d83fa0f28232588c5` | `4a1f9823922881a9e210b9889067afeb0063b6106c7f5669791e6199c830d420` | `f1e041ecfc6a7d35ad041fcaef753adce13e51e21f871acf79a4052663a06345` |

5. `.4` 继承 OQ-3 已验证的业务边界，不把这些边界当成新的抽取修复：`300750.SZ` 物理页 21 的动力电池销量 541 GWh、页 22 的储能电池销量 121 GWh，以及产销表中的电池系统销量 661 GWh，分别保留，不得相加为 662，也不得互相替换。`603659.SH` 页 14 的加工量与页 19 的销量分开。产能类别、项目阶段、比较符 `超过` 与 `已达`、库存脚注，以及基膜 20 亿平方米这一条事实，保持原绑定。`920015.BJ` 未披露的数量保持 `not_disclosed`。`302132.SZ` 无法分类统计保持 `not_applicable`。`legal_empty` 只包裹具体 coverage，不改写成虚构数量。
6. 将来的 source-review 从年报建立分母。局部通过必须同时满足：四份报告独立重读，source recall 为 100%，source accuracy 为 100%，critical numeric errors 为 0，并且上面的业务边界核对通过。制品哈希齐全本身不是业务通过。不得预填 38/38。OQ-1 的 critical numeric error 1 留在历史行，不能改成 0。
7. 纳入判断留在 source-review 通过之后。即使 `.4` 取得局部 gate，该 gate 也只属于这个计划、这四份报告和 `extract_operating_quantities`。`stage4_aggregate_expansion_gates_met` 保持 false。旧 ledger `openspec/changes/archive/2026-09-29-review-stage4-aggregate-admission-and-restricted-promotion/ledger.md` 只读。
8. `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。

## Risks / Trade-offs

- [把 OQ-3 的 38/38 抄成 `.4` 的分数] → 分母来自 `.4` 自己的重读。
- [用 541 与 121 相加替换 661] → 三个数量继续分开。
- [为了消除 critical numeric error 1 而改 OQ-1] → 历史行只读。新证据只来自 `.4`。
- [局部通过后直接恢复 aggregate] → 本卡不产生 aggregate `expansion_gates_met=true`。

## Migration Plan

无部署。1.1 通过前只有范围文档。通过后的顺序是：最小实现与定向测试，再受控 replay，再独立 source-review，再纳入判断，最后归档。不能从已归档的恢复合同直接启动 replay。

## Open Questions

`.4` 的 recall、accuracy、critical numeric errors 和局部 gate 尚未发生，不在本卡预填。`.4` 能否成为当前产销量纳入行，留到 source-review 通过之后。业务通过和制品绑定必须同时成立。

## Acceptance

1.1 通过。新计划隔离在 `replay/20261001/`，默认计划仍是 `.2026-09-28.3`。dossier 只修路径。四报告身份及 PDF 哈希与 OQ-1、OQ-2、OQ-3 相同；来源页范围继承 OQ-3。历史 enqueue 不改。`run.json` 按现有 schema 通过 enqueue 哈希绑定计划，不新增字段。本卡不改 Python，也不创建 replay。
