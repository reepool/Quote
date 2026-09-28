## Context

三份 operating-quantity 观察现在叠在一起，但不能合成一次通过：

| 观察 | 位置 | 结果 |
| --- | --- | --- |
| 阶段 4 原始 replay | `scope-manufacturing-materials-stage4-operating-quantities-holdout/replay/20260928` | recall 26/36，accuracy 26/27，critical numeric errors 1，gate false |
| 璞泰来 repair `.2` | `repair-operating-quantity-603659-coverage-and-evidence-binding/replay/20260928.2` | recall 36/38，accuracy 36/36，critical numeric errors 0，coverage 13/13，gate false |
| CATL repair `.3` | 已归档 `2026-09-28-repair-operating-quantity-catl-segment-sales-coverage/replay/20260928.3` | recall 38/38，accuracy 38/38，critical numeric errors 0，coverage 13/13，gate true，且只覆盖该切片 |

璞泰来 `.2` 的 source-review 已经说明：它自己的 23 条目标事实正确；四报告回归少了宁德时代第 21 页动力电池 541 GWh 和第 22 页储能电池 121 GWh。CATL `.3` 修复的是这两条，不是重跑或改写 `.2`。

璞泰来 tasks 里的 3.2 仍未勾选，因为它记录的是未通过的复核；3.3 仍未勾选，因为它当时写成「等 source-review 通过才收口」。那次 source-review 已经发生，结论是不通过。活动 change 因此停在「复核已写完，但不能标成通过，也不能假装没审过」。

## Goals / Non-Goals

**Goals:**

- 把收口决定写成可审核合同，并停在未勾选的 1.1。
- 冻结 `.2` 四份制品和 36/38 结果。
- 写明目标修复、失败回归和后续 CATL 修复是三件不同的事。
- 选定一种不伪造通过的归档方式。

**Non-Goals:**

- 1.1 通过前不改 Python，不重写 replay，不归档。
- 不把 38/38 写回 `.2`。
- 不把三个观察合并成一次 gate。
- 不启动扩大、生产或六章制造业包。

## Decisions

1. **失败观察可以归档，而且必须保持失败。**
   仓库已经这样处理过：`2026-09-25-expand-company-profile-m4-first-expansion` 以 `expansion_gates_met=false` 归档；`2026-09-26-repair-common-core-first-expansion-owned-page-gaps` 以 recall 8/9、gate false 归档。规范没有要求 gate 为 true 才能把历史 change 移入 archive。要求的是不要把未通过改写成通过。

2. **不为了让璞泰来 gate 变 true 而重放。**
   再跑一次 successor 会生成新的观察，不能替换 `.2` 的 36/38。CATL `.3` 已经是那次后续修复。再创建 `.4` 只会复制 `.3`，并让人误以为 `.2` 被重算过。因此本 reconciliation 不创建新 replay。

3. **1.1 通过后的收口只动账本。**
   通过后，把璞泰来 3.2 保持为「已独立复核、结果不通过、不得勾成通过」，把 3.3 写成「按保留失败观察的方式归档」。归档使用日期目录和 `git mv`。enqueue、run、result、source_review 必须是 100% 重命名。哈希保持：
   - enqueue `b38b3fb11ea3f3a59b21f3072ac719ed7c7fbe4bc1063ea39294b9ca8afa79bc`
   - run `387340ccfaa95237010b4fe7fa2cffd4a8467a11cc3b4b5679260ac97b246319`
   - result `17c990a55ce099459fdf8c24683fbc34499c5819fd987e6483d93c8846ece571`
   - source_review `81ed17c742fc6f64e1e2aa88b5e5ac8068c73c036e03374b1757b3d7c45575ea`

4. **三个结果继续分列。**
   主规格只描述这次收口决定。它引用另外两次观察的哈希和分数，但不把它们收成自己的 gate。

## Risks / Trade-offs

- [把 3.2 勾成通过] → 3.2 记录的是失败复核，归档说明里保持未通过。
- [用 CATL 38/38 替换 36/38] → 两个目录和两套哈希都保留。
- [为了归档再跑 replay] → 本决定明确不做。新 replay 只会制造第四个观察。
- [活动 change 继续挂着] → 1.1 通过后才归档；1.1 否决则维持活动状态并改决定。

## Migration Plan

1. 1.1 只审这个收口决定。通过前不归档。
2. 通过后只改璞泰来 change 的 design/tasks 账本，并同步一条保留失败观察的主规格。
3. `git mv` 到 `openspec/changes/archive/2026-09-28-repair-operating-quantity-603659-coverage-and-evidence-binding/`，复核四份制品哈希不变。
4. 不改控制面，不启动下一次扩大。

## Open Questions

- 无。若 1.1 认为失败观察不能归档，再另开 successor replay；那不是本决定。
