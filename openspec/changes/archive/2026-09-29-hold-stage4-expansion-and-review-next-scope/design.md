## Context

阶段 4 已归档的观察按章节、计划和报告集分开。下面的数字来自各自的 source-review，本 change 不重算、不回填。

材料投入，`extract_material_inputs`：

- 原始 `openspec/changes/archive/2026-09-26-scope-manufacturing-materials-stage4-minimum-slice/replay/20260926`：recall 19/23，accuracy 19/19，critical numeric errors 0，`expansion_gates_met=false`。
- 后续局部修复 `openspec/changes/archive/2026-09-27-repair-material-input-procurement-and-materials-table-coverage/replay/20260926.2`，计划 `manufacturing_materials_stage4_material_inputs.2026-09-26.2`：recall 23/23，accuracy 23/23，critical numeric errors 0，`expansion_gates_met=true`。这个 true 只覆盖那三份冻结报告、该计划和该章节。

产销量，`extract_operating_quantities`：

- 原始 `openspec/changes/archive/2026-09-29-scope-manufacturing-materials-stage4-operating-quantities-holdout/replay/20260928`，计划 `.2026-09-27.1`：recall 26/36，accuracy 26/27，critical numeric errors 1，`expansion_gates_met=false`。失败点是第 15 页 PVDF 与「勃姆石和氧化铝」的 Evidence/对象绑定。enqueue `5fffde878890fb43c136c4e0082f4eba32fc9e65402e166a96cca164396fab7a`，run `3fa107b4bf914cf7603ee1e2e73937392a04ad95bf52df4e68d5bff304a5e9cf`，result `67e37d98ed0d0dc57f9672f6ef224482db52fc3e9ce96ece8f366866a1e87302`，source-review `5568d4ee73cbc332c63cb935fba42a13ea99e4dfe0344f6b700ad4fcc212ab0b`。
- Putailai `.2`：`openspec/changes/archive/2026-09-28-repair-operating-quantity-603659-coverage-and-evidence-binding/replay/20260928.2`，计划 `.2026-09-28.2`：recall 36/38，accuracy 36/36，critical numeric errors 0，`expansion_gates_met=false`。
- CATL `.3`：`openspec/changes/archive/2026-09-28-repair-operating-quantity-catl-segment-sales-coverage/replay/20260928.3`，计划 `.2026-09-28.3`：recall 38/38，accuracy 38/38，critical numeric errors 0，`expansion_gates_met=true`。这个 true 只覆盖四份冻结报告、该计划和该章节。

分部财务，`extract_segment_financials`：

- 原始 `openspec/changes/archive/2026-09-29-scope-manufacturing-materials-stage4-segment-financials/replay/20260928`，计划 `.2026-09-28.1`：recall 122/132，accuracy 129/133，critical numeric errors 4，`expansion_gates_met=false`。
- column-binding `.2`：`openspec/changes/archive/2026-09-29-repair-segment-financial-column-binding-and-cell-coverage/replay/20260929.2`，计划 `.2026-09-29.2`：recall 102/132，accuracy 114/142，critical numeric errors 0，`expansion_gates_met=false`。
- footnote `.3`：`openspec/changes/archive/2026-09-29-repair-segment-financial-footnote-dimension-binding/replay/20260929.3`，计划 `.2026-09-29.3`：recall 132/132，accuracy 144/144，critical numeric errors 0，`expansion_gates_met=true`。这个 true 只覆盖四份冻结报告、该计划和该章节。

固定两家公司 9/9 和 common-core `owned_page_facts=v8` 的 8/9 不属于这三条竖切，也不并入本索引的通过结论。

## Goals / Non-Goals

**Goals:**

- 把上述观察写成只读索引，并声明没有 aggregate gate。
- 保持 `scale_quality_claim_allowed=false` 和 `production_authorization=not_authorized`。
- 规定下一项只评估 `extract_counterparties_and_concentration` 是否具备合法研究范围。

**Non-Goals:**

- 1.1 通过前不改 Python，不入队，不 replay。
- 不把多个局部 true 拼成 Stage 4 整体通过。
- 不启用完整六章包，不打开 regime 作为本轮实现。
- 不从已批准清单外补报告。
- 不改 identity、publication、closure、mode、checkpoint。
- 不重算、不回填任何已归档指标。

## Decisions

1. 观察索引只引用已归档 source-review 和路径。本 change 不生成新的 recall 或 gate。
2. 任一章节的局部 true 只在其计划、报告集和章节内有效。跨章节相加不是质量门。
3. 下一项候选固定为现有 `extract_counterparties_and_concentration`。1.1 通过后只做 dossier 评估：已批准四份报告之内，至少三份，覆盖 SZSE、SSE、BSE，并至少有两种披露形态。达不到就停止。
4. dossier 评估通过之前，不得写实现、不得入队、不得 replay。即使 dossier 通过，实现也必须是另一个 change。
5. 1.1 通过前，本文件中的索引只是待审合同，不授权下一章开工。

## Risks / Trade-offs

- [把 23/23、38/38、132/132 读成整体通过] → 索引写明每个 true 的计划、报告集和章节，并声明没有 aggregate gate。
- [客户供应商评估滑成六章包] → 候选只有一个现有章节，其余章节保持关闭。
- [样本不够时从清单外补报告] → 合同要求停止。
- [1.1 未过就开始 dossier 或代码] → tasks 把 dossier 放在 1.1 之后，并把实现排除在本 change 之外。

## Close-out

本 change 只保留阶段 4 的只读观察索引和客户供应商 dossier 结论。已批准四份 2025 年报的 dossier 评估已经完成。`extract_counterparties_and_concentration` 的实现、replay 和 source-review 在独立 change `openspec/changes/archive/2026-09-29-scope-manufacturing-materials-stage4-counterparties-and-concentration/` 中完成并归档。那里的局部 45/45 不写入本索引，也不产生 aggregate `expansion_gates_met=true`。`scale_quality_claim_allowed` 保持 false，`production_authorization` 保持 `not_authorized`。阶段 4 停在多条独立研究切片已完成、整体质量门未建立、生产未授权。不启动六章包、下一轮扩大、规模质量或生产，也不为已归档局部观察另建 successor replay。

## Dossier assessment

2026-09-29 只读核对已批准清单中的四份 2025 年报。文件 SHA-256 与 manifest 一致：300750 `c1527297…048ec9`，603659 `4e81f553…fa35b6`，920015 `4d2c1612…70695a`，302132 `605394bd…5e3020`。页码是 pypdf 一基物理页。页面文本哈希是 `extract_text().strip()` 的 SHA-256。本节不填写 recall、accuracy、critical numeric errors 或 `expansion_gates_met`。

结论：四份报告都有可读、可绑定的本章证据，覆盖 SZSE、SSE、BSE，并且至少有两种真实披露形态。样本门槛达到，没有 coverage gap，不补充清单外发行人。本 change 仍不改 Python、不入队、不 replay。实现必须另立 change。

三种披露形态分开保留：

1. 匿名排名行表。300750 物理页 28 用「第一名」到「第五名」列出客户和供应商，金额与占比分列。
2. 只披露前五名合计、不列交易对手。603659 物理页 21 是叙述句；302132 物理页 16 是字段行。版式不同，名称覆盖都是 `not_disclosed`。
3. 具名聚合身份与字母匿名身份混排，并有「是否存在关联关系」列。920015 物理页 17–18。

四份报告的章节主语都是「公司」。本节不把「公司」升为合并主体，`subject_scope` 保持 `unclear`。期间是 2025 年报的年度口径。这些章节没有因缺页、缺表头或缺单位而标成 `extraction_failed` 的单元格。

### 300750.SZ，SZSE，宁德时代

物理页 27，文本哈希 `83a28d5ebb0cdf320040e2a5a76455365171d337e7c5641b2f097deb76ed465c`。章节「已签订的重大销售合同截至本报告期的履行情况」，表头含对方当事人、本报告期履行金额、本期确认的销售收入金额，单位千元。匿名客户「客户 A(1)」，履行金额和本期确认销售收入都是 58,159,202；合同总金额为「-」。注 (1) 写明保密协议，不便披露客户具体名称。这是 `report_local_anonymous` 的 Relationship candidate，不与其他报告的「客户 A」合并。重大采购合同为「不适用」，该合同项 `not_applicable`。

物理页 28，文本哈希 `23bb76548dbf87f5da42077d8df738f533aa8a0b286bcb313ce5b61ab036c444`。章节「主要销售客户和主要供应商情况」。

- 客户集中度：前五名合计销售金额 165,061,533 千元，占年度销售总额 38.96%。合计行只做 concentration Measurement，不生成 Relationship。
- 客户关系：公司前 5 大客户资料的五行都是报告内匿名排名，第一名 58,159,202 千元、13.73%，第二名 47,127,609、11.12%，第三名 30,201,701、7.13%，第四名 15,419,319、3.64%，第五名 14,153,702、3.34%。排名身份只在本报告、customer 关系内有效。
- 供应商集中度：前五名合计采购金额 59,938,203 千元，占年度采购总额 10.38%。
- 供应商关系：前 5 名供应商同样是第一名至第五名。第一名 23,318,360 千元、4.04%，第二名 11,601,437、2.01%，第三名 9,241,133、1.60%，第四名 8,258,913、1.43%，第五名 7,518,360、1.30%。supplier 的「第一名」不与 customer 的「第一名」合并。
- 相关方比例：客户关联方销售额占年度销售总额 0.00%，供应商关联方采购额占年度采购总额 0.00%。这是集中度组成部分，不是 Relationship。
- 同一金额不合并：页 27「客户 A(1)」与页 28 客户「第一名」都是 58,159,202 千元，原文没有写明是同一当事人，保持两条。
- 合法空值：主要客户其他情况说明、主要供应商其他情况说明，以及贸易业务收入占比超过 10%，均为「不适用」，`not_applicable`。

### 603659.SH，SSE，璞泰来

物理页 21，文本哈希 `a6b57ec2977c21b3742a6484517b747f15d01f33cc8aac21b3e5a2fc5ceef658`。章节「主要销售客户及主要供应商情况」。同一控制合并列示的情况说明为「无」，这句不生成聚合身份 Relationship。

- 客户集中度：前五名客户销售额 913,511 万元，占年度销售总额 58.14%。单位以这句原文的万元为准。
- 供应商集中度：前五名供应商采购额 115,002 万元，占年度采购总额 13.98%。
- 相关方比例：两类关联方金额都是 0 万元、占年度总额 0%。仍是集中度，不是 Relationship。
- 客户和供应商名称：本节没有交易对手行，name coverage 为 `not_disclosed`。
- 合法空值：单个客户或供应商占比超过 50%、前五名新增或严重依赖，以及风险警示下的前五名客户和供应商明细，均为「不适用」，`not_applicable`。物理页 22 延续贸易业务前五名供应商「不适用」。页 22 文本哈希 `cd95236465db78a3a3b9793bacaaba2b611ee3f51cb9ad9f8c3b2554201da7d0`。
- 后文其他应收款前五名和关联方往来不属于本章，不拿来回填前五名客户或供应商名称。

### 920015.BJ，BSE，锦华新材

物理页 17，文本哈希 `4c850ade55e57bacc3b0b903fab9293f0fd521eab4152fb18f3d2eb1d57d1d4e`。章节「主要客户情况」，单位元。表头为序号、客户、销售金额、年度销售占比%、是否存在关联关系。客户表跨到物理页 18，文本哈希 `4bcafaaa1421fb8af6cd603b5173c1f1dd80872d571ebb0f4da5d16d2833b692`。

- 客户关系：1 浙江衢州硅宝化工有限公司同一控制下企业 178,099,027.98 元、17.25%、关联关系「是」，身份是 `report_local_aggregate`。2 J 公司 102,138,447.77、9.89%、否。3 K 公司 96,572,440.35、9.36%、否。4 L 公司 77,381,436.14、7.50%、否。5 在物理页 18，M 公司 64,767,267.08、6.27%、否。J/K/L/M 都是本报告内匿名身份，不跨报告合并。
- 客户集中度：合计 518,958,619.32 元、50.27%。合计行只做 concentration Measurement。
- 相关方行：客户第 1 行的关联关系为「是」。页 18 另有一句，洪根卸任已超过 12 个月后，报告仍把浙江衢州硅宝同一控制下企业认定为关联方。这句解释该聚合身份，不另造一个前五名客户。
- 供应商关系：章节「主要供应商情况」，单位元。1 巨化集团有限公司及其控制的企业 176,687,834.41 元、23.42%、关联关系「是」，`report_local_aggregate`。2 A 公司 64,084,947.81、8.49%、否。3 B 公司 53,396,542.16、7.08%、否。4 C 公司 46,894,579.64、6.22%、否。5 D 公司 46,729,645.68、6.19%、否。A/B/C/D 只在本报告 supplier 关系内有效。
- 供应商集中度：合计原文为 `387,793,549.7` 元、51.40%。保留这个印刷 token，不补数字。合计行不生成 Relationship。
- 合法空值：贸易业务收入占营业收入比例超过 10% 为「不适用」，`not_applicable`。
- 页 129 的应收账款余额前五名属于信用风险说明，不并入本章客户表，也不把 94.72% 写成销售集中度。

### 302132.SZ，SZSE，中航成飞

物理页 16，印刷页码 15，文本哈希 `279c094d74baeaac175a9e38dce960874b83f34113e3e255f4354b8f0a4f3bb9`。章节「主要销售客户和主要供应商情况」，单位元。

- 客户集中度：前五名客户合计销售金额 72,672,513,444.91 元，占年度销售总额 96.44%。
- 供应商集中度：前五名供应商合计采购金额 51,613,058,963.17 元，占年度采购总额 74.16%。
- 相关方比例：关联方销售额占年度销售总额 4.93%，关联方采购额占年度采购总额 42.17%。只作为集中度，不从后文关联方往来回填名称，也不把前五名 name coverage 改成 observed。
- 客户和供应商名称：本节没有交易对手行，`not_disclosed`。
- 合法空值：物理页 15 的重大销售合同、重大采购合同为「不适用」，`not_applicable`。页 15 文本哈希 `d5b8211968aeb05676f5a17ac7fbe29eedad48b9c92b5f75555efa75c97b395b`。该空值不改变页 16 的集中度 observed。
