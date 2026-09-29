## Context

已归档的 aggregate 合同把统一报告集定为四份 2025 年报，其中包含 `302132.SZ`。材料投入计划 `manufacturing_materials_stage4_material_inputs.2026-09-26.2` 只冻结了 `300750.SZ`、`603659.SH`、`920015.BJ`。该计划因此不能满足 aggregate 报告集，合同结论是 hold。这个缺口不是把 `302132.SZ` 补进 MI-2 就能消除的。`302132.SZ` 当时只作为 regime 基线进入批准清单，没有作为 `extract_material_inputs` 的定义样本接受过。本 change 只决定这份已批准年报是否适合该章节，不重开 aggregate，也不创建 successor replay。

## Goals / Non-Goals

**Goals:**

- 冻结唯一评估对象：`302132.SZ` 的已批准 2025 年报，以及现有 `extract_material_inputs`。
- 规定 2.1 新建材料投入 dossier 时必须核对的披露位置和每条候选的证据绑定。
- 规定 2.2 只能在 dossier 审核后选择适合或不适合，并且两种结果都不在本卡实现或 replay。

**Non-Goals:**

- 不改 Python，不入队，不 replay，不预填 recall、accuracy、critical numeric errors 或 gate。
- 不改 MI-1、MI-2 或任何历史制品，不把 `302132.SZ` 补入 MI-2。
- 不把旧 regime dossier 当作材料投入审核，不重新审核重大重组或 package regime。
- 不增加清单外发行人，不重开 aggregate gate。
- 不启动 restricted-promotion、六章包、规模质量或生产。
- 不改 publication、closure、mode、identity、checkpoint。

## Decisions

1. 评估对象只有一份已批准报告。身份从现有四报告绑定复制，不另选 PDF：
   - instrument `302132.SZ`，交易所 SZSE
   - `report_id` `asset_0a488da55636b09107be6d719c9ebf39`
   - `document_version` `ver_2d20ba3aebc5fac6c562cd619695995a`
   - `report_period` `2025-12-31`
   - `published_at` `2026-04-28T16:00:00+00:00`
   - PDF `content_hash` `605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020`
   样本身份 `manufacturing-materials-302132-2025-regime` 只说明它曾是 regime 基线，不是材料投入结论。
2. 章节只有现有 `extract_material_inputs`。不评估经营体制、产销量、分部财务、客户供应商，也不启用六章包。
3. 2.1 新建本 change 内的 `dossier.md`。它不能复用 `docs/development/company_profile_manufacturing_materials_dossier_302132_2025_regime.md`，也不能复用其他章节为这份 PDF 写过的页码范围。1.1 不创建该文件。
4. dossier 按来源位置分别记录，不能把不同位置合成一条事实：
   - 采购模式及具名采购对象
   - 正式主要原材料表
   - 成本构成中的具名投入
   - 关联采购中的具名材料行
   - 风险章节中的明确原材料披露
   - 读过后仍成立的 `not_disclosed`、`not_applicable`、`unclear` 或 `extraction_failed`
5. 每条候选绑定物理页、正式栏目和表头、bounded quote、页面文本 hash、主体、报告期、source-native 名称及必要单位。物理页是 pypdf 的 1-based 顺序。页面文本 hash 是该页 `extract_text().strip()` 的 SHA-256。印刷页码不是物理页，不能互相替代。缺任一绑定的候选不是已观察事实。
6. 具名投入只来自报告原文。不从航空制造常识补材料。能源、销售对象、存货金额和泛称“直接材料”本身不是具名投入。重组前与重组后的业务事实保持分开。`legal_empty` 只能包裹一个覆盖状态，不能代替 `not_disclosed`、`not_applicable`、`unclear` 或 `extraction_failed`，也不能伪装成已观察事实。
7. 2.2 在 dossier 审核之后只写一种结果。适合：允许以后另立独立的四报告 material-input successor 范围 change，本卡仍不实现、不 replay。不适合：记录原因并停止，aggregate 继续 hold，不从清单外补发行人。1.1 和 2.1 都不预填这个结果。

## Risks / Trade-offs

- [把 regime 基线排除当成材料投入不适合] → 本卡要求新 dossier，并禁止复用 regime 文件和 regime 样本身份作为结论。
- [dossier 适合后顺手创建 replay] → 适合只授权以后另立范围 change。本卡不写实现、不入队、不 replay。
- [用其他章节的四报告结果填补材料投入] → 其他章节的页码和 gate 不能移入本 dossier，也不能改写 MI-2。
- [为了凑齐四报告而补发行人] → 不适合时停止。清单外发行人不能进入。

## Migration Plan

无部署。1.1 通过前只保留本 change 的范围文档。2.1 才新增 `dossier.md`。历史 replay、source-review、账本和 aggregate 合同保持原字节。

## Open Questions

`302132.SZ` 是否适合 `extract_material_inputs` 尚未判断。该判断留到 2.2，不能在本设计中预填。
