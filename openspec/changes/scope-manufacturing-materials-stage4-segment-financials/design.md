## Context

已批准制造/材料清单只有四份 2025 年报：`300750.SZ`、`603659.SH`、`920015.BJ`、`302132.SZ`。现有章节 `extract_segment_financials` 已经区分 `segment_dimension`、`operating_revenue`、`operating_cost` 和 `gross_margin_reported`。字段账本已规定合并抵消项使用 `row_class=consolidation_adjustment`，行上的收入、成本和毛利率仍单独保存并继承该标记。

客户供应商仍是 conditional 增强。本竖切不提前打开它。

## Goals / Non-Goals

**Goals:**

- 把分部财务最小竖切写成可审核合同。1.1 已通过独立范围审核；2.1 dossier 仍未开始。
- 只复用 `extract_segment_financials`。
- 定义样本至少三份，覆盖 SZSE、SSE、BSE，且只来自已批准清单。
- 先写 dossier，再标字段义务和空值状态。
- 同一正式表内分开维度、收入、成本、报告毛利率和合并抵消。

**Non-Goals:**

- 1.1 通过前不写 dossier，不改 Python，不入队，不 replay。
- 不从收入和成本重算毛利率。
- 不把金额表收成 Activity，不用管理层摘要替代正式表。
- 不把「公司」默认提升为合并集团。
- 不启用客户供应商、regime、六章包、DCF、交易、价格敏感性或旧 writer。
- 不进入 common-core production，不改 identity、publication、closure、mode、checkpoint 或既有 replay。
- 不预填指标，不授权扩大、规模质量或生产。

## Decisions

1. **只打开现有分部财务章节。**
   不新建章节，不把分部财务和客户供应商捆在一起。客户供应商留在本竖切之后。

2. **样本合同沿用已批准四份报告。**
   三份稳定报告已覆盖三个交易所。`302132.SZ` 是清单内唯一重组样本，只能在新的分部财务 dossier 确认适合后进入。旧 regime dossier 不能代替这份 dossier。不适合就停，不从清单外补发行人。

3. **dossier 先于字段义务。**
   每份报告先记录正式表的维度、收入、成本、毛利率和抵消项，再决定 required、conditional、optional 和四种 coverage 状态。页码和分部名只留在 dossier，不进入抽取规则。

4. **金额列保持金额，毛利率只用报告值。**
   营业收入和营业成本是金额 Measurement。毛利率只有来源直接给出百分数时才是 `gross_margin_reported`。没有毛利率时记 `not_disclosed` 或 dossier 确认的其他 coverage 状态，不用 `(收入-成本)/收入` 补算。合并抵消项是 `consolidation_adjustment`，不是普通分部，也不是「其他」合计。

5. **叙述不能替代表格。**
   管理层讨论里的收入占比或毛利变化，只有在正式表同一单元格能锚定时才可作脚注。单独的摘要句不是分部财务事实。

6. **研究隔离。**
   通过后的输出只到新的隔离目录，`accepted_for_review`，provider 调用保持由后续实现卡约束。identity 保持 `owned_page_facts=v8` 加 `material_input_facts=v1`。`scale_quality_claim_allowed` 保持 false，`production_authorization` 保持 `not_authorized`。

## Risks / Trade-offs

- [只用两家交易所] → 定义样本必须同时出现 SZSE、SSE、BSE。
- [302132 的重组披露没有可用分部表] → dossier 不确认就不纳入，且不另找清单外报告。
- [用收入减成本算出毛利率] → 只接受报告直接披露的毛利率。
- [把抵消项当成一个产品分部] → 抵消项保留 `consolidation_adjustment`。
- [把摘要叙述当成表格证据] → 没有正式表锚点就不是本切片的事实。
- [局部 38/38 被当成生产授权] → 产销量和材料投入的历史 gate 保持独立，本 change 不预填指标。

## Migration Plan

1. 1.1 只审范围。通过前不写 dossier，不实现。
2. 1.1 通过后先确认 `302132.SZ` 的分部财务 dossier；不适合则定义样本不含它。
3. 再写其余 dossier，并确认至少两种披露形态。
4. dossier 审核通过后才做最小实现、受控 replay 和独立 source-review。
5. 回滚本 change 只删除这份范围文档；不删除已有 replay，不改控制面。

## Open Questions

- 无。1.1 审核前不把 `302132.SZ` 预定为已经适合，也不预设分部财务召回或 gate。
