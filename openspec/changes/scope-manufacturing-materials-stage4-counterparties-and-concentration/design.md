## Context

`hold-stage4-expansion-and-review-next-scope` 的 dossier 已确认四份已批准 2025 年报都能绑定 `extract_counterparties_and_concentration`，并覆盖 SZSE、SSE、BSE 和至少三种披露形态。本 change 只把该阅读收成实现范围。1.1 通过前不重读年报、不重算指标、不写代码。

已有章节入口在研究工作流和 Stage 5 中。实现必须复用该入口，不另建平行抽取器。输出留在研究隔离目录，disposition 为 `accepted_for_review`。

## Goals / Non-Goals

**Goals:**

- 固定四份已批准年报和三种披露形态。
- 把客户关系、供应商关系、集中度、匿名身份、关联方行和合法空值写成可审核的边界。
- 保持控制面为 `not_authorized`，并且不产生 aggregate stage-4 gate。

**Non-Goals:**

- 1.1 通过前不实现、不入队、不 replay、不预填指标。
- 不启用业务概览、经营体制、六章包、DCF、交易或价格敏感性。
- 不把「公司」提升为合并主体。
- 不从相关交易、其他应收款或信用风险表回填前五名名称。
- 不新增发行人，不改 identity、publication、closure、mode 或 checkpoint。

## Decisions

1. 样本就是 dossier 已核对的四份报告。定义样本已覆盖三个交易所和三种披露形态，本 change 不再扩大清单。
2. 匿名身份的范围是本报告、关系类型和排名或标签。customer 的「第一名」与 supplier 的「第一名」是两条。`客户 A(1)` 与客户「第一名」金额同为 58,159,202 千元时仍是两条，因为原文没有写明同一当事人。
3. 前五名合计金额、占比，以及关联方占年度总额的比例，都是 concentration Measurement。只有具体交易对手行或重大合同当事人行才形成 Relationship candidate。合计行不生成 Relationship。
4. 只披露合计的章节，名称覆盖为 `not_disclosed`。原文「不适用」的合同或明细表为 `not_applicable`。证据在但主语、单位、期间或表头不能唯一确定时为 `unclear`。页、表头、单位或续表无法绑定时为 `extraction_failed`。
5. 920015 供应商合计保留印刷 token `387,793,549.7`。实现不得补位。
6. 证券代码、物理页和当事人标签只出现在 fixture 与观察记录中。抽取规则使用表头、关系类型、匿名标签结构和 coverage 语义，不写死公司、页码或产品名。
7. 后续研究 bundle 禁止写入 recall、accuracy、critical numeric errors、`expansion_gates_met` 和 source-review。这些只能由通过后的独立复述产生。

## Risks / Trade-offs

- [把 dossier 阅读当成 gate true] → 本 change 不填写指标，1.1 只审核范围。
- [金额相同就合并合同行和排名行] → 合同要求原文写明同一性，金额相同不足。
- [用关联交易表补上未列名的前五名] → 前五名 name coverage 保持 `not_disclosed`，其他表的关系即使存在也独立，并且不在本章回填。
- [实现滑进 common-core 或六章包] → 输出只限研究隔离目录和 `accepted_for_review`。
