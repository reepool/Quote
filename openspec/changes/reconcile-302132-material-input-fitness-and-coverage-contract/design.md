## Context

`openspec/changes/archive/2026-09-30-assess-302132-material-input-four-report-successor-scope/design.md` 的 Fitness 把 `302132.SZ` 记为 unsuitable，因为 dossier 没有可绑定的具名材料投入事实。该 dossier 本身把采购模式、成本构成、关联采购和供应商合计记为 `not_disclosed` 或 `not_applicable`，没有把泛称、能源、存货金额或“采购商品”写成 material_input fact。

上位需求 `docs/development/company_profile_manufacturing_materials_requirements.md` 的材料投入行写明：未具名可 `not_disclosed`，不允许从行业常识补原料。coverage 枚举包含 `observed`、`not_disclosed`、`not_applicable`、`extraction_failed`、`unclear`。`openspec/specs/manufacturing-materials-stage4-minimum-slice/spec.md` 写明：合法未披露记为 legal-empty 或 not disclosed；缺页、表头、单位或证据绑定才是 extraction failure；`required` 是必须检查，不是保证披露。同一 spec 也写明，原三份报告被选为最小切片，是因为它们各自有公司自有的具名投入句。

这两类句子现在同时存在。本 change 只决定哪一种定义约束 `302132.SZ` 能否成为第四个材料投入样本，不改任何已归档字节。

## Goals / Non-Goals

**Goals:**

- 冻结对照范围和两份不得改写的历史文本。
- 把“至少一条具名 fact”和“完整审核后的合法 coverage”写成两个可选择的 suitable 定义。
- 规定 2.2 只能选择其中一个，并写明该选择对 successor 和 aggregate hold 的后果。

**Non-Goals:**

- 不在 1.1 或 2.1 预选保留 unsuitable 或改为 coverage-only suitable。
- 不改已归档 assessment、dossier、MI-1、MI-2、aggregate ledger、replay 或 source-review。
- 不把 `302132.SZ` 补入 MI-2，不重开 aggregate gate。
- 不创建 successor scope、Python 改动、入队或 replay。
- 不启动 restricted-promotion、六章包、规模质量或生产。
- 不改 publication、closure、mode、identity、checkpoint。

## Decisions

1. 对照只读这些材料：
   - `docs/development/company_profile_manufacturing_materials_requirements.md` 的材料投入行、coverage 枚举，以及 `required` 表示必须检查的相关句子
   - `openspec/specs/manufacturing-materials-stage4-minimum-slice/spec.md` 的材料投入验收和 legal non-disclosure 要求
   - `openspec/changes/archive/2026-09-30-assess-302132-material-input-four-report-successor-scope/dossier.md`
   - 同一归档中的 proposal、design、spec、tasks，尤其是 design.md 的 Fitness
2. 已归档 dossier 的页码、引文、hash 和 coverage 保持为证据。本 change 不重读年报，也不把 `not_disclosed` 改写成 observed fact。Fitness 中“没有具名事实，所以 unsuitable”是待裁决的推断，不是本卡已经接受的终局。
3. 2.1 只并列两种定义，不选择：
   - 定义一：suitable 要求该报告至少产生一个可绑定的具名 material_input fact。没有该事实就是样本不适合。
   - 定义二：suitable 只要求该报告被完整审核，并对无具名材料给出有证据的 `not_disclosed` 或 `not_applicable`。`not_disclosed` 不生成事实，但仍是合法 coverage。
4. 2.1 必须单独记下这个判断点：采购模式、成本构成、关联采购和供应商合计已经读完，且没有具名材料。这究竟是“章节不可评估”，还是“该章节合法未披露”。1.1 和 2.1 都不回答它。
5. 2.2 只写一种结果。
   - 保留 unsuitable：说明 aggregate 样本门槛为何要求每份报告都有具名材料事实，并说明该要求为何不违反上位 `not_disclosed` 合同。Stage 4 继续 hold，不开 successor。
   - 纠正为 coverage-only suitable：不改旧 dossier 和历史 replay。只记录旧评估把合法 coverage 误当成样本不适合，并授权以后另立独立的四报告材料投入 successor 范围卡。本卡仍不实现、不入队、不 replay。
6. 只有 2.2 明确选择 coverage-only suitable 之后，下一张才可以是 successor scope。选择保留 unsuitable 时，阶段 4 继续 hold。3.1 只归档本 reconciliation，不在归档时另创 successor。

## Risks / Trade-offs

- [用本卡直接改写已归档 unsuitable] → 历史归档、dossier 和 replay 保持原字节。新结论只写在本 change。
- [把合法 coverage 再判成抽取失败] → 对照必须保留上位合同的 `not_disclosed` 与 `extraction_failed` 边界。
- [2.2 尚未选择就创建 successor] → successor scope 只能出现在 coverage-only suitable 被明确写下之后，而且必须是另一张 change。
- [为了让四报告集成立而补发行人] → 两种结果都不授权清单外发行人，也不把 `302132.SZ` 补入 MI-2。

## Migration Plan

无部署。1.1 通过前只保留本 change 的范围文档。历史 assessment 继续停在已归档的 unsuitable，直到 2.2 另写结论。

## Open Questions

`302132.SZ` 的完整 `not_disclosed` 是样本不适合，还是合法的 coverage-only 样本，留到 2.2。本设计不预填。
