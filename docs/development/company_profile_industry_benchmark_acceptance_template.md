# 公司画像行业 Benchmark 验收报告模板

> 文档类型 / artifact type：`company_profile_industry_benchmark_acceptance_template`
> 模板版本：`company_profile_industry_benchmark_acceptance.v1`
> 状态 / review status：阶段 2 current / `approved`
> 行业边界：实例化时必填；模板本身不包含具体行业结论
> owner：公司画像需求治理
> reviewer：阶段 2 OpenSpec 合同审核
> 上位需求：`company_profile_product_and_industry_semantic_requirements.md`

## 0. 验收身份

记录 industry package/boundary version、industry requirements、sample manifest、gold annotation、受验版本、owner/reviewer/date、`pass|hold`。阶段 2/3 的 production authorization 固定为 `not_authorized`。

## 1. 样本覆盖

| 维度 | 目标 | 实际 | 未覆盖边界 | 阻塞 |
|---|---:|---:|---|---|
| 报告数 | `>=3` | `<n>` | `<gap>` | `yes/no` |
| 公司数 | `>=2` | `<n>` | `<gap>` | `yes/no` |
| focus 外挑战报告 | `>=2` | `<n>` | `<gap>` | `yes/no` |
| 交易所/披露模板 | `<target>` | `<actual>` | `<gap>` | `yes/no` |
| business regime | `<target>` | `<actual>` | `<gap>` | `yes/no` |

不得用低相关公司填充交易所覆盖；无代表样本时引用 `coverage_gap`。

## 2. 字段级指标

阈值由行业 requirements 基于样本风险填写，不设跨行业统一总分。

| field_id | level | samples | required coverage | value correct | unit correct | subject correct | period correct | evidence | legal empty | failure honest | prohibited inference | result |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `<field>` | `<level>` | `<n>` | `<ratio>` | `<ratio>` | `<ratio>` | `<ratio>` | `<ratio>` | `<ratio>` | `<ratio>` | `<ratio>` | `<n>` | `pass/hold` |

## 3. 章节任务指标

| chapter_family | task | requested | observed | not_disclosed | not_applicable | extraction_failed | unclear | dropped required | result |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| `<family>` | `<task>` | `<n>` | `<n>` | `<n>` | `<n>` | `<n>` | `<n>` | `<n>` | `pass/hold` |

## 4. Legal empty 与失败诚实性

核对未披露只在目标章节完整读取后使用；不适用由 checklist 决定；required 页不可读/超预算/OCR/表格失败为 `extraction_failed`；主体/期间/单位不能唯一化为 `unclear`；聚合源不覆盖正式报告失败；不存在空数组成功。

## 5. Blocking failures

以下任一非零即 `hold`，不受平均分影响：

| blocker | count | affected ids | owner | exit condition |
|---|---:|---|---|---|
| required chapter/table silently omitted | `<n>` | `<ids>` | `<owner>` | `<condition>` |
| fact/derivation/assumption confused | `<n>` | `<ids>` | `<owner>` | `<condition>` |
| unsupported/checklist-external inference | `<n>` | `<ids>` | `<owner>` | `<condition>` |
| source unit/value overwritten | `<n>` | `<ids>` | `<owner>` | `<condition>` |
| subject/period forced | `<n>` | `<ids>` | `<owner>` | `<condition>` |
| LLM/research prose introduced new fact | `<n>` | `<ids>` | `<owner>` | `<condition>` |
| extraction failure relabelled empty success | `<n>` | `<ids>` | `<owner>` | `<condition>` |

## 6. Reviewer 分歧

| annotation_id | topic | annotator | reviewer | evidence | disposition | blocking |
|---|---|---|---|---|---|---|
| `<id>` | `<topic>` | `<position>` | `<position>` | `<anchors>` | `resolved/unclear` | `yes/no` |

未解决的主体、单位、字段或 coverage 分歧保持 `unclear`。

## 7. 未覆盖边界

逐项引用 manifest 的 `coverage_gap`，说明受影响 requirement、风险、补样触发条件，以及为何允许 pass 或必须 hold。不得只报告成功样本。

## 8. 结论与签字

记录 artifact completeness、terminology consistency、sample sufficiency、annotation review、field/chapter acceptance、blocking count、final `pass/hold`、下一步 `stage_3_research_only|remediation`。明确不授权生产 schema/prompt/selector/writer/resolver、LLM 执行、数据库迁移或生产启用。由 research owner、independent reviewer、acceptance reviewer 签字。
