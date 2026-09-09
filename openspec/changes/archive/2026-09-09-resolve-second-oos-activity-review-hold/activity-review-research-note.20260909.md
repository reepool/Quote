# 中南钢铁 Activity 离线人工裁决说明

## 结论

三条 Activity 已依据同一份不可变 Evidence 接受为研究事实。报告状态由 `hold` 重新派生为 `usable_with_caveats`；这不是新的 LLM 运行，也不是生产批准。

## 原文与 Evidence

- 样本：`000717.SZ` 广东中南钢铁股份有限公司 2025 年年度报告
- Evidence：`stage5-evidence-37a588de249dec04022042e7`，物理页 10
- 原文：公司主营范围包括制造、加工、销售钢铁冶金产品、金属制品、焦炭、煤化工产品（危险化学品除外）、技术开发、转让、引进及咨询服务。

该句中“公司”是并列动词“制造、加工、销售”的直接语法主语。因此仅把三条候选的 `actor_basis` 从 `explicit_economic_relationship` 修正为 `direct_grammatical_actor`；`activity_actor` 与 `source_actor` 均保持“公司”，`subject_scope` 继续为 `unclear`。

## 裁决

| Review ID | Activity | actor_basis | subject_scope |
| --- | --- | --- | --- |
| `review:stage5-1a520da8e1ed124293c09c05` | 制造 / produces | `direct_grammatical_actor` | `unclear` |
| `review:stage5-aacb6456d075ec3f9b7834ce` | 加工 / processes | `direct_grammatical_actor` | `unclear` |
| `review:stage5-04b80062ff6f7104b26cdb21` | 销售 / sells | `direct_grammatical_actor` | `unclear` |

原先的第四项 `explicit_activity / required_coverage_missing` 是派生项；三条候选接受后，`explicit_activity` coverage 变为 `observed`，该派生项随现有状态计算关闭。

## 结果核验

- 源 run：`stage55-second-oos-completion-000717-20260909-c`
- 源 manifest SHA-256：`bdb417e3cff651e47b0c19e5cc8ee3beb61ce64b7eeb1fd7bbf1608e55778de5`
- 源 report SHA-256：`ad101238d795eca0acb29330ba5107d4f5776f5781675c1e78182a6b4e9027dc`
- 派生结果 SHA-256：`df33ff142fc5cddd9f285dae8397224247a99daa677987b6bc567d5479dac69c`
- accepted records：`71 → 74`
- remaining human review：`4 → 0`
- benchmark：`pass`
- provider calls：`0`
- report status：`hold → usable_with_caveats`
- production authorization：`not_authorized`

## 使用边界

该结果可作为带主体限定的研究画像使用；不得把“公司”升为 `issuer` 或 `consolidated_group`，不得写入 approved 表、scheduler/backfill、商品暴露、价值链、DCF 或 Stage 6。正式源 bundle 保持逐字节不变。
