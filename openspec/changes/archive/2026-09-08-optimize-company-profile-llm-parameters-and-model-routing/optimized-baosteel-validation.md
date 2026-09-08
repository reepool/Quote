# Optimized BaoSteel out-of-sample validation

Date: 2026-09-08

## Execution result

- Run ID: `stage55-oos-600019-optimized-20260908-b`
- Input: frozen BaoSteel (`600019.SH`) 2025 annual report manifest and nine-scope Evidence plan.
- Concrete profile: `semantic_extraction__scorpio_grok` (`grok-4.6`).
- Provisional run parameters: 16,000 extract tokens, 12,000 verify tokens, 300-second request deadline, 27-call ceiling, no outer shell timeout.
- Result: atomic isolated bundle committed; report `hold`; overall `hold`; `production_authorization=not_authorized`.
- Calls: 18/18 success (9 extract + 9 verify), zero repair, zero retry, zero DNS/transport/deadline/schema-route failure.
- Provider latency total: 2,053,586 ms (about 34 minutes 14 seconds); observed wall time about 34 minutes 16 seconds.
- Accepted for research review: 171 records (106 Measurement, 38 Relationship, 14 Segment, 8 BusinessEvent, 4 Activity, 1 BusinessOverview). Of these, 116 retain `subject_scope=unclear` and remain restricted from consolidated aggregation, ranking, and cross-company comparison.
- Manifest SHA-256: `05fc6c750c643bd333ae955f58fd09b53e17987b7ac3cd66ec8f408888a70df0`.
- Report SHA-256: `dd21f159599957f1f3059455aee908d7f1a4c3fba9eade93f44eb4db7198faef`.

## Parameter findings

Two calls returned valid locally validated content but carried `provider_output_budget_exceeded`:

- Related-party extract: requested 16,000, provider-reported output 16,487.
- Related-party verify: requested 12,000, provider-reported output 14,798.

This is not a transport failure, but it proves that the provisional caps lack safe measured headroom for Grok usage reporting. The final operator defaults are therefore 20,000 for extract/repair and 18,000 for verify. The 300-second deadline remains supported: the slowest successful call was about 247 seconds. A 180-second request timeout would have interrupted valid work, and the former 420-second shell wrapper would have terminated the complete workflow long before its bounded completion.

## Why the report remains hold

The report is not held because the LLM failed to return. Three scopes are incomplete due to semantic/adapter rules, and one accepted adjustment row triggers the subject-promotion gate. These are retained as out-of-sample findings; this parameter change does not rerun or relax them.

### HR-01: distinct capacity-utilization rows collide

- Classification: adapter/workflow occurrence mismatch.
- Physical page: 22.
- Evidence: `stage5-evidence-589613a9b93259643aadffb4`.
- Original table cells: `铁 5,036 4,847 96%`; `钢 5,473 5,078 93%`; `坯材 5,402 5,183 96%` under `产能（万吨） / 产量（万吨） / 产能利用率`.
- Runtime targets: `stage5-7a13989828e4009ca9f7f551` (铁, 96%) and `stage5-dafa122bb1b91478acf78f51` (坯材, 96%).
- Current result: both are unresolved as `occurrence_semantic_conflict`, which then leaves required `capacity_utilization` coverage missing.
- Assessment: the facts are distinct because `measured_object` differs. Equal value, period, metric, and Evidence page must not merge different row objects.
- Recommended later decision: include source row/measured object in this occurrence identity; do not change either source value.

### HR-02: top-five supplier totals are bound to the customer field

- Classification: model/adapter field-binding mismatch.
- Physical page: 16.
- Evidence: `stage5-evidence-79d16ebb69dca82870741c51`.
- Original text: `前五名客户销售额571.9亿元，占年度销售总额18.0%；其中...关联方销售额308.4亿元，占年度销售总额9.7%。前五名供应商采购额951.7亿元，占年度采购总额32.3%；其中...关联方采购额652.2亿元，占年度采购总额22.1%。`
- Runtime targets: `stage5-45b9550b6d0dce8d42139201` (前五名供应商采购额 951.7 亿元) and `stage5-e523f4e2d92a0fd418762f43` (关联方采购额 652.2 亿元).
- Current result: both carry `field_id=customer_concentration` with `metric_type=supplier_purchase_amount`, so the contract blocks them as `metric_not_allowed`; customer and supplier coverage remain incomplete despite six other accepted totals/shares.
- Recommended later decision: keep the supplier metrics, but bind them to the supplier field family; do not discard or relabel the source values as customer facts.

### HR-03: related-party transaction amounts exceed the concentration scope

- Classification: new disclosure form / request-contract boundary.
- Physical pages: 69-70.
- Evidence: `stage5-evidence-8b40ae5a46ecb8d40a491084` and `stage5-evidence-89b273876eafa658fc5c30e0`.
- Original examples: `欧冶云商股份有限公司 销售钢铁产品等 ... 11,260`；`宝武资源有限公司 采购原燃料 ... 17,286`；`采购商品小计 50,148`；`接受劳务小计 6,806`；`产品销售占本报告期营业收入的比例为7.5%，商品采购...17.0%，接受劳务...2.3%。`
- Current result: 42 records are accepted, mainly named related-party Relationships; 18 additional Measurements are blocked as `metric_not_allowed`, followed by missing customer/supplier concentration coverage. The blocked items are genuine related-party transaction amounts or service/purchase ratios, not top-five concentration facts.
- Recommended later decision: keep them blocked in this concentration scope. If these facts are desired, add a separate related-party transaction field family/change rather than widening customer concentration.

### HR-04: `分部间抵消` is not explicit group wording

- Classification: adapter subject-promotion mismatch.
- Physical page: 14.
- Evidence: the frozen segment table Evidence for `segment_industry_product_region_mode`.
- Original row: `分部间抵消 -213,151 -212,856 /` under `主营业务分行业情况`.
- Runtime targets: Segment `stage5-65844562a0bc2f0ac79f2858`, revenue `stage5-32f6bd02037b0a4178accf3b`, and cost `stage5-487e54d8e7db0d0388bac456`.
- Current result: the adapter gives `row_class=consolidation_adjustment`, `subject_scope=consolidated_group`, and `subject_basis=direct_source_wording`; the frozen subject gate correctly rejects the promotion because the cited wording says neither `合并` nor `本集团/集团`.
- Recommended later decision: retain the adjustment row and values, but keep subject scope `unclear` unless separate affirmative group Evidence exists. Do not weaken the illegal-group-promotion gate.

## Bounded conclusion

The optimized parameters and concrete Grok route complete the full LLM execution path reliably for this sample. The sample still remains `hold` because it exposed four semantic generalization issues outside parameter tuning. No additional LLM run is authorized by this change. Stage 6, approved tables, legacy backfill, CommodityExposure, ValueChainRole, and DCF remain closed.
