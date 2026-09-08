# Closure baseline

- Authoritative input run: `stage55-policy-four-20260907-a`
- Bundle path: `var/company_profile_stage5/20260907/run-stage55-policy-four-20260907-a`
- Bundle status: overall `failed`; production authorization `not_authorized`
- Historical-bundle rule: immutable input for diagnosis only; no record, disposition, coverage, projection, or Benchmark result may be rewritten or copied into a new run.

## Closure scopes

1. `manufacturing-materials-300750-2025 / segment_product_industry_region`
   - Extract completed, independent verify failed at the provider boundary.
   - Required segment coverage remained incomplete.
   - Closure action: rerun the scope under a new run ID; no semantic contract change.
2. `manufacturing-materials-300750-2025 / top_five_customer_rows`
   - Five ranked `report_local_anonymous` Relationships and their concentration Measurements were accepted.
   - One additional `前五名客户` aggregate Relationship was correctly blocked with only `object_not_allowed`.
   - The blocked supplemental candidate incorrectly downgraded `counterparty_relationship` coverage to `unclear` and made the task incomplete.
3. `manufacturing-materials-603659-2025 / segment_product_and_adjustment`
   - `合并抵消项` Segment, operating revenue, and operating cost preserved `row_class=consolidation_adjustment`, `subject_scope=consolidated_group`, and `subject_basis=direct_source_wording`.
   - Independent verify blocked those three records only with `subject_unsupported` despite the explicit source-native adjustment row label.

## Frozen boundaries

- `production_authorization=not_authorized` remains mandatory.
- No stage-six reset, legacy backfill, approved-table write, CommodityExposure, ValueChainRole, DCF, scheduler, API, Telegram, PDF/OCR, or LLM-gateway change is authorized.
- Gold remains post-run evaluation-only and is not runtime input.
- Live execution budget is one three-scope preflight and, only if it passes, one complete four-report run.
