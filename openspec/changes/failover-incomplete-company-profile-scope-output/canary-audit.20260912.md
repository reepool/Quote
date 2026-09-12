# Company-profile completeness canary audit

Date: 2026-09-12

## Decision

The unseen `600426.SH` annual-report canary completed once through the current
four-member `semantic_extraction` pool. The immutable report result is `hold`,
not an execution failure. Eight of nine scopes completed; the segment-financial
scope remained incomplete after a partition reconciliation conflict.

This result closes the canary honestly. It does not authorize a targeted rerun,
historical splice, production write, Stage 6, approved-table publication,
scheduler/backfill, commodity-exposure publication, value-chain publication, or
DCF use. `production_authorization=not_authorized` remains authoritative.

## Frozen inputs

- Company: 山东华鲁恒升化工股份有限公司 (`600426.SH`)
- Report period: `2025-12-31`
- PDF: `data/filings/announcements/blobs/22/22e75dc409e246b252871345de029e8aebadb74875d9e445025b7ea20ad469ad.pdf`
- PDF SHA-256: `22e75dc409e246b252871345de029e8aebadb74875d9e445025b7ea20ad469ad`
- PDF size/pages: `1,399,477` bytes / `184` physical pages
- Canary manifest SHA-256: `342ecc8bfd624296ce7e3c2035632ef38f43fe83f462df59dc72927c38fbdd4f`
- Evidence plan SHA-256: `6b32ac3a1438a4c36680f58d20bf75921cf01c25d4c3dade807816c6ff4c809b`
- Preparation receipt SHA-256: `e57b4b9d82a554f5512219dd4c6a6e1c6684f0a191117e26ba12d8a74e6123f8`
- Preparation result: six chapters, nine scopes, provider calls `0`

The Evidence plan contains no Gold answer, `energy_input`, expected value,
subject answer, or model-specific instruction. Page 27 steam disclosure remains
under the existing contract, where `material_input` governs explicitly named raw
material or energy inputs; no new semantic field was added.

## Immutable run

- Run ID: `stage55-completeness-canary-600426-20260912-a`
- Run directory: `var/company_profile_stage5/20260912/run-stage55-completeness-canary-600426-20260912-a`
- Run manifest SHA-256: `104675c507ea2756adc444e3a926aee3a01547869d5eecb158d29993452371d5`
- Report SHA-256: `8b691065997f710899cb448f73b2058c1bfdc9e809a164f390f9be6a1fec5d4e`
- Report status: `hold`
- Benchmark decision: `hold`
- Stage traces: `19`, all final status `success`
- Gateway observation: one GLM `response_parse_error` used the existing single
  same-source schema/parse repair and then succeeded. No failed response was
  spliced into the result.
- Accepted dispositions: `50`
- Blocked dispositions: `2`
- Semantic records before disposition filtering: `52`
- Human-review items: `7` (`5` segment-derived, `2` counterparty candidates)
- Subject scope: all `52` produced records use
  `consolidated_group/report_default_group_scope`; no issuer or group wording was
  invented, and no named subsidiary/segment was widened.

The earlier sandbox attempt was interrupted before any bundle or temporary
bundle directory was written after Scorpio and ZAI both reported DNS failures.
The authorized out-of-sandbox execution used the same frozen inputs, run ID,
route, timeout, and dynamic token policy. Scorpio, ZAI, and DeepSeek transmission
was authorized by the user.

## Pool and token observations

| Model | Stage traces | Input tokens | Output tokens | Trace latency |
|---|---:|---:|---:|---:|
| `gemini-3.8-flash-high` | 5 | 28,264 | 18,907 | 62.502 s |
| `glm-5.3-flash` | 5 | 18,790 | 21,166 | 407.074 s |
| `grok-4.6` | 5 | 42,350 | 35,397 | 518.829 s |
| `deepseek-flash` | 4 | 11,504 | 18,008 | 74.411 s |
| Total | 19 | 100,908 | 93,478 | 1,062.816 s |

All nine scopes evaluated to the existing dynamic-token `base` tier:
`20,000` extract and `18,000` verify tokens. Observed outputs remained below the
selected caps. This canary therefore provides no evidence for raising the base
budget; the dynamic threshold remains available for materially larger scopes.

## Chapter results

| Chapter/scope | Result | Records | Review | Notes |
|---|---|---:|---:|---|
| overview / `business_overview` | complete | 5 | 0 | overview and four activities accepted |
| segment / `segment_industry_product_region_mode` | incomplete | 0 | 5 | partition reconciliation conflict |
| quantities / `production_sales_inventory_table` | complete | 12 | 0 | production, sales, inventory accepted |
| quantities / `capacity_table_and_changes` | complete | 19 | 0 | capacity and utilization accepted |
| inputs / `product_upstream_materials` | complete | 4 | 0 | source-native inputs accepted |
| inputs / `raw_material_procurement_and_consumption` | complete | 4 | 0 | source-native inputs accepted |
| counterparties / `customer_supplier_concentration` | complete | 8 | 2 | two misrouted customer-share candidates blocked |
| regime / `reported_business_change` | complete | 0 | 0 | explicit `not_applicable` |
| regime / `operating_model_change` | complete | 0 | 0 | explicit `not_applicable` |

## Exact hold blocker

The segment table is physically present on pages 10-11 and directly discloses
`营业收入`, `营业成本`, and `毛利率` by industry, product, region, and sales
mode. Each of the three metric partitions returned schema-valid rows, but row
reconciliation stopped on:

> segment partition row identity conflict: subject_scope: 'consolidated_group' != 'unclear'

The source table includes, for example:

> 化工 23,147,278,645.59 19,630,885,312.74 15.19

> 新能源新材料相关产品 15,557,084,444.74 13,890,509,743.36 10.71

This is an adapter reconciliation bug between the report-default group convention
and partition-local normalization, not missing disclosure and not provider
unavailability. It is the only report-level hold blocker and should be the first
P1 correction in the next change. The immutable canary is not rewritten or rerun.

## Defended semantic errors

Two candidates were correctly blocked as `object_not_allowed`. Both used
`field_id=supplier_concentration` while their measured objects were customer
facts (`前五名客户` and `前五名客户中的关联方`). The source page 13 states:

> 前五名客户销售额440,689.86万元，占年度销售总额12.70%；其中前五名客户销售额中关联方销售额0万元，占年度销售总额0%。

The verifier preserved the customer/supplier boundary instead of accepting the
misclassification. Other customer and supplier concentration records were
accepted, and totals-only disclosure did not create named Relationships.

## Research-only output boundary

The research view remains `data_status=research_fixture` and contains Evidence-
linked overview, activities, operating quantities, capacity, disclosed inputs,
and concentration facts. Segment facts are absent because the owning scope did
not complete. `commodity_exposure.status=not_assessed` and
`value_chain_position.status=insufficient_evidence`; neither is published.

The authoritative four-report manifest remains unchanged at
`00fa20045d4df037a8984cff4b56af3f19ac533f4c4b9e311e7f130bf5158687`.

## Next bounded work

1. Normalize report-default group scope consistently before segment partition
   row identity comparison.
2. Persist routed attempt/failover summaries into future Stage 5 traces; the
   common gateway already owns the attempt lineage, while this immutable bundle
   retains only the final successful route trace.
3. Add a focused regression for customer metrics emitted under the supplier
   field, without changing the current verifier decision.

These are follow-up implementation items. They do not authorize a second canary
run under this change or any production activation.
