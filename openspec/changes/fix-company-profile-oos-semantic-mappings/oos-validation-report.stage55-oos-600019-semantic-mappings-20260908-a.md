# BaoSteel OOS semantic-mapping validation

Date: 2026-09-08

## Execution

- Run ID: `stage55-oos-600019-semantic-mappings-20260908-a`
- Sample: `manufacturing-materials-oos-600019-2025` (BaoSteel, 600019.SH, 2025 annual report)
- Route: `semantic_extraction__scorpio_grok` (`grok-4.6`)
- Parameters: extract/repair 20,000 output tokens; verify 18,000; request deadline 300 seconds; provider-call ceiling 27
- Provider calls: 18/18 successful (9 extract + 9 verify), no repair, no retry, no DNS/transport/deadline/schema failure in the authorized run
- Report status: `usable_with_caveats`; report benchmark: `pass`; overall run status: `hold`
- Accepted-for-review records: 171
- Production authorization: `not_authorized`
- Manifest SHA-256: `a396ecd341c6f50c9999c4127137b93811fe799150925bbd42099cffb850a9bb`
- Report SHA-256: `c0b814cab8c98f00c69e4bd73736b0a0522cfcb33cfd88193389f1cf0590214f`

The first sandbox attempt with this run ID was stopped after two connect-stage DNS failures and produced no bundle. The authorized external-network execution above is the sole committed run for this ID.

## Four frozen semantic targets

1. **Equal utilization rows**: `铁` and `坯材` at 96% are both accepted. Each has a table Evidence anchor with its own `row_label`; their occurrence IDs are distinct.
2. **Supplier direction**: supplier purchase amount/share records, including 951.7 and 652.2 亿元 where present, are bound to `supplier_concentration`, not `customer_concentration`.
3. **Related-party scope**: the related-party scope contains 15 accepted Relationship records and zero Measurement records. Transaction amounts and service/purchase ratios are not relabelled as concentration.
4. **Inter-segment elimination**: the `分部间抵消` adjustment row retains revenue/cost values and `row_class=consolidation_adjustment`, but its `subject_scope` is `unclear`; it is not promoted to `consolidated_group`.

## Scope closure

All nine prepared scopes have `task_complete=true`; the report benchmark has no task-completion, chapter, subject-promotion, production-isolation, or provider-call blockers. Scope summary:

```json
[
  {
    "scope_id": "business_overview",
    "task_complete": true,
    "record_count": 6,
    "accepted_count": 6,
    "provider_calls": [
      "extract",
      "verify"
    ],
    "coverage": [
      {
        "field_id": "business_overview_source",
        "status": "observed",
        "reason_code": null
      },
      {
        "field_id": "explicit_activity",
        "status": "observed",
        "reason_code": null
      }
    ]
  },
  {
    "scope_id": "segment_industry_product_region_mode",
    "task_complete": true,
    "record_count": 55,
    "accepted_count": 55,
    "provider_calls": [
      "extract",
      "verify"
    ],
    "coverage": [
      {
        "field_id": "segment_dimension",
        "status": "observed",
        "reason_code": null
      },
      {
        "field_id": "operating_revenue",
        "status": "observed",
        "reason_code": null
      },
      {
        "field_id": "operating_cost",
        "status": "observed",
        "reason_code": null
      },
      {
        "field_id": "gross_margin_reported",
        "status": "observed",
        "reason_code": null
      }
    ]
  },
  {
    "scope_id": "product_volume_table",
    "task_complete": true,
    "record_count": 21,
    "accepted_count": 21,
    "provider_calls": [
      "extract",
      "verify"
    ],
    "coverage": [
      {
        "field_id": "production_volume",
        "status": "observed",
        "reason_code": null
      },
      {
        "field_id": "sales_volume",
        "status": "observed",
        "reason_code": null
      },
      {
        "field_id": "inventory_volume",
        "status": "observed",
        "reason_code": null
      }
    ]
  },
  {
    "scope_id": "capacity_and_steel_process_tables",
    "task_complete": true,
    "record_count": 21,
    "accepted_count": 21,
    "provider_calls": [
      "extract",
      "verify"
    ],
    "coverage": [
      {
        "field_id": "production_capacity",
        "status": "observed",
        "reason_code": null
      },
      {
        "field_id": "production_volume",
        "status": "observed",
        "reason_code": null
      },
      {
        "field_id": "sales_volume",
        "status": "observed",
        "reason_code": null
      },
      {
        "field_id": "capacity_utilization",
        "status": "observed",
        "reason_code": null
      }
    ]
  },
  {
    "scope_id": "material_energy_narrative",
    "task_complete": true,
    "record_count": 1,
    "accepted_count": 1,
    "provider_calls": [
      "extract",
      "verify"
    ],
    "coverage": [
      {
        "field_id": "material_input",
        "status": "observed",
        "reason_code": null
      }
    ]
  },
  {
    "scope_id": "material_supply_table",
    "task_complete": true,
    "record_count": 2,
    "accepted_count": 2,
    "provider_calls": [
      "extract",
      "verify"
    ],
    "coverage": [
      {
        "field_id": "material_input",
        "status": "observed",
        "reason_code": null
      }
    ]
  },
  {
    "scope_id": "top_five_totals_and_legal_empty_names",
    "task_complete": true,
    "record_count": 8,
    "accepted_count": 6,
    "provider_calls": [
      "extract",
      "verify"
    ],
    "coverage": [
      {
        "field_id": "counterparty_relationship",
        "status": "not_disclosed",
        "reason_code": "source_reason_unspecified"
      },
      {
        "field_id": "customer_concentration",
        "status": "observed",
        "reason_code": null
      },
      {
        "field_id": "supplier_concentration",
        "status": "observed",
        "reason_code": null
      }
    ]
  },
  {
    "scope_id": "related_party_sales_purchases_and_services",
    "task_complete": true,
    "record_count": 15,
    "accepted_count": 15,
    "provider_calls": [
      "extract",
      "verify"
    ],
    "coverage": [
      {
        "field_id": "counterparty_relationship",
        "status": "observed",
        "reason_code": null
      }
    ]
  },
  {
    "scope_id": "business_change_and_consolidation_scope",
    "task_complete": true,
    "record_count": 8,
    "accepted_count": 8,
    "provider_calls": [
      "extract",
      "verify"
    ],
    "coverage": [
      {
        "field_id": "business_regime",
        "status": "observed",
        "reason_code": null
      }
    ]
  }
]
```

## Benchmark boundary

The repository's 24-annotation post-run evaluator is intentionally not used as this run's result: those annotations belong to the four-report manufacturing/materials authority and do not contain a BaoSteel OOS sample. Running it against this one-sample bundle would report the other 23 samples as `sample missing`, which is not a semantic failure of this run. This change therefore records the focused OOS acceptance above and does not claim a new four-report Gold score.

The historical four-report bundles and Gold baseline remain immutable. Stage 6, approved tables, legacy backfill, commodity exposure, value-chain publication, and DCF remain closed.
