# Offline evaluation audit — 2026-09-08

Evaluation identity:
`refine-period-event-semantics-20260908-a`

Runtime source:
`var/company_profile_stage5/20260907/run-stage55-closure-four-20260907-a`

## Immutability check

- input `manifest.json` SHA-256:
  `00fa20045d4df037a8984cff4b56af3f19ac533f4c4b9e311e7f130bf5158687`
- input `post-run-benchmark.json` SHA-256:
  `ce417e81039a863d0c9093fd2bd2fa2bb2e12d81cd2df0f440790210e6084188`
- both hashes remained unchanged after evaluation;
- no extract, repair, verify, PDF, Evidence-plan, network, or LLM operation ran;
- the new result is stored outside the authority bundle in
  `offline-evaluation.stage55-closure-four-20260907-a.20260908-a.json`.

## Result

- Gold: 18/24 passed
  - `exact_match`: 2
  - `semantic_match`: 13
  - `accepted_with_uncertainty`: 3
  - `failed`: 5
  - `gold_contract_conflict`: 1
- fixture guards: 4/4 passed
- real-report negative cases: 15 evaluated and 15 passed; 4 remained untriggered
- benchmark decision: `hold`
- research slice: `research_slice_usable`
- production authorization: `not_authorized`

## The only changed Gold outcomes

| Annotation | Previous | New | Closed rule | Runtime target |
|---|---|---|---|---|
| `mm-603659-adjustment-revenue` | failed | semantic_match | `same_year_duration_period_equivalence` | `stage5-289785b351dc9d8b25edf03c` |
| `mm-603659-adjustment-cost` | failed | semantic_match | `same_year_duration_period_equivalence` | `stage5-7aac1c285da07298187a8acc` |
| `mm-920015-product-margin` | failed | accepted_with_uncertainty | `supported_non_group_subject_refinement` | `stage5-fae607ec1a48c1e023e82c76` |
| `mm-302132-regime-effective` | failed | accepted_with_uncertainty | `chengfei_transfer_effective_directional_equivalence` | `stage5-3ced7c219a56fd3ef760566c` |

## Required unchanged outcomes

| Annotation | Retained outcome | Reason |
|---|---|---|
| `mm-300750-produces-battery-system` | failed | runtime and Gold source objects differ |
| `mm-603659-adjustment-row` | failed | required Segment structure is absent |
| `mm-603659-adjustment-margin` | failed | no accepted runtime margin record exists |
| `mm-920015-capacity-under-construction` | failed | immutable runtime qualifier lacks expected completion |
| `mm-302132-restated-2024-revenue` | failed | runtime and Gold physical anchors differ |
| `mm-302132-volume-not-applicable` | gold_contract_conflict | frozen disclosure policy remains `not_disclosed` |

This is an offline benchmark refinement, not another four-report run and not a
production approval. Stage six and all legacy publication paths remain closed.
