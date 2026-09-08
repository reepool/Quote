# Immutable offline-evaluation baseline

The sole runtime input for this change is:

`var/company_profile_stage5/20260907/run-stage55-closure-four-20260907-a`

The authority bundle remains immutable. This change does not run an LLM, read or
re-plan PDF Evidence, merge another run, or overwrite the existing benchmark.

## Authority hashes

- `manifest.json`: `00fa20045d4df037a8984cff4b56af3f19ac533f4c4b9e311e7f130bf5158687`
- `post-run-benchmark.json`: `ce417e81039a863d0c9093fd2bd2fa2bb2e12d81cd2df0f440790210e6084188`

The baseline records `research_slice_status=research_slice_usable`, benchmark
`decision=hold`, Gold 14/24, and
`production_authorization=not_authorized`.

## Outcomes allowed to change in the new offline evaluation

- `mm-603659-adjustment-revenue`: same-year duration-period equivalence.
- `mm-603659-adjustment-cost`: same-year duration-period equivalence.
- `mm-920015-product-margin`: Evidence-supported non-group subject refinement.
- `mm-302132-regime-effective`: the single directional, same-anchor,
  `2025-01-06` event adjudication.

## Outcomes required to remain unresolved

- `mm-603659-adjustment-row`: missing required Segment structure.
- `mm-603659-adjustment-margin`: missing accepted runtime fact.
- `mm-920015-capacity-under-construction`: the immutable record lacks the
  expected-completion qualifier.
- `mm-300750-produces-battery-system`: source object and Gold object differ.
- `mm-302132-restated-2024-revenue`: the physical runtime and Gold anchors differ.
- `mm-302132-volume-not-applicable`: remains `gold_contract_conflict` under the
  frozen disclosure contract.

The maximum expected Gold result is therefore approximately 18/24. A lower
score is acceptable when the closed rules do not support a match; score chasing
does not authorize changing Gold expectations or runtime records.

## Explicitly excluded paths

Stage six, legacy reset/backfill, approved-table writes, CommodityExposure,
ValueChainRole, DCF, scheduler, API, Telegram, and all production publication
remain closed.
