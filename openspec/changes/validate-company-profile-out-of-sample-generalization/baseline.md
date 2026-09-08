# Out-of-sample validation baseline

Frozen on 2026-09-08 before Evidence planning, extract, or verify execution.

## Immutable in-sample references

The completed four-report authority remains:

`var/company_profile_stage5/20260907/run-stage55-closure-four-20260907-a`

- `manifest.json` SHA-256:
  `00fa20045d4df037a8984cff4b56af3f19ac533f4c4b9e311e7f130bf5158687`
- `post-run-benchmark.json` SHA-256:
  `ce417e81039a863d0c9093fd2bd2fa2bb2e12d81cd2df0f440790210e6084188`
- Authority result: `research_slice_usable`
- Authority production boundary: `production_authorization=not_authorized`

The later offline Gold evaluation remains:

`openspec/changes/archive/2026-09-08-refine-company-profile-period-and-event-semantics/offline-evaluation.stage55-closure-four-20260907-a.20260908-a.json`

- SHA-256:
  `4151aed0bc75119c9d4c3338d699e5b2f4c8d30437b8e2796300d24efc1469aa`
- Result: Gold 18/24; 2 exact, 13 semantic, 3 accepted with uncertainty,
  5 failed, and 1 contract conflict
- Evaluation mode: offline only; the authority bundle was not modified

Neither reference is an input record source for the out-of-sample run. The new
run may consume the current semantic contracts and policy implementation, but
must not copy Evidence, candidates, dispositions, accepted records, Gold
annotations, fixture results, or benchmark outcomes from either reference.

## Frozen out-of-sample selection

The sole selected sample is Baoshan Iron & Steel Co., Ltd. (`600019.SH`), 2025
annual report, sample ID `manufacturing-materials-oos-600019-2025`.

Selection was made from the existing official local annual-report inventory.
The asset is `current`, `local_valid`, production-visible source material, and
its PDF integrity status is `valid`. Production visibility of the source PDF
does not authorize publication of any derived company-profile result.

The issuer, sample identity, CNINFO announcement ID `1225257227`, asset ID
`asset_50bd3ff0072ae0e7f7307798d86fd1a0`, and PDF SHA-256 were checked against:

- the approved four-report sample manifest;
- company-profile Gold and negative annotations;
- Stage 5 targeted, probe, preflight, and complete-run artifacts;
- manufacturing/materials adjudication ledgers and archived OpenSpec changes.

No company-profile evidence, Gold, targeted-run, or adjudication match was
found. Two unrelated repository references to `600019.SH` remain outside this
work: a financial numeric-fact batch note and a generic business-profile
governance unit-test symbol. They did not use this annual report to shape the
manufacturing/materials extraction or acceptance contract.

## Selection rationale and limitations

The sample is an integrated steel manufacturer with processing/distribution,
chemical, and information-technology activities. Its annual report uses an SSE
full-report form with business-model narrative, industry/product revenue and
cost tables, production/sales disclosures, customer disclosures, consolidation
changes, and project/capacity material. This differs materially from the prior
battery-system, battery-material, fine-chemical, and aviation samples and is a
useful bounded generalization pressure test.

Known limitations are frozen rather than avoided: the group and operating
structure is complex; disclosures may distinguish steel manufacturing,
processing/distribution, chemical operations, subsidiaries, and consolidation
scope. These can expose subject-scope and activity-boundary caveats. They are
not a reason to replace the sample after runtime output is observed.

Evidence pages and request scopes are deliberately not frozen in this file.
They belong to tasks 2.1-2.3 and must be prepared only after the user confirms
this sample freeze. No LLM call has been made for this sample.
