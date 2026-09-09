## Shadow batch result

The one contracted provider-bearing batch completed on 2026-09-09 with batch ID
`manufacturing-materials-shadow-gemini-20260909-a`. It used only
`semantic_extraction__scorpio_gemini`, the frozen twenty-report manifest, the frozen
Evidence plan, and provider-free preparation audit v2. No historical run, comparison
model, or targeted rerun contributed records.

- Batch result hash: `017c0fc7f63f4d9925d5981c9d9543f359f2f936aabb7d0022a9994985a6f995`
- Reports persisted: 20/20; report-level typed output files remain separate.
- Semantic execution completed: 18/20 (90%); two reports are `failed`, eighteen are
  `hold`, and none are yet `usable` or `usable_with_caveats`.
- Accepted research records: 1,310; immutable Evidence traceability: 100%.
- Provider calls: 320; failed call traces: 16; two responses emitted the existing
  `provider_output_budget_exceeded` warning.
- Aggregate provider latency: 4,701,642 ms; input tokens: 2,001,081; output tokens:
  1,059,216.
- Unresolved human review workload: median 7.5/report, p90 15/report.
- Source-text review package v3: 308 rows (46 blockers, 157 unresolved items, 105
  stable chapter samples), hash
  `376f8e18f19c29e0cdfdc06fd3fbab2ee05208792a155a485baef2e2f2a0e632`.
- Empirical readiness audit v3: `hold`, hash
  `8a31bca26494da52db6945e9fe75357825cdc655d7fdc02df363287ae271b414`.

## Principal findings

The application path is reusable: automatic Evidence preparation, report-local
semantic execution, independent persistence, failure isolation, research projection,
and immutable audit generation all ran end to end. DNS and sandbox connectivity were
not report-level failure causes when the approved external execution path was used.

The batch is not scale-ready. The dominant problem is Evidence-plan specificity, not
the absence of annual-report facts. Every generated scope currently carries the full
chapter field set. A selected page that supports only part of that set therefore creates
`required_coverage_missing`, especially in segment, overview, counterparty, and
operating-quantity scopes. This produced 127 such human-review reasons and made all
twenty reports incomplete at the report gate. Secondary findings were 15 schema-invalid
candidate reviews, six incomplete-table-context reviews, three provider-unavailable
extract failures across two reports, and high output-token/latency variance.

The readiness decision remains `hold`. A production-promotion design change is not
justified. The next vertical slice should refine automatic scope-to-field assignment and
bounded table context using the frozen batch findings, then validate that change on a
new unseen cohort or a separately authorized replay. It must not reinterpret this batch,
splice outputs, open Stage 6, or write approved production data.

`production_authorization=not_authorized` remains unchanged.
