## Context

The authoritative routing/continuation replay
`manufacturing-materials-shadow-routing-continuation-gemini-20260910-a` completed all
20 reports with 672 accepted records, 100% Evidence traceability, 79/81 sampled
precision, and zero critical semantic errors, but all reports remained `hold`.
Segment financials accounted for 30 of 72 report blockers and 135 of 273 unresolved
review rows. Twenty-five high-cardinality segment scopes completed their provider
partitions before local merge/schema validation rejected the results.

The archived change
`2026-09-10-repair-company-profile-shadow-segment-financial-completion` repaired that
existing merge/owner path and proved it with 174 provider-free tests. Its closure audit
retains `provider_calls=0`, the original replay as `hold`, and
`production_authorization=not_authorized`. The repair has not yet been measured by a
new provider-bearing cohort run.

The existing `ManufacturingMaterialsShadowBatchService`, immutable report-local store,
source-review package builder, readiness audit, and replay comparison remain the only
authoritative execution and evaluation owners.

## Goals / Non-Goals

**Goals:**

- Bind one new replay identity to the frozen twenty-report cohort/PDFs, v5 Evidence
  plan and audits, the prior empirical baseline, the archived segment repair closure,
  and the exact implementation hashes that passed provider-free verification.
- Prove every input and output-absence condition locally before provider creation.
- Execute the full cohort exactly once with the frozen Gemini profile and budgets.
- Measure whether segment completion, report usability, sampled precision, and review
  workload improve without recurrence of frozen critical semantic errors.
- Persist a complete source review, readiness decision, segment recurrence audit, and
  immutable before/after comparison regardless of whether the result is `ready`,
  `hold`, or `failed`.

**Non-Goals:**

- No cohort, PDF, Evidence plan, prompt, schema, model, output-token, timeout,
  provider-call, Gold, subject-policy, or readiness-threshold change.
- No targeted report/scope rerun, second batch, historical-result splice, or old bundle
  mutation.
- No generic replay framework, new semantic service, database, approved writer,
  scheduler/backfill, Stage 6, commodity exposure, value-chain publication, or DCF.

## Decisions

1. **Use one explicit replay contract and mode.** The new batch identity is
   `manufacturing-materials-shadow-segment-repair-gemini-20260910-a`. The contract
   reuses the routing/continuation replay's cohort, v5 plan, preparation/correction
   audits, model profile, output budgets, deadline, and provider-call ceiling. It adds
   the archived segment closure audit file hash
   `fedefd4eae5eed10beb6062c587bc9aadc6bc30bf6e3844549416deed6a18626`
   and internal audit hash
   `9a08c9203eee674aa3015f05bfd36dafe44cc143c03f657569c367c20edc2c38`
   as supporting inputs.

2. **Bind the implementation that was provider-free verified.** Admission checks the
   closure audit and its recorded hashes for `stage5_provider.py`,
   `shadow_evidence.py`, and their focused tests. Drift fails before provider creation;
   later unrelated code cannot silently claim this empirical result.

3. **The first semantic request consumes the replay.** A read-only Scorpio/Gemini
   preflight may run first and may be repeated outside the sandbox only when the
   sandbox result is a likely DNS or network-permission failure. Once semantic
   execution begins, typed transport/schema/report failures remain part of the sole
   authoritative batch and do not authorize a replacement run.

4. **Reuse the existing report-local service and evaluation owners.** The operator adds
   only admission/mode selection. It does not create a second semantic loop or modify
   provider output. Review and readiness are generated from the immutable new batch,
   and comparison binds both old and new artifacts by content hash.

5. **Evaluate the original business gates and the repaired blocker family.** Readiness
   still requires execution completion at least 95%, usable reports at least 90%,
   Evidence traceability 100%, sampled precision at least 99%, zero critical semantic
   errors, unresolved median at most 2, and unresolved p90 at most 5. The recurrence
   audit separately reports the prior 30 segment blockers, 135 segment unresolved
   rows, 25 high-cardinality local failures, and all new segment failure reasons.

## Risks / Trade-offs

- **[The repaired merge reveals a different source-bound blocker]** → Record the exact
  new distribution and close this replay; any fix belongs to a later bounded change.
- **[The provider returns schema-invalid, oversized, or transport failures]** → Preserve
  typed report-local traces and finish the sole batch without parameter tuning or
  rerun.
- **[A favorable result is mistaken for production approval]** → Keep
  `production_authorization=not_authorized` in every artifact and prohibit all
  production consumers.
- **[Historical or implementation inputs drift]** → Fail locally before creating the
  provider and do not consume the replay identity.
