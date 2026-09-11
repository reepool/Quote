# External Ownership Replay Closeout

- Terminal result: `hold`.
- The sole authorized external replay completed all 20 reports under
  `manufacturing-materials-shadow-operating-ownership-external-gemini-20260911-a`.
  No replacement batch, targeted rerun, candidate splice, model substitution, Evidence
  change, timeout change, or token-budget change was performed.
- Execution completion was 100%: 20 persisted successful reports, 0 report-level execution
  failures, 318 provider traces, 1,443 accepted research records, and 100% accepted-Evidence
  traceability.
- The external path removed the prior uniform pre-HTTP sandbox failure as the experiment
  blocker. The replay retained 29 typed `candidate_schema_invalid` semantic call failures;
  no terminal DNS, deadline, HTTP, or truncation failure invalidated a report.
- Aggregate recorded usage was 1,992,457 input tokens, 1,312,590 output tokens, and
  4,324,483 ms provider latency. Six successful responses carried
  `provider_output_budget_exceeded` plus `provider_output_budget_exceeded_valid_response`;
  they remained valid under the frozen `20000/18000` request budgets.

## Source review and readiness

- All 266 review rows received a source-bound outcome: 42 blockers, 127 unresolved rows,
  92 stable chapter samples, and 5 caveat samples. Blockers and unresolved rows remain
  retained; reviewing them did not convert them into accepted facts.
- The precision gate reviewed 97 chapter/caveat samples: 93 correct, 4 noncritical errors,
  0 critical semantic errors, for 95.8763% sampled precision.
- The four noncritical findings are one material legal-empty Evidence-owner issue, one
  business-overview completeness issue, and two segment legal-empty Evidence-owner issues.
- Only one report was `usable_with_caveats`; nineteen remained `hold`. Usable-report rate
  was 5%, unresolved-review median was 6, and p90 was 10. These metrics, together with the
  precision result, correctly keep empirical readiness at `hold`.

## Ownership recurrence

- All 17 frozen ownership-affected scopes were evaluated.
- Five scopes recovered completely; twelve still reproduced
  `candidate_schema_invalid` with incomplete required coverage.
- Across the 17 scopes, accepted records increased by 30 and unresolved review items fell
  by 31. The causal conclusion is partial recovery with persistent schema failures, not a
  favorable generalized closure.
- Relative to the evidence-role baseline, the full cohort reduced provider-failed calls by
  6, required-coverage findings by 20, and p90 unresolved review by 8, but accepted records
  decreased by 68 and sampled precision decreased by 4.12 percentage points. These mixed
  deltas are preserved rather than optimized through another replay.

## Boundary and follow-up observation

- `production_authorization=not_authorized` remains binding. Stage 6, approved storage,
  scheduler/backfill, commodity exposure, value-chain publication, and DCF remain closed.
- Dynamic output budgets are reasonable as a separate controlled change: select a fixed
  base/large/very-large tier before a request using Evidence bytes/count, field count,
  table rows/numeric occurrences, expected candidates, and partitions; keep small scopes at
  the base budget; partition above the highest tier; never increase a budget after failure
  and rerun the same scope. Because Scorpio/Gemini may return more than the requested budget,
  actual output, warning, response-size, and cost limits must remain separately observable.
