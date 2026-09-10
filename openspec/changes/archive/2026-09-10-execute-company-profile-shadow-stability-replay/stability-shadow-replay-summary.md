# Post-stability shadow replay empirical summary

## Decision

The sole authorized replay `manufacturing-materials-shadow-stability-gemini-20260909-a` completed all twenty reports, but the reviewed result remains `hold`. Execution stability is no longer the limiting gate; source-page ownership, semantic correctness, and unresolved-review workload are. The result does **not** justify production promotion. All facts remain research-only and `production_authorization=not_authorized`.

## Immutable execution result

- Frozen cohort: 20 reports; corrected v3 Evidence plan; fixed `semantic_extraction__scorpio_gemini` route and budgets.
- Persisted and execution-completed reports: 20/20; report status: 20 `hold`, 0 `failed`.
- Provider calls: 341; 329 successful traces and 12 failed `candidate_schema_invalid` traces preserved as scope-level `hold` diagnostics; no report ended `failed` because of DNS, deadline, or provider unavailability.
- Accepted research facts: 809; accepted-fact Evidence traceability: 100%.
- Total latency: 4,807,040 ms; input tokens: 1,886,855; output tokens: 1,284,597.
- Seven successful responses reported more than the requested 20,000 output tokens; the maximum was 45,635. These were preserved as provider observations and did not relax the frozen replay contract.
- Source batch manifest SHA-256: `4cf47fb399419aeb55d2c00c61d038f7fdaee74056954eee8b23caf3ae3c30de`.
- Source review package SHA-256: `c393bcb34d3417d1a15da59f21e3b2a063457715a79e63fc9d5b143f61057ea8`.

No second batch, targeted rerun, model substitution, cross-run splice, timeout/token change, Evidence-plan change, or mutation of the committed batch occurred.

## Source-bound review

The frozen package contains 334 rows: 53 blockers, 186 unresolved/adjudication rows, and 95 core chapter samples. Every row received an outcome. Blockers and unresolved rows remain non-accepted; they are not counted as precision successes merely because conservative retention was confirmed.

The contracted precision set is the 95 core chapter samples:

- Correct: 86.
- Noncritical error: 7.
- Critical error: 2.
- Sampled precision: 90.53%.

The two critical errors are substantive:

1. `000060.SZ`: the Evidence explicitly states that consolidation scope changed, but runtime closes `business_regime` as `not_applicable`, omitting the control-scope event.
2. `000420.SZ`: runtime turns `前五名客户合计` into a Relationship identity; a totals-only aggregate must remain a Measurement and must not create a counterparty Relationship.

The seven noncritical errors are localized but recurrent production-quality gaps:

- a cost-component label (`原材料及燃动费`) is emitted as a business Segment;
- three material legal-empty conclusions use shareholder, quantity, or revenue pages rather than governed procurement/material Evidence;
- two business-regime legal-empty conclusions use audit-procedure pages rather than the business/control-change section;
- one production-volume legal empty is inferred from a business-model/procurement page rather than an operating-quantity disclosure.

## Comparison with the prior refined replay

The comparison is hash-bound to the immutable prior replay `manufacturing-materials-shadow-refined-gemini-20260910-a` and the new stability replay, and is stored outside both batch directories.

- Execution completion: 75% -> 100% (+25 percentage points).
- Failed reports: 5 -> 0; hold reports: 15 -> 20.
- Sampled precision: 76.42% -> 90.53% (+14.11 percentage points).
- Critical semantic errors: 2 -> 2 (no improvement).
- Unresolved review median: 4.5 -> 8.5; p90: 11 -> 12.
- Accepted records: 1,348 -> 809, reflecting stricter unresolved retention rather than a favorable usability result.
- Provider calls: 323 -> 341; failed traces: 9 -> 12.
- Total latency fell by 646,717 ms and input tokens fell by 71,301, while output tokens increased by 141,639.

The execution-path improvement is real, but it did not produce a usable report or satisfy the precision and review-workload gates.

## Readiness gates

Passed:

- cohort and provider-free preparation complete;
- execution completion at least 95%;
- accepted-fact Evidence traceability 100%;
- contracted source review complete.

Failed:

- usable reports at least 90% (actual 0%);
- sampled precision at least 99% (actual 90.53%);
- zero critical semantic errors (actual 2);
- unresolved-review median at most 2 (actual 8.5);
- unresolved-review p90 at most 5 (actual 12).

The empirical readiness decision is therefore `hold`.

## Next minimal sequence toward scaled production

1. Fix Evidence ownership only for the observed wrong-page families: material/procurement, business/control regime, and operating quantities. Do not build a new retrieval platform.
2. Fix the observed semantic guards: an evidenced consolidation change must prevent `business_regime=not_applicable`; totals-only rows must not create Relationships; cost components must not become business segments.
3. Prove these fixes provider-free with the frozen error rows and focused Stage 5 tests before any new provider-bearing cohort run.
4. Only after those checks pass, authorize one new immutable cohort replay and recompute the same review/readiness gates. A favorable result may justify a separate restricted production-promotion proposal, but it does not itself authorize production.

Stage 6, approved writers, scheduler/backfill, commodity exposure, value-chain publication, and DCF remain closed.
