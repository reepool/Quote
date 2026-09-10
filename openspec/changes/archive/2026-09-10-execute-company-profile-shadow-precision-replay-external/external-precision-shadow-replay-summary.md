# External precision shadow replay empirical summary

## Decision

The sole authorized external replay `manufacturing-materials-shadow-precision-external-gemini-20260910-a` is `hold`. Running the provider-bearing Python/httpx process outside the sandbox eliminated the recurring DNS/connectivity blocker, but the result does not satisfy production-readiness gates. All records remain research-only and `production_authorization=not_authorized`.

No second batch, targeted rerun, model substitution, prompt/Evidence/semantic change, token or deadline change, candidate splice, or source-batch mutation occurred.

## Immutable execution result

- Frozen cohort and provider-free preparation: 20/20 reports.
- Persisted report results: 20/20; execution-completed: 19/20 (95%).
- Report status: 19 `hold`, 1 `failed` (`600121.SH`). The failed report contains report-local scope results, but multiple `candidate_schema_invalid` and `provider_unavailable` traces prevent execution completion.
- Provider traces: 319 total, 286 success and 33 failed (29 `candidate_schema_invalid`, 4 `provider_unavailable`).
- Accepted research facts: 783; accepted-fact Evidence traceability: 100%.
- Unresolved human-review workload: median 11, p90 17.
- Provider accounting: 4,802,909 cumulative latency ms; 1,670,605 input tokens; 1,277,997 output tokens.
- Nine successful responses exceeded the requested per-call output budget; the largest reported 29,189 output tokens. The responses were retained with `provider_output_budget_exceeded` warnings and did not relax the frozen request contract.
- Source batch manifest SHA-256: `7d53ea10109ffc07d80363d93750b06e4fbb57eacad89765f6beeb86e74c6f1e`.
- Source review package SHA-256: `5eb5c78ffe8a2da23e59db0e3163e54a87d73e64f0430758a9ad4e990b6a3fa3`.

The external execution returned normal HTTP successes and no replay-level DNS, connect-timeout, or deadline blocker. Observed HTTP 400/503 responses and response-parse failures were handled only by the already-frozen bounded behavior; they are retained as execution findings rather than used to authorize tuning in this batch.

## Source-bound review

The frozen package contains 391 rows: 68 blockers, 240 unresolved/adjudication rows, and 83 stable chapter samples. Every row received an outcome. Blockers and unresolved rows remain conservative non-accepted findings and are not counted as precision successes.

The contracted precision set is the 83 chapter samples:

- Correct: 75.
- Noncritical error: 8.
- Critical error: 0.
- Sampled precision: 90.36%.

The eight noncritical errors are bounded and source-identifiable:

1. `600022.SH`: `production_capacity=not_disclosed` uses overview/cost Evidence rather than the operating-quantity/capacity owner.
2. `600111.SH`: `material_input=not_disclosed` uses the controlling-shareholder page rather than procurement/material Evidence.
3. `600075.SH`: `production_capacity=not_disclosed` uses industry background rather than the capacity owner.
4. `000552.SZ`: `production_capacity=not_disclosed` uses industry background even though the report overview discloses approved annual capacity.
5. `000059.SZ`: `segment_dimension=not_disclosed` uses industry/business background rather than segment or revenue/cost Evidence.
6. `000055.SZ`: an accepted BusinessOverview target is only a cross-reference and lacks a substantive business description.
7. `920033.BJ`: `material_input=not_applicable` uses research/EHS context rather than the procurement/material owner.
8. `920078.BJ`: operating-quantity legal-empty coverage uses business-model/procurement Evidence rather than the quantity owner.

## Closed-error recurrence check

All nine previously closed precision-error families were checked against the new source-bound package and accepted records.

- Seven did not recur, including the prior consolidation-change omission, totals-only counterparty Relationship, cost-component Segment, unsupported group-subject promotion, and generic material Relationship classes.
- Two Evidence-owner errors recurred on different pages/samples: material ownership (`600111.SH`) and operating-quantity ownership (`920078.BJ`).
- Full accepted-record scans found zero cost-component Segments, zero top-five aggregate Relationships, zero unsupported `consolidated_group` promotions, and zero generic material Relationships.

This establishes that the remaining dominant problem is Evidence ownership/legal-empty routing and unresolved workload, not a need to enlarge the semantic schema or LLM output budget.

## Immutable comparison with the stability baseline

Compared with `manufacturing-materials-shadow-stability-gemini-20260909-a` using the same cohort and corrected v3 Evidence plan:

- Execution completion: 100% -> 95%; one report moved from `hold` to `failed`.
- Sampled precision: 90.53% -> 90.36% (-0.16 percentage points).
- Critical semantic errors: 2 -> 0.
- Accepted records: 809 -> 783 (-26).
- Unresolved median: 8.5 -> 11; p90: 12 -> 17.
- Provider calls: 341 -> 319, but failed traces increased from 12 to 33.
- Input tokens fell by 216,250 and output tokens by 6,600; cumulative latency was effectively flat (-4,131 ms).

The elimination of the two prior critical semantic errors is meaningful, but it is outweighed for readiness by zero usable reports, one execution-failed report, recurring Evidence-owner errors, and increased unresolved workload.

## Readiness gates

Passed:

- cohort and provider-free preparation complete;
- execution completion at least 95%;
- accepted-fact Evidence traceability 100%;
- contracted source review complete;
- critical semantic errors zero.

Failed:

- usable reports at least 90% (actual 0%);
- sampled precision at least 99% (actual 90.36%);
- unresolved-review median at most 2 (actual 11);
- unresolved-review p90 at most 5 (actual 17).
