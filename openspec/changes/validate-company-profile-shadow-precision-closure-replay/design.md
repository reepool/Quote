## Context

The authoritative replay `manufacturing-materials-shadow-stability-gemini-20260909-a` completed 20/20 reports with 809 accepted research facts and 100% Evidence traceability, but source review measured 90.53% sampled precision, two critical errors, seven noncritical errors, and unresolved-review median/p90 of 8.5/12. Change `close-company-profile-shadow-reviewed-precision-errors` closed those exact nine defects provider-free with audit file SHA-256 `a7dcdcc1b7d59cb2093b3043d92b20b0a0f673364c1016bfa364ac8960689f87`.

The current operator, replay-admission owner, report-local shadow service, review builder, readiness builder, and comparison helpers already implement the required business path. This change adds only a narrow admission for one empirical replay after precision closure; it does not create another execution or evaluation framework.

## Goals / Non-Goals

**Goals:**

- Execute exactly one new-ID provider-bearing replay of the frozen twenty-report cohort through the corrected precision path.
- Bind the existing manifest/PDFs, corrected v3 Evidence plan/preparation/correction audits, stability evidence, nine-error closure audit, Gemini profile, budgets, and output identity before network I/O.
- Preserve report isolation, physical call accounting, typed failures, immutable output, and the frozen source-review rule.
- Quantify execution completion, report usability, accepted facts, Evidence traceability, provider calls/failures/tokens/latency, unresolved-review median/p90, sampled precision, and reviewed critical/noncritical errors against the prior authoritative replay.
- Decide only whether a separate restricted production-promotion proposal is empirically justified.

**Non-Goals:**

- No Evidence, prompt, semantic vocabulary, verifier, model, token/output/deadline/call budget, Gold, or readiness-threshold change.
- No targeted reruns, failed-scope repair runs, cross-run/model candidate splicing, or mutation of historical batches and audits.
- No approved writer, scheduler/backfill, commodity exposure, value-chain publication, DCF, Stage 6, confidence platform, or generic experiment framework.

## Decisions

### 1. Add one narrow precision-closure replay contract

The operator will admit batch ID `manufacturing-materials-shadow-precision-closure-gemini-20260910-a` only in a new precision-closure replay mode. The contract reuses the stability replay manifest, corrected v3 plan, preparation, correction and execution-stability hashes and additionally requires the archived provider-free precision-closure audit hash. A reused batch ID, existing output directory, changed input, or missing closure artifact fails before provider execution.

Alternative rejected: reuse or reinterpret the consumed stability replay contract. That would destroy the one-run lineage and obscure whether the precision fixes were actually exercised.

### 2. Keep the provider policy and Evidence frozen

The replay uses `semantic_extraction__scorpio_gemini`, 20,000 extract/repair output tokens, 18,000 verify output tokens, a 300-second request deadline, and a 600 physical-call ceiling. It uses the same corrected v3 Evidence plan and source-review sampling rule. No result may trigger parameter or Evidence tuning inside this change.

Alternative rejected: tune the model or budgets before replay. That would confound the measured effect of the reviewed precision fixes.

### 3. Treat the first semantic call as the single empirical trial

A read-only route/profile preflight may run before execution and may be repeated outside the sandbox only for a likely sandbox DNS/network-permission failure. Once the first semantic provider request starts, the resulting batch is authoritative and must close as `ready`, `hold`, or `failed`; no replacement ID or targeted rerun is allowed.

Alternative rejected: restart after provider variance or an unfavorable report. That would select the outcome and invalidate scale evidence.

### 4. Reuse the existing report, review, readiness, and comparison owners

The CLI remains a thin adapter. `ManufacturingMaterialsShadowBatchService` owns the report loop and immutable output. Existing review/readiness helpers produce source-bound rows and empirical metrics. A comparison artifact outside both batch directories validates inputs by hash and records prior/new absolute values and deltas, including whether all nine previously reviewed errors recur.

Alternative rejected: add a second replay service or a new Benchmark platform. There is one current caller and the existing owners already provide the required semantics.

### 5. Separate empirical readiness from production authorization

The validation is complete for an honest `ready`, `hold`, or `failed` result. Only a result meeting the frozen execution, usability, traceability, source-review, precision, critical-error, and review-workload gates may justify a later restricted production-promotion proposal. Every current record remains research-only with `production_authorization=not_authorized`.

## Risks / Trade-offs

- **[Provider variance remains visible]** → Preserve every typed failure and run once without cherry-picking a replacement.
- **[The fixed cases pass but new source shapes fail]** → Report new critical/noncritical findings separately; do not broaden rules during the replay.
- **[Review workload is material]** → Freeze the same source-review rule and complete it before a favorable readiness result; do not infer precision from runtime acceptance alone.
- **[A ready result is mistaken for production approval]** → Keep readiness and production authorization separate and retain every downstream production path closed.

## Migration Plan

1. Add and test the narrow replay admission and CLI mode without provider calls.
2. Validate all frozen hashes, budgets, route, closure evidence, and the new output identity.
3. Run one read-only preflight and then the sole twenty-report replay.
4. Complete the frozen source review and generate readiness/comparison artifacts outside historical batch directories.
5. Record `ready`, `hold`, or `failed`, verify immutability and production closure, archive the change, and stop. Before execution, rollback may remove only the new admission code; after execution, the committed batch must never be deleted or rewritten.

## Open Questions

None.
