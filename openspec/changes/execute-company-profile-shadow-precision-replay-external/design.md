## Context

Batch `manufacturing-materials-shadow-precision-closure-gemini-20260910-a` started inside the Codex sandbox on September 10, 2026. Two Python/httpx semantic requests each ended in `dns_failure` after the frozen 300-second deadline, a third was stopped, and zero report files were persisted. Its archived failure audit is immutable. The earlier curl probe was therefore not sufficient evidence that the actual provider-bearing runtime had network access.

The user has explicitly authorized sandbox-external access when this recurring DNS pattern appears. The existing operator and shadow service remain authoritative; the only changed operational condition is where the process is launched.

## Goals / Non-Goals

**Goals:**

- Use a new immutable batch ID and require the same frozen cohort, v3 Evidence, audits, Gemini profile, budgets, and review gates.
- Launch the provider-bearing process directly through the authorized external-sandbox path.
- Execute the twenty reports once, preserve typed failures, and produce source review/readiness/comparison artifacts when inputs exist.
- Decide whether empirical evidence justifies a separate restricted production-promotion proposal.

**Non-Goals:**

- No retry of or mutation to the failed sandbox identity.
- No model/Evidence/prompt/semantic/budget/readiness changes and no targeted rerun.
- No generic connectivity framework, approved writes, scheduler/backfill, Stage 6, commodity exposure, value-chain publication, or DCF.

## Decisions

### 1. Use a distinct external replay identity

Add `manufacturing-materials-shadow-precision-external-gemini-20260910-a` under a dedicated operator mode. Admission remains hash-bound to all inputs from the precision-closure contract. The failed sandbox directory is retained and explicitly cannot satisfy the new identity.

### 2. External permission applies to the whole provider-bearing process

The command is launched with `require_escalated` from the start. A separate curl preflight is not used as a substitute for process-level access. This is an operational permission change, not an LLM transport or retry change.

### 3. Keep the single-trial empirical contract

After the first semantic call, the new batch is authoritative and closes as `ready`, `hold`, or `failed`. Report isolation may continue within the same batch, but no second ID, targeted run, or candidate splice is permitted.

### 4. Reuse existing owners and gates

The CLI only selects the new contract. The existing service owns execution and persistence; existing review/readiness/comparison helpers own evaluation. Frozen gates remain 95% execution, 90% usable reports, 100% traceability, at least 99% sampled precision, zero critical errors, and unresolved median/p90 no greater than 2/5.

## Risks / Trade-offs

- **[External network still fails]** → Preserve typed results and close failed; do not tune or retry a second batch.
- **[Run is lengthy]** → Allow the single process to finish under existing report isolation and monitor persisted report count without altering it.
- **[Empirical result remains hold]** → Use exact source-bound findings for the next bounded semantic decision, not a broad platform rewrite.
- **[Ready is mistaken for approved]** → Retain `production_authorization=not_authorized`; a separate promotion proposal is still required.
