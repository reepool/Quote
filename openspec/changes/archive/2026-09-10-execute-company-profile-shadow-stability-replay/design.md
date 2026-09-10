## Context

The immutable refined replay `manufacturing-materials-shadow-refined-gemini-20260910-a` used the frozen twenty-report cohort but completed only 15 reports. The subsequent provider-free correction produced Evidence plan `manufacturing_materials_shadow.2026-09-09.3` with plan hash `4f009c767dd0b75bce267fdba94c069272bf44a33e133dd5ca5dff2928da72ec`, preparation-audit hash `052155d3c948de7af257937783595a46028d96dfb8922d533354a5a4648f5ad8`, and correction-audit hash `14e0cd52fb2f698d5ef9f5417167c81e6139b25234113d4efcc42463a86b9e91`. The archived stability change then fixed the bounded repair payload, output-budget classification, and high-cardinality segment partitioning and proved them through provider-free regressions plus one bounded Gemini repair probe.

The existing operator, `ManufacturingMaterialsShadowBatchService`, Stage 5 semantic owner, and shadow audit builders already provide the required business path. This change exists only to exercise that path once against the frozen cohort and measure the result; it must not become another tuning loop.

## Goals / Non-Goals

**Goals:**

- Execute exactly one new-ID provider-bearing replay of the same twenty reports using the corrected v3 Evidence plan and stabilized LLM path.
- Reject any changed cohort, PDF, plan, preparation, correction evidence, model route, provider budget, or reused output identity before semantic provider execution.
- Preserve report-local isolation, typed transport/schema/truncation failures, physical partition accounting, and immutable output.
- Produce the existing source-bound review and readiness artifacts plus an immutable comparison with the prior refined replay.
- Decide only whether the empirical result justifies a separate restricted production-promotion design.

**Non-Goals:**

- Changing Evidence selection, semantic enums, prompts, verification policy, Gold, model selection, output-token budgets, deadlines, retry counts, or readiness thresholds.
- Targeted reruns, failed-scope repair runs, cross-run/model splicing, or mutation of any historical plan, batch, review, or audit.
- Building a generic experiment platform, new storage system, confidence service, production writer, scheduler/backfill path, commodity/value-chain publication, DCF, or Stage 6.

## Decisions

### 1. Use a new single-use replay contract rather than reopening the consumed refined contract

The operator will add one narrow stability-replay admission bound to batch ID `manufacturing-materials-shadow-stability-gemini-20260909-a`, the existing twenty-report manifest hash, the corrected v3 plan/preparation/correction hashes, the archived stability evidence, and a non-existent output directory. The existing refined replay contract and its output remain unchanged.

Alternative rejected: replace the old refined contract's batch ID or hashes. That would reinterpret an already completed immutable run and make the audit lineage ambiguous.

### 2. Keep the provider policy fixed

The replay uses `semantic_extraction__scorpio_gemini`, 20,000 extract/repair output tokens, 18,000 verify output tokens, a 300-second request deadline, and a 600 physical-call ceiling. High-cardinality segment partitions and bounded repair consume that existing ceiling; no extra budget is created for them.

Alternative rejected: tune budgets from the previous failures. The stability change deliberately fixed execution behavior without changing the measured provider policy, so this replay must test that isolated change.

### 3. Preflight connectivity once, then treat the first provider call as consuming the replay

Before execution, a read-only Scorpio/profile check may run once and may be repeated outside the sandbox only when the sandbox produces a likely DNS or network-permission failure. The preflight does not create semantic output and does not consume the replay. Once the first semantic provider call begins, the resulting batch is authoritative even if it finishes `hold` or `failed`; no replacement ID or targeted run is permitted in this change.

Alternative rejected: restart after an unfavorable provider result. That would select on provider variance and invalidate the scale measurement.

### 4. Reuse the existing report loop and audit owners

The CLI remains a thin adapter. It selects the new replay contract and delegates all report execution to `ManufacturingMaterialsShadowBatchService`. The existing review-package and readiness builders operate on the committed batch. The existing comparison helper binds the prior and new manifests, report files, review packages, and readiness audits by hash and records absolute values and deltas.

Alternative rejected: add a separate stability replay service or duplicate the report loop. There is one real caller and the current owners already implement the required behavior.

### 5. Separate research readiness from production authorization

The validation is complete for any honest `ready`, `hold`, or `failed` result. Only an actual result meeting the frozen readiness thresholds may justify proposing a later restricted production-promotion change. Every artifact remains `accepted_for_review`/research-only with `production_authorization=not_authorized`; this change cannot write approved data or open downstream production consumers.

## Risks / Trade-offs

- **[Provider variance remains visible]** → Preserve all typed failures and latency/token metrics, run only once, and do not cherry-pick a replacement batch.
- **[The corrected plan differs from the reviewed contract]** → Validate the exact manifest, PDF, v3 plan, preparation, and correction hashes before network I/O.
- **[Partitioning increases physical calls]** → Charge every partition and repair to the frozen 600-call ceiling and retain parent/index/count lineage.
- **[A successful execution is mistaken for production approval]** → Keep readiness and production authorization as separate fields and retain all production paths closed.

## Migration Plan

1. Add the narrow stability-replay admission and provider-free tests for exact inputs, budgets, new identity, and rejection of historical/reused outputs.
2. Validate the frozen inputs and stability artifacts without provider calls.
3. Perform one connectivity preflight, then execute the single full twenty-report replay.
4. Generate and source-review the frozen review package, then build the readiness and immutable before/after audits.
5. Record `ready`, `hold`, or `failed`, archive the complete evidence, and stop. Rollback may remove only the new operator contract/code before execution; it must never remove or rewrite a committed batch.

## Open Questions

None.
