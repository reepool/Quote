## Context

The immutable v1 batch `manufacturing-materials-shadow-gemini-20260909-a` used 168 scopes and assigned 568 field/scope pairs. Its 20 reports produced 1,310 traceable research facts, but only 18 executions completed and no report was usable because the broad per-scope field contract generated 127 `required_coverage_missing` findings. The archived provider-free refinement rebuilt the same manifest as v2 with 165 scopes and 374 positively supported field/scope pairs, reducing Evidence payload characters by 30.5% while preserving hashes and traceability.

The existing owner chain is already sufficient: `ShadowEvidencePreparer` validates the frozen plan, `ManufacturingMaterialsShadowBatchService` executes report-local scopes through the Stage 5 owner, and `shadow_batch_audit` creates source review and readiness outputs. This change adds only the missing controlled-replay admission and before/after comparison needed to test the business effect of v2.

## Goals / Non-Goals

**Goals:**

- Execute exactly one complete replay of the same frozen twenty-report cohort with the archived v2 plan and a new batch identity.
- Keep model profile and provider budgets equal to the v1 run so the Evidence plan is the intended independent variable.
- Verify all replay inputs and historical baseline outputs by content hash before provider execution or comparison.
- Produce source-bound review material and a deterministic before/after audit that separates execution, semantic, review-workload, token, and latency changes.
- Decide whether the observed result is sufficient to begin a later restricted production-promotion design, while retaining research-only authorization.

**Non-Goals:**

- Tuning prompts, output limits, timeouts, models, semantic fields, subject rules, verifier rules, report gates, or Gold.
- Targeted scope reruns, retries beyond the existing gateway contract, result splicing, or mutation/reinterpretation of v1 artifacts.
- Adding a generic experiment framework, model-ranking system, confidence platform, production writer, scheduler/backfill, commodity exposure, value-chain output, DCF, or Stage 6.

## Decisions

### 1. Treat the replay as a single-use hash-bound execution contract

The operator receives the archived manifest, v2 Evidence plan, and a v2 provider-free preparation audit. Admission requires matching manifest revision/hash, PDF hashes, plan hash, twenty report identities, 100% Evidence traceability, and zero unsupported assignments, missing required owners, or incomplete table contexts. The batch identity is fixed to `manufacturing-materials-shadow-refined-gemini-20260910-a`, and an existing output directory is rejected.

The logical profile remains `semantic_extraction__scorpio_gemini`; extract output is 20,000 tokens, verify output is 18,000 tokens, each request deadline is 300 seconds, and the batch call ceiling is 600. This controls the comparison without claiming those parameters are globally optimal.

Alternative rejected: rerun failed scopes from v1. That would combine different request contracts and would not show whether v2 works as a complete batch.

### 2. Reuse the current semantic owner and add no second execution loop

The existing `semantic-run` operator remains a thin adapter over `ManufacturingMaterialsShadowBatchService`. The only operator change allowed is explicit replay admission/identity validation required to prevent accidental use of a non-v2 plan or different parameters. All extraction, verification, disposition, projection, and report status behavior remains owned by the Stage 5 service.

Alternative rejected: a dedicated replay runner that copies the report loop. It would create a second semantic path and weaken comparability.

### 3. Add one immutable comparison artifact outside both batch directories

A small strict model/function loads the immutable v1 and refined batch manifests, report files, review packages, and readiness audits. It records input paths and hashes, then compares:

- persisted and execution-completed reports;
- `usable`/`usable_with_caveats`/`hold`/`failed` distribution;
- accepted and traceable records;
- provider calls/failures, total input/output tokens, and latency;
- unresolved-review median/p90;
- counts for `required_coverage_missing`, `candidate_schema_invalid`, `table_context_incomplete`, transport/deadline failures, and other reason codes;
- source-review coverage, sampled precision, critical errors, and readiness decision.

The artifact reports absolute values and deltas; it does not convert a worse result into a pass or edit either source. It retains `production_authorization=not_authorized`.

Alternative rejected: compare summary Markdown only. The summaries are not sufficient to prove report-file identity or recompute reason counts.

### 4. Separate empirical improvement from production promotion

The replay is complete whether its outcome is `ready`, `hold`, or `failed`. A later production-promotion design is justified only if the existing readiness gates are actually met: at least 95% execution completion, at least 90% usable reports, 100% Evidence traceability, completed source review with at least 99% sampled precision and zero critical semantic errors, and median/p90 unresolved review counts no greater than 2/5.

Even a `ready` research replay does not authorize approved-table writes or downstream publication. It only supplies evidence for a separate change with explicit restricted-promotion and rollback rules.

### 5. Connectivity preflight is operational and outside the sandbox when needed

Before the sole provider-bearing run, perform one read-only Scorpio connectivity/profile check. If sandbox DNS or connection behavior fails, repeat that check with the user-authorized external permission path. A failed preflight does not consume the one semantic replay. Once provider calls start, the resulting full batch is authoritative and no replacement batch is launched inside this change.

## Risks / Trade-offs

- **[Provider variance obscures the v2 effect]** → Keep route and budgets fixed, disclose transport/schema failures separately, and avoid a second run that would cherry-pick variance.
- **[The old and new batch have different identities or missing files]** → Hash-validate both complete inputs before comparison and fail without writing the audit.
- **[Review workload falls only because fields were accidentally omitted]** → Require positive field support, required-field owners, six-core-chapter status, 100% traceability, and source review; do not use lower row count alone as success.
- **[A successful replay is mistaken for production approval]** → Preserve `accepted_for_review`, research projection restrictions, and `production_authorization=not_authorized` in every artifact.
- **[Manual source review is expensive]** → Review exactly the frozen package rule: every blocker/caveat/unresolved/adjudication plus the first accepted or legal-empty result per core chapter. Do not expand into a new Gold corpus.

## Migration Plan

1. Add replay admission and immutable comparison tests using local fixtures.
2. Generate and freeze the v2 preparation audit; validate all baseline/v2 hashes provider-free.
3. Run one external connectivity preflight, then one complete refined Gemini batch with the fixed identity and budgets.
4. Generate the source review, reviewed readiness audit, and before/after comparison outside both batch directories.
5. If gates pass, record only that a later restricted-promotion design may begin; otherwise record the exact blockers and stop without targeted reruns.
6. Archive the change with all audit artifacts. Rollback removes only the new code/artifacts and leaves both immutable batches untouched.

## Open Questions

None.
