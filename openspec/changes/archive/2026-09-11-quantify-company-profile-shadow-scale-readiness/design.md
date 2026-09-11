## Context

The authoritative external replay has 20 complete reports, 1,443 accepted records, 100% Evidence traceability, 97 reviewed precision rows, and a `hold` readiness decision. A later provider-free change fixed call-wide loss of valid siblings when one extract item is invalid, but the historical raw provider payloads were not persisted. The system therefore cannot replay the new adapter against those exact responses or truthfully claim how many candidates would have been recovered.

## Goals / Non-Goals

**Goals:**

- Bind the frozen replay, reviewed readiness, source review, and invalid-item audit by hash.
- Compute observed failure exposure by report and scope.
- Compute conservative facts and clearly labelled optimistic bounds from persisted data.
- Prove whether invalid-item isolation alone could satisfy the frozen scale gates.
- Preserve a reproducible audit generator and a concise business closeout.

**Non-Goals:**

- Reconstruct raw provider responses or assert historical candidate salvage.
- Recalculate report status as if unknown candidates had existed.
- Run an LLM, change Evidence, prompts, token budgets, models, timeouts, or retries.
- Implement adaptive token selection; that remains a separate trace-derived change.
- Authorize production or Stage 6.

## Decisions

1. **Use the reviewed archive as the empirical readiness source.** The batch-local readiness file is the intentionally immutable pre-review snapshot. The archived empirical readiness and outcomes are the complete source-review result and are hash-bound to the identical review package.

2. **Report three evidence levels.** `observed` values reproduce frozen metrics; `deterministic_bound` values follow directly from persisted report/failure identities; `unavailable` values identify questions that require absent raw payloads or a future replay. This prevents an optimistic counterfactual from being presented as a new run.

3. **Use an absolute usability upper bound.** Reports that are already hold with zero failed provider calls cannot be fixed by invalid-item isolation. Even the deliberately impossible best case where every failure-affected hold becomes usable leaves those unaffected holds unchanged.

4. **Use unresolved rows only as an optimistic workload floor.** Removing every unresolved row located in a failed scope is a maximum-removal counterfactual, not an expected outcome. It may show whether workload gates are theoretically reachable but cannot replace empirical review.

5. **Keep the generator inside the OpenSpec change.** This is a one-input historical audit, not a production service or reusable platform. The generator validates exact hashes and writes only the declared audit artifact.

## Risks / Trade-offs

- **Risk: optimistic bounds are mistaken for projected performance.** → Label each metric with its evidence level and retain the actual `hold` as the only readiness decision.
- **Risk: source-review artifacts drift from the batch.** → Validate batch ID, result hash, review-package hash, row identities, and file hashes before calculation.
- **Risk: the audit encourages another unbounded replay.** → State that it neither authorizes nor requires a replay; a future empirical run needs its own contract.
- **Risk: adaptive token work is mixed into semantic remediation.** → Exclude token implementation from this change and carry only the already-established follow-up decision.
