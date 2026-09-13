## Context

The immutable September 12 current-MVP batch produced seven confirmed noncritical
semantic errors. Their extract/verify traces cover DeepSeek, Grok, GLM, and Gemini, so
the common cause is the shared Stage 5 contract and Evidence-owner logic. The same batch
also recorded twelve `deadline_exceeded` failures with null provider/model fields because
Stage 5 discarded safe routed-error lineage. Separately, the archived Evidence-role replay
still has a runnable compatibility path whose implementation-hash proof can no longer
match current code.

This change maps to governance requirement FR-12: an obsolete compatibility entry point
is removed after its historical result has been archived. The authoritative owners remain
the existing Stage 5 provider/normalizer and common LLM routed client.

## Goals / Non-Goals

**Goals:**

- Close the three shared semantic defect families with deterministic, provider-free rules.
- Let one slow routed source yield bounded time to another eligible model without exceeding
  the logical request deadline or hop limit.
- Preserve safe concrete-source attempt lineage in both successful and failed Stage 5 traces.
- Delete the obsolete Evidence-role replay execution surface and its active tests.

**Non-Goals:**

- No per-company exception, new semantic field, second retry framework, or prompt-tuning loop.
- No modification or rerun of historical/shadow bundles and no new twenty-report batch.
- No global model ranking, timeout increase, production approval, Stage 6, or downstream use.

## Decisions

1. **Use deterministic admission/normalization after model output.** BusinessOverview is
   accepted only when its exact source substring contains a substantive business declaration
   or direct activity. Metric-only and regulatory boilerplate candidates are rejected. A
   source-native business clause followed by risk commentary may be narrowed only to an exact
   leading substring; no wording is invented.
2. **Make legal-empty ownership stricter than factual mention.** Material and segment
   legal-empty results require an owning disclosure class plus an owner-specific source shape.
   A risk paragraph mentioning raw-material prices or an industry paragraph containing
   `细分行业` cannot prove non-disclosure.
3. **Keep canonical event semantics, sanitize only source-native metadata.** If a
   BusinessEvent `source_native.name` is absent from cited Evidence, it becomes null. The
   canonical event type/value/Evidence remain unchanged.
4. **Reserve time in the existing routed request.** When failover is enabled and another hop
   is possible, a physical source attempt receives at most 80% of the remaining logical
   execution budget, while respecting the configured minimum useful attempt window. A timed
   out physical attempt remains a transient attempt failure and may select another member;
   all attempts still share one absolute deadline and finite hop limit.
5. **Expose existing gateway lineage instead of building another audit layer.** Stage 5 trace
   fields receive selected profile, source label, failover count, and safe attempts from the
   existing `LlmResponse`/`LlmError` lineage.
6. **Delete, do not repair, obsolete compatibility.** Remove the completed one-shot
   `evidence-role-semantic-replay` and `current-mvp-semantic-replay` executable code and tests.
   Git, OpenSpec artifacts, and immutable batch output are the historical record; no old hash is
   rewritten.

## Risks / Trade-offs

- [A strict overview guard may reject a terse but useful sentence] → retain direct activity
  and explicit primary-business declarations; regression-test the four observed shapes.
- [Reserved failover time can stop a very slow first model that might eventually succeed] →
  reserve only when a real alternate hop exists, keep the total deadline unchanged, and use
  the already-authorized model pool rather than retrying the same source indefinitely.
- [Removing a replay mode breaks an undocumented caller] → repository-wide reference scan
  shows only the operator and its tests; archived artifacts remain readable.

## Migration Plan

1. Remove both obsolete one-shot modes, proof validators, contracts, CLI options, and active tests.
2. Add provider-free semantic and routed-timeout regression tests.
3. Apply the narrow owner/overview/source-native and lineage/failover changes.
4. Run focused company-profile and gateway suites. Rollback is the single task commit; no
   data migration or historical artifact mutation occurs.

## Open Questions

None. Production promotion remains a separate decision after a later empirical validation.
