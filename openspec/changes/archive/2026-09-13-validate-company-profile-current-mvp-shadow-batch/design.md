## Context

The latest complete twenty-report batch used the ownership-aware v6 Evidence plan but a
Gemini-only route and an older Stage 5 implementation. It completed execution yet reached
only one `usable_with_caveats` report, 95.88% sampled precision, and median/p90 review
workload of 6/10. Since that run, current code has added four-member bounded failover,
dynamic output budgets, incomplete-output failover, and the P1 segment-subject fix. The
old result remains a useful baseline but is not a current-version readiness result.

The existing shadow operator also calls `_provider_for_scope` through its former
signature and does not pass `dynamic_output_tokens`; this is a direct execution blocker
for any new shadow batch and must be fixed before provider admission.

## Goals / Non-Goals

**Goals:**

- Run the existing twenty-report cohort once with the latest v6 Evidence plan, current
  Stage 5 implementation, logical four-model pool, and current dynamic token policy.
- Preserve report isolation, typed failures, immutable output, source-bound review, and
  the existing empirical readiness gates.
- Produce one decision that supports either a restricted-promotion proposal or a
  bounded list of common P1 blockers, rather than per-company retry work.

**Non-Goals:**

- No new Evidence selection, semantic field, prompt tuning, model ranking, token formula,
  retry framework, Gold change, or historical-output mutation.
- No targeted rerun, candidate splice, second batch inside this change, approved write,
  scheduler/backfill, CommodityExposure, ValueChainRole, DCF, or Stage 6 activation.
- No staging or committing the task-start LLM configuration changes; runtime admission
  records the effective four-member pool fingerprint without exposing credentials.

## Decisions

1. Reuse the frozen twenty-report manifest and ownership-aware v6 Evidence plan. This
   keeps source material constant and measures the current implementation and shared
   pool instead of introducing a new sample-selection variable.
2. Add one `current-mvp-semantic-replay` mode to the existing operator. It delegates to
   `ManufacturingMaterialsShadowBatchService`; a second semantic loop or service is not
   introduced.
3. Bind the logical route `semantic_extraction`, exactly four eligible members, common
   `json_object` support, and effective bounded failover before provider construction.
   The admission receipt stores a credential-free route description/fingerprint.
4. Use the existing base budgets `20000/18000`, 300-second per-request deadline, and
   600-call batch ceiling with dynamic output budgets enabled. The existing scope-size
   tiers remain authoritative; no threshold is changed in this validation.
5. Repair the shadow operator's stale `_provider_for_scope` call by explicitly forwarding
   the dynamic-budget flag. Old frozen modes continue with their historical fixed-budget
   behavior; only the new mode enables dynamic budgets.
6. Source review includes every blocker/caveat/unresolved item plus one stable accepted or
   legal-empty result per core chapter. Existing gates remain: execution >=95%, usable
   reports >=90%, Evidence traceability 100%, review complete, sampled precision >=99%,
   zero critical semantic errors, and unresolved review median/p90 <=2/5.
7. A `ready` result authorizes only a later restricted-promotion proposal. `hold` or
   `failed` completes this validation and yields common failure classes; it does not
   authorize report-specific reruns.

## Risks / Trade-offs

- **[The effective pool config is task-start dirty state]** → Validate and hash the
  credential-free runtime route description in the admission receipt; do not stage or
  alter the existing config file.
- **[Four-model output variation reduces direct baseline comparability]** → Keep the
  cohort, Evidence, readiness rules, and semantic implementation fixed, and report model
  traces rather than treating the baseline as a paired deterministic experiment.
- **[The batch remains hold]** → Close honestly and rank only recurring correctness or
  completion defects; isolated disclosure forms remain caveats/manual work.
- **[Network restrictions appear as DNS failure]** → Perform one read-only preflight and
  use the already authorized external execution path for Scorpio, ZAI, and DeepSeek when
  the sandbox exhibits the known permission-related resolution failure.
