## Context

The September 13 common-MVP fix removed three cross-model contract defects: accepting a
risk-only sentence as BusinessOverview, binding legal-empty material/segment coverage to
non-owning Evidence, and accepting an unsupported English business-event name. The same
change also preserved bounded four-model failover and attempt lineage. Those fixes are
provider-free proven, but have not yet been exercised together on a small set of annual
reports absent from every prior company-profile validation cohort.

The authoritative execution owner remains `ManufacturingMaterialsShadowBatchService` and
the existing Stage 5 provider path. The effective `semantic_extraction` route currently
contains four eligible providers and the Stage 5 CLI already owns finite scope-size token
selection. This change measures those existing behaviors; it does not redesign them.

## Goals / Non-Goals

**Goals:**

- Freeze three to four genuinely fresh manufacturing/materials reports and their complete
  six-chapter Evidence plans before provider access.
- Execute one immutable research-only cohort through the current four-model logical route
  and dynamic token policy.
- Determine whether the three fixed defect families recur and whether each report is
  research usable under the current acceptance policy.
- Preserve exact source material, request/attempt lineage, typed failures, and human-review
  inputs so the result supports the next scale-readiness decision.

**Non-Goals:**

- No new semantic schema, confidence engine, model-ranking framework, token algorithm,
  retry subsystem, PDF parser, automatic Evidence planner, or production datastore.
- No post-call prompt, field, model, deadline, token, Evidence, or acceptance-policy tuning.
- No targeted rerun, report splice, historical replay, Gold authoring, production approval,
  Stage 6, approved write, scheduler/backfill, CommodityExposure, ValueChainRole, or DCF.

## Decisions

1. **Use a small diverse cohort rather than another twenty-report replay.** Three to four
   reports are enough to expose whether the shared fixes transfer across different
   manufacturing/materials disclosure forms while keeping source review complete. Sample
   identities must be absent from all prior authority, OOS, canary, shadow, targeted, Gold,
   and adjudication inputs. A larger statistical batch is deferred until this contract
   produces source-reviewed evidence.

2. **Freeze samples and Evidence before implementation and provider access.** The manifest
   records issuer, exchange, report period/title, local-valid PDF path/hash/page count,
   business form, disclosure form, selection reason, and limitations. Each report then
   receives the existing six chapter families: overview plus Activity; segment or legal
   empty; operating quantity/capacity or legal empty; material input or legal empty;
   counterparty/concentration or legal empty; and business regime event or explicit
   `not_applicable`. Freezing first prevents choosing pages or samples to fit model output.

3. **Add only the narrow admission needed for the frozen cohort.** The existing operator
   may gain one fresh-cohort mode/contract, but it must delegate report execution and
   persistence to `ManufacturingMaterialsShadowBatchService`. It must validate sample,
   PDF, Evidence-plan, field-closure, logical-route, dynamic-token-policy, output-identity,
   and production-boundary fingerprints before the first semantic request. The historical
   four-report and shadow contracts remain unchanged.

4. **Use the current logical pool and token calculation as fixed inputs.** The route is
   `semantic_extraction` with the four currently eligible models. Provider failover may
   occur only inside the existing bounded logical request. Dynamic extract/verify budgets
   remain selected from prepared scope size by the existing policy; this validation records
   chosen budgets and actual usage but does not alter thresholds or formulas.

5. **One provider-bearing batch means one immutable identity.** A read-only connectivity
   preflight may resolve/connect to configured Scorpio, ZAI, and DeepSeek endpoints and
   produces no semantic candidate. After the first semantic request, the cohort runs once
   and closes from its observed report-local results. Sandbox DNS failure uses the user's
   authorized external path; it does not create a replacement batch identity.

6. **Review substance, not formatting.** For every report, the review package contains
   every blocker, caveat, unresolved item, and recurrence candidate plus the first accepted
   or legal-empty result in stable order for each core chapter. It records source quote or
   table cells, physical page, Evidence ID, runtime target, model attempt lineage,
   disposition, restriction, and recommendation. Formatting differences alone are not
   failures; unsupported values, wrong Evidence ownership, semantic substitution, or
   execution incompleteness remain failures or holds under current policy.

7. **Close empirically without production promotion.** The audit reports execution
   completion, report status distribution, accepted facts, Evidence traceability, dynamic
   budget selection/usage, provider/model attempts and failures, human-review workload, and
   recurrence counts for the three fixed defect families. `hold` or typed `failed` completes
   the experiment honestly. A favorable result supports only a later, separate restricted
   production proposal.

## Risks / Trade-offs

- **[A small cohort is not population-scale proof]** → State only the observed cohort
  result and use it to decide whether a larger rollout validation is justified.
- **[Manual Evidence freezing can bias the sample]** → Record the complete six-chapter
  checklist, legal-empty decisions, known limitations, and hashes before provider access.
- **[Task-start LLM configuration is dirty]** → Read and fingerprint a credential-free
  effective route for admission, but do not edit, stage, or commit that file.
- **[A provider outage can obscure semantics]** → Preserve typed failures and model-attempt
  lineage; use the authorized external network path when sandbox DNS is the cause, then
  close without changing the semantic contract.
- **[One-shot execution leaves fixable report issues]** → Treat them as measured findings
  for a later change; do not reintroduce per-company tuning into this validation.
