## 1. Configuration and Public Contracts

- [ ] 1.1 Rename `config/11_llm.json` to `config/13_llm.json`; update every explicit repository reference, configuration template, and deployment document without changing configuration loader behavior.
- [ ] 1.2 Define validated non-secret configuration models for logical routes, pools, members, source labels, concrete-profile mappings, weights, borrowing, failover, circuits, and explicit provider resources; fail closed on invalid routes or configuration collisions.
- [ ] 1.3 Configure initial disabled/conservative Pipio Grok and Luna concrete profiles using `QUOTE_LLM_PIPIO_GROK_API_KEY` and `QUOTE_LLM_PIPIO_LUNA_API_KEY`, respectively; document controlled quota-bucket validation and do not store key values.
- [ ] 1.4 Add a public logical-profile facade for enablement, non-secret description, effective limits, supported capabilities, and deterministic route fingerprint, with a compatible result for unrouted profiles.
- [ ] 1.5 Extend public response/error lineage models with logical profile, selected profile, stable source label, route fingerprint, failover count, and redacted attempt records while retaining fixture and direct-profile compatibility.
- [ ] 1.6 Add unit tests for configuration validation, environment-key resolution, logical facade behavior, fingerprint stability, direct-profile fallback, response compatibility, and secret redaction.

## 2. Pool Scheduling and Lifecycle

- [ ] 2.1 Implement an injectable process-local `LlmPoolCoordinatorRegistry` keyed by validated pool configuration identity, with explicit snapshot, cleanup, cancellation, and test lifecycle APIs.
- [ ] 2.2 Implement bounded pool admission and a hard total logical-execution concurrency cap shared by all logical profiles mapped to the pool.
- [ ] 2.3 Implement deterministic weighted deficit round-robin member selection for compatible, enabled, eligible members, with deterministic test hooks and documented bounded fairness tolerance.
- [ ] 2.4 Implement optional idle-capacity borrowing while preserving pool and concrete-member limits; expose dispatch, active, wait, borrowing, and eligibility counters in non-secret snapshots.
- [ ] 2.5 Keep concrete profile limiter and provider/account coordinator leases at concrete-attempt scope; ensure pool activity is neither leaked nor double-counted during retries, repair, cancellation, or failover.
- [ ] 2.6 Add fake-clock/fake-transport concurrency tests for shared caps, fairness, borrowing, queue limit, cancellation, coordinator cleanup, and no permit leaks.

## 3. Routed Execution, Deadline, and Health

- [ ] 3.1 Refactor `LlmClient.complete()` into a stable logical routing entrypoint and a private concrete execution path so concrete calls do not recursively resolve routes or recreate logical request identity.
- [ ] 3.2 Introduce a shared absolute logical execution-budget context covering post-admission retries, repair, backoff, failover selection, and concrete attempts; preserve existing initial queue-time semantics and cancellation behavior.
- [ ] 3.3 Implement bounded same-source retry/repair followed by classified cross-source failover using `failover.on`, `max_hops`, remaining budget, untried compatible members, and minimum next-attempt budget.
- [ ] 3.4 Implement per-member closed/open/half-open circuit behavior, bounded probes, cooldown, recovery, and non-secret metrics separately from provider quota/cooldown management.
- [ ] 3.5 Ensure authentication, configuration, cancellation, and deadline failures fail closed by default; allow authentication failover only through explicit configuration plus high-priority operational logging.
- [ ] 3.6 Add fake-clock/fake-transport tests for one shared deadline, retry-before-failover ordering, source exhaustion, circuit open/half-open recovery, failure-class policy, provider-report de-duplication, and terminal safe lineage.

## 4. Application Boundary and Source Persistence

- [ ] 4.1 Replace direct logical-profile configuration access in `data_manager.py` with the public logical-profile facade for enablement, RPM/effective limits, resolver decisions, lifecycle, resume, and runtime identity.
- [ ] 4.2 Replace direct profile access in `research/business_profile_production_rollout.py` and any remaining application, API, scheduler, script, or research module identified by the static scan.
- [ ] 4.3 Update business-profile semantic runtime/input identity, checkpoint, artifact, and audit envelopes to use logical profile plus route fingerprint, not a randomly selected concrete model identity.
- [ ] 4.4 Propagate public source/route lineage through CNInfo title classification, corporate-action extraction, independent verification, async pipeline/resume, and persistence; preserve historical rows with null or `legacy_unknown` source rather than guessing.
- [ ] 4.5 Propagate public source/route lineage through business-profile semantic extraction, structured extraction, verification, async production, rollout, and legacy adapter candidate/audit envelopes without modifying business JSON schemas or evidence gates.
- [ ] 4.6 Add migration/compatibility tests for historical persisted LLM analysis and semantic artifacts that lack source lineage, and verify no public gateway path can write approved financial facts directly.

## 5. Existing LLM Business Compatibility Matrix

- [ ] 5.1 Run and record a repository-wide static scan for `LlmClient`, `LlmClientProtocol`, `LlmRequest`, `llm_config.profiles`, concrete profile fields, `resource_for_profile`, and `11_llm.json`; classify each result as gateway-internal, migrated application usage, test fixture, or documentation.
- [ ] 5.2 Add offline fake-transport regressions for `data_sources/cninfo_announcement_title_llm.py`, including source-label propagation and stable logical profile submission.
- [ ] 5.3 Add offline fake-transport regressions for `data_sources/cninfo_corporate_action_llm.py` and `data_sources/cninfo_corporate_action_pipeline.py`, covering extraction, independent verifier, failure/resume, candidate-only behavior, persistence, and source lineage.
- [ ] 5.4 Add offline fake-transport regressions for `research/business_profile_semantic_extraction.py`, `research/business_profile_semantic_runtime.py`, `research/business_profile_production_rollout.py`, `research/business_profile_async_production.py`, and `research/business_profile_llm.py`, covering runtime identity, checkpoint/resume, review path, and legacy adapter compatibility.
- [ ] 5.5 Exercise application lifecycle, scheduler, `scripts/research_business_profile_semantic_production.py`, `scripts/dev_validation/validate_common_llm_gateway_live.py`, and `scripts/dev_validation/benchmark_llm_orchestration.py` in a non-network configuration so routed construction and teardown remain valid.
- [ ] 5.6 Confirm all business modules use only stable logical profiles and public gateway/facade APIs; make the static scan a release gate with documented allowed `utils/llm` internal exceptions.

## 6. Validation, Rollout, and Documentation

- [ ] 6.1 Run focused unit tests for configuration, models, client, orchestration, and each affected CNInfo/business-profile integration; then run the full relevant existing LLM test suite without keys or network access.
- [ ] 6.2 Run explicit controlled single-source smoke tests for Grok and Luna separately, verifying authentication, model access, structured output capability, usage/request identifiers, timeout behavior, and provider-resource quota mapping; keep these tests out of normal unit tests.
- [ ] 6.3 Run a controlled two-source staged-load validation at 10, 25, and 50 logical concurrent requests, verifying total pool cap, configured long-run weight tolerance, idle borrowing, failover, circuit metrics, and no provider resource overrun.
- [ ] 6.4 Document rollout, rollback, quota decisions, operational dashboard/alert fields, and the distinction between model-key redundancy and cross-provider disaster recovery.
- [ ] 6.5 Run OpenSpec strict validation and repository-required review of the final implementation diff; resolve confirmed issues, rerun focused validation, and record all unchanged baseline worktree modifications separately from this change.
