# Validation Record

## Offline Validation

- Common routed/config/pool/gateway/orchestration suite: 120 passed before the
  lifecycle follow-up; focused lifecycle/gateway rerun: 90 passed.
- Routed failover suite after preserving single-source terminal errors: 11 passed.
- CNInfo title classification: 13 passed.
- CNInfo corporate-action extraction, validation, resume, persistence, and
  promotion: 102 passed.
- CNInfo scheduler: 6 passed.
- Business-profile legacy adapter: 9 passed.
- Business-profile production rollout: 13 passed.
- Business-profile semantic extraction: 19 passed.
- Business-profile semantic runtime: 35 passed.
- Business-profile async production: 34 passed.
- Business-profile semantic maintenance scheduler: 12 passed.
- Corporate-action API routes: 5 passed.
- Source-lineage persistence and logging separation: 6 passed.
- Offline staged pool benchmark passed at 10, 25, and 50 logical requests. Peak
  transport/connection counts matched each configured pool cap; 3:1 dispatch
  counts were 8:2, 19:6, and 38:12. Synthetic first-event/transport latency
  maxima were 10.565/11.085 ms, 10.839/11.870 ms, and 11.596/13.997 ms;
  shutdown durations were 0.058/0.065/0.089 ms. Every level reported
  `fd_delta=0`, unique logical identities, zero 429/5xx/timeout/parse/schema
  failures, and an empty pool registry after shutdown.

## Controlled Live Validation

Synthetic non-sensitive Chinese business text was used. No key value, request
header, full prompt, or raw provider body was recorded in this artifact.

- `pipio:grok-4.5`: passed authentication, streaming first event, structured
  `json_object` output, actual model/source lineage, usage, request identifiers,
  timeout governance, and clean pool shutdown. The provider reported 587 output
  tokens against a requested 500-token budget, which the gateway surfaced as
  `provider_output_budget_exceeded` without rejecting the otherwise valid result.
- `pipio:gpt-5.6-luna`: did not pass. Both bounded same-source attempts returned
  HTTP 503 and were classified as `transient_transport_error`; the route failed
  closed and released all pool/provider state. Authentication, actual model,
  structured output, and usage could not be proven from this run.

The first controlled run for each source passed the single-source smoke gate.
Luna subsequently returned intermittent HTTP 503 responses; those failures are
recorded as provider instability and were handled by retry/cooldown/fail-closed
logic rather than hidden.

## Latest Controlled Routing Validation

- Single-source Grok and Luna were run against the same synthetic Chinese input.
  Each successful run verified authentication, actual model, streaming first
  event, structured `json_object` output, usage, request IDs, route lineage,
  timeout governance, and clean shutdown. Grok reported a provider output-token
  overrun warning while preserving a valid candidate; Luna's successful run had
  no warning.
- A second controlled run made Luna unreachable and temporarily gave Luna the
  preferred scheduling weight. Luna failed with `transient_transport_error`,
  then Grok completed the same logical request successfully. The final response
  had `failover_count=1` and two redacted attempt records; the route fingerprint
  remained the logical request identity and the shared pool was empty after
  shutdown.
- A later Luna-only probe returned HTTP 503 twice. The gateway classified it as
  `transient_transport_error`, applied provider cooldown/congestion accounting,
  failed closed when no eligible member remained, and released all resources.

The offline staged-load record is complete. A provider-backed 10-concurrency
stage was then run and failed its acceptance gate: 9 of 10 logical requests
succeeded, both Luna dispatches returned HTTP 503 and failed over successfully
to Grok, and one Grok request exhausted its 120-second deadline. No 429 occurred;
the pool recorded two provider 5xx failures, two requested/succeeded failovers,
one timeout, and no open circuit. Successful request latencies ranged from
4.061 to 119.374 seconds, total stage time was 120.287 seconds, Python traced
peak memory was 9,856,434 bytes, maximum RSS was 145,568 KiB, `fd_delta=0`,
shutdown took 3.194 ms, and the pool registry was empty afterward. Because the
10-concurrency stage failed success/provider-stability thresholds, the required
gate blocked 25- and 50-concurrency provider-backed stages. This is a rollout
failure, not an implementation-test omission; the offline 25/50 stages remain
the evidence for pool concurrency and weighted fairness.

## Candidate-Only Rollout Controls

- The live validator only returns an outer candidate envelope and never imports
  a business writer, approval path, or production fact table. Its temporary
  single-source configuration is process-local and discarded on shutdown.
- CNInfo/company-action/company-profile regression suites continue to enforce
  schema, evidence, candidate-only, review, and promotion gates; source lineage
  is persisted separately from business JSON.
- Rollout remains disabled by default in `config/13_llm.json`. Enablement is
  staged as: single-source smoke, controlled failover, offline 10/25/50 load,
  then operator-approved production route activation. Rollback disables the
  route/pool and leaves stored source lineage untouched.
- `pipio:grok` and `pipio:luna` remain independent provider resources until
  quota validation proves otherwise. Pool snapshots/logs expose concurrency,
  RPM, cooldown, circuit, failover, latency, and source-ratio fields for
  operational alerts. Both models currently share the Pipio URL, so this is
  model/key redundancy rather than cross-provider disaster recovery.

## Final Review

- OpenSpec strict validation passed with:
  `openspec validate add-weighted-llm-pool-routing --type change --strict --no-interactive`.
  The command also attempted optional PostHog telemetry, which failed because
  outbound DNS is unavailable; this did not affect the local validation result.
- `git diff --check` and Python compilation checks passed for the implementation
  and test files in this change.
- The repository `codex review --uncommitted` attempt was started, but the review
  service could not refresh its model because its authentication token was
  invalidated. The review process was stopped after it failed to produce a
  final finding set.
- An equivalent manual review of the change-owned routing, lifecycle, lineage,
  logging, configuration, and application-boundary diff found one confirmed
  transparency issue: the controlled live-validation CLI inspected concrete
  pool/profile mappings directly. Source selection for controlled smoke tests
  was moved into `LlmConfig.controlled_source_config()`, and the CLI now supplies
  only a logical profile plus source label. Focused routing/config tests passed
  after the fix; no remaining confirmed correctness, leakage, or permit/lifecycle
  issue was found. Existing baseline modifications outside this change were
  excluded from that assessment.
- Because Luna's controlled smoke still returns HTTP 503, this review does not
  mark the live rollout gates complete.

## Requirements Coverage

The change artifacts cover the requirements document as follows:

| Requirements document | OpenSpec coverage |
| --- | --- |
| Configuration layers, key names, file rename, validation | `weighted-llm-pool-routing`: logical profiles, configuration migration, fail-closed validation; tasks 1.1-1.6 |
| Weighted pool, total concurrency, borrowing, provider limits | `weighted-llm-pool-routing`: pool fairness and provider quota coordination; tasks 2.1-2.6 |
| Failover, deadline, retry/repair, circuit breaker, identity | `weighted-llm-pool-routing`: bounded failover, identity/idempotency, member health; tasks 3.1-3.7 |
| Source labels, response lineage, persistence and rollback | `common-llm-gateway` response contract plus `weighted-llm-pool-routing` envelopes/migration; tasks 4.3-4.7 |
| Application transparency and existing business callers | logical-profile boundary and business-compatibility requirements; tasks 4.1-4.2 and 5.1-5.7 |
| System logging and observability | `weighted-llm-pool-routing`: LLM logger architecture, level mapping, redaction, snapshots; tasks 2.7-2.9 |
| Offline regression, controlled smoke, staged rollout and gates | business compatibility and data migration/live rollout requirements; tasks 6.1-6.5 |
