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

### Completion Audit Rerun (2026-08-07)

The current `master` implementation was rerun rather than relying only on the
earlier validation record:

- Common gateway, orchestration, weighted pool, routed client, routing config,
  source-lineage persistence, logging separation, and staged-validator tests:
  148 passed.
- CNInfo corporate-action semantic processing: 102 passed; scheduler: 6 passed;
  title classification: 13 passed; read-only lineage/API routes: 4 passed.
- Business-profile legacy adapter: 9 passed; semantic extraction: 19 passed;
  semantic runtime: 35 passed; async production: 49 passed; maintenance
  scheduler: 12 passed; production rollout: 13 passed.
- Source-lineage apply-twice/rollback compatibility and corporate-action schema
  migration: 3 passed.
- The first combined cross-file CNInfo run stopped producing output after 93
  cases because of the repository session-scoped asyncio fixture behavior. Each
  affected file was then run in a separate bounded process and passed with the
  counts above; the interrupted aggregate run is not counted as a pass.
- The offline 10/25/50 benchmark passed again with 8:2, 19:6, and 38:12 source
  counts, peak transport concurrency 10/25/50, unique request/hash/business
  identities, `fd_delta=0`, and an empty pool registry after every shutdown.
- `scripts/research_business_profile_semantic_production.py plan` completed
  against an isolated `/tmp` database/artifact/checkpoint configuration with
  the network kill switch enabled. It reported `llm_calls=0`, wrote only the
  temporary plan artifact/checkpoint, and shut down shared LLM resources.
- `validate_common_llm_gateway_live.py` failed closed before transport when the
  route remained disabled. Repository static scans found no business-layer
  `llm_config.profiles` or `resource_for_profile()` access, and
  `config/13_llm.json` remained the sole project JSON owner of the top-level
  `llm` key.

## Controlled Live Validation

Synthetic non-sensitive Chinese business text was used. No key value, request
header, full prompt, or raw provider body was recorded in this artifact.

- `pipio:grok-4.5`: passed authentication, streaming first event, structured
  `json_object` output, actual model/source lineage, usage, request identifiers,
  timeout governance, and clean pool shutdown. The provider reported 587 output
  tokens against a requested 500-token budget, which the gateway surfaced as
  `provider_output_budget_exceeded` without rejecting the otherwise valid result.
- `pipio:gpt-5.6-luna`: did not pass in this initial round. Both bounded
  same-source attempts returned
  HTTP 503 and were classified as `transient_transport_error`; the route failed
  closed and released all pool/provider state. Authentication, actual model,
  structured output, and usage could not be proven from this run.

Grok passed the initial single-source smoke gate. A later successful Luna run,
recorded below, completed Luna's single-source capability evidence. Luna also
returned intermittent HTTP 503 responses in subsequent probes; those failures
are recorded as provider instability and were handled by
retry/cooldown/fail-closed logic rather than hidden.

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
the evidence for pool concurrency and weighted fairness. OpenSpec task 6.5
therefore remains incomplete: live rollout cannot be declared complete until a
provider-backed 10-concurrency rerun passes and the gated 25/50 stages also pass.

On 2026-08-07, the provider-stability prerequisite was checked again with the
controlled Luna-only validator and the same synthetic non-sensitive Chinese
input. Both bounded attempts returned HTTP 503 and were classified as
`transient_transport_error`. The gateway recorded the two failures, reduced the
provider adaptive bulk limit from 8 to 6 after the configured soft-failure
threshold, failed closed with redacted route lineage, and shut down with zero
active pool work. Because the source was still unstable, the provider-backed
10-concurrency gate was not rerun and the 25/50 stages remained blocked by the
required lower-stage gate.

Later on 2026-08-07, a successful Luna single-source probe established that the
provider-stability prerequisite had recovered, so the provider-backed
10-concurrency gate was rerun with the reproducible staged validator. All 10
logical requests succeeded: primary dispatch and result counts were 8 Grok and
2 Luna, exactly matching the deterministic 3:1 small-batch schedule; there were
no 429, provider 5xx, timeout, parse, schema, or exhausted-failover outcomes.
First-event latency ranged from 1.312 to 117.715 seconds and total request
latency from 3.936 to 119.840 seconds; total stage time was 120.064 seconds.
Peak transport concurrency was 10, Python traced peak memory was 9,841,236
bytes, maximum RSS was 146,368 KiB, and shutdown took 1.765 ms. The transport
had zero active calls after shutdown, `fd_delta=0`, all 10 request IDs and
hashes were unique, both circuits were closed, and the pool registry was empty.
Grok returned valid structured candidates with
`provider_output_budget_exceeded` warnings on several requests; these warnings
were retained in result lineage and did not hide a transport, parse, or schema
failure.

At the time of that run, the checked-in provider resources remained
conservatively limited to hard concurrency 10 and 10 RPM per source, and quota
ownership had not yet been confirmed. The later deployment confirmation and
rerun below supersede that quota-ownership uncertainty without raising either
provider limit.

### Independent-Quota Staged Rerun (2026-08-07)

The deployment owner confirmed that the Grok and Luna API Keys have independent
quota. The controlled staged validator therefore kept two provider resources,
each with hard concurrency 10 and 10 RPM, while requesting logical pool stages
10, 25, and 50. Higher logical stages were allowed to queue behind those
provider caps; the validator did not increase or bypass either source limit.

The 10-concurrency gate completed all 10 logical requests successfully in
111.807 seconds, with nine final Grok results and one final Luna result. Primary
dispatches were nine Grok and two Luna because one Luna source execution failed
over successfully to Grok. Luna returned HTTP 503 twice during its bounded
same-source attempts; one Grok execution returned HTTP 503 once and then
succeeded on same-source retry. The corrected concrete-attempt accounting
reported exactly three provider 5xx failures, zero rate limits, and zero
timeouts; HTTP 503 was no longer double-counted as a timeout, and retry failures
were retained even when the source later succeeded.

The stage otherwise remained clean: both circuits were closed, transport peak
was 10 against aggregate confirmed provider capacity 20, provider configuration
matched the confirmed per-source limits, first-event latency ranged from 1.702
to 109.329 seconds, total latency ranged from 4.582 to 111.535 seconds,
`fd_delta=0`, shutdown took 1.907 ms, transport active count returned to zero,
and the pool registry was empty. The acceptance gate failed only for
`nonzero_provider_5xx`, so the required gate correctly did not start the 25- or
50-concurrency stages. Task 6.5 remains incomplete pending a stable provider
window in which 10 passes before 25 and 50 are attempted.

### Independent-Quota Low-Cap Rerun (2026-08-07)

The deployment owner reconfirmed that the two API Keys have independent quota.
To avoid assuming equal usable concurrency during validation, the staged
validator was extended with repeatable validation-only per-resource caps. These
caps modify only the process-local controlled configuration and do not change
`config/13_llm.json` or claim a new upstream quota limit. Focused tests cover
valid lower caps, unknown resources, non-positive/excessive values, duplicate
CLI assignments, and effective provider-limit reporting.

The 10-concurrency stage was rerun with `pipio:grok=2` and `pipio:luna=1`,
keeping the resources independent and their configured RPM at 10 each. All 10
logical requests eventually succeeded. Luna had two failed logical source
executions, each with two bounded HTTP 503 attempts, for four provider 5xx
events; both cross-source failovers to Grok succeeded. There were zero 429,
timeout, parse, schema, or exhausted-failover outcomes. Both circuits remained
closed.

Total dispatches were five Grok and seven Luna. Six Luna dispatches were
explicit borrowed-capacity dispatches, so the non-borrowed normal counts were
five Grok and one Luna. The validator now records total, borrowed, and normal
dispatch counts separately and applies the configured-weight gate only to
normal dispatches. This removed the prior false weight-ratio rejection while
preserving the strict provider-failure gate. The stage failed only for
`nonzero_provider_5xx`; therefore 25 and 50 were not started.

The stage took 86.950 seconds. First-event latency ranged from 1.183 to 21.417
seconds and successful total latency from 3.916 to 37.741 seconds. Peak
transport concurrency was 2 against the validation aggregate cap of 3, shutdown
took 1.796 ms, transport active count returned to zero, `fd_delta=0`, and the
pool registry was empty. Task 6.5 remains incomplete until a clean 10 stage is
followed by clean gated 25 and 50 stages.

### Independent-Quota Low-Cap Stability Rerun (2026-08-07)

The deployment owner confirmed again that the Grok and Luna API Keys have
independent quota. The validator therefore retained separate `pipio:grok` and
`pipio:luna` provider resources even though both profiles use the same Base
URL. The process-local validation caps remained `pipio:grok=2` and
`pipio:luna=1`; production `config/13_llm.json` was not modified.

The gated 10-concurrency stage completed all 10 logical requests successfully
in 265.104 seconds, with all final responses returned by Grok. Luna received
four source executions and every one exhausted two bounded attempts with HTTP
503, producing eight concrete provider 5xx events. All four Luna-to-Grok
failovers succeeded, with zero 429, timeout, parse, schema, or exhausted-
failover outcomes. The Luna member circuit opened independently while the Grok
member circuit remained closed, directly exercising separation of the two Key
quota/health resources.

Total dispatches were ten Grok and four Luna. Three Luna dispatches borrowed
idle capacity, leaving non-borrowed normal dispatch counts of ten Grok and one
Luna. Because the provider failures made the normal ratio materially different
from the configured weights, the gate reported `nonzero_provider_5xx` and
dispatch-ratio failures for both source labels. First-event latency ranged from
2.397 to 56.797 seconds and successful total latency from 21.240 to 86.600
seconds. Peak transport concurrency was 2 against the validation aggregate cap
of 3; provider limits and identities matched, shutdown took 1.823 ms, transport
active count returned to zero, `fd_delta=0`, and the pool registry was empty.
The staged command stopped after 10 and did not execute 25 or 50. Task 6.5
remains incomplete pending a clean provider window.

### Completion and Provider-Stability Rerun (2026-08-07)

The current committed implementation was rerun against the requirement-level
offline matrix. The common gateway/configuration/orchestration/pool/routed-
client/staged-validator and source-lineage group passed 160 tests. CNInfo title,
semantic resolution, pipeline, audit/storage/incremental, scheduler, API, and
migration groups passed 13, 102, 17, and 75 tests respectively. The business-
profile adapter, semantic extraction, runtime, async production, maintenance,
and rollout groups passed 9, 19, 35, 49, 12, and 13 tests respectively, for 504
passing tests in this rerun. Files affected by the repository's session-scoped
asyncio fixture interaction were executed in separate bounded processes; every
individual process exited successfully.

The offline staged benchmark also passed again at 10, 25, and 50 logical
requests. Its 3:1 dispatch counts were 8:2, 19:6, and 38:12; peak transport and
connection counts were exactly 10, 25, and 50. All stages reported unique
request/hash/business identities, `fd_delta=0`, and an empty pool registry after
shutdown.

Before spending another provider-backed batch, controlled single-source Grok
and Luna probes were run with the same synthetic non-sensitive Chinese input.
Both sources exhausted two bounded 300-second attempts. Each attempt was
classified as a retryable HTTP 408/transient transport timeout, each provider
resource independently entered adaptive congestion handling, and both routes
failed closed after the second attempt. Both pool lifecycles stopped with zero
active work. Since neither source produced a clean single-source response in
this provider window, the 10-concurrency gate was not started and the required
25/50 promotion remained blocked. Task 6.5 remains incomplete; the acceptance
thresholds were not weakened.

### Independent-Quota Resumed Preflight (2026-08-08)

The deployment owner supplied a Pipio usage-console screenshot showing successful
`grok-4.5` streaming requests under the Grok credential on 2026-08-07. The records
included first-response and total latency plus input/output token counts. This
corroborates that the Grok Key was accepted and used by the intended model, but it
does not by itself establish Luna health, current provider stability, structured-
output correctness, or any staged-concurrency acceptance result.

With the owner-confirmed independent quota mapping, controlled Grok and Luna
single-source probes were then started concurrently against the same synthetic,
non-sensitive Chinese input. Both requests passed local configuration validation,
entered separate `pipio:grok` and `pipio:luna` provider resources, selected the
expected concrete profile/source label, and began their first provider attempt.
Neither source returned a first event before its 300-second attempt limit. Both
sources exhausted a second bounded 300-second attempt and classified both attempts
as HTTP 408 / `transient_transport_error`.

The two provider resources independently entered adaptive congestion handling,
demonstrating that the same Base URL did not merge their runtime quota state. Each
controlled route failed closed with no business result, stopped its pool lifecycle
at `active=0`, and left no matching validation Python process. Since both single-
source prerequisites failed, the provider-backed 10-concurrency gate was not
started; 25 and 50 were consequently withheld. Production configuration and the
real `.env` were unchanged, and task 6.5 remains incomplete.

A second independent-quota preflight window began at 00:46:18 on 2026-08-08.
Grok and Luna again entered their expected separate resources and concrete
profiles, but both first attempts reached the 300-second limit at 00:51:18 and
were classified as HTTP 408 / `transient_transport_error`. Because any timeout
already makes the strict preflight window ineligible for promotion, the
remaining same-source retries were cancelled rather than spending another 600
provider-seconds. No 10/25/50 load stage was started, both validation processes
exited through the bounded cancellation path, and no matching Python process
remained.

A third controlled window began at 01:05:37 on 2026-08-08. Both sources again
passed local admission and selected their expected independent resource and
concrete profile. Neither source produced a first event; Grok and Luna both
reached the first 300-second attempt limit at 01:10:37 with HTTP 408 /
`transient_transport_error`. The external 360-second validation-process bound
then expired with exit code 124 before another full retry could be spent. No
matching validation Python process remained. This is the third consecutive
controlled window blocked by the same provider-response failure, so external
provider stability is now the sole unresolved prerequisite for task 6.5. The
10/25/50 provider-backed stages remain correctly withheld.

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
- `pipio:grok` and `pipio:luna` are independent provider resources, matching
  the deployment owner's confirmed independent Key quotas. Pool snapshots/logs
  expose concurrency, RPM, cooldown, circuit, failover, latency, and
  source-ratio fields for operational alerts. Both models currently share the
  Pipio URL, so this is model/key redundancy rather than cross-provider
  disaster recovery.

## Final Review

- OpenSpec strict validation passed with:
  `openspec validate add-weighted-llm-pool-routing --type change --strict --no-interactive`.
  The command also attempted optional PostHog telemetry, which failed because
  outbound DNS is unavailable; this did not affect the local validation result.
- `git diff --check` and Python compilation checks passed for the implementation
  and test files in this change.
- The repository `codex review --uncommitted` attempt was retried with a bounded
  180-second window. Its authentication token remained invalidated; it inspected
  both change-owned and unrelated baseline files but timed out without a final
  finding set. Findings from unrelated baseline changes were excluded.
- An equivalent manual review of the change-owned routing, lifecycle, lineage,
  logging, configuration, and application-boundary diff found one confirmed
  transparency issue: the controlled live-validation CLI inspected concrete
  pool/profile mappings directly. Source selection for controlled smoke tests
  was moved into `LlmConfig.controlled_source_config()`, and the CLI now supplies
  only a logical profile plus source label. Focused routing/config tests passed
  after the fix; no remaining confirmed correctness, leakage, or permit/lifecycle
  issue was found. Existing baseline modifications outside this change were
  excluded from that assessment.
- A follow-up manual review of the reproducible staged validator found that
  explicit concurrency/RPM confirmation alone did not establish whether the two
  Keys own independent quota buckets or share one account bucket. The validator
  now requires `--confirmed-quota-scope`, rejects a scope inconsistent with the
  configured provider-resource mapping, enforces strictly increasing stages,
  and stops immediately after a failed gate. Eleven focused tests pass after
  this fix.
- Independent quota ownership is now confirmed and the staged validator keeps
  each resource capped at hard concurrency 10 and 10 RPM. Provider-backed
  10-stage reruns encountered HTTP 503 responses, and the latest single-source
  preflight encountered bounded HTTP 408/timeouts on both sources. The gate
  correctly withheld 25/50 and live rollout remains incomplete.
- A later validation-only low-cap rerun kept the independent resources at Grok
  2 and Luna 1 concurrent attempts. It confirmed that borrowed dispatches must
  be excluded from the normal weighted-fairness ratio, but Luna still produced
  four HTTP 503 attempt failures. The strict gate again withheld 25/50; this is
  a provider-stability blocker, not grounds to weaken the acceptance criteria.
- The repository-required `codex review --uncommitted` was retried for this
  follow-up with a bounded 180-second window. The review token was still
  invalidated, the process spent most of the run inspecting unrelated baseline
  changes, and it timed out without a final finding set. An equivalent manual
  review was therefore limited to the staged validator, its tests, and the
  OpenSpec/requirements updates. No confirmed correctness, secret-leakage,
  resource-lifecycle, or gate-weakening issue remained. A possible separate
  per-source failover-dispatch counter was classified as a future observability
  enhancement: every current failover-triggering error already independently
  fails the strict live gate, so it cannot authorize a higher stage.
- The completion/provider-stability rerun made another bounded review attempt.
  Authentication again failed with HTTP 401, after which the process inspected
  unrelated baseline BaoStock changes. Those findings were excluded. Manual
  review of the only change-owned diff confirmed that the recorded test counts,
  provider timeouts, gate decision, and still-open task 6.5 are consistent and
  contain no secret values.

### Facade Boundary Follow-up and Resumed Live Windows (2026-08-08)

The release-gate static scan found that the controlled live benchmark still
inspected `config.profiles` and called `resource_for_profile()` while constructing
its ephemeral provider-stage configuration. This was a confirmed application
boundary issue, so concrete profile/resource mutation was moved into the public
`LlmConfig.controlled_stage_config()` facade. The benchmark now consumes only
the logical profile, source/resource summaries, and non-secret runtime limits
from `describe_logical_profile()`. The follow-up facade/benchmark tests passed
54 cases, Python compilation and `git diff --check` passed, and the repository-
wide static scan returned no application-layer concrete-profile access.

The provider windows used the same synthetic non-sensitive Chinese input:

- Grok single-source smoke passed at 10:28:57-10:29:11 with HTTP 200,
  streaming first event, structured output, usage, request IDs, and
  `pipio:grok-4.5` lineage. Luna returned two HTTP 503 responses in that
  window.
- Luna single-source smoke recovered at 10:40:20-10:40:25 with HTTP 200,
  first event, structured output, usage, request IDs, and
  `pipio:gpt-5.6-luna` lineage.
- The first post-fix provider-backed 10-stage process completed provider calls
  but exposed a validator result-aggregation `NameError` before writing a valid
  gate result. The stale `pool` reference was corrected to facade-provided
  source weights, and the live validator tests passed before retrying.
- The corrected independent-quota 10-stage run completed all 10 logical
  requests, but the strict gate rejected it for one 429 and three provider 5xx
  events. One Luna failure successfully failed over to Grok; all resources were
  released and the registry was empty. Per-source cap and identity checks passed,
  but 25/50 were withheld as required.
- A validation-only low-cap rerun (`pipio:grok=2`, `pipio:luna=1`) completed all
  10 logical requests and three successful failovers, but still recorded seven
  Luna provider 5xx events. It was rejected for `nonzero_provider_5xx` and did
  not start 25/50. These caps changed only the ephemeral validation config and
  did not modify production `config/13_llm.json` or quota claims.

At that point task 6.5 remained incomplete: a clean provider-backed 10 stage
followed by clean gated 25 and 50 stages had not yet been observed. The
acceptance thresholds and stage gating remained strict.

### Continuation Gate Attempt (2026-08-08 10:56-10:57)

The formal independent-quota run was repeated with the unchanged controlled
configuration (`10` logical concurrency, `10` confirmed per-source concurrency,
and `10` confirmed provider RPM). All `10` logical requests completed and all
`10` produced streaming first-event measurements. The strict gate rejected the
stage for one provider rate limit and four provider 5xx events; two source
failovers succeeded. No timeout, parse, schema, identity, provider-limit,
transport, file-descriptor, or shutdown/registry leak was observed. The staged
runner therefore executed only stage `10` and correctly withheld stages `25` and
`50`.

The non-secret result is retained at:

```text
/tmp/quote_llm_live_10_25_50_20260808_continuation.json
```

Immediately afterward, a bounded single-source Luna smoke made two attempts and
received HTTP `503` on both. This confirms that the remaining failure is an
external provider-stability condition rather than URL construction, routing
identity, or local pool cleanup. Production configuration was not changed.

### Offline Completion Audit Rerun (2026-08-08)

- Common gateway, orchestration, pool, routed client, routing config, live-stage
  validator, logging, and source-lineage migration tests: `162 passed`.
- CNInfo title/corporate-action/pipeline/scheduler/API/lineage compatibility:
  `144 passed`.
- Business-profile legacy adapter, extraction, runtime, async production,
  rollout, and maintenance scheduler: `137 passed`.
- Corporate-action document storage, incremental compatibility, and schema
  migration: `65 passed`.
- Python compilation, `git diff --check`, repository application-boundary scan,
  and `openspec validate add-weighted-llm-pool-routing --type change --strict
  --no-interactive`: passed.
- `codex review --uncommitted` was attempted again but the review service
  returned `HTTP 401 token_invalidated` and the bounded command timed out while
  inspecting unrelated baseline changes. An equivalent manual review of the
  change-owned files found no confirmed bug or regression; baseline
  BaoStock/backtest changes were not modified or included.

### Deployment-Owner Live-Capacity Deferral (2026-08-08)

The deployment owner explicitly accepted deferring provider-backed high-
concurrency validation because repeated controlled windows continued to return
provider 429/503 responses. This closes the implementation task on the existing
offline and controlled-failure evidence; it does not convert any failed or
unexecuted live stage into a pass.

The implementation evidence includes deterministic offline 10/25/50 load,
bounded same-source retry, cross-source failover, circuit open/half-open/recovery,
provider adaptive concurrency decrease/recovery, RPM/cooldown governance, and
resource-clean shutdown tests. The failed live result and single-source 503
evidence remain retained above. Production `config/13_llm.json` continues to
set both the global LLM switch and `shared_semantic` pool to disabled.

Provider-backed 10 -> 25 -> 50 remains an unpassed production-enablement
runbook gate. After provider recovery, operations must start again at 10 and
may proceed to 25/50 only when the preceding stage passes. Until then, no live
rollout certification is claimed.

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
| Offline regression, controlled smoke, staged rollout and gates | business compatibility and data migration/live rollout requirements; tasks 6.1-6.7; repeated provider-backed failures are retained, and the deployment owner explicitly deferred unpassed 10/25/50 production-capacity certification while keeping the production route/pool disabled |
