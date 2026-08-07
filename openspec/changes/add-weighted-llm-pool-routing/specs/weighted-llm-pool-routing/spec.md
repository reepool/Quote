## ADDED Requirements

### Requirement: Logical profiles SHALL resolve through configured pools without exposing concrete profiles
The common LLM gateway SHALL treat `LlmRequest.profile` as a stable logical business profile. A configured logical route SHALL resolve that profile to an enabled pool and then to one compatible concrete profile. Concrete profiles SHALL retain provider URL, model, API-key environment variable, output capabilities, and provider-resource mapping. A logical profile with no route SHALL retain the existing direct-profile behavior for backward compatibility.

Business, research, scheduler, API, and CLI modules MUST obtain enablement, non-secret description, effective limits, and route fingerprint through a public `utils.llm` logical-profile facade. They MUST NOT read `llm_config.profiles`, resolve concrete profile names, API-key environment variables, provider URLs, models, or provider resources.

#### Scenario: Existing logical request remains valid after route enablement
- **WHEN** an existing caller submits `LlmRequest(profile="semantic_extraction")` and a route for that logical profile is enabled
- **THEN** `LlmClient.complete()` selects a compatible concrete source internally and the caller does not supply a concrete profile name

#### Scenario: Unrouted legacy profile remains compatible
- **WHEN** a profile has no configured route
- **THEN** the gateway follows the existing direct-profile execution path and returns a response compatible with the pre-route contract

#### Scenario: Application code cannot inspect a concrete profile
- **WHEN** application code needs to determine whether `semantic_extraction` is usable or form a runtime identity
- **THEN** it uses the public logical-profile facade and its route fingerprint without accessing `llm_config.profiles`

### Requirement: A pool SHALL enforce total logical concurrency and deterministic weighted fairness
Each configured pool SHALL enforce a hard process-local `total_concurrency` across all logical profiles routed to it. The pool SHALL use a deterministic weighted-fair scheduler, such as weighted deficit round robin, with strictly positive integer member weights. Weight SHALL define long-run dispatch share among eligible healthy members, not a permanent per-member semaphore reservation.

When `borrow_idle_capacity=true`, a pool MAY dispatch to another eligible healthy member when weighted candidates are unavailable; this MUST NOT exceed pool total concurrency or a concrete member/profile limit. Scheduler state, dispatch counters, active counts, borrowing, queue wait, and member eligibility SHALL be observable in a non-secret pool snapshot. Waiting, cancellation, and shutdown SHALL not leak capacity permits.
Weight acceptance SHALL use non-borrowed normal dispatch counts. Total dispatches and per-source borrowed dispatch counts SHALL be recorded separately and MUST NOT cause borrowed capacity to be misclassified as a weighted-fairness violation.

#### Scenario: Shared pool cap spans two business profiles
- **WHEN** title classification and semantic extraction are routed to a pool with `total_concurrency=10`
- **THEN** at most ten logical LLM executions are active in that pool even if both profiles submit requests concurrently

#### Scenario: Weighted members receive proportional dispatches
- **WHEN** two healthy compatible members with weights 3 and 1 process a sustained eligible request stream without capacity constraints
- **THEN** dispatch counts converge toward a 3:1 ratio within a documented bounded scheduler tolerance

#### Scenario: Idle capacity can be borrowed
- **WHEN** a preferred weighted member is open, disabled, at its local limit, or lacks a mapping for the logical profile and another member is eligible
- **THEN** the eligible member may receive the request when borrowing is enabled while the pool total cap remains enforced

### Requirement: Provider quota coordination SHALL remain independent from routing
Pool routing SHALL not replace concrete profile or provider-resource concurrency, RPM, cooldown, or adaptive congestion controls. A pool admission covers one logical execution, including retry, repair, and failover; a provider coordinator lease SHALL cover only one concrete transport attempt. The pool SHALL not report schema-validation failures as provider congestion, and each concrete failure SHALL be reported to provider coordination at most once.

Provider resources SHALL be configured from verified quota buckets rather than inferred from matching Base URLs. The deployment owner has confirmed that the Pipio Grok and Luna API Keys have independent quota, so the initial profiles SHALL use two explicit non-secret provider resources with independent concurrency, RPM, cooldown, and adaptive congestion state. The configuration model SHALL retain support for verified shared quota mappings for future credentials, but the initial Grok/Luna deployment MUST NOT merge its two resources merely because their Base URL matches.

Effective in-flight capacity SHALL remain the minimum of business-stage worker capacity, pool `total_concurrency`, selected concrete-profile concurrency, current provider-resource adaptive concurrency, and HTTP connection-pool capacity. Pool member weight SHALL remain separate from existing provider `workload_weights`; history backfill, daily work, title classification, body extraction, and semantic verification SHALL retain their workload identity and existing fair admission within a shared provider resource. Runtime snapshots SHALL identify the active limiting layer.

#### Scenario: Shared URL does not force a shared quota resource
- **WHEN** two concrete profiles use the same Pipio Base URL but quota validation shows independent keys
- **THEN** configuration may assign distinct provider resources and each resource maintains its own limits

#### Scenario: One logical request does not double count a pool permit on failover
- **WHEN** a request changes from one concrete member to another after a classified failure
- **THEN** it retains its original pool execution permit until the final success or terminal error

#### Scenario: Pool weight does not replace workload fairness
- **WHEN** multiple business workloads share one provider resource through a weighted LLM pool
- **THEN** source selection uses pool member weight while provider admission independently uses the existing workload identity and workload weights

### Requirement: Failover SHALL be bounded by one logical execution deadline
The gateway SHALL attempt bounded same-source retries and schema repair before cross-source failover. A route SHALL use one absolute execution deadline after its first transport-send admission; it SHALL apply to same-source retry, backoff, repair, member selection, failover, and all subsequent concrete attempts. Each transport attempt timeout SHALL be the minimum of the selected concrete profile attempt timeout, request override, and remaining route budget. A new attempt MUST NOT begin when remaining execution budget is below configured minimum attempt budget.

Failover SHALL require a terminal classified error in configured `failover.on`, an untried eligible member, remaining deadline, and an unused `max_hops` budget. Configuration, route validation, cancellation, and deadline failures MUST NOT fail over. Authentication failures MUST NOT fail over unless `allow_auth_failover=true`; that exceptional path SHALL emit a high-priority operational signal.

#### Scenario: First source timeout leaves bounded failover time
- **WHEN** the first concrete source consumes part of a 60-second logical execution deadline and then fails with a retryable timeout
- **THEN** the next eligible source receives only the remaining budget, not a new 60-second timeout

#### Scenario: No eligible source yields a classified terminal error
- **WHEN** all compatible members are unavailable, open, already attempted, or have exhausted the hop/deadline budget
- **THEN** the gateway returns or raises a classified fail-closed route error with safe attempt lineage and no business data

### Requirement: Logical identity and idempotency SHALL remain stable across source selection
The logical request identity SHALL include business input, logical profile, route fingerprint, prompt/schema/parser versions, and caller metadata used by the existing runtime identity contract. It MUST NOT include the concrete source selected for one execution. The actual-attempt identity SHALL additionally include selected profile, source label, actual model, and attempt sequence.

The deterministic route fingerprint SHALL cover normalized pool/route configuration, member source labels, logical-to-concrete mappings, and output-contract-relevant concrete-profile capabilities. It MUST exclude API-key values, current health/circuit state, runtime counters, and the random/current source selection. Changing route capability or revision SHALL change the fingerprint; changing only ephemeral runtime state SHALL not.

The caller idempotency key SHALL remain unchanged across same-source retry and cross-source failover. Local and provider request IDs SHALL remain distinct for every actual attempt, and the gateway MUST NOT claim persistent idempotency across providers or credentials.

#### Scenario: Failover does not create a new business input identity
- **WHEN** equivalent business input succeeds once on Grok and once after failover to Luna under the same route configuration
- **THEN** both executions have the same logical input/runtime identity while their actual-attempt identities and source lineage differ

#### Scenario: Route capability change invalidates reusable runtime identity
- **WHEN** member mappings or output-contract capabilities change while ephemeral health and counters do not participate
- **THEN** the route fingerprint and dependent runtime identity change so old checkpoints are not silently reused

#### Scenario: Idempotency key is preserved across attempts
- **WHEN** one logical request retries and fails over between concrete members
- **THEN** every attempt retains the caller idempotency key while recording its own local and provider request IDs

### Requirement: Pool member health SHALL use a circuit breaker separate from quota control
Each pool member SHALL maintain `closed`, `open`, and `half_open` circuit states. Configured classified failures SHALL open the circuit after its threshold; after cooldown, only the configured bounded half-open probe capacity may be admitted. A successful probe SHALL close and reset the circuit; a failed probe SHALL reopen it. Circuit state SHALL be testable with an injected fake clock and observable without secrets.

#### Scenario: Open member is skipped during routing
- **WHEN** a member circuit is open and another compatible healthy member exists
- **THEN** new logical requests are dispatched to the healthy member rather than the open member

#### Scenario: Half-open probe recovers a member
- **WHEN** an open member's cooldown expires and its permitted probe succeeds
- **THEN** the member changes to closed and resumes weighted eligibility

### Requirement: Public envelopes SHALL identify the selected source without contaminating business schemas
`LlmResponse` and classified terminal errors SHALL expose `logical_profile`, `selected_profile`, and stable non-secret `source_label`. Route lineage SHALL include pool identity, route fingerprint, failover count, and redacted per-attempt facts sufficient for audit, including selected profile/source/model, timing, error category, and request identifiers where available. It MUST NOT include API keys, authorization values, full sensitive prompts, or raw provider diagnostics by default.

The gateway SHALL NOT inject source metadata into `response.data` or a business JSON schema. Business adapters SHALL copy public lineage into their own candidate/audit/persistence envelopes. Historical stored records without source information SHALL remain readable and represent the source as null or `legacy_unknown`; implementations MUST NOT infer it from an editable model string.

#### Scenario: Successful response names the actual LLM source
- **WHEN** a logical semantic request is served by the Grok member
- **THEN** the public response has `logical_profile="semantic_extraction"`, the configured concrete profile, and `source_label="pipio:grok-4.5"`

#### Scenario: Source data is absent from validated business JSON
- **WHEN** a business adapter validates an LLM JSON payload against its versioned schema
- **THEN** source label and route lineage are available only in the outer gateway/business envelope and are not required JSON fields

### Requirement: Configuration and environment migration SHALL be explicit and non-secret
The LLM configuration file SHALL be renamed from `config/11_llm.json` to `config/13_llm.json`; all repository documentation, templates, tests, and tooling references SHALL be updated. The JSON configuration SHALL name only environment-variable identifiers, never API-key values.

The initial Pipio concrete profiles SHALL use `QUOTE_LLM_PIPIO_GROK_API_KEY` for `grok-4.5` and `QUOTE_LLM_PIPIO_LUNA_API_KEY` for `gpt-5.6-luna`. Deployment migration SHALL inject both variables before enabling the route and SHALL preserve `load_project_environment(override=False)` behavior. The former generic key MAY be removed only after configured routes and controlled smoke validation succeed.

#### Scenario: Missing one concrete key fails only its member
- **WHEN** the Luna environment variable is absent while the Grok member is configured and healthy
- **THEN** configuration diagnostics identify Luna as unavailable without exposing secrets, and an enabled route can use Grok if policy permits

#### Scenario: Configuration file discovery remains deterministic
- **WHEN** the renamed LLM JSON is loaded with other project JSON configuration files
- **THEN** lexical discovery and shallow top-level merge continue to load `llm` independently of the futures configuration file

### Requirement: Existing LLM business integrations SHALL be checked before release
Implementation acceptance SHALL include a repository-wide static scan of `LlmClient`, `LlmClientProtocol`, `LlmRequest`, `llm_config.profiles`, concrete profile access, and legacy configuration-file references. It SHALL include offline fake-transport regression tests for the following existing integrations: CNInfo announcement-title classification; CNInfo corporate-action extraction, independent verification, async pipeline, resume and persistence; business-profile semantic extraction, runtime identity, production rollout, async production and legacy adapter; application lifecycle; scheduler and production scripts; common-gateway live-validation/benchmark scripts in non-network mode.

Controlled live smoke tests, when credentials are explicitly supplied, SHALL test each source separately before any two-source load test. Normal unit tests MUST require neither keys nor network access.

#### Scenario: Existing business callers use the routed public gateway
- **WHEN** the full offline compatibility matrix is run with a two-member fake pool
- **THEN** each business workflow submits its stable logical profile through the public client and preserves candidate-only/evidence-gated behavior

#### Scenario: Static scan prevents configuration leakage
- **WHEN** release validation scans application-layer modules
- **THEN** no production business module directly reads `llm_config.profiles` or concrete provider/model/key configuration

### Requirement: Route configuration SHALL be validated completely and fail closed
Pool and route names SHALL be non-empty and unique in their scopes. `total_concurrency`, `queue_size`, and member `weight` SHALL accept only non-boolean positive integers, and the initial implementation SHALL accept only `weighted_fair` strategy. Source labels SHALL be non-secret and unique within a pool. A route SHALL reference an enabled pool, and every pool member SHALL map that logical profile to an existing enabled concrete profile whose source label matches the member.

Every selectable concrete profile SHALL explicitly retain its structured-output, stream, timeout, retry, output-token, local concurrency, RPM, API-key environment, model, URL, and provider-resource settings. All members for a logical profile SHALL satisfy its required structured-output contract. Pool total concurrency MUST NOT exceed the project hard limit. Routing MUST preserve provider-resource/profile/workload RPM inheritance, and configuration MUST NOT use multiple resource names to bypass one verified shared quota bucket. A route/concrete-profile name collision and multiple project JSON owners of the top-level `llm` key SHALL fail configuration validation before network or scheduler startup.

#### Scenario: Boolean and fractional limits are rejected
- **WHEN** `total_concurrency`, `queue_size`, or `weight` is a boolean, zero, negative, or fractional value
- **THEN** configuration loading fails with a classified non-secret validation error

#### Scenario: Incompatible concrete member is rejected
- **WHEN** a member profile is missing, disabled, has a mismatched source label, or lacks the logical profile's required structured-output capability
- **THEN** the route fails validation before it can accept a request

#### Scenario: Shared quota cannot be split to evade limits
- **WHEN** two credentials are verified to consume one upstream quota bucket but configuration maps them to independent resource names
- **THEN** configuration validation fails rather than allowing aggregate concurrency or RPM to exceed that bucket

### Requirement: LLM observability and logs SHALL use the system logging architecture
All common LLM client, transport, pool, circuit, and provider-coordination logging SHALL use the existing `LLM` logger managed by `utils/logging_manager.py` and `config/01_log.json`. The implementation SHALL reuse system formatting, console behavior, task-domain file routing, module-level controls, and configured rotation; it MUST NOT create a separate handler, log directory, or rotation subsystem. The `LLM` logger SHALL be explicitly configurable in the system module logging configuration.

Detailed process events SHALL use `DEBUG`, including queue operations, member eligibility/exclusion and weighted scheduler state, borrowing, lease waits/releases, retry/repair/backoff planning, remaining deadline, half-open probe details, snapshot collection, and shutdown cleanup. Important lifecycle and routing nodes SHALL use `INFO`, including pool/registry startup and shutdown, route admission, source selection, first provider attempt, failover selection, route completion/exhaustion, and circuit open/half-open/recovery. Recoverable provider/schema/deadline and degraded-source conditions SHALL use `WARNING`; terminal configuration, all-source, lifecycle, lease/state-integrity, and internal-contract failures SHALL use `ERROR` with safe exception context.

Logs SHALL use stable event names and parameterized non-secret fields. As available, events SHALL include logical profile, pool, source label, selected profile, local/provider request ID, request hash, workload, run/stage/business item, attempt/failover count, queue/elapsed/remaining timing, and classified error code. Logs MUST NOT include API keys, Authorization, Cookie, complete document text, complete prompts/responses, or raw provider error bodies.

The pool snapshot SHALL report configured and effective concurrency plus the active bottleneck; active/waiting/oldest wait; per-source weight, dispatch, active, waiting, ratio, success, error, 429, 5xx, timeout, parse, and schema counts; circuit/cooldown/probe state; failover requested/succeeded/exhausted by category; queue/execution/failover/total latency; logical/workload/run/stage/business-item correlation; and provider-resource concurrency/RPM/cooldown state.

#### Scenario: Process detail is available only at debug level
- **WHEN** an eligible request waits, is evaluated by the weighted scheduler, borrows capacity, retries, or releases a lease
- **THEN** the `LLM` logger emits parameterized `DEBUG` events and does not promote those routine details to `INFO`

#### Scenario: Important route nodes are visible at info level
- **WHEN** a route is admitted, selects or changes source, completes or exhausts, or a circuit changes state
- **THEN** the `LLM` logger emits an `INFO` event carrying safe correlation and source fields

#### Scenario: Logs remain redacted
- **WHEN** configuration, a prompt, or a provider response contains credentials or document text and the route succeeds or fails
- **THEN** captured DEBUG/INFO/WARNING/ERROR records contain none of those secrets or complete content

### Requirement: Business compatibility SHALL preserve each existing workflow contract
CNInfo announcement-title regression SHALL preserve stable logical profile use, batch chunking, isolated retry, out-of-order item identity, per-event source lineage, failover without item cross-wiring, business schema, and applicability validation. Corporate-action extraction and independent verification SHALL preserve distinct source lineage joined by the same `source_event_key`, route-fingerprint input identity, resume/completed-result reuse without duplicate analysis, deterministic evidence gates, unchanged automatic-promotion rules, and compatible database/audit queries.

Business-profile regression SHALL cover atomic activity/relationship extraction, structured-table extraction, and independent verification through logical profiles. It SHALL preserve route-fingerprint runtime identity, `SemanticRunAudit`/artifact source, checkpoint/resume, rework, promotion manifest, source revision, network kill switch, scope/candidate/release gates, and the distinction between business structured-source fallback and LLM source failover. The legacy adapter SHALL preserve synchronous and asynchronous APIs, injected fake clients, source envelope, shutdown behavior, and no independent provider configuration.

Application lifecycle validation SHALL prove one shared pool/coordinator in one application lifetime, `load_project_environment(override=False)` precedence, logical-profile selection and source reporting in live validation, benchmark redaction, and shutdown with no waiting tasks, permits, sockets, HTTP clients, or coordinator/circuit state leaked.

#### Scenario: Title routing preserves item identity
- **WHEN** title chunks complete out of order and one item fails over
- **THEN** each event retains its own result, applicability decision, and actual-source lineage without cross-wiring

#### Scenario: Corporate-action resume remains idempotent across sources
- **WHEN** an extraction or verifier resumes after a different concrete source was previously selected
- **THEN** route-fingerprint identity reuses the correct completed candidate and does not create a duplicate analysis or relax promotion gates

#### Scenario: Structured business fallback is not model failover
- **WHEN** business-profile structured-source fallback and an LLM member failover occur in the same run
- **THEN** runtime audit records them as separate mechanisms and all existing scope, evidence, and release gates remain enforced

### Requirement: Data migration, rollback, and live rollout SHALL be gated and non-destructive
Source-lineage database and artifact migrations SHALL be idempotent, repeatable, backward-readable, and rollback-tested. They MUST NOT overwrite or delete existing analyses. Historical missing source values SHALL remain null or `legacy_unknown`. Disabling or rolling back a route/pool SHALL preserve already stored source and attempt lineage, and APIs, task reports, and audit queries that expose an LLM result SHALL include its source label.

Controlled live validation SHALL be explicitly enabled and SHALL use the same synthetic non-sensitive Chinese input for single-source comparison. Each source SHALL first verify authentication, actual model, streaming/first-event behavior, usage, structured output, timeout, and quota-resource mapping. A two-source test SHALL then exercise logical routing and explicit member disablement/failure before staged 10, 25, and 50 concurrency levels.

Each level SHALL record success, 429/5xx, first-event and total latency, dispatch ratio, failover, memory, connection count, and shutdown duration. A level MUST NOT proceed when the preceding level fails its acceptance thresholds. Live outputs SHALL remain candidates and MUST NOT bypass quality holdout, evidence, or promotion gates.

#### Scenario: Reapplying and rolling back a migration preserves history
- **WHEN** the source-lineage migration is applied twice and then its rollback path is exercised against legacy and new records
- **THEN** no existing analysis or lineage is overwritten or deleted and all historical records remain readable

#### Scenario: Failed load stage stops promotion
- **WHEN** the 10- or 25-concurrency stage violates its configured success, quota, latency, resource, or shutdown thresholds
- **THEN** validation records the failure and does not start the next concurrency stage
