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
Each configured pool SHALL enforce a hard process-local `max_concurrency` across all logical profiles routed to it. The pool SHALL use a deterministic weighted-fair scheduler, such as weighted deficit round robin, with strictly positive integer member weights. Weight SHALL define long-run dispatch share among eligible healthy members, not a permanent per-member semaphore reservation.

When `borrow_idle_capacity=true`, a pool MAY dispatch to another eligible healthy member when weighted candidates are unavailable; this MUST NOT exceed pool total concurrency or a concrete member/profile limit. Scheduler state, dispatch counters, active counts, borrowing, queue wait, and member eligibility SHALL be observable in a non-secret pool snapshot. Waiting, cancellation, and shutdown SHALL not leak capacity permits.

#### Scenario: Shared pool cap spans two business profiles
- **WHEN** title classification and semantic extraction are routed to a pool with `max_concurrency=10`
- **THEN** at most ten logical LLM executions are active in that pool even if both profiles submit requests concurrently

#### Scenario: Weighted members receive proportional dispatches
- **WHEN** two healthy compatible members with weights 3 and 1 process a sustained eligible request stream without capacity constraints
- **THEN** dispatch counts converge toward a 3:1 ratio within a documented bounded scheduler tolerance

#### Scenario: Idle capacity can be borrowed
- **WHEN** a preferred weighted member is open, disabled, at its local limit, or lacks a mapping for the logical profile and another member is eligible
- **THEN** the eligible member may receive the request when borrowing is enabled while the pool total cap remains enforced

### Requirement: Provider quota coordination SHALL remain independent from routing
Pool routing SHALL not replace concrete profile or provider-resource concurrency, RPM, cooldown, or adaptive congestion controls. A pool admission covers one logical execution, including retry, repair, and failover; a provider coordinator lease SHALL cover only one concrete transport attempt. The pool SHALL not report schema-validation failures as provider congestion, and each concrete failure SHALL be reported to provider coordination at most once.

Provider resources SHALL be configured from verified quota buckets rather than inferred from matching Base URLs. The Pipio Grok and Luna profiles SHALL use explicit non-secret resources that can be configured as shared or independent after controlled validation.

#### Scenario: Shared URL does not force a shared quota resource
- **WHEN** two concrete profiles use the same Pipio Base URL but quota validation shows independent keys
- **THEN** configuration may assign distinct provider resources and each resource maintains its own limits

#### Scenario: One logical request does not double count a pool permit on failover
- **WHEN** a request changes from one concrete member to another after a classified failure
- **THEN** it retains its original pool execution permit until the final success or terminal error

### Requirement: Failover SHALL be bounded by one logical execution deadline
The gateway SHALL attempt bounded same-source retries and schema repair before cross-source failover. A route SHALL use one absolute execution deadline after its first transport-send admission; it SHALL apply to same-source retry, backoff, repair, member selection, failover, and all subsequent concrete attempts. A new attempt MUST NOT begin when remaining execution budget is below configured minimum attempt budget.

Failover SHALL require a terminal classified error in configured `failover.on`, an untried eligible member, remaining deadline, and an unused `max_hops` budget. Configuration, route validation, cancellation, and deadline failures MUST NOT fail over. Authentication failures MUST NOT fail over unless `allow_auth_failover=true`; that exceptional path SHALL emit a high-priority operational signal.

#### Scenario: First source timeout leaves bounded failover time
- **WHEN** the first concrete source consumes part of a 60-second logical execution deadline and then fails with a retryable timeout
- **THEN** the next eligible source receives only the remaining budget, not a new 60-second timeout

#### Scenario: No eligible source yields a classified terminal error
- **WHEN** all compatible members are unavailable, open, already attempted, or have exhausted the hop/deadline budget
- **THEN** the gateway returns or raises a classified fail-closed route error with safe attempt lineage and no business data

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
