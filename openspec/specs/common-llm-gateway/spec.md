# common-llm-gateway Specification

## Purpose

The common LLM gateway provides a provider-neutral, fail-closed asynchronous contract for semantic-analysis workflows. It centralizes configuration, transport, structured-output validation, retry/deadline governance, rate limits, redacted observability, and request lineage while leaving business semantics and candidate approval to domain adapters.

## Requirements

### Requirement: Profile configuration is non-secret and fail-closed
The gateway SHALL load named logical and concrete profiles from the project configuration, keep provider/base URL/model/capability settings configurable, default every concrete profile to disabled, and reject a call before network I/O when its resolved route/profile is disabled or required configuration is invalid. A logical profile MAY resolve through a configured pool to a concrete profile, or use the legacy direct profile path when no route exists. API keys MUST be resolved only from the environment variable named by the selected concrete profile's `api_key_env`; the gateway MUST reject missing keys and MUST NOT support anonymous fallback. Business callers MUST NOT read concrete profile configuration directly.

#### Scenario: Disabled profile makes no request
- **WHEN** a caller invokes `complete` for a logical profile whose direct profile is disabled or whose route has no eligible enabled concrete profile
- **THEN** the gateway raises a classified configuration/disabled error before calling the transport

#### Scenario: Missing key fails closed
- **WHEN** an enabled selected concrete profile names `QUOTE_LLM_SCORPIO_GROK_API_KEY` but that environment variable is absent or empty
- **THEN** the gateway raises `authentication_error` before calling the transport

#### Scenario: URL normalization avoids duplicate version paths
- **WHEN** a selected concrete profile base URL includes or omits a trailing `/v1` and its endpoint is `/v1/chat/completions`
- **THEN** the resolved URL contains exactly one `/v1` and only `http` or `https` schemes are accepted

### Requirement: The gateway SHALL expose a stable asynchronous request and response contract
The public package SHALL expose `LlmClient.complete(request: LlmRequest)`, where a request supports a stable logical profile, role/content messages, optional JSON Schema or Pydantic schema, schema name/version, model/temperature/max output token overrides, a business timeout/deadline, idempotency key, and non-prompt metadata. A response SHALL include status, validated data, controlled raw content, provider/model, finish reason, usage, local/provider request IDs, request/response hashes, schema lineage, actual output mode, latency, attempt count, and warnings. It SHALL additionally include `logical_profile`, `selected_profile`, `source_label`, and safe route-attempt lineage when routing is used; those new fields SHALL have compatible defaults for direct-profile and existing test fixtures.

#### Scenario: Two schemas share one client without state leakage
- **WHEN** two concurrent calls use different schema names and versions on the same client
- **THEN** each response validates against its own schema and contains matching lineage without cross-call fields or validators

#### Scenario: Missing upstream optional fields are explicit
- **WHEN** the provider omits usage, provider request ID, or finish reason
- **THEN** the response carries null for the missing field and a warning, never a fabricated value

#### Scenario: Metadata is not sent to the model
- **WHEN** a request includes tracing metadata
- **THEN** metadata is available in local envelope/audit context but absent from the serialized provider messages and prompt

#### Scenario: Routed response includes source identity outside business data
- **WHEN** a routed request succeeds after one or more concrete-source attempts
- **THEN** its response identifies the logical and selected profile, source label, route fingerprint, and safe attempts without adding fields to the validated business data

### Requirement: Structured output SHALL be selected and validated conservatively
The gateway SHALL support `json_schema`, `json_object`, `prompt_only`, and `auto` modes. Native JSON Schema SHALL be preferred when explicitly configured; `json_object` and `prompt_only` SHALL perform local JSON parsing and complete schema validation; `prompt_only` SHALL require explicit profile permission. `auto` SHALL select only from explicit capabilities or a cached capability result and MUST NOT blindly trial modes on every request.

#### Scenario: Native schema mode sends strict schema
- **WHEN** a profile allows `json_schema` and a request supplies a schema
- **THEN** the provider payload contains the normalized strict schema and the returned object passes local validation

#### Scenario: JSON object mode rejects invalid nested output
- **WHEN** a provider returns JSON with an invalid nested field or missing required property in `json_object` mode
- **THEN** the gateway classifies the failure as `schema_validation_error` after bounded retry/repair and does not return business data

#### Scenario: Prompt-only mode is opt-in
- **WHEN** a request selects `prompt_only` for a profile that does not explicitly allow it
- **THEN** the gateway rejects the request as a configuration error without a provider call

#### Scenario: Repair cannot invent a value
- **WHEN** schema validation fails and one repair attempt is allowed
- **THEN** the repair input contains the original response and validation errors, and a repair that still fails causes a classified failure rather than field deletion or guessed values

### Requirement: Retries and profile-level limits SHALL be bounded and classified
The gateway SHALL retry only transient transport/DNS/timeout failures, HTTP 408/429, explicitly retryable 5xx, and bounded response parse/schema failures. It SHALL NOT retry configuration, 400, 401, 403, or 404 failures. Concurrency and requests-per-minute limits SHALL be shared by profile, and all waits and transport attempts SHALL honor the request deadline and cancellation.

#### Scenario: Rate limit retries with backoff
- **WHEN** the provider returns 429 and retry budget and deadline remain
- **THEN** the gateway waits a bounded Retry-After/exponential backoff and retries, recording attempt count and rate-limit classification

#### Scenario: Authentication is not retried
- **WHEN** the provider returns 401 or 403
- **THEN** the gateway raises `authentication_error` without another provider request

#### Scenario: Shared concurrency cap
- **WHEN** more calls for one profile are submitted than its `max_concurrency`
- **THEN** excess calls wait on the shared profile limiter and no more than the configured number is in flight

#### Scenario: Cancellation and deadline are fail-closed
- **WHEN** a caller cancels while waiting, backing off, or performing I/O, or the overall deadline expires
- **THEN** the gateway stops further attempts and raises `cancelled` or `deadline_exceeded` without returning partial data

### Requirement: Security and audit output SHALL be redacted and traceable
The gateway MUST never log or include in errors an API key, Authorization header, Cookie, or complete sensitive prompt/response by default. It SHALL generate stable request/response hashes, a local request ID, optional provider request ID, idempotency key lineage, latency, usage, model, schema versions, and a categorized error. Requests containing untrusted document text SHALL be accompanied by a caller-owned system instruction that treats document text as data rather than executable instructions.

#### Scenario: Secret does not appear in diagnostics
- **WHEN** a transport fails after receiving an Authorization header
- **THEN** logs, exceptions, and response warnings contain neither the key nor the complete header

#### Scenario: Hashes are stable for equivalent requests
- **WHEN** equivalent requests are serialized with different dictionary ordering or local trace metadata
- **THEN** they produce the same request hash, while a changed message/schema produces a different hash

#### Scenario: Untrusted text requires safety context
- **WHEN** a business adapter submits document text marked as untrusted without the required system safety instruction
- **THEN** the gateway rejects the request before network I/O

### Requirement: Business adapters SHALL own semantics and remain candidate-only
The gateway SHALL not import or implement financial/business modules, prompts, document retrieval, OCR, evidence approval, database writers, schedulers, or DCF decisions. The business-profile adapter SHALL retain section/page/text hashes, its versioned business schema and prompt, and candidate-only/evidence validation while delegating generic routing, HTTP, authentication, retries, response parsing, and source lineage to the public gateway. Business adapters SHALL use the gateway's public logical-profile facade rather than actual profile configuration.

#### Scenario: Business schema remains responsible for semantic validation
- **WHEN** a syntactically valid response contains an unknown fact catalog field or invalid evidence reference
- **THEN** the business adapter rejects it after the gateway returns schema-valid data

#### Scenario: LLM cannot overwrite production facts
- **WHEN** a business adapter receives a successful gateway response
- **THEN** the result remains a candidate envelope and no public gateway path writes a financial fact or DCF input

#### Scenario: Offline tests use fake transport
- **WHEN** common gateway or business adapter unit tests execute
- **THEN** they inject a fake transport and require no live model, API key, or internet access

### Requirement: Repair payloads preserve provider-compatible structured output
When a locally parsed response fails JSON or schema validation and the configured bounded repair allowance remains, the gateway MUST keep the original structured-output mode, response schema, token field, deadline, and request lineage. It MUST submit the complete prior response and validation error in one appended user correction envelope and MUST NOT append a new system message after an assistant response. The repaired response MUST pass the unchanged local schema or end as a typed failure.

#### Scenario: JSON-object response requires one repair
- **WHEN** a `json_object` provider returns content that fails local schema validation and one repair is allowed
- **THEN** the second physical request keeps `response_format={"type":"json_object"}` and appends one user correction envelope containing the prior response and validation error
- **AND** no post-assistant system message or relaxed schema is sent

#### Scenario: Repair is rejected by the provider
- **WHEN** the corrected repair payload receives a non-retryable provider rejection or exhausts its bounded attempts
- **THEN** the gateway returns the typed provider failure with request and attempt lineage
- **AND** it does not return the invalid original content or start another repair loop

### Requirement: Output-budget diagnostics distinguish valid excess from truncation
The gateway MUST decide response success from local parsing and schema validation, not from provider-reported token usage alone. A locally valid response whose reported output usage exceeds the requested budget MUST remain successful and carry `provider_output_budget_exceeded_valid_response`. Content that ends for a token-limit reason and cannot pass local parsing or schema validation MUST be classified as `response_truncated`, with safe requested/observed budget and finish-reason diagnostics when available. Missing usage MUST remain unknown and MUST NOT be fabricated as an excess.

#### Scenario: Provider exceeds the requested budget but returns valid JSON
- **WHEN** provider usage reports more output tokens than requested and the complete response passes local JSON and schema validation
- **THEN** the gateway returns success with `provider_output_budget_exceeded_valid_response`
- **AND** it does not retry, discard, or silently relabel the response as truncated

#### Scenario: Token-limit response is incomplete
- **WHEN** the provider finish reason indicates a token limit and the returned content is incomplete or schema-invalid
- **THEN** the final failure is typed `response_truncated` after the existing bounded repair policy
- **AND** diagnostics preserve the finish reason and available budget data without exposing full prompt content

### Requirement: Routed execution reserves bounded failover time
When a logical route has failover enabled and another hop remains, the gateway MUST bound the
current physical source attempt so it does not consume the entire logical execution deadline.
An attempt timeout caused by that bound MUST remain a transient, lineage-recorded source failure
eligible for the configured next member. All source attempts MUST continue sharing one absolute
logical deadline and MUST NOT exceed the configured hop limit.

#### Scenario: First source stalls within a multi-model route
- **WHEN** the first selected source does not respond before its bounded routed-attempt window and another eligible hop remains
- **THEN** the gateway records a transient attempt timeout and selects another eligible member with the remaining logical budget

#### Scenario: No alternate hop remains
- **WHEN** failover is disabled, the hop limit is exhausted, or the remaining time is below the configured useful-attempt minimum
- **THEN** the gateway does not start another source attempt and returns the authoritative typed failure
