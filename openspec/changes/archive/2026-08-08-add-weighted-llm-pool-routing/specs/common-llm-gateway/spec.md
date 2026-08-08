## MODIFIED Requirements

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
