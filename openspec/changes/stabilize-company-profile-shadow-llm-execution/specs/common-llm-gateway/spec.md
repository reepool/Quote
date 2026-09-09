## ADDED Requirements

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
