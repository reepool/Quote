## ADDED Requirements

### Requirement: Common gateway with workload-specific bounds
The system SHALL use the common LLM gateway for company-profile semantic extraction while applying explicit company-profile input, item, output, and timeout bounds.

#### Scenario: Company-profile request is submitted
- **WHEN** deterministic extraction requires semantic structured fallback
- **THEN** the request uses the common gateway with company-profile workload metadata and explicit bounds

### Requirement: Evidence-gated row isolation
The system SHALL validate every structured row against exact filing evidence and SHALL preserve valid rows without accepting invalid rows from the same response.

#### Scenario: Mixed valid and invalid rows
- **WHEN** a schema-valid response contains at least one exact-evidence-supported row and at least one unsupported row
- **THEN** the supported rows are retained and the unsupported rows are recorded as bounded machine-rework diagnostics

#### Scenario: No valid rows survive
- **WHEN** a response is empty or every row fails local evidence validation
- **THEN** the field family remains incomplete and is routed to machine rework

### Requirement: Safe semantic diagnostics
The system SHALL persist safe structured diagnostics for every failed or partially accepted company-profile semantic attempt.

#### Scenario: Provider timeout or validation failure
- **WHEN** the gateway times out or local JSON, schema, scope, evidence, numeric, or unit validation fails
- **THEN** the checkpoint or exception records a stable failure category, safe request identity, attempt count, usage when available, finish reason, warnings, and bounded validation details without raw content or credentials

### Requirement: Provider output-budget visibility
The system SHALL expose when a provider reports output usage above the requested company-profile limit without truncating a potentially valid JSON response.

#### Scenario: Provider exceeds requested output tokens
- **WHEN** reported output usage is greater than the request limit
- **THEN** the semantic audit records the provider budget-overrun warning and observed usage for operational retry and model selection decisions
