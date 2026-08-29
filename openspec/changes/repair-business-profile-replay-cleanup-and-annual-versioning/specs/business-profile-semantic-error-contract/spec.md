## ADDED Requirements

### Requirement: Anonymous relationship typing
The system MUST distinguish ordinary anonymous relationships from anonymous concentration facts. Ordinary anonymous contracts MAY omit `disclosed_share`; concentration facts MUST include a finite `disclosed_share`.

#### Scenario: Anonymous contract
- **WHEN** a relationship names a masked counterparty such as `客户 A(1)` and is supported by a contract row
- **THEN** it is accepted as an ordinary relationship without requiring `disclosed_share`

#### Scenario: Anonymous concentration
- **WHEN** a relationship is labeled as a top-five customer or supplier concentration fact
- **THEN** missing or invalid `disclosed_share` produces a deterministic semantic validation failure

### Requirement: Error taxonomy preservation
The system MUST preserve separate reason codes for business-rule validation, schema validation, evidence provenance, unit normalization, and provider congestion. Worker layers MUST NOT rewrite deterministic semantic failures as `gateway_failure`.

#### Scenario: Business validation failure
- **WHEN** conversion rejects a validly received model response because of a local business rule
- **THEN** the work item is classified as machine rework with the business-rule reason and is not retried as provider congestion

#### Scenario: Gateway failure
- **WHEN** the provider returns a retryable transport, timeout, or rate-limit failure
- **THEN** the work item is classified as gateway failure and receives the configured bounded backoff retry

### Requirement: Batch response identity validation
The system MUST validate that each verification response contains exactly one decision for every requested target ID, without duplicates or unknown IDs, and MUST report omissions as schema failure rather than gateway failure.

#### Scenario: Missing target decision
- **WHEN** a model response omits one requested target ID
- **THEN** the batch is classified as schema failure and only the missing target is reworked

#### Scenario: Duplicate target decision
- **WHEN** a model response returns the same target ID more than once
- **THEN** the batch is classified as schema failure and no gateway retry is scheduled
