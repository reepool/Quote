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

### Requirement: Reachable structured-language handling
The structured extraction language contract MUST have one reachable behavior. If strict language validation is intended to trigger a repair request, the language exception MUST escape row-level validation and the repair request MUST be executable. If row-level soft rejection is intended, invalid summaries MUST be retained as typed row diagnostics and the dead document-level repair branch MUST be removed. Adding an unused function parameter alone is not sufficient.

#### Scenario: English-only structured summary
- **WHEN** a structured row contains an English-only `semantic_summary_zh`
- **THEN** the configured strict-repair or soft-rejection path is actually executed and its audit records the outcome without a `TypeError`

### Requirement: Share normalization has one owner
`disclosed_share` MUST have one canonical internal unit of fraction. A value already in fraction form MUST NOT be divided by 100 again merely because a model also supplied `%`; percent-form values MUST be converted exactly once, with the source and normalized values recorded.

#### Scenario: Fraction with percent label
- **WHEN** the model returns `disclosed_share=0.352` and `disclosed_share_unit="%"`
- **THEN** the system either rejects the contradictory payload deterministically or applies an explicitly documented compatibility rule, but MUST NOT silently persist `0.00352`

#### Scenario: Sub-one-percent disclosure is fail-closed
- **WHEN** the model returns a value between `0` and `1` together with a percent unit, such as `disclosed_share=0.5` and `disclosed_share_unit="%"`
- **THEN** the system records a deterministic ambiguous/contradictory-share diagnostic and sends the row to machine rework; it MUST NOT guess whether the value means `0.5%` or a canonical fraction

### Requirement: Null source fallback
When a semantic activity contains both `source_value` and `value`, an explicit null `source_value` MUST fall back to the non-null `value`; the same rule applies to source unit fields.

#### Scenario: Null source value
- **WHEN** `source_value=null` and `value=4.18`
- **THEN** the normalized activity preserves `4.18` as the source value used for downstream conversion
