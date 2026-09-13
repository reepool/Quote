## ADDED Requirements

### Requirement: Anonymous relationship typing
The system MUST distinguish ordinary anonymous relationships from anonymous concentration facts. Ordinary anonymous contracts MAY omit `disclosed_share`; concentration facts MUST include a finite `disclosed_share`.

#### Scenario: Anonymous contract
- **WHEN** a relationship names a masked counterparty such as `客户 A(1)` and is supported by a contract row
- **THEN** it is accepted as an ordinary relationship without requiring `disclosed_share`

#### Scenario: Masked ordinary relationship reaches promotion
- **WHEN** an ordinary contract relationship has a masked disclosed label such as `客户 A(1)`, exact contract evidence, and identity status `disclosed_name_only`
- **THEN** the identity is complete for anonymous publication, no entity-catalog proposal is required, and promotion MUST NOT emit `catalog_proposal` solely because the legal entity is intentionally undisclosed

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
`disclosed_share` MUST have one canonical internal unit of fraction. The semantic response MUST represent source-native disclosure value/unit separately from the canonical value. The program MUST convert a source percent exactly once and persist both source and normalized values/units. A legacy payload that claims `disclosed_share` is already canonical while pairing it with `%` MUST fail closed rather than be guessed or silently divided again.

#### Scenario: Fraction with percent label
- **WHEN** the model returns `disclosed_share=0.352` and `disclosed_share_unit="%"`
- **THEN** the legacy contradictory payload is rejected deterministically and MUST NOT be silently persisted as `0.00352`

#### Scenario: Source-native percent is converted once
- **WHEN** the model returns `disclosed_share_source_value=0.5` and `disclosed_share_source_unit="%"`
- **THEN** the program persists canonical `disclosed_share=0.005`, records both source fields and normalized fraction metadata, and does not reject the legitimate sub-one-percent disclosure

#### Scenario: Ordinary percent disclosures share one rule
- **WHEN** source-native concentration values are `2%` or `14.5%`
- **THEN** the program deterministically stores canonical fractions `0.02` and `0.145` through the same conversion owner

### Requirement: Joint extraction failure is not duplicated
A joint semantic extraction result or failure MUST be shared by all field families covered by the same document, selected sections, schema, prompt, and runtime identity within one run. A deterministic validation failure MUST NOT cause a sibling field family to issue the same LLM request again.

#### Scenario: First family rejects the shared response
- **WHEN** a joint response fails deterministic relationship validation before an artifact can be published
- **THEN** later field families receive the same typed failure without another provider call, and token accounting records one request for that joint input

### Requirement: Null source fallback
When a semantic activity contains both `source_value` and `value`, an explicit null `source_value` MUST fall back to the non-null `value`; the same rule applies to source unit fields.

#### Scenario: Null source value
- **WHEN** `source_value=null` and `value=4.18`
- **THEN** the normalized activity preserves `4.18` as the source value used for downstream conversion
