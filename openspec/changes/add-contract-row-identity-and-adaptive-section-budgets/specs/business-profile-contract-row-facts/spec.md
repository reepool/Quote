## ADDED Requirements

### Requirement: Structured operating facts preserve source row identity
The system MUST assign every newly extracted structured operating fact a deterministic `source_row_key` derived from its immutable table evidence, ordered raw cells, row label, and row ordinal; when an explicit contract reference is present it MUST also preserve `contract_reference_raw`.

#### Scenario: Two contracts share the same product label
- **WHEN** a source table contains two rows labelled `多晶硅料` with different contract amounts or other raw cells
- **THEN** the system MUST create two distinct row keys and MUST NOT merge the rows into one fact solely because their product labels match

#### Scenario: A row has no explicit contract number
- **WHEN** a source table has distinct rows but no contract number or counterparty
- **THEN** the system MUST use the evidence-derived row key and retain the row as a separate source fact

### Requirement: Fact identity includes the row key without changing source values
The system MUST include `source_row_key` in the durable operating-fact identity and metadata while preserving the LLM's raw numeric value and raw unit unchanged; normalization and derived calculations MUST remain programmatic.

#### Scenario: Same subject and fact type have different rows
- **WHEN** two rows have the same report period, subject, and fact type but different row keys
- **THEN** their record identities MUST remain distinct and both raw values MUST be available for downstream aggregation

#### Scenario: Existing approved fact is replayed
- **WHEN** an approved legacy fact is replayed under `result_policy=reuse`
- **THEN** the system MUST preserve the approved row and MUST NOT overwrite it with a conflicting candidate solely due to the new row-key field

### Requirement: Ambiguous row groups receive targeted stronger-model review
The system MUST detect same-subject row groups that would otherwise conflict and MAY submit only those groups to the configured stronger reasoning profile, defaulting to `gpt-5.6-terra`; the review MUST return a closed JSON decision referencing supplied row keys.

#### Scenario: Stronger review confirms separate contracts
- **WHEN** the review identifies two or more distinct contract rows
- **THEN** the system MUST retain each row as a separate candidate or approved fact and MUST record the review decision and evidence context

#### Scenario: Stronger review is inconclusive or unavailable
- **WHEN** the review times out, violates the response schema, or cannot establish row boundaries
- **THEN** the system MUST retain all source-supported rows, mark the group `ambiguous_same_subject_rows`, and MUST NOT silently select one amount

#### Scenario: Non-ambiguous rows are processed
- **WHEN** a row group has one row key or no conflicting identity
- **THEN** the system MUST NOT invoke the stronger review model for that group

### Requirement: Row-level ambiguity does not block unrelated facts
The system MUST isolate an ambiguous row group to its own diagnostics and publication decision without discarding unrelated operating facts from the same report or field family.

#### Scenario: One table group is ambiguous
- **WHEN** one product group requires review and other rows have valid evidence and units
- **THEN** unrelated rows MUST continue through normalization and publication according to existing governance
