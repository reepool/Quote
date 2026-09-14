# Business Profile Integrity

## MODIFIED Requirements

### Requirement: Repository-compatible semantic persistence
The system MUST persist valid activity and relationship semantic results using only repository-supported top-level fields, while retaining source-row and contract-reference inputs in metadata and in identity/hash calculation.

#### Scenario: Activity with source identity metadata
- **WHEN** an activity producer returns a source row key or contract reference
- **THEN** the repository payload contains those values under metadata, the upsert succeeds, and the resulting identity remains stable.

#### Scenario: Relationship with source identity metadata
- **WHEN** a relationship producer returns a source row key or contract reference
- **THEN** the repository payload contains those values under metadata and the relationship is persisted without an unknown-field error.

### Requirement: Temporal conflict protection
The system MUST use the actual activity or relationship primary key when checking temporal versions and MUST NOT treat empty supersession pointers as valid matches.

#### Scenario: Conflicting activity versions
- **WHEN** two activity candidates share a temporal identity but disagree on material content
- **THEN** at most one can be approved and the other is held or rejected with a conflict reason.

#### Scenario: Distinct source rows
- **WHEN** two disclosed contract rows have distinct source-row or contract identity
- **THEN** both remain separately queryable and eligible for approval.

### Requirement: Unresolved named relationships remain facts
The system MUST retain a named relationship when entity-catalog resolution fails, with the raw name and evidence, a null entity id, and an explicit unresolved/catalog-pending status.

#### Scenario: Counterparty absent from catalog
- **WHEN** an LLM returns a named supplier or customer not present in the entity catalog
- **THEN** the relationship is persisted as unresolved for review and is not silently discarded or assigned a fabricated local entity id.

#### Scenario: Counterparty later resolves
- **WHEN** a later catalog refresh resolves the same semantic relationship and the resolved relationship is approved
- **THEN** catalog-pending exceptions for that semantic assertion and its prior unresolved relationship candidates are closed without deleting the audit records.

### Requirement: Publication gates are externally supplied
Commodity exposure publication MUST use the current runtime promotion manifest and MUST fail closed or hold when the manifest is missing, stale, mismatched, or has a failed required gate.

#### Scenario: Missing publication manifest
- **WHEN** a publisher is called without a matching runtime manifest
- **THEN** no approved publication is written and the result records an input-gap/held reason.

#### Scenario: Valid manifest
- **WHEN** all required gates in a matching manifest pass
- **THEN** the approved exposure can be published with the manifest identity recorded.

### Requirement: Diagnostic history access is bounded
History and candidate diagnostics MUST require trusted identity and the business-profile diagnostic scope; approved public profile and exposure reads MUST retain their existing access behavior.

#### Scenario: Unauthenticated history request
- **WHEN** a caller requests business-profile history without diagnostic authorization
- **THEN** the endpoint rejects the request and does not disclose candidate or internal diagnostic records.

### Requirement: Uncertain dimensions and periods are explicit
The system MUST NOT classify an activity with an unknown dimension as physical production volume and MUST preserve the source period basis or write `unknown` when it cannot be determined.

#### Scenario: Unknown production unit
- **WHEN** a produces activity has an unresolved unit dimension
- **THEN** it remains a pending/other measurement with raw value and unit, not a production-volume fact.

#### Scenario: Point-in-time activity conversion
- **WHEN** an activity carries an inventory, capacity, or other non-full-year period basis
- **THEN** conversion provenance uses that basis; it does not overwrite it with `full_year`.
