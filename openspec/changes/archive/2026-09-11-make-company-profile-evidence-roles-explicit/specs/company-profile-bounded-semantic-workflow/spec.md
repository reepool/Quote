## ADDED Requirements

### Requirement: Provider Evidence catalogs identify field ownership
The provider-facing Evidence catalog MUST classify each Evidence item as `field_owner`, `context_only`, or `mixed`, and MUST list the exact checklist field IDs that own it. The roles MUST be derived from existing prepared field bindings without changing immutable Evidence identities or source content.

#### Scenario: Context-only Evidence is present
- **WHEN** a request includes an Evidence item with no prepared field binding
- **THEN** the provider catalog marks it `context_only` with an empty owning-field list
- **AND** the item remains available for context but cannot be treated as field-owning proof

#### Scenario: One Evidence item supports multiple fields
- **WHEN** the same Evidence item is prepared with bindings for two checklist fields
- **THEN** the provider catalog marks it `mixed` and lists both exact field IDs
- **AND** it does not authorize the Evidence for any unlisted field

### Requirement: Legal-empty verification requires owning Evidence
Legal-empty coverage MUST be accepted only when the cited Evidence owns the requested field or the source explicitly contains the requested field's governed not-applicable disclosure. Context-only Evidence alone MUST remain insufficient, and verification MUST continue returning the existing typed block or unclear outcome rather than mutating the candidate.

#### Scenario: Context-only Evidence is used for legal-empty coverage
- **WHEN** a model cites contextual page text but no Evidence item owns the requested checklist field
- **THEN** verification blocks or leaves the coverage unresolved with the existing typed coverage reason
- **AND** it does not treat the contextual text as proof that the field is undisclosed or not applicable

#### Scenario: Control-scope wording is narrower than business regime
- **WHEN** Evidence says only that consolidation scope or control did not change
- **THEN** verification does not accept that statement as broader principal-business, product, service, or statistical-regime no-change coverage
- **AND** an owning broader regime disclosure is still required

### Requirement: Segment candidates require segment-owning Evidence
A cost-component or generic operating-cost row MUST NOT be emitted as a business Segment unless the supplied field-owning Evidence explicitly identifies a segment dimension and segment row. The existing closed schema and semantic validator MUST remain authoritative when the model violates this instruction.

#### Scenario: Cost composition is mistaken for a segment
- **WHEN** a model cites a cost-component table as the basis for a business Segment without a field-owning segment dimension
- **THEN** the response is rejected or remains unresolved through the existing schema/semantic path
- **AND** the cost fact is not relabeled as a business segment
