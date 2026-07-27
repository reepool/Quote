## ADDED Requirements

### Requirement: Special-event eligibility
The system SHALL limit TDX-backed asymmetric approval to current, unresolved
CNInfo observations whose persisted CNInfo classification is restructuring
capitalization, share-reform distribution, or performance-compensation.

#### Scenario: Ordinary dividend is present
- **WHEN** an unresolved CNInfo observation is an annual or special dividend
- **THEN** the TDX-backed asymmetric approval path reports it as out of scope
  and writes no review

### Requirement: Conservative TDX event match
The system SHALL approve a special CNInfo event only when exactly one persisted
TDX XDXR row for the same instrument has compatible date roles and all
normalized economic fields within the configured strict tolerance.

#### Scenario: Unique matching event
- **WHEN** one TDX row matches the CNInfo cash, combined bonus and
  capitalization shares, rights shares, and rights price and its date is
  compatible
- **THEN** the event is eligible for asymmetric approval

#### Scenario: Economic difference
- **WHEN** any supported normalized economic field exceeds tolerance
- **THEN** the event remains unresolved and the report includes field
  differences

#### Scenario: Multiple valid rows
- **WHEN** more than one TDX row satisfies the match contract
- **THEN** the event remains unresolved with an ambiguous-match reason

### Requirement: Role-aware date compatibility
The system SHALL accept a TDX XDXR date only when it equals the CNInfo ex-date,
share-arrival date, or pay date, or falls from the CNInfo record date through
the next three persisted trading sessions.

#### Scenario: Next-session XDXR date
- **WHEN** CNInfo records the shareholder record date and the matching TDX row
  is dated on the next trading session
- **THEN** the date is compatible

#### Scenario: Announcement-only proximity
- **WHEN** a TDX row is close only to the CNInfo announcement date
- **THEN** the date is not compatible

#### Scenario: Calendar evidence is unavailable
- **WHEN** session distance from a record date cannot be established
- **THEN** the event remains unresolved

### Requirement: Auditable asymmetric approval
The system SHALL persist a unique match as a resolved review whose lineage
contains `approval_classification=approved_asymmetric`, the policy version,
the unchanged CNInfo terms, the selected TDX row, date evidence, and normalized
field differences.

#### Scenario: Match is written
- **WHEN** write mode processes a unique matching event
- **THEN** the review becomes terminal and nonblocking while raw CNInfo and TDX
  rows remain unchanged

#### Scenario: Dry run is requested
- **WHEN** dry-run mode processes matching and nonmatching events
- **THEN** it returns the same classifications without writing reviews, terms,
  evidence, or governance state

### Requirement: Persisted-data-only execution
The TDX-backed asymmetric approval path MUST NOT download documents, run OCR,
classify announcement titles, or invoke an LLM.

#### Scenario: Full backlog comparison
- **WHEN** the unresolved special-event backlog is compared
- **THEN** network access and LLM invocation counts remain zero
