## ADDED Requirements

### Requirement: Exact announcement-level operator disposition
The system SHALL represent an operator-approved non-XDXR disposition using an exact CNInfo announcement key, instrument ID, expected normalized title, reviewer, decision basis, and approval date.

#### Scenario: Exact approved announcement
- **WHEN** an announcement key, instrument ID, and normalized title match an approved non-XDXR decision
- **THEN** the system SHALL classify that announcement as operator-verified non-XDXR without creating or changing a corporate-action event or adjustment factor

#### Scenario: Decision identity drift
- **WHEN** a decision key is present but the instrument ID or normalized title differs from the approved identity
- **THEN** the system SHALL retain the announcement for conservative processing and report the decision identity mismatch

### Requirement: Daily scan respects approved dispositions
The CNInfo daily announcement scan SHALL apply exact non-XDXR decisions to both newly acquired announcements and carried pending announcements before generic exceptional-title routing.

#### Scenario: Approved carried announcement
- **WHEN** a pending exceptional announcement matches an approved non-XDXR decision
- **THEN** carryover revalidation SHALL exclude the announcement and clear its unmatched-special candidate only when that instrument has no other pending special, semantic, or factor work

#### Scenario: Unapproved exceptional announcement
- **WHEN** an exceptional announcement has no exact approved disposition and no matching structured event
- **THEN** the system SHALL continue to defer it for review under the existing unmatched-special policy

### Requirement: Safe and auditable operational application
The system SHALL provide a preview-first, idempotent operation for applying frozen announcement decisions to the configured research store.

#### Scenario: Default preview
- **WHEN** the operator runs the decision tool without an apply flag
- **THEN** the tool SHALL validate the decision manifest, announcement audit identity, and pending queue impact without writing the database

#### Scenario: Valid apply
- **WHEN** the operator explicitly applies valid decisions to the configured project research database
- **THEN** the tool SHALL atomically remove only matching pending entries, preserve announcement audit rows, record decision diagnostics, and report before and after queue counts

#### Scenario: Repeated apply
- **WHEN** the same approved decisions are applied again
- **THEN** the tool SHALL succeed without removing unrelated pending work or creating duplicate audit diagnostics

#### Scenario: Production identity mismatch
- **WHEN** the live announcement audit identity or frozen manifest differs from the approved decision
- **THEN** the tool SHALL abort before any database write
