## ADDED Requirements

### Requirement: DCE calendar anti-bot failure is bounded per provider run
The calendar backfill SHALL stop repeating full DCE browser readiness work after the configured direct and proxy routes have failed within one provider run.

#### Scenario: All bounded DCE routes fail
- **WHEN** direct access and every configured DCE proxy lease fail challenge validation, business-request validation, or a hard browser timeout
- **THEN** the provider SHALL open a run-scoped DCE circuit breaker
- **AND** remaining requested DCE dates SHALL be reported unresolved without starting another browser readiness cycle
- **AND** the backfill result SHALL retain warning or blocked status according to existing unresolved-date rules

#### Scenario: A later run starts
- **WHEN** a new provider instance is created for a subsequent scheduled or manual run
- **THEN** the DCE circuit breaker SHALL start closed and permit a new bounded access attempt
