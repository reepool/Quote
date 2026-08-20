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

#### Scenario: Rolling re-probe fails for a date with strong evidence
- **WHEN** the current official probe is unresolved but the stored calendar row is `backfilled_verified` from official daily rows or an official closure notice
- **THEN** the backfill SHALL preserve the stored verified classification
- **AND** it SHALL report the current probe failure as preserved evidence diagnostics rather than a blocking unresolved date

#### Scenario: Route exhaustion includes a timeout summary
- **WHEN** the DCE client reports that all bounded routes are exhausted and its sanitized last error mentions a timeout
- **THEN** the failure SHALL remain a non-retryable DCE route-exhaustion classification
- **AND** outer provider and calendar retry loops SHALL NOT wait or repeat the open circuit
