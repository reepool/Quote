## ADDED Requirements

### Requirement: Corporate-action scheduler separates daily and full reference refresh
The scheduler SHALL configure normal CNInfo corporate-action daily sync with
targeted TDX reference refresh and SHALL provide a configurable periodic
invocation for full-market TDX refresh.

#### Scenario: Normal daily schedule runs
- **WHEN** the daily CNInfo corporate-action job starts
- **THEN** its effective TDX refresh mode SHALL be `targeted`

#### Scenario: Periodic reference sweep runs
- **WHEN** the configured weekly full-reference schedule starts
- **THEN** it SHALL invoke the corporate-action workflow with TDX mode `full` without changing CNInfo primary-source authority

### Requirement: Scheduler reports primary and reference states independently
The corporate-action task report SHALL distinguish CNInfo primary readiness,
TDX reference readiness, and cross-source reconciliation status.

#### Scenario: CNInfo succeeds while TDX reference is partial
- **WHEN** all CNInfo primary work completes but a TDX reference or reconciliation check is incomplete
- **THEN** the report SHALL state that CNInfo primary readiness succeeded and identify the partial condition as reference-only
