## ADDED Requirements

### Requirement: Scheduler exposes manual corporate-action validation
The scheduler SHALL expose A-share multi-source corporate-action validation as a manual-only data-management task.

#### Scenario: Operator runs a targeted validation
- **WHEN** the operator supplies a date range, exchanges, or instrument IDs
- **THEN** the scheduler SHALL pass the bounded scope to the validation service and return event, official-evidence, cumulative-factor, coverage, warning, and error summaries

#### Scenario: Task is not manually invoked
- **WHEN** normal cron registration is evaluated
- **THEN** the validation task SHALL NOT be scheduled automatically

#### Scenario: Validation task reports conflicts
- **WHEN** the validation service returns material unresolved evidence
- **THEN** the scheduler SHALL report `partial` rather than `success` and SHALL preserve representative follow-up samples
