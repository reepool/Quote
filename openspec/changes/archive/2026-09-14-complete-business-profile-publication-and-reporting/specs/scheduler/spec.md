## ADDED Requirements

### Requirement: Business-profile reports reflect actual publication state
Scheduler and Telegram business-profile reports SHALL derive their status, severity, phase, coverage, and publication counters from the current task result rather than missing-field defaults or a fixed warning level.

#### Scenario: Targeted backfill succeeds
- **WHEN** a targeted backfill completes without retries, terminal failures, or publication blockers
- **THEN** its summary and detail notifications use success or informational severity
- **AND** no successful detail message is prefixed as a warning

#### Scenario: Rollout readiness is absent
- **WHEN** a targeted expanded result has reconciliation coverage but no rollout-readiness object
- **THEN** the report displays annual-report coverage calculated from reconciliation counts
- **AND** does not display zero solely because rollout readiness is absent

#### Scenario: Candidate-only work completes
- **WHEN** a shadow phase completes a work item without promotion
- **THEN** the report labels the outcome candidate-only
- **AND** reports candidate and verified counts separately from published counts

#### Scenario: Complete publication is partial
- **WHEN** supported rows publish but governed downstream gaps remain
- **THEN** the report uses warning severity
- **AND** includes the remaining machine-rework reasons and published counts
