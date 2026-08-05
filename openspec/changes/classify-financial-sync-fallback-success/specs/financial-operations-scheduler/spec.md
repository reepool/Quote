## MODIFIED Requirements

### Requirement: Financial Task Telegram Reporting
Financial scheduler tasks SHALL render operator-facing Telegram reports that separate job success, accepted gaps, pending disclosure anomalies, unresolved data-quality blockers, and source-routing outcomes.

#### Scenario: Incremental task completes with pending disclosure anomalies
- **WHEN** the financial disclosure incremental task finishes with pending delisting risk or delayed-disclosure candidates
- **THEN** the Telegram report SHALL include candidate count, fetched count, written count, skipped count, pending recheck count, pending delisting risk count, accepted gap count, blocking count, and next action guidance

#### Scenario: Task reports source routing
- **WHEN** a financial disclosure incremental or reconciliation task attempts targeted financial repair
- **THEN** the Telegram report SHALL summarize CNInfo data20 official attempts, CNInfo successes, CNInfo missing/ambiguous facts, Sina/THS fallback attempts, and fallback successes
- **AND** SHALL identify whether final collection used CNInfo, fallback, or both
- **AND** SHALL not change a successful completion to `degraded` solely because the official source was incomplete when fallback completed all targets

#### Scenario: Task has blocking field defects
- **WHEN** a financial task encounters missing required facts without lifecycle or announcement evidence
- **THEN** the Telegram report SHALL classify those items as blockers
- **AND** it SHALL NOT merge them into accepted gaps
