## MODIFIED Requirements

### Requirement: Financial Disclosure Incremental Sync
The system SHALL provide a financial disclosure incremental task that scans CNInfo announcement metadata to discover candidate financial report updates before fetching financial statement data.

#### Scenario: Formal periodic report announcement creates candidate
- **WHEN** CNInfo announcement scanning finds a formal annual report, semiannual report, first-quarter report, third-quarter report, periodic-report correction, periodic-report revision, delayed-disclosure notice, or periodic-report-related suspension/delisting-risk notice for an active A-share instrument
- **THEN** the task SHALL infer the corresponding report period only when the title contains an unambiguous report-period phrase
- **AND** it SHALL create a candidate keyed by `instrument_id`, `report_period`, and `announcement_id`
- **AND** it SHALL run Financial L1 targeted import only for candidates that are missing locally, incomplete, pending recheck, or have evidence of changed source data
- **AND** it SHALL exclude persisted `accepted_disclosure_gap` rows from the daily candidate pool; those rows SHALL remain available to bounded reconciliation

#### Scenario: Active pending state is eligible
- **WHEN** a persisted `pending_recheck` or `pending_delisting_risk` state has `pending_recheck_until` at or after the current Shanghai time
- **THEN** the incremental task SHALL include it as a candidate subject to the configured candidate limit
- **AND** it SHALL preserve the original pending deadline when the state is updated

#### Scenario: Pending state has expired
- **WHEN** a persisted pending state has a non-null `pending_recheck_until` before the current Shanghai time
- **THEN** the incremental task SHALL NOT issue a repair request for that state
- **AND** it SHALL record the state as `pending_recheck_expired` while preserving its original first-pending time, deadline, announcement id, and title

#### Scenario: Candidate limit is reached
- **WHEN** eligible new events and active pending states exceed the configured candidate limit
- **THEN** the task SHALL select candidates deterministically using the configured exchange/profile/report-period balancing policy
- **AND** it SHALL report the unlimited eligible count, selected count, and source breakdown

#### Scenario: Noisy non-primary announcements are filtered out
- **WHEN** CNInfo announcement scanning finds a title for a performance briefing, briefing preview, English version, illustrated version, inquiry letter, inquiry reply, accounting-firm special explanation, investor reception day, report abstract, performance forecast, performance pre-increase/pre-decrease notice, earnings preview, or other non-primary announcement
- **AND** the title does not contain an explicit delayed-disclosure or delisting-risk signal tied to a periodic report
- **THEN** the task SHALL NOT create a financial maintenance candidate
- **AND** it SHALL count the announcement as filtered or selected-without-event evidence rather than `pending_recheck`

#### Scenario: Ambiguous report period is not guessed
- **WHEN** a selected announcement contains a year that refers to an investor event, reception day, inquiry workflow, briefing activity, or performance forecast rather than the periodic report period
- **THEN** the task SHALL NOT infer a report period from that year
- **AND** it SHALL keep audit evidence without enqueueing targeted financial repair

#### Scenario: Unchanged local financial facts are skipped
- **WHEN** a candidate instrument-period already has all required local-core facts with compatible mapping version and source evidence
- **THEN** the incremental task SHALL skip rewriting that instrument-period
- **AND** it SHALL record the skip reason in the run manifest

### Requirement: Pending Delisting Risk Classification
The financial operations scheduler SHALL classify active instruments with financial disclosure anomalies related to suspension or delisting risk without treating them as confirmed delisted instruments.

#### Scenario: Disclosure anomaly explains missing reports
- **WHEN** a still-active instrument has missing structured financial statements for a report period
- **AND** a CNInfo announcement indicates delayed periodic report disclosure, trading suspension, delisting risk warning, possible termination of listing, or equivalent disclosure risk
- **THEN** the system SHALL classify the instrument-period as `pending_delisting_risk` or `periodic_report_delayed_or_suspended`
- **AND** the classification SHALL include announcement ID, title, time, first detected time, and retry horizon
- **AND** the missing report SHALL not be reported as a field-mapping defect

#### Scenario: Pending retry horizon is fixed
- **WHEN** a pending disclosure state is revisited before its retry horizon expires
- **THEN** the system SHALL retain the original `first_pending_at` and `pending_recheck_until` values
- **AND** it SHALL NOT roll the horizon forward merely because the state was scanned again

#### Scenario: Pending retry horizon expires
- **WHEN** the pending retry horizon expires without required facts becoming ready
- **THEN** the system SHALL persist `pending_recheck_expired` as the terminal maintenance status
- **AND** it SHALL leave the instrument eligible for a future new formal announcement or reconciliation run

#### Scenario: Pending delisting risk does not alter master delisting state
- **WHEN** an instrument-period is classified as pending delisting risk
- **THEN** the system SHALL NOT set `instruments.status=delisted` based on that classification alone
- **AND** confirmed delisting status SHALL remain governed by the instrument master governance source policy

### Requirement: Financial Task Telegram Reporting
Financial scheduler tasks SHALL render operator-facing Telegram reports that separate job success, accepted gaps, pending disclosure anomalies, unresolved data-quality blockers, and source-routing outcomes.

#### Scenario: Incremental task completes with pending disclosure anomalies
- **WHEN** the financial disclosure incremental task finishes with pending delisting risk, delayed-disclosure candidates, or expired pending states
- **THEN** the Telegram report SHALL include candidate count, fetched count, written count, skipped count, pending recheck count, pending delisting risk count, expired pending count, accepted gap count, blocking count, and next action guidance

#### Scenario: Task reports source routing
- **WHEN** a financial disclosure incremental or reconciliation task attempts targeted financial repair
- **THEN** the Telegram report SHALL summarize CNInfo data20 official attempts, CNInfo successes, CNInfo missing/ambiguous facts, Sina/THS fallback attempts, fallback successes, and unresolved blockers
- **AND** the task status SHALL be `degraded` when official source-routing errors remain even if fallback writes succeed

#### Scenario: Task has blocking field defects
- **WHEN** a financial task encounters missing required facts without lifecycle or announcement evidence
- **THEN** the Telegram report SHALL classify those items as blockers
- **AND** it SHALL NOT merge them into accepted gaps
