# financial-operations-scheduler Specification

## Purpose
The Financial Operations Scheduler owns operator-facing financial maintenance
tasks, including manual full import, announcement-driven incremental repair,
bounded reconciliation, and Telegram status reporting for financial statement
operations.
## Requirements
### Requirement: Manual Financial L1 Full Import Task
The system SHALL expose a `financial_l1_full_import` task that wraps the Financial L1 local-core full import and can be invoked through Telegram `/run` without being registered as an automatic cron job.

#### Scenario: Operator runs manual full import
- **WHEN** an operator sends `/run financial_l1_full_import`
- **THEN** the scheduler SHALL start the Financial L1 full import with configured report-period window, exchanges, database path, batch size, and resume policy
- **AND** the task SHALL write financial facts only to the configured financial database, normally `data/financials.db`
- **AND** the task SHALL produce a Telegram maintenance report with manifest path, progress path, batch counts, ready counts, accepted gap counts, blocking counts, and final status

#### Scenario: Manual task is not scheduled
- **WHEN** the scheduler registers automatic jobs
- **THEN** `financial_l1_full_import` SHALL remain visible in `/status`
- **AND** it SHALL NOT be registered with APScheduler as a cron trigger when `manual_only=true`

### Requirement: Financial Disclosure Incremental Sync
The system SHALL provide a financial disclosure incremental task that scans CNInfo announcement metadata to discover candidate financial report updates before fetching financial statement data.

#### Scenario: Formal periodic report announcement creates candidate
- **WHEN** CNInfo announcement scanning finds a formal annual report, semiannual report, first-quarter report, third-quarter report, periodic-report correction, periodic-report revision, delayed-disclosure notice, or periodic-report-related suspension/delisting-risk notice for an active A-share instrument
- **THEN** the task SHALL infer the corresponding report period only when the title contains an unambiguous report-period phrase
- **AND** it SHALL create a candidate keyed by `instrument_id`, `report_period`, and `announcement_id`
- **AND** it SHALL run Financial L1 targeted import only for candidates that are missing locally, incomplete, pending recheck, or have evidence of changed source data

#### Scenario: Noisy non-primary announcements are filtered out
- **WHEN** CNInfo announcement scanning finds a title for a performance briefing, briefing preview, English version, illustrated version, inquiry letter, inquiry reply, accounting-firm special explanation, investor reception day, report abstract, or other non-primary announcement
- **AND** the title does not contain an explicit delayed-disclosure or delisting-risk signal tied to a periodic report
- **THEN** the task SHALL NOT create a financial maintenance candidate
- **AND** it SHALL count the announcement as filtered or selected-without-event evidence rather than `pending_recheck`

#### Scenario: Ambiguous report period is not guessed
- **WHEN** a selected announcement contains a year that refers to an investor event, reception day, inquiry workflow, or briefing activity rather than the periodic report period
- **THEN** the task SHALL NOT infer a report period from that year
- **AND** it SHALL keep audit evidence without enqueueing targeted financial repair

#### Scenario: Unchanged local financial facts are skipped
- **WHEN** a candidate instrument-period already has all required local-core facts with compatible mapping version and source evidence
- **THEN** the incremental task SHALL skip rewriting that instrument-period
- **AND** it SHALL record the skip reason in the run manifest

### Requirement: Financial Disclosure Reconciliation Sync
The system SHALL provide a bounded reconciliation task for financial statements that repairs missed announcements, silent source changes, and historical local gaps without blindly rewriting all data.

#### Scenario: Weekly reconciliation finds missing core facts
- **WHEN** reconciliation checks the configured rolling report-period window and finds a missing or incomplete required core fact
- **THEN** it SHALL enqueue that instrument-period for targeted Financial L1 import
- **AND** it SHALL call the shared financial maintenance repair router to attempt official CNInfo data20 structured repair before Sina/THS fallback when CNInfo data20 is configured for that exchange, instrument, report period, and canonical fact
- **AND** it SHALL report missing facts, attempted repairs, successful repairs, accepted gaps, and remaining blockers

#### Scenario: Complete instrument-period is unchanged
- **WHEN** reconciliation finds a complete instrument-period with unchanged source evidence
- **THEN** it SHALL not rewrite financial facts for that instrument-period

### Requirement: Pending Delisting Risk Classification
The financial operations scheduler SHALL classify active instruments with financial disclosure anomalies related to suspension or delisting risk without treating them as confirmed delisted instruments.

#### Scenario: Disclosure anomaly explains missing reports
- **WHEN** a still-active instrument has missing structured financial statements for a report period
- **AND** a CNInfo announcement indicates delayed periodic report disclosure, trading suspension, delisting risk warning, possible termination of listing, or equivalent disclosure risk
- **THEN** the system SHALL classify the instrument-period as `pending_delisting_risk` or `periodic_report_delayed_or_suspended`
- **AND** the classification SHALL include announcement ID, title, time, first detected time, and retry horizon
- **AND** the missing report SHALL not be reported as a field-mapping defect

#### Scenario: Pending delisting risk does not alter master delisting state
- **WHEN** an instrument-period is classified as pending delisting risk
- **THEN** the system SHALL NOT set `instruments.status=delisted` based on that classification alone
- **AND** confirmed delisting status SHALL remain governed by the instrument master governance source policy

### Requirement: Financial Task Telegram Reporting
Financial scheduler tasks SHALL render operator-facing Telegram reports that separate job success, accepted gaps, pending disclosure anomalies, unresolved data-quality blockers, and source-routing outcomes.

#### Scenario: Incremental task completes with pending disclosure anomalies
- **WHEN** the financial disclosure incremental task finishes with pending delisting risk or delayed-disclosure candidates
- **THEN** the Telegram report SHALL include candidate count, fetched count, written count, skipped count, pending recheck count, pending delisting risk count, accepted gap count, blocking count, and next action guidance

#### Scenario: Task reports source routing
- **WHEN** a financial disclosure incremental or reconciliation task attempts targeted financial repair
- **THEN** the Telegram report SHALL summarize CNInfo data20 official attempts, CNInfo successes, CNInfo missing/ambiguous facts, Sina/THS fallback attempts, fallback successes, and unresolved blockers

#### Scenario: Task has blocking field defects
- **WHEN** a financial task encounters missing required facts without lifecycle or announcement evidence
- **THEN** the Telegram report SHALL classify those items as blockers
- **AND** it SHALL NOT merge them into accepted gaps

### Requirement: Financial Reports Shall Separate Source Collection From Readiness

Financial disclosure incremental and reconciliation reports SHALL expose separate counts for official requests, official structured responses parsed, official numeric facts written, strict canonical-ready targets, fallback-required targets, fallback successes, and unresolved blockers. The report SHALL not use `failed` as the sole label for a target that was parsed successfully but lacks a strict canonical field.

#### Scenario: CNInfo supplies partial official facts and fallback completes the target
- **WHEN** CNInfo parses an instrument-period but leaves `equity_parent` missing
- **AND** THS/Sina fallback fills that missing field
- **THEN** the report SHALL show official parse success and fallback-required/fallback-success counts separately
- **AND** the final target SHALL be successful unless another blocking defect remains

#### Scenario: Official acquisition fails and fallback also fails
- **WHEN** official transport or parsing fails for a target
- **AND** configured fallback cannot produce the required canonical facts
- **THEN** the report SHALL classify the target as unresolved/blocking
- **AND** SHALL include the bounded source diagnostics and missing canonical facts

### Requirement: Official Validation Shall Use Production Canonical Fact Names

The CNInfo official batch validation invoked by maintenance SHALL receive and evaluate the same profile-specific canonical required facts used by production readiness, including `net_income_parent` and `equity_parent`, rather than an unrelated legacy alias list.

#### Scenario: Bank and non-bank targets use the same canonical contract
- **WHEN** maintenance validates CNInfo data20 for a bank or non-bank instrument-period
- **THEN** the validator SHALL evaluate the configured profile-specific canonical fact list
- **AND** SHALL retain source-native `equity_total` as a non-parent fact when `equity_parent` is unavailable
