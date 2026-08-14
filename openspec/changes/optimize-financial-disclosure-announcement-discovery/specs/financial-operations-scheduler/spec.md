## MODIFIED Requirements

### Requirement: Financial Disclosure Incremental Sync
The system SHALL provide a financial disclosure incremental task that discovers candidate financial report updates through provider-filtered periodic-report and narrowly scoped disclosure-anomaly announcement streams before fetching financial statement data.

#### Scenario: Formal periodic report announcement creates candidate
- **WHEN** upstream-filtered announcement scanning finds a formal annual report, semiannual report, first-quarter report, third-quarter report, periodic-report correction, or periodic-report revision for an active A-share instrument
- **THEN** the task SHALL infer the corresponding report period only when the title contains an unambiguous report-period phrase
- **AND** it SHALL create a candidate keyed by `instrument_id`, `report_period`, and `announcement_id`
- **AND** it SHALL run Financial L1 targeted import only for candidates that are missing locally, incomplete, pending recheck, or have evidence of changed source data

#### Scenario: SSE or SZSE periodic reports are discovered
- **WHEN** incremental discovery scans SSE or SZSE without an operator search override
- **THEN** it SHALL request first-quarter, semiannual, third-quarter, and annual report categories through the normalized announcement category interface
- **AND** it SHALL NOT scan the unfiltered market announcement stream for ordinary report candidates

#### Scenario: BSE periodic reports are discovered
- **WHEN** incremental discovery scans BSE
- **THEN** it SHALL use the BSE official advanced-filter endpoint with verified annual, semiannual, first-quarter, third-quarter, and correction subtypes
- **AND** it SHALL NOT scan the full CNInfo NEEQ announcement stream
- **AND** BSE disclosure anomalies SHALL use the official expected-late-disclosure subtype and the same explicit-report-period selector

#### Scenario: Periodic-report disclosure anomaly creates evidence
- **WHEN** a narrow disclosure-anomaly query finds a delayed-disclosure or trading-risk notice with an unambiguous first-quarter, semiannual, third-quarter, or annual report phrase
- **THEN** the task SHALL retain the announcement as periodic-report anomaly evidence
- **AND** it SHALL classify the corresponding instrument-period according to the existing delayed-disclosure or pending-risk policy

#### Scenario: Generic trading risk does not create a financial candidate
- **WHEN** an announcement mentions suspension, delisting risk, or possible termination without an unambiguous periodic-report phrase
- **THEN** the task SHALL NOT create a financial maintenance candidate from that announcement

#### Scenario: Noisy non-primary announcements are filtered out
- **WHEN** provider-filtered scanning finds a title for a subsidiary, progress update, performance briefing, briefing preview, English version, illustrated version, inquiry letter, inquiry reply, accounting-firm special explanation, investor reception day, report abstract, or other non-primary announcement
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

### Requirement: Pending Delisting Risk Classification
The financial operations scheduler SHALL classify active instruments with financial disclosure anomalies explicitly related to a report period without treating them as confirmed delisted instruments.

#### Scenario: Disclosure anomaly explains missing reports
- **WHEN** a still-active instrument has missing structured financial statements for a report period
- **AND** a provider-filtered anomaly announcement contains an unambiguous phrase for that report period and indicates delayed disclosure, trading suspension, delisting risk warning, possible termination of listing, or equivalent disclosure risk
- **THEN** the system SHALL classify the instrument-period as `pending_delisting_risk` or `periodic_report_delayed_or_suspended`
- **AND** the classification SHALL include announcement ID, title, time, first detected time, and retry horizon
- **AND** the missing report SHALL not be reported as a field-mapping defect

#### Scenario: Pending delisting risk does not alter master delisting state
- **WHEN** an instrument-period is classified as pending delisting risk
- **THEN** the system SHALL NOT set `instruments.status=delisted` based on that classification alone
- **AND** confirmed delisting status SHALL remain governed by the instrument master governance source policy

## ADDED Requirements

### Requirement: Financial Announcement Discovery Completeness
Financial disclosure discovery SHALL expose provider completeness separately from transport errors and SHALL NOT report a complete successful scan when any required announcement stream is incomplete.

#### Scenario: Announcement page bound is exhausted
- **WHEN** a required market/category/keyword stream reaches its configured page bound before a watermark or final page
- **THEN** the financial task SHALL record the stream source, exchange, category or keyword, and stop reason
- **AND** the parent financial task SHALL report `degraded` when partial candidates are available or `failed` when discovery produced no usable candidates
- **AND** it SHALL NOT commit a new provider cursor for the incomplete stream

#### Scenario: CNInfo page count fields disagree
- **WHEN** CNInfo's reported page count is lower than the page count derived from its positive total-record fields and the effective page size
- **THEN** discovery SHALL use the larger valid page estimate
- **AND** it SHALL read the final partial page before marking the stream complete

#### Scenario: Filtered stream completes
- **WHEN** every required announcement stream reaches its prior watermark, a short or empty page, or the reconciled final page without provider errors
- **THEN** discovery SHALL be eligible to commit its provider cursor
- **AND** announcement discovery SHALL NOT degrade the parent task status
