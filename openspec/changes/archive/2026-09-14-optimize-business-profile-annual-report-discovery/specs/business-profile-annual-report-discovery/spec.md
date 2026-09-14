## ADDED Requirements

### Requirement: Category-filtered annual discovery
The system SHALL request the official annual-report category together with exchange and bounded publication dates, and SHALL still apply the local full-document classifier before persisting a frontier item.

#### Scenario: Annual category includes summaries
- **WHEN** an upstream annual category returns both a full annual report and its summary
- **THEN** the system persists the full report candidate and excludes the summary from the business-profile frontier

#### Scenario: Provider receives a source-neutral category
- **WHEN** business-profile discovery requests `annual_report`
- **THEN** each eligible provider translates it to its governed official category parameters without requiring the business caller to supply provider tokens

### Requirement: Resumable page-bound discovery
The system SHALL preserve selected primary-source records and split or retain the requested date window when an announcement scan reaches its page bound before completion.

#### Scenario: Multi-day window reaches the page bound
- **WHEN** CNInfo returns partial records with `max_pages_exhausted` for a multi-day window
- **THEN** the system persists those records, creates bounded child windows, and does not replace the result with an incompatible fallback response

#### Scenario: Single-day window reaches the page bound
- **WHEN** a single publication day cannot be split further
- **THEN** the system retains that day as incomplete, exposes the condition, and does not mark its cursor complete

### Requirement: Current-season bootstrap and targeted repair
The default latest-annual bootstrap SHALL scan the current filing season first and SHALL use bounded rotating instrument queries only for active issuers whose expected latest annual period remains absent from the frontier.

#### Scenario: Unscoped bootstrap starts in the current year
- **WHEN** an operator starts latest-annual backfill without instruments or an explicit start date
- **THEN** the market discovery start is January 1 of the knowledge-cutoff year

#### Scenario: Company is missing after market discovery
- **WHEN** an active issuer has no frontier full annual report for the expected current annual period
- **THEN** the system may include it in a bounded rotating instrument-scoped annual-category lookback without rescanning older market-wide windows

### Requirement: Corrected full report precedence
For one issuer and annual report period, the system SHALL select the newest corrected or revised full report when one is known and SHALL NOT enqueue or download an earlier original that has not begun acquisition.

#### Scenario: Correction is discovered before original
- **WHEN** a corrected full report is inserted before the original because the source is newest-first
- **THEN** the correction remains active and the subsequently observed original is marked superseded

#### Scenario: Correction is discovered after original is queued
- **WHEN** an original latest-annual work item is pending or retryable and a corrected full report becomes available
- **THEN** the original work is superseded before further acquisition and only the correction remains claimable

#### Scenario: Original was archived before correction existed
- **WHEN** the original PDF was already acquired before a correction was published
- **THEN** the system preserves the immutable original asset, processes the correction as active, and does not delete historical evidence

### Requirement: Annual-report abbreviation support
The document classifier SHALL recognize official `YYYY年报` and `YYYY年年报` full-report titles as annual reports while continuing to exclude summaries, translations, correction notices, and related announcements.

#### Scenario: BSE abbreviated full report
- **WHEN** a PDF title is `2025年年报`
- **THEN** the system classifies it as a full annual report for period `2025-12-31`

#### Scenario: BSE abbreviated summary
- **WHEN** a PDF title is `2025年年报摘要`
- **THEN** the system excludes it as an annual-report summary

### Requirement: Discovery and supersession telemetry
The production report SHALL expose category mode, page-bound windows, targeted repair counts, frontier changes, and work supersession counts needed to monitor a long-running unattended backfill.

#### Scenario: Long-running cycle is inspected
- **WHEN** an operator checks a completed discovery/backfill cycle
- **THEN** the report distinguishes market-category records, incomplete-window backlog, missing-company repair attempts, and originals superseded by corrected full reports
