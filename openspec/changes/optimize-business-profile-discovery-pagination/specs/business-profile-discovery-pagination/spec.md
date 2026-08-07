## ADDED Requirements

### Requirement: Total-page-aware multi-day planning
The system SHALL inspect a valid provider-reported total-page count for an explicitly enabled multi-day annual-report discovery window and SHALL stop after the first page when the count exceeds the configured per-window page allowance.

#### Scenario: Dense filing-season window exceeds the allowance
- **WHEN** CNInfo page one reports more total pages than the configured allowance for a splittable multi-day annual-report window
- **THEN** the system preserves the selected page-one records, reports an incomplete resumable result, and splits the date window without reading the remaining allowance

#### Scenario: Reported total fits the allowance
- **WHEN** CNInfo page one reports a total-page count within the configured allowance
- **THEN** the system continues the bounded scan until the provider result is complete or another existing stop condition occurs

#### Scenario: Provider total is absent or invalid
- **WHEN** CNInfo does not provide a valid total-page count
- **THEN** the system retains the existing bounded scan behavior and does not infer completeness from the missing value

### Requirement: Safe historical single-day continuation
The system SHALL persist and use a next-page checkpoint only for an incomplete single-day discovery window whose publication date is earlier than the run cutoff.

#### Scenario: Closed single day exceeds one page chunk
- **WHEN** an already-ended single-day window reaches its page allowance before completion
- **THEN** the system persists the next page and resumes that same date from the checkpoint in a later cycle

#### Scenario: Fresh or current-day window is incomplete
- **WHEN** a fresh window or current-day single-date window is incomplete
- **THEN** the system does not trust a page offset and retains watermark-based overlapping discovery

#### Scenario: Resumed chunk reaches the provider last page
- **WHEN** a historical single-day continuation reaches the reported final page or a short page
- **THEN** the system marks the window complete and removes it from the pending backlog

### Requirement: Pagination compatibility and observability
The system SHALL retain compatibility with legacy pending-window state and SHALL expose total pages, start page, next page, and preflight stop reasons in discovery diagnostics when available.

#### Scenario: Legacy pending window is loaded
- **WHEN** a persisted window contains only start date, end date, and kind
- **THEN** the system processes it from page one without migration or operator intervention

#### Scenario: Operator inspects a preflight split
- **WHEN** a dense window is preflight-split or a historical single day is checkpointed
- **THEN** the discovery report and logs identify the window, provider total, scanned page range, stop reason, and pending continuation state

### Requirement: Existing evidence guarantees remain unchanged
The system SHALL continue to apply the upstream annual-report category, local full-report classifier, announcement identity deduplication, corrected-report precedence, and partial-result persistence to all preflight and resumed records.

#### Scenario: Preflight page contains summary and full report
- **WHEN** the first page of a preflight scan contains both an annual-report summary and a formal full report
- **THEN** the system excludes the summary, persists the full-report frontier candidate, and safely permits the child window to observe the same announcement again
