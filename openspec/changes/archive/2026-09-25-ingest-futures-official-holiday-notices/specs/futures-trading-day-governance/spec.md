## ADDED Requirements

### Requirement: Official holiday notices define full-exchange closures
The trading-day governance service SHALL derive full-exchange closed dates from each configured exchange's own official holiday notice. A shared date list across exchanges SHALL NOT replace per-exchange notice evidence.

#### Scenario: Closure range is parsed
- **WHEN** an official notice for one exchange states an inclusive full-day closure range
- **THEN** the service SHALL store that notice in `futures_calendar_notices`
- **AND** it SHALL write one governed closed calendar row for each date in the range with `quality_flag=backfilled_verified`, `source_profile=exchange_official_holiday_notice`, `classification_rule=official_holiday_notice`, the notice id, and the notice URL
- **AND** a bare month-day SHALL use the notice's stated year, while an explicit other year in the same sentence SHALL be kept

#### Scenario: The same closure is announced by another exchange
- **WHEN** a second exchange publishes the same closure dates in its own notice
- **THEN** the service SHALL store a separate notice and separate calendar rows for that exchange

#### Scenario: Notice text does not yield a confident range
- **WHEN** a fetched holiday notice contains no confident closure range
- **THEN** the service SHALL create a review-required record
- **AND** it SHALL NOT write guessed closed or open rows from that notice

#### Scenario: One stated date disagrees with a verified row
- **WHEN** a notice closes a date already verified as a trading day, or opens a date already verified as closed for a different reason than the weekend rule
- **THEN** the service SHALL create a review-required record for that date and SHALL NOT overwrite it
- **AND** other confident ranges in the same notice SHALL still be written
- **AND** a date the notice closes that is already closed, including a deterministic weekend, SHALL be kept closed and MAY be updated to `official_holiday_notice`

#### Scenario: A later notice revises the year
- **WHEN** a newer official notice for the same exchange states a different status for dates already derived from an earlier notice
- **THEN** the service SHALL apply only the dates that newer notice explicitly states
- **AND** it SHALL NOT drop closures from the earlier notice that the newer notice does not mention

### Requirement: Night-session suspension is not a closed trading day
The governance service SHALL record an official night-session suspension without changing that date's trading-day status.

#### Scenario: Notice suspends only the night session
- **WHEN** an official notice says a named date has no night session and does not include that date in a full-day closure
- **THEN** the service SHALL store the suspension on the notice
- **AND** it SHALL NOT set `is_trading_day=false` for that date
- **AND** when that date already is or later becomes a verified trading day, the calendar row metadata SHALL include the night-session suspension

### Requirement: Official notice overrides the weekend closure rule
A weekend SHALL remain closed only when no official notice explicitly marks that weekend date as a trading day.

#### Scenario: Notice names a weekend as open
- **WHEN** an official holiday notice explicitly marks a Saturday or Sunday as a trading day
- **THEN** the governed calendar row SHALL be a trading day sourced from that notice
- **AND** the deterministic weekend rule SHALL NOT overwrite it as closed

#### Scenario: Notice agrees the weekend is closed
- **WHEN** an official notice only repeats that a weekend is closed, or says nothing about that weekend
- **THEN** the existing weekend closure rule SHALL still close that date

### Requirement: Notice-closed dates are absent from target trading dates
Target-date expansion SHALL exclude dates governed as closed by an official holiday notice.

#### Scenario: Sync window contains a notice holiday
- **WHEN** trading-day governance expands a range that includes a date closed by `official_holiday_notice`
- **THEN** that date SHALL NOT be a target trading date
- **AND** the expansion SHALL count it as a skipped non-trading day
- **AND** it SHALL NOT be an unresolved or review-required blocker
