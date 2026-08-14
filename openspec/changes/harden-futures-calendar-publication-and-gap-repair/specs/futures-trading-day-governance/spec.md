## ADDED Requirements

### Requirement: Governance must resolve publication-aware expected trading dates
Trading-day governance SHALL resolve an expected latest trading date separately for each exchange from the run time, exchange timezone, configured publication cutoff, and verified calendar evidence.

#### Scenario: Current trading date has not reached publication cutoff
- **WHEN** a run starts before the selected exchange's publication cutoff on a verified trading date
- **THEN** governance SHALL use the latest verified trading date whose publication is already due as the expected latest date
- **AND** it SHALL NOT require current-date settlement bars

#### Scenario: Current trading date has reached publication cutoff
- **WHEN** a run starts at or after the selected exchange's publication cutoff on a verified trading date
- **THEN** governance SHALL expose the current trading date as that exchange's expected latest date

#### Scenario: Run occurs on a verified non-trading date
- **WHEN** a run starts on a weekend or officially verified closed date
- **THEN** governance SHALL expose the latest preceding publication-eligible verified trading date as the expected latest date

### Requirement: Governance target dates must include recent uncovered gaps
The daily target-date resolver SHALL combine publication-aware governed dates with missing recent price-coverage dates from the configured three-to-five-natural-day repair window.

#### Scenario: Recent trading date has no persisted price coverage
- **WHEN** a verified trading date in the repair window has no persisted bars for the resolved exchange scope
- **THEN** governance SHALL include that date in the target set even when a later date already has coverage

#### Scenario: Positive evidence repairs a formerly closed date
- **WHEN** official calendar repair changes a recent date from weakly closed or unresolved to verified trading
- **THEN** governance SHALL re-expand the target set after repair
- **AND** it SHALL include the repaired date when price coverage is missing

#### Scenario: Recent weekday remains unresolved
- **WHEN** a weekday in the repair window has neither positive trading evidence nor explicit closure evidence
- **THEN** governance SHALL expose it as an unresolved blocker
- **AND** it SHALL NOT silently omit it as a calendar skip

### Requirement: Target-date diagnostics must expose exchange freshness inputs
Governance SHALL return the date inputs needed to audit target selection for every requested exchange.

#### Scenario: Target dates are resolved
- **WHEN** governance resolves a daily or bounded futures request
- **THEN** its result SHALL include the requested range, publication cutoff used, publication-eligible as-of date, governed target dates, expected latest trading date, recent uncovered dates, calendar repairs, and unresolved blockers for each exchange
