## MODIFIED Requirements

### Requirement: Calendar governance supports observation and publication periods

The system SHALL support source-appropriate calendar governance for daily observations, monthly observations, source publication timestamps, availability timestamps, release grace windows, and policy effective periods.

#### Scenario: Monthly benchmark is synced

- **WHEN** a World Bank or FRED/IMF monthly copper price is synced
- **THEN** the system SHALL store it with a governed monthly observation date
- **AND** point-in-time lookup SHALL use the latest observation whose governed `available_at` is on or before the valuation cutoff.

#### Scenario: Policy event is synced

- **WHEN** a thermal-coal long-term contract or policy-price event is imported
- **THEN** the system SHALL store effective start and end dates
- **AND** it SHALL NOT fabricate daily prices unless an explicit derived series rule is configured.
- **AND** configured events SHALL validate commodity identity, source profile, effective dates, currency, unit, price-range ordering, and evidence URL before persistence.
- **AND** an official reasonable range SHALL preserve its lower and upper bounds without fabricating a midpoint or labeling the range as an observed transaction price.

#### Scenario: NBS thermal-coal ten-day benchmark is synced

- **WHEN** the official NBS Shanxi premium-blend 5500 kcal market-price series is synchronized
- **THEN** the system SHALL govern the product name, 5500 kcal specification, CNY-per-ton unit, and official article lineage before persistence
- **AND** it SHALL use the ten-day period end as `observation_date` while preserving period start, period end, expected release timestamp, grace deadline, actual publication timestamp, local first-seen timestamp, governed `available_at`, availability quality, and evidence separately
- **AND** period ends SHALL follow the source convention of days 10, 20, and 30, with February using its actual final day, rather than assuming every lower period ends on the calendar month end
- **AND** normal expected release timestamps SHALL follow the configured Asia/Shanghai 14th, 24th, and following-month 4th convention for the upper, middle, and lower periods respectively
- **AND** source fetching SHALL receive only periods that are due under the release plan, including the configured grace policy
- **AND** it SHALL identify the value as a wholesale/sales market-price reference rather than a futures price or long-term-contract price.
- **AND** a due historical ten-day period without a discovered official article after grace SHALL be reported as an unresolved source-period warning rather than silently omitted or inferred.
- **AND** a period before its planned release or within its grace window SHALL NOT be reported as unresolved.
- **AND** the NBS adapter SHALL normalize historical title variants such as `1-10`, `1日-10日`, and Chinese dash forms before period governance.
- **AND** official schedule cancellations or rescheduling SHALL be represented as configured observation exceptions with a reason and evidence URL.
- **AND** the report SHALL distinguish theoretical periods, not-due periods, grace periods, governed exceptions, discovered periods, delayed publications, unresolved periods, and source failures.
- **AND** governed exceptions SHALL NOT downgrade the task, while unresolved periods SHALL remain warnings.
- **AND** an upstream business rejection or anomalous empty broad search SHALL stop exact-period requests and SHALL be reported as a source failure rather than a legitimate empty dataset.

#### Scenario: Public-web daily page is missing

- **WHEN** a 100ppi daily page is missing for a requested date
- **THEN** the system SHALL record a source gap or warning
- **AND** it SHALL NOT fill the date with a weekday estimate.

#### Scenario: Daily source has no exchange calendar

- **WHEN** a FRED, EIA, or 100ppi daily series is governed
- **THEN** the calendar SHALL contain only source-observed dates or explicitly published source dates
- **AND** weekdays without source evidence SHALL NOT be marked as verified expected observations.

#### Scenario: Exchange-traded overseas source is enabled

- **WHEN** an LME or another exchange-traded overseas series is enabled
- **THEN** its adapter SHALL provide exchange-calendar evidence or source-observed trading-date evidence with an explicit quality label
- **AND** absent source dates SHALL remain unresolved rather than being inferred as weekdays or exchange closures.

## ADDED Requirements

### Requirement: Commodity calendar persistence is availability-aware and backward compatible
The system SHALL extend special-commodity publication-calendar persistence additively with temporal availability fields while preserving existing rows, legacy status fields, and callers that do not request point-in-time semantics.

#### Scenario: Existing database is initialized
- **WHEN** a database contains the legacy `commodity_publication_calendar` schema
- **THEN** initialization SHALL add missing temporal columns and indexes without dropping or rewriting existing rows
- **AND** existing calendar read APIs SHALL remain usable.

#### Scenario: Legacy row lacks availability evidence
- **WHEN** an existing row has no actual publication or first-seen evidence
- **THEN** migration SHALL leave `available_at` null
- **AND** it SHALL NOT backfill availability from `observation_date` or a fixed lag.

### Requirement: Commodity point-in-time consumers fail closed
Special-commodity storage SHALL support an availability cutoff, and DCF commodity context SHALL use it so valuation inputs include only observations locally governed as available by the valuation time.

#### Scenario: DCF valuation precedes publication
- **WHEN** a commodity observation date is on or before the valuation date but `available_at` is after the valuation cutoff
- **THEN** DCF SHALL exclude the observation
- **AND** it SHALL NOT silently fall back to observation-date-only selection.

#### Scenario: No governed observation remains
- **WHEN** all candidate commodity observations are unavailable or have missing availability evidence at the valuation cutoff
- **THEN** DCF readiness SHALL report a temporal availability dependency gap
- **AND** it SHALL NOT perform a remote fetch during valuation.
