## ADDED Requirements

### Requirement: Temporal availability is modeled independently from observation time
The system SHALL represent an observation period separately from its expected release timestamp, grace deadline, actual publication timestamp, local first-seen timestamp, effective availability timestamp, availability quality, evidence, and lifecycle status.

#### Scenario: Planned release exists without published data
- **WHEN** an observation period has a configured expected release timestamp but no publication or first-seen evidence
- **THEN** the system SHALL preserve the release plan without assigning `available_at`
- **AND** it SHALL NOT treat the observation date as evidence of availability.

#### Scenario: Naive timestamp is supplied
- **WHEN** temporal governance receives a datetime without timezone information
- **THEN** it SHALL reject the value with an explicit validation error
- **AND** it SHALL NOT interpret the value using the host timezone.

### Requirement: Release lifecycle status is deterministic
The system SHALL derive lifecycle status from the release plan, configured grace window, availability evidence, governed exceptions, source failures, and the timezone-aware evaluation timestamp.

#### Scenario: Release is not due
- **WHEN** the evaluation timestamp is before the expected release timestamp
- **THEN** status SHALL be `not_due`
- **AND** absence of data SHALL NOT be reported as a warning or blocker.

#### Scenario: Release is within grace
- **WHEN** the expected release timestamp has passed, the grace deadline has not passed, and no availability evidence exists
- **THEN** status SHALL be `due_in_grace`
- **AND** absence of data SHALL NOT be reported as an unresolved gap.

#### Scenario: Release is overdue
- **WHEN** the grace deadline has passed and no availability, cancellation, rescheduling, or source-failure evidence exists
- **THEN** status SHALL be `unresolved_gap`
- **AND** the observation period SHALL remain actionable.

#### Scenario: Data arrives after grace
- **WHEN** governed availability evidence is later than the grace deadline
- **THEN** status SHALL be `delayed_available`
- **AND** `available_at` SHALL preserve the evidenced timestamp.

#### Scenario: Provider fails for a due period
- **WHEN** a due source request returns a transport failure, business rejection, or anomalous invalid payload
- **THEN** status SHALL be `source_failure`
- **AND** the failure SHALL NOT be represented as a legitimate empty publication.

### Requirement: Governed exceptions require evidence
The system SHALL support cancellation and rescheduling exceptions for an exact observation scope only when the exception includes a reason and evidence URL.

#### Scenario: Publication is cancelled
- **WHEN** an evidenced cancellation matches an observation period
- **THEN** status SHALL be `cancelled`
- **AND** the absent observation SHALL NOT downgrade the synchronization task.

#### Scenario: Publication is rescheduled
- **WHEN** an evidenced rescheduling exception supplies a replacement release timestamp
- **THEN** status SHALL be `rescheduled`
- **AND** due-state evaluation SHALL use the replacement timestamp and its grace deadline.

#### Scenario: Exception lacks evidence
- **WHEN** a cancellation or rescheduling exception omits its reason or evidence URL
- **THEN** configuration validation SHALL fail
- **AND** the exception SHALL NOT suppress an unresolved gap.

### Requirement: Point-in-time eligibility uses governed availability
The system SHALL expose a reusable eligibility check that includes an observation only when its governed `available_at` is on or before the requested timezone-aware cutoff.

#### Scenario: Observation was published after valuation
- **WHEN** an observation date precedes a valuation cutoff but its `available_at` follows the cutoff
- **THEN** the observation SHALL be excluded from the point-in-time result.

#### Scenario: Availability evidence is missing
- **WHEN** a point-in-time read encounters an observation without governed `available_at`
- **THEN** the observation SHALL be excluded
- **AND** the consumer SHALL be able to report an availability-quality gap.

#### Scenario: Operational read has no cutoff
- **WHEN** a caller requests current operational observations without a point-in-time cutoff
- **THEN** the domain storage API MAY preserve its existing observation-date behavior
- **AND** the result SHALL NOT be represented as point-in-time safe.

### Requirement: Existing domain calendars retain ownership
The shared temporal contract SHALL be adoptable by source domains without replacing their source-specific calendar evidence, storage, or trading/publication semantics.

#### Scenario: FX and futures remain unmigrated
- **WHEN** this change is deployed
- **THEN** existing FX publication and futures trading calendar services SHALL remain behaviorally compatible
- **AND** no migration of their persisted calendar tables SHALL be required.
