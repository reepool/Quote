## ADDED Requirements

### Requirement: Effective unit rules replay the originating artifact immediately
The business-profile semantic runtime SHALL refresh effective runtime unit rules and replay the persisted structured response during the same stage invocation when registration produces an effective unit rule.

#### Scenario: Newly approved rule resolves the response
- **WHEN** structured conversion encounters an unknown unit and the governed proposal lifecycle auto-approves a rule for it
- **THEN** the runtime SHALL convert the persisted response with a fresh rule overlay without another extraction LLM call
- **AND** it SHALL NOT charge another work attempt or leave the originating item in machine rework solely for that unit

#### Scenario: Inline replay still cannot resolve the response
- **WHEN** the refreshed overlay does not resolve every required conversion
- **THEN** the runtime SHALL preserve conversion-pending diagnostics and SHALL NOT publish an unproved normalized value

### Requirement: Unresolved units are isolated by row
The business-profile semantic runtime SHALL preserve unresolved source values and units without blocking independently convertible rows from the same structured response.

#### Scenario: One row uses a cross-dimensional alternative unit
- **WHEN** one operating-fact row uses a unit such as `T/KL` that has no single dimensionally valid canonical multiplier and other rows are valid
- **THEN** the unresolved row SHALL remain in the immutable semantic artifact with a row-level conversion-pending reason
- **AND** independently valid rows SHALL remain eligible for canonical publication

#### Scenario: Every row is conversion pending
- **WHEN** every structured operating-fact row has an unresolved unit
- **THEN** the artifact SHALL remain replayable with its raw values, raw units, evidence, and reason codes
- **AND** the system SHALL perform zero fabricated conversion and zero duplicate extraction LLM calls

### Requirement: Unit proposals cannot invent base-token proof
The unit-rule proof engine SHALL require mechanically governed lexical and dimensional coverage for every source-unit base token before auto-approval.

#### Scenario: Model maps an unknown classifier to a known count primitive
- **WHEN** a proposal for an unknown source token claims a governed count primitive that does not lexically cover that token
- **THEN** the proposal SHALL be quarantined with an explicit unproved-source-token reason

#### Scenario: Source unit is deterministically governed
- **WHEN** a source unit and magnitude are present in the versioned deterministic catalog
- **THEN** program conversion SHALL resolve it without invoking the unit-proposal LLM

### Requirement: Known Chinese operating units have deterministic semantics
The versioned unit catalog SHALL govern `项` and `艘` as count classifiers and SHALL govern float-glass `重箱/重量箱` as mass rather than generic count.

#### Scenario: Alternative count classifiers are reported
- **WHEN** an annual-report table reports `套/项`
- **THEN** the program SHALL interpret both tokens as same-dimension count alternatives with multiplier one

#### Scenario: Ship count is reported
- **WHEN** an annual-report table reports `艘`
- **THEN** the program SHALL normalize it to canonical count with multiplier one

#### Scenario: Ten-thousand weight cases are reported
- **WHEN** a float-glass annual-report table reports `万重箱` or `万重量箱`
- **THEN** the program SHALL preserve the source unit and normalize it as mass using 500 metric tonnes per source unit
- **AND** it SHALL NOT normalize it as ten-thousand generic count units

### Requirement: Later unit resolution replays persisted work automatically
The unit registry SHALL make a newly effective deterministic or operator-corrected rule eligible to replay affected conversion-pending artifacts and owning work items without a new extraction request.

#### Scenario: Quarantined rule is superseded by deterministic catalog support
- **WHEN** catalog reconciliation replaces a quarantined rule with an effective deterministic rule
- **THEN** affected semantic artifacts SHALL be marked for conversion replay and owning completed or machine-rework work SHALL become resumable without attempt cost
