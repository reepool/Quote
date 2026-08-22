## ADDED Requirements

### Requirement: Announcement-only semantic processing is explicitly switchable
The system SHALL support `disabled`, `shadow`, and `active` announcement-only XDXR semantic modes without changing the versioned deterministic title policy.

#### Scenario: Semantic processing is disabled
- **WHEN** announcement-only semantic mode is `disabled`
- **THEN** the system SHALL NOT call the announcement-only LLM and SHALL use the deterministic title classifier as the authoritative queue decision

#### Scenario: Semantic processing is shadowed
- **WHEN** announcement-only semantic mode is `shadow`
- **THEN** the system SHALL record semantic outcomes but SHALL NOT change deterministic queue membership

#### Scenario: Enabled semantic processing fails
- **WHEN** mode is `active` or `shadow` and document or LLM processing fails
- **THEN** the system SHALL retain the candidate for conservative processing and SHALL report semantic execution as partial

### Requirement: Related announcements form one provisional event case
The system SHALL group related announcement-only notices into one stable provisional corporate-action case without creating a CNInfo source event.

#### Scenario: Later implementation notice matches an existing case
- **WHEN** a later announcement has the same instrument and action family and falls within the bounded association horizon of an existing case
- **THEN** the system SHALL add it to that case and SHALL NOT create an independent XDXR event or duplicate semantic unit

#### Scenario: Announcement belongs to a separate action
- **WHEN** no compatible bounded case exists for the instrument and action family
- **THEN** the system SHALL create a new provisional case keyed by its first announcement identity

### Requirement: One case selects the most suitable announcement evidence
The system SHALL process a bounded multi-announcement evidence bundle once per case and SHALL select one primary announcement plus optional supporting announcements.

#### Scenario: More authoritative notice arrives
- **WHEN** an implementation, completion, correction, or source-backed notice is more suitable than the current primary announcement
- **THEN** the system SHALL update the same case primary evidence and retain supersession lineage without duplicating the event decision

#### Scenario: Completion notice omits implementation terms
- **WHEN** a completion notice confirms execution but omits terms present in an earlier implementation notice
- **THEN** the system SHALL retain the implementation notice as primary or supporting evidence according to term completeness rather than selecting solely by publication time

### Requirement: Semantic routing uses likelihood and confidence conservatively
In `active` mode, the system SHALL apply configurable high and low XDXR likelihood thresholds only when judgment confidence meets the configured floor.

#### Scenario: High-likelihood case
- **WHEN** likelihood meets the high threshold and judgment confidence meets the floor
- **THEN** the case SHALL remain in the active pending queue as probable XDXR

#### Scenario: Low-likelihood case
- **WHEN** likelihood meets the low threshold and judgment confidence meets the floor
- **THEN** the case SHALL leave the active daily queue and SHALL remain durably available as inactive watch

#### Scenario: Ambiguous case
- **WHEN** likelihood is between thresholds or judgment confidence is below the floor
- **THEN** the case SHALL remain uncertain and SHALL NOT be automatically dismissed

### Requirement: Later source evidence reactivates inactive cases
The system SHALL reactivate inactive announcement cases when later authoritative company-action or factor-path evidence affects the same instrument.

#### Scenario: CNInfo or TDX event appears
- **WHEN** a new or changed CNInfo event or an in-window TDX event appears for an instrument with an inactive case
- **THEN** the system SHALL return the case to active semantic review and SHALL reconsider the best announcement bundle

#### Scenario: Material factor-path evidence appears
- **WHEN** reconciliation exposes a new material factor-path conflict or source-only event for an instrument with an inactive case
- **THEN** the system SHALL reactivate the case without treating that signal alone as an approved canonical event

### Requirement: Announcement-only decisions cannot alter canonical evidence
Announcement-only semantic results SHALL remain workflow routing state until associated with the existing governed structured-event lifecycle.

#### Scenario: Model classifies probable XDXR
- **WHEN** an announcement-only case receives a high-likelihood result
- **THEN** the system SHALL retain and refresh the case but SHALL NOT create a synthetic CNInfo observation or modify canonical factors

#### Scenario: Structured event later becomes available
- **WHEN** a governed CNInfo structured event is later associated with the case
- **THEN** the existing structured-event resolution and canonical promotion owners SHALL remain authoritative
