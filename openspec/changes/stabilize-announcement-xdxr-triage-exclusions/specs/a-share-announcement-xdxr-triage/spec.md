## ADDED Requirements

### Requirement: Lockup-Share Listing Announcements Are Deterministically Non-XDXR

Title-based candidate discovery SHALL deterministically exclude lockup-share
listing announcements from announcement-only XDXR triage across title variants,
without invoking the LLM classifier.

#### Scenario: SSE variant without 份 is excluded
- **WHEN** a title contains `限售股` and `上市流通` (for example `公司股改限售股上市流通公告` or `限售股上市流通公告`) and carries none of the implementation markers `转增股本实施`, `转增股份实施`, `股份到账`, `股权登记`, `除权`, `除息`
- **THEN** discovery SHALL classify the title as `deterministic_exclusion:restricted_shares_listing_circulation` and SHALL NOT select it for semantic review

#### Scenario: Existing SZSE variant stays excluded
- **WHEN** a title contains `限售股份` and `上市流通` under the same guard conditions
- **THEN** discovery SHALL keep excluding it exactly as before

#### Scenario: Combined genuine distributions stay selectable
- **WHEN** a lockup-listing title also carries a genuine distribution implementation marker (`权益分派实施`, `利润分配实施`, `现金红利发放`, `分红派息实施`) or an implementation guard marker (`转增股本实施`, `股份到账`, `股权登记`, `除权`, `除息`)
- **THEN** discovery SHALL NOT exclude the title through the lockup rule

### Requirement: Fully-Excluded Announcement-Only Cases Become Terminal

Announcement-only triage governance SHALL move an existing case to the terminal
`deterministic_excluded` routing status when none of its announcements is
retained after deterministic exclusion filtering.

#### Scenario: All announcements excluded
- **WHEN** an existing non-terminal case's announcements all classify as deterministically excluded and none matches a structured event
- **THEN** the case's routing status SHALL become `deterministic_excluded`
- **AND** any stale processing error recorded on the case SHALL be cleared
- **AND** the case SHALL NOT be dispatched to the LLM classifier in subsequent runs
- **AND** its announcements SHALL NOT appear in unmatched special-announcement lists

#### Scenario: Structured evidence keeps precedence
- **WHEN** a case has an announcement matched to a structured corporate-action event
- **THEN** the case's routing status SHALL be `structured_event_available` and SHALL NOT be excluded by this rule

#### Scenario: Partial exclusion still refreshes
- **WHEN** only part of a case's announcements is deterministically excluded
- **THEN** governance SHALL refresh the case from the retained announcements without terminating it, as before

### Requirement: Non-XDXR Verdicts Survive Action-Family Divergence

The announcement-only triage classifier SHALL accept an LLM decision whose
action family differs from the case's marker-derived family when the decision's
disposition is `non_xdxr`, and SHALL keep rejecting family divergence for every
other disposition.

#### Scenario: Excluding verdict with divergent family is accepted
- **WHEN** the LLM returns `disposition=non_xdxr` with an action family different from the case's
- **THEN** the decision SHALL be accepted
- **AND** the decision's action family SHALL be the case's family with the LLM's label preserved as a diagnostic
- **AND** the case SHALL route by the existing likelihood/confidence thresholds (typically `inactive_watch`)

#### Scenario: Non-excluding verdict with divergent family still fails
- **WHEN** the LLM returns any disposition other than `non_xdxr` with an action family different from the case's
- **THEN** validation SHALL fail with an action family mismatch error, as before
