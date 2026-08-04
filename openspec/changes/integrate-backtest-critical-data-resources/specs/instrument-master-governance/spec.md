## ADDED Requirements

### Requirement: Master governance emits governed security-state transitions
Shared instrument-master governance SHALL compare newly accepted current master observations with prior accepted state and emit eligible forward security-state or lifecycle transitions with provenance.

#### Scenario: ST marker changes in current master evidence
- **WHEN** an accepted current master observation changes between normal, ST, and *ST state
- **THEN** governance SHALL record an observed transition with observation time, local availability, source profile, prior state, new state, and quality
- **AND** it SHALL make the transition available to the historical-security-state interval builder

#### Scenario: Current observation lacks an effective date
- **WHEN** a current master or current ST list proves only the observed state
- **THEN** governance SHALL NOT assign a historical effective date earlier than the observation evidence
- **AND** it SHALL not claim coverage for the preceding unknown interval

### Requirement: Official lifecycle evidence remains authoritative for historical transitions
Instrument-master governance SHALL reuse official exchange lists and source-neutral announcement evidence for effective lifecycle transitions and SHALL not weaken existing confirmed-delisting authority.

#### Scenario: Announcement enriches an observed transition
- **WHEN** retained official evidence supplies a publication time and effective date for an ST, suspension, resumption, or delisting transition
- **THEN** governance SHALL link or promote that evidence according to source authority while preserving the original observation

#### Scenario: Current source conflicts with official lifecycle evidence
- **WHEN** a supplemental current list conflicts with accepted official lifecycle evidence
- **THEN** official evidence SHALL retain authority for confirmed lifecycle fields
- **AND** the conflict SHALL be reported for review rather than silently rewriting history

### Requirement: State emission reuses master refresh work
Security-state transition detection SHALL run from the persisted result of the shared master refresh and SHALL not repeat the same full-market master request.

#### Scenario: Multiple guarded jobs reuse fresh master state
- **WHEN** the freshness guard skips repeated upstream fetches
- **THEN** state-transition emission SHALL use the same accepted local refresh evidence and idempotency markers
- **AND** unchanged state SHALL not produce duplicate transition events or change records

#### Scenario: Historical job skips current governance
- **WHEN** a historical workflow uses the existing current-master isolation policy
- **THEN** it SHALL not manufacture historical security events from a newly fetched current universe
