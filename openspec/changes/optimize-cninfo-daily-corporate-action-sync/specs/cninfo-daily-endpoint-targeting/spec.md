## ADDED Requirements

### Requirement: Daily candidates retain CNInfo source profiles
The system SHALL represent daily structured refresh work as deduplicated
`(instrument_id, source_profile)` targets for dividend and allotment profiles.

#### Scenario: Recent dividend event creates one target
- **WHEN** a recent CNInfo event is sourced from the dividend profile
- **THEN** the candidate plan SHALL include its instrument only for `cninfo_dividend`

#### Scenario: Uncertain announcement expands to both profiles
- **WHEN** an announcement is relevant but its structured endpoint cannot be determined reliably
- **THEN** the candidate plan SHALL include dividend and allotment targets for that instrument

#### Scenario: Duplicate evidence is merged
- **WHEN** multiple discovery sources select the same instrument and profile
- **THEN** the candidate plan SHALL execute that endpoint target at most once

### Requirement: Retry remains endpoint-specific
The system SHALL retry only the CNInfo source profile that previously failed
unless independent candidate evidence requires another profile.

#### Scenario: Allotment endpoint failed
- **WHEN** the retry inventory contains a transient `cninfo_allotment` failure
- **THEN** the retry plan SHALL NOT add `cninfo_dividend` solely because of that failure

### Requirement: Historical profile-complete calls remain compatible
The system SHALL preserve the existing behavior of explicit historical calls
that request both CNInfo scopes or do not provide an endpoint-target plan.

#### Scenario: Legacy backfill omits target profiles
- **WHEN** an existing caller requests a historical corporate-action backfill without endpoint targets
- **THEN** the system SHALL use the caller's configured source profiles with no required interface migration

### Requirement: Final transient retry is bounded and throttled
The system SHALL perform no more than one final low-speed pass for exhausted
transient endpoint targets through the shared source throttle.

#### Scenario: Final pass recovers a target
- **WHEN** a transient endpoint target succeeds on the final pass
- **THEN** the result SHALL count that target as recovered and SHALL NOT retain it as an error

#### Scenario: Permanent error occurs
- **WHEN** an endpoint target fails with a non-retryable validation or parameter error
- **THEN** the target SHALL NOT enter the final transient retry pass

### Requirement: Daily TDX reference refresh has bounded modes
The system SHALL support targeted and full TDX refresh modes and SHALL use
targeted mode for normal daily execution.

#### Scenario: Targeted daily refresh runs
- **WHEN** the normal daily task resolves effective TDX mode to `targeted`
- **THEN** it SHALL refresh only deduplicated CNInfo candidates, relevant announcements, retry or carryover instruments, and a bounded rotating sample

#### Scenario: Full refresh is requested
- **WHEN** an operator or periodic scheduler invocation selects `full`
- **THEN** the existing full-market TDX refresh path SHALL remain available

### Requirement: Daily sync exposes performance and source metrics
The structured result and operator report SHALL expose endpoint targets and
requests by CNInfo profile, final retry outcomes, TDX mode and scope size,
limiter metrics, and per-stage durations including anomaly LLM duration.

#### Scenario: Daily task completes
- **WHEN** a daily corporate-action sync finishes
- **THEN** operators SHALL be able to attribute elapsed time and failures to discovery, CNInfo endpoints, TDX reference refresh, factor rebuild, limiter waits, or LLM anomaly governance

### Requirement: CNInfo bulk retrieval remains evidence-gated
The system MUST NOT add a market-wide CNInfo structured retrieval path unless
bounded dates, pagination completeness, and repeatable request behavior are
verified.

#### Scenario: Bulk contract cannot be verified
- **WHEN** investigation cannot establish a complete and repeatable bulk endpoint contract
- **THEN** production SHALL continue using the supported endpoint-target path and record the investigation outcome
