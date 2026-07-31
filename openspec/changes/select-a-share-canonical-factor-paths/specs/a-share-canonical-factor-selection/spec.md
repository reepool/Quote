## ADDED Requirements

### Requirement: Comparable independent factor paths
The system SHALL rebuild CNInfo, TDX, and complete Sina `hfq-factor` observations into positive
adjacent event ratios and latest-session unit-anchored cumulative paths without modifying
the source observations.

#### Scenario: Sources use different absolute cumulative bases
- **WHEN** two sources have proportional cumulative paths with different absolute scales
- **THEN** the normalized comparison reports them as equivalent

#### Scenario: Invalid event ratio
- **WHEN** a source path contains a non-finite or non-positive event ratio
- **THEN** that source path is ineligible for automatic source consensus

### Requirement: Event and cumulative agreement
The system MUST require both bounded event-jump differences and bounded normalized
cumulative-path differences before treating two source paths as agreeing.

#### Scenario: Offsetting event errors
- **WHEN** two event errors offset in the final cumulative value
- **THEN** the paths remain in conflict and cannot form an automatic consensus

#### Scenario: Trading-session date shift
- **WHEN** equivalent factor jumps occur within the configured trading-session distance
- **THEN** the comparison records a shifted agreement without changing either source date

### Requirement: Continuity-segment isolation
The system SHALL select and accumulate one factor path independently within each legal
subject and price-continuity segment.

#### Scenario: Non-continuous absorption merger
- **WHEN** lineage metadata marks a transition as `price_continuity=non_continuous`
- **THEN** the system starts a new segment, resets its cumulative baseline, and does not
  create a synthetic factor at the transition

### Requirement: Deterministic three-source selection
The system SHALL select one complete source path per continuity segment using deterministic
consensus rules and SHALL NOT splice individual source events inside that segment.

#### Scenario: Three sources agree
- **WHEN** eligible CNInfo, TDX, and Sina paths agree
- **THEN** the system selects CNInfo and records high-confidence three-source evidence

#### Scenario: CNInfo agrees with one independent source
- **WHEN** CNInfo agrees with either TDX or Sina
- **THEN** the system selects CNInfo and records the agreeing source

#### Scenario: Independent sources agree on ordinary actions
- **WHEN** TDX and Sina agree, CNInfo differs, and the segment contains only ordinary
  symmetric actions
- **THEN** the system selects the independent consensus path and records why CNInfo was not
  selected

#### Scenario: All sources disagree
- **WHEN** no two eligible paths agree and the CNInfo path is complete
- **THEN** the system selects CNInfo with low confidence and emits an auditable conflict

#### Scenario: CNInfo is incomplete
- **WHEN** no eligible consensus exists and the CNInfo path is incomplete
- **THEN** the segment remains blocked and no source silently fills the missing path

### Requirement: Governed special-action policy
The system MUST retain the governed CNInfo path for segments containing share reform,
restructuring, compensation, debt conversion, debt settlement, asymmetric distributions,
or another approved special-action classification.

#### Scenario: TDX and Sina agree against special CNInfo path
- **WHEN** TDX and Sina agree but the CNInfo segment is governed as a special action
- **THEN** the system keeps CNInfo and records the disagreement as market-account evidence

### Requirement: Versioned selection provenance
The system SHALL persist selected source, source profile, confidence, evidence count,
segment boundary, agreement set, score inputs, and decision reason for every canonical
candidate segment.

#### Scenario: Candidate inspection
- **WHEN** an operator queries the candidate version quality report
- **THEN** the report explains each low-confidence, overridden, or blocked segment without
  requiring direct database inspection

### Requirement: Staging-only candidate construction
The system SHALL build canonical rows only in an explicit versioned staging candidate and
MUST NOT change production reads or source paths.

#### Scenario: Dry-run preview
- **WHEN** source selection runs with `dry_run=true`
- **THEN** it returns coverage, selection, and conflict summaries without database writes

#### Scenario: Candidate write
- **WHEN** selection runs with `dry_run=false` and `build_canonical=true`
- **THEN** it writes a staging candidate and status report but does not promote production

#### Scenario: Incomplete candidate write
- **WHEN** canonical rows or instrument statuses are only partially written
- **THEN** the candidate is not promotion-eligible
