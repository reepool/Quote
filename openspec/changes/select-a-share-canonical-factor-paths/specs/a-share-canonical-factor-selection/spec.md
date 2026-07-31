## ADDED Requirements

### Requirement: Comparable independent factor paths
The system SHALL rebuild CNInfo, TDX, and the existing BaoStock-plus-Sina legacy composite
path into positive adjacent event ratios and latest-session unit-anchored cumulative paths
without modifying source data.

#### Scenario: Sources use different absolute cumulative bases
- **WHEN** two sources have proportional cumulative paths with different absolute scales
- **THEN** the normalized comparison reports them as equivalent

#### Scenario: Legacy factor field contains cumulative levels
- **WHEN** legacy BaoStock rows store cumulative levels in both factor columns
- **THEN** selection reads a pre-range cumulative anchor and compares adjacent ratios
  without modifying the legacy table

#### Scenario: Legacy provider cumulative bases differ
- **WHEN** a BaoStock-to-Sina source switch has a cross-provider cumulative ratio that
  materially conflicts with the stored positive adjacent event factor
- **THEN** selection uses the stored event factor, rebuilds an internal continuous
  cumulative chain, and preserves the provider level and conflict diagnostic without
  modifying the legacy table

#### Scenario: Legacy provider switch lacks a valid bridge
- **WHEN** a provider switch has no positive stored adjacent event factor, or a prefix row
  cannot be normalized before the requested start date
- **THEN** the legacy path is ineligible for automatic source consensus and the failure
  remains visible in bounded diagnostics

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

#### Scenario: Difference distribution
- **WHEN** three-source reconciliation completes
- **THEN** the report separates exact, shifted, unmatched, and conflicting events and
  summarizes relative event-factor differences in bounded buckets before cumulative-path
  differences are interpreted

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
- **WHEN** eligible CNInfo, TDX, and legacy composite paths agree
- **THEN** the system selects CNInfo and records high-confidence three-source evidence

#### Scenario: CNInfo agrees with one independent source
- **WHEN** CNInfo agrees with either TDX or the legacy composite
- **THEN** the system selects CNInfo and records the agreeing source

#### Scenario: Independent sources agree on ordinary actions
- **WHEN** TDX and the legacy composite agree, CNInfo differs, and the segment contains only ordinary
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

#### Scenario: TDX and legacy agree against special CNInfo path
- **WHEN** TDX and the legacy composite agree but the CNInfo segment is governed as a special action
- **THEN** the system keeps CNInfo and records the disagreement as market-account evidence

### Requirement: Factor-path completeness is separate from endpoint audit coverage
The system MUST determine CNInfo and TDX voting eligibility from their derived factor-path
state and MUST NOT reject a completed path only because recent instrument-specific endpoint
status intervals do not cover the full requested history.

#### Scenario: Announcement-driven CNInfo daily sync skips an unaffected instrument
- **WHEN** the CNInfo path has no pending event or historical gap but recent endpoint status
  does not include the requested end date
- **THEN** CNInfo remains eligible and the endpoint gap is reported only as an audit warning

#### Scenario: TDX recent refresh status covers only an incremental window
- **WHEN** the TDX factor path has no pending event but endpoint status covers only recent dates
- **THEN** TDX remains eligible and the endpoint interval is reported only as an audit warning

#### Scenario: Empty path has no completion evidence
- **WHEN** a source has no derived event in a continuity segment and no complete zero-event
  coverage evidence for the requested range
- **THEN** that source is unavailable for consensus in the segment

### Requirement: Existing legacy composite voting path
The system SHALL read `adjustment_factors` as one legacy composite source and SHALL preserve
its BaoStock/Sina row lineage without assigning separate votes to those row sources.

#### Scenario: Rebased legacy path exists
- **WHEN** an instrument has valid legacy factor rows in the requested continuity segment
- **THEN** the composite path is eligible as the third source without an external download

#### Scenario: Legacy path has no rows
- **WHEN** an instrument has no valid legacy factor rows
- **THEN** legacy is unavailable and is not treated as a complete zero-event vote

#### Scenario: Legacy rows exist only in another continuity segment
- **WHEN** valid legacy rows exist for the instrument but not in the requested continuity
  segment
- **THEN** legacy is unavailable in that segment and cannot form an empty-path consensus

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
