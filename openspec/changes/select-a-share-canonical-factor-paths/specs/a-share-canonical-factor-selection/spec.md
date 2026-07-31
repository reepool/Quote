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

#### Scenario: Delisted lifecycle has only a complete TDX history
- **WHEN** the lifecycle has ended, CNInfo has no event rows, TDX has a complete non-empty
  path, and legacy provides no eligible conflicting path
- **THEN** the system selects TDX with `historical_single_source` confidence and records a
  bounded low-confidence audit decision

#### Scenario: Historical TDX events contradict CNInfo zero-event coverage
- **WHEN** a completed lifecycle has CNInfo zero-event endpoint evidence but TDX contains a
  complete non-empty event path and legacy provides no eligible conflicting path
- **THEN** the non-empty TDX history takes the guarded historical single-source branch
  instead of selecting the empty CNInfo path

#### Scenario: Active lifecycle has only one reference source
- **WHEN** CNInfo is incomplete for an active instrument and only TDX or legacy is eligible
- **THEN** the segment remains blocked

#### Scenario: Active instrument has an ended continuity segment
- **WHEN** an active instrument contains an earlier non-continuous lineage segment and only
  TDX supplies that segment's events
- **THEN** the segment remains blocked and is not treated as a completed delisted lifecycle

#### Scenario: Historical reference sources disagree
- **WHEN** CNInfo is incomplete and eligible TDX and legacy paths disagree
- **THEN** the segment remains blocked even if the lifecycle has ended

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
  coverage evidence anchored at the instrument lifecycle start
- **THEN** that source is unavailable for consensus in the segment

#### Scenario: Announcement-driven tail lacks per-instrument status
- **WHEN** CNInfo has accepted zero-event coverage from the instrument lifecycle start and
  no pending event or historical gap, but later announcement-driven maintenance did not
  request the unaffected instrument
- **THEN** the empty CNInfo path remains eligible and the tail interval gap is retained as
  an audit warning

#### Scenario: Empty CNInfo archive is contradicted
- **WHEN** an empty CNInfo path has endpoint coverage but TDX supplies a complete non-empty
  path for the same segment
- **THEN** the empty CNInfo path does not silently win ordinary source selection

#### Scenario: Coverage is bounded by listing lifecycle
- **WHEN** an instrument lists after the requested start or delists before the requested end
- **THEN** zero-event coverage and segment completeness are evaluated only from listing
  through delisting within the requested range

#### Scenario: Confirmed delisted instrument lacks delisting date
- **WHEN** the master explicitly marks an instrument `status=delisted` without an explicit
  delisting date and local quotes provide a last observed trading date
- **THEN** that date bounds the completed lifecycle for source selection and audit

#### Scenario: Automatic deactivation is not delisting evidence
- **WHEN** an instrument is inactive because of missing or stale local data but is not
  explicitly marked `status=delisted`
- **THEN** its last local quote does not end the lifecycle or enable historical fallback

#### Scenario: Instrument lists on the requested end date
- **WHEN** a CNInfo-supported instrument's lifecycle starts on the requested end date and
  CNInfo, TDX, and legacy all contain no event on that boundary
- **THEN** the empty CNInfo path is accepted as a low-confidence listing-boundary
  zero-event decision and does not block the full-market candidate

#### Scenario: Unsupported exchange reaches listing boundary
- **WHEN** an instrument is outside CNInfo-supported exchanges
- **THEN** the listing-boundary shortcut does not fabricate CNInfo completion evidence

#### Scenario: Source event exists on listing boundary
- **WHEN** CNInfo, TDX, or legacy contains an event on the listing boundary
- **THEN** the zero-event shortcut is not applied and normal source selection rules govern

### Requirement: Production comparison tolerance
The system SHALL default cross-provider event-factor and cumulative-path agreement to a
0.1% relative tolerance while preserving explicit operator overrides and difference
distribution reporting.

#### Scenario: Normal provider precision difference
- **WHEN** corresponding ordinary factor jumps differ by no more than 0.1% and cumulative
  paths remain within the same bound
- **THEN** the paths may form consensus and the exact difference remains auditable

#### Scenario: Explicit stricter preview
- **WHEN** an operator supplies a smaller positive tolerance
- **THEN** the supplied tolerance is used without being silently widened

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
