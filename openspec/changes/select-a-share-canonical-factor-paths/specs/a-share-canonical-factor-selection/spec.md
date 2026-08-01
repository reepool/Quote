## ADDED Requirements

### Requirement: Comparable independent factor paths
The system SHALL rebuild CNInfo, TDX, and the existing BaoStock-plus-Sina composite
path into positive adjacent event ratios and latest-session unit-anchored cumulative paths
without modifying source data.

#### Scenario: Sources use different absolute cumulative bases
- **WHEN** two sources have proportional cumulative paths with different absolute scales
- **THEN** the normalized comparison reports them as equivalent

#### Scenario: Legacy factor field contains cumulative levels
- **WHEN** BaoStock rows in the composite path store cumulative levels in both factor columns
- **THEN** selection reads a pre-range cumulative anchor and compares adjacent ratios
  without modifying the physical composite table

#### Scenario: Legacy provider cumulative bases differ
- **WHEN** a BaoStock-to-Sina source switch has a cross-provider cumulative ratio that
  materially conflicts with the stored positive adjacent event factor
- **THEN** selection uses the stored event factor, rebuilds an internal continuous
  cumulative chain, and preserves the provider level and conflict diagnostic without
  modifying the physical composite table

#### Scenario: Legacy provider switch lacks a valid bridge
- **WHEN** a provider switch has no positive stored adjacent event factor, or a prefix row
  cannot be normalized before the requested start date
- **THEN** the composite path is ineligible for automatic source consensus and the failure
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
- **WHEN** eligible CNInfo, TDX, and BaoStock-Sina composite paths agree
- **THEN** the system selects CNInfo and records high-confidence three-source evidence

#### Scenario: CNInfo agrees with one independent source
- **WHEN** CNInfo agrees with either TDX or the BaoStock-Sina composite
- **THEN** the system selects CNInfo and records the agreeing source

#### Scenario: Independent sources agree on ordinary actions
- **WHEN** TDX and the BaoStock-Sina composite agree, CNInfo differs, and the segment contains only ordinary
  symmetric actions
- **THEN** the system selects the independent consensus path and records why CNInfo was not
  selected

#### Scenario: All sources disagree
- **WHEN** no two eligible paths agree and the CNInfo path is complete
- **THEN** the system selects CNInfo with low confidence and emits an auditable conflict

#### Scenario: CNInfo is incomplete
- **WHEN** no eligible consensus exists and the CNInfo path is incomplete
- **THEN** the segment remains blocked and no source silently fills the missing path

#### Scenario: Delisted lifecycle has a complete TDX history
- **WHEN** the lifecycle has ended, CNInfo has no event rows, TDX has a complete non-empty
  path, and the BaoStock-Sina composite path is absent or conflicting
- **THEN** the system selects TDX with `historical_single_source` confidence and records a
  bounded low-confidence audit decision including any composite conflict

#### Scenario: Historical TDX events contradict CNInfo zero-event coverage
- **WHEN** a completed lifecycle has CNInfo zero-event endpoint evidence but TDX contains a
  complete non-empty event path
- **THEN** the non-empty TDX history takes the guarded historical single-source branch
  instead of selecting the empty CNInfo path

#### Scenario: Active lifecycle has only one reference source
- **WHEN** CNInfo is incomplete for an active instrument and only TDX or the composite is eligible
- **THEN** the segment remains blocked

#### Scenario: Active instrument has an ended continuity segment
- **WHEN** an active instrument contains an earlier non-continuous lineage segment and only
  TDX supplies that segment's events
- **THEN** the segment remains blocked and is not treated as a completed delisted lifecycle

#### Scenario: Historical reference sources disagree
- **WHEN** CNInfo has no event rows, eligible TDX and BaoStock-Sina composite paths
  disagree, and the lifecycle has ended
- **THEN** the system selects the complete TDX path with
  `historical_single_source` confidence and preserves the disagreement in audit evidence

### Requirement: Reviewed whole-lifecycle source overrides
The system SHALL apply strictly validated reviewed source overrides before automatic
consensus without changing any source observation.

#### Scenario: Reviewed complete TDX override
- **WHEN** a reviewed catalog fixes an instrument's whole lifecycle to TDX and its TDX
  path is complete and non-empty
- **THEN** every continuity segment selects TDX, records `reviewed_source_override`
  confidence, and preserves the catalog version and reason

#### Scenario: Reviewed override path is unavailable
- **WHEN** the configured source path is incomplete or empty across the governed lifecycle
- **THEN** the segment remains blocked with an explicit override-ineligible reason

#### Scenario: Reviewed lifecycle contains a complete zero-event segment
- **WHEN** the configured source path is non-empty across the lifecycle and one continuity
  segment has explicit complete zero-event evidence
- **THEN** that segment retains the reviewed source decision without creating a factor row

#### Scenario: Invalid override catalog
- **WHEN** an entry has an unknown instrument identifier, source, scope, review date, or
  reason
- **THEN** catalog loading fails closed before candidate construction

#### Scenario: No reviewed overrides remain
- **WHEN** the versioned catalog contains an empty `instruments` object
- **THEN** candidate construction proceeds without reviewed source overrides

#### Scenario: Comparison-only rebuild does not construct a candidate
- **WHEN** three-source evidence is rebuilt with `build_canonical=false`
- **THEN** the override catalog is not loaded because no source decision is applied

### Requirement: Governed special-action policy
The system MUST retain the governed CNInfo path for segments containing share reform,
restructuring, compensation, debt conversion, debt settlement, asymmetric distributions,
or another approved special-action classification.

#### Scenario: TDX and the composite agree against special CNInfo path
- **WHEN** TDX and the BaoStock-Sina composite agree but the CNInfo segment is governed as a special action
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
  CNInfo, TDX, and the composite all contain no event on that boundary
- **THEN** the empty CNInfo path is accepted as a low-confidence listing-boundary
  zero-event decision and does not block the full-market candidate

#### Scenario: Unsupported exchange reaches listing boundary
- **WHEN** an instrument is outside CNInfo-supported exchanges
- **THEN** the listing-boundary shortcut does not fabricate CNInfo completion evidence

#### Scenario: Source event exists on listing boundary
- **WHEN** CNInfo, TDX, or the composite contains an event on the listing boundary
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

### Requirement: Existing BaoStock-Sina composite voting path
The system SHALL read `adjustment_factors` as one `baostock_sina_composite` source and SHALL preserve
its BaoStock/Sina row lineage without assigning separate votes to those row sources.

#### Scenario: Rebased composite path exists
- **WHEN** an instrument has valid BaoStock-Sina composite factor rows in the requested continuity segment
- **THEN** the composite path is eligible as the third source without an external download

#### Scenario: Composite path has no rows
- **WHEN** an instrument has no valid BaoStock-Sina composite factor rows
- **THEN** the composite source is unavailable and is not treated as a complete zero-event vote

#### Scenario: Composite rows exist only in another continuity segment
- **WHEN** valid BaoStock-Sina composite rows exist for the instrument but not in the requested continuity
  segment
- **THEN** the composite source is unavailable in that segment and cannot form an empty-path consensus

### Requirement: Versioned selection provenance
The system SHALL persist selected source, source profile, confidence, evidence count,
segment boundary, agreement set, score inputs, and decision reason for every canonical
candidate segment.

#### Scenario: Candidate inspection
- **WHEN** an operator queries the candidate version quality report
- **THEN** the report explains each low-confidence, overridden, or blocked segment without
  requiring direct database inspection

#### Scenario: Blocked decisions coexist with low-confidence samples
- **WHEN** blocked, low-confidence, and historical single-source decisions all exist
- **THEN** the report exposes a separate bounded `blocked_decisions` collection before
  other conflict samples without changing their existing confidence labels

#### Scenario: Reviewed source overrides remain auditable
- **WHEN** a reviewed whole-lifecycle source override is applied successfully
- **THEN** the report exposes a bounded `reviewed_source_override_samples` collection
  with the instrument, selected source, reason, and catalog version

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

### Requirement: Validated full-market canonical promotion
The system SHALL promote a canonical staging version only through an explicit confirmed
operation that revalidates persisted rows, coverage states, quality gates, scope, and
freshness.

#### Scenario: Promotion preview
- **WHEN** an operator requests promotion with `dry_run=true`
- **THEN** the system reports every promotion gate without changing the stable canonical
  version or production read activation

#### Scenario: Confirmed eligible promotion
- **WHEN** a full-market staging version is current, all persisted blocking gates pass,
  row and coverage counts agree, no instrument is incomplete, and `confirm=true`
- **THEN** the system atomically replaces the stable canonical version and preserves the
  staging version for audit

#### Scenario: Staging persistence mismatch
- **WHEN** the staging report row count, instrument count, summed event count, or persisted
  coverage states disagree
- **THEN** promotion fails before deleting or replacing any stable canonical row

#### Scenario: Stale or targeted staging version
- **WHEN** the staging candidate does not cover the full SSE/SZSE universe through the
  latest completed trading session
- **THEN** promotion remains blocked even if its selected rows contain no factor error

### Requirement: Runtime canonical activation and rollback
The system SHALL activate canonical reads through an atomic project-runtime manifest and
SHALL support an explicit rollback to the BaoStock-Sina composite path.

#### Scenario: Promotion and activation succeed
- **WHEN** the stable canonical version is promoted and activation is requested
- **THEN** subsequent factor reads use that canonical version and the choice survives an
  application restart

#### Scenario: Activation persistence fails
- **WHEN** stable promotion succeeds but the activation manifest cannot be replaced
- **THEN** the prior production read path remains active and the operation reports partial
  completion without deleting the promoted version

#### Scenario: Explicit composite rollback
- **WHEN** an operator confirms rollback
- **THEN** production reads return to the BaoStock-Sina composite path without deleting
  any canonical or source evidence

#### Scenario: Invalid activation manifest
- **WHEN** the runtime manifest has an unsupported dataset, missing canonical version, or
  invalid JSON
- **THEN** factor reads fail closed to the configured compatibility path and expose the
  activation error in provenance

### Requirement: Atomic incremental canonical continuation
The system SHALL maintain an active stable canonical version by atomically replacing only
the affected instruments from a validated local three-source staging candidate.

#### Scenario: Daily affected instruments are valid
- **WHEN** daily maintenance changes corporate actions for an instrument and its targeted
  candidate passes every applicable non-full-market blocking gate
- **THEN** the stable canonical version replaces only that instrument's rows, coverage
  state, and decision evidence

#### Scenario: Newly listed instrument lacks canonical coverage
- **WHEN** an active SSE/SZSE instrument has no status in the active canonical version
- **THEN** daily maintenance includes it in the targeted local selection scope even when it
  has no corporate-action event

#### Scenario: Targeted candidate is blocked
- **WHEN** any affected segment is blocked or a targeted write count is incomplete
- **THEN** the stable canonical rows remain unchanged and the affected instrument stays in
  the daily factor retry queue

#### Scenario: Canonical reads are not active
- **WHEN** production reads use the BaoStock-Sina composite path
- **THEN** daily maintenance does not merge a targeted staging candidate into a stable
  canonical version
