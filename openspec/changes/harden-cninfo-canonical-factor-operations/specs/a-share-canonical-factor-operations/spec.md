## ADDED Requirements

### Requirement: Canonical Series Readiness Is Lightweight
The system SHALL read production canonical readiness without loading or parsing full-market selection decisions.

#### Scenario: Adjusted quote is requested
- **WHEN** a caller requests adjusted quotes for one instrument
- **THEN** the system SHALL read only compact series status, that instrument's coverage state, that instrument's decisions, and that instrument's factors
- **AND** it SHALL NOT retain a full-market report in an instrument cache entry

#### Scenario: Adjusted quote is requested outside the A-share canonical scope
- **WHEN** a caller requests adjusted quotes for a non-A-share or B-share stock while the tracked default selects the A-share canonical series
- **THEN** the system SHALL use that market's maintained BaoStock/Sina composite factors
- **AND** absent coverage in the A-share canonical series SHALL NOT make the request unavailable

### Requirement: Canonical Decisions Are Normalized And Pageable
The system SHALL persist canonical selection decisions separately from compact series summaries and SHALL expose bounded decision queries.

#### Scenario: Full-market candidate is persisted
- **WHEN** a canonical candidate contains decisions for multiple instruments
- **THEN** each decision SHALL be stored under its series version, instrument, and continuity segment
- **AND** the series report SHALL contain aggregates and bounded samples rather than the complete decision array

#### Scenario: Existing report is migrated
- **WHEN** an existing series contains decisions only in its report JSON
- **THEN** migration SHALL copy and verify all decisions before compacting the report
- **AND** any count or identity mismatch SHALL leave the original report intact

#### Scenario: Existing report has not yet been migrated
- **WHEN** a promoted series has no normalized decision rows but still embeds valid decisions in its legacy report
- **THEN** adjusted quote reads SHALL temporarily resolve only the requested instrument from that report
- **AND** ordinary reads SHALL return to normalized bounded storage after migration

#### Scenario: Legacy series receives a targeted merge
- **WHEN** a promoted target still embeds decisions in its legacy report and a targeted staging subset is ready to merge
- **THEN** the system SHALL normalize and verify the complete target decision set in the same transaction before replacing the targeted subset
- **AND** decisions for untouched instruments SHALL remain available after the report is compacted

### Requirement: Canonical Activation Is Durable And Fail-Safe
The tracked production default SHALL identify the promoted stable canonical series, and invalid runtime activation state SHALL NOT silently select another factor dataset.

#### Scenario: Runtime activation manifest is absent
- **WHEN** the application starts without a runtime activation manifest
- **THEN** it SHALL use the tracked canonical production default

#### Scenario: Runtime activation manifest is invalid
- **WHEN** the activation manifest exists but fails validation
- **THEN** adjusted factor availability SHALL report the activation error
- **AND** the system SHALL NOT silently fall back to the BaoStock/Sina composite

#### Scenario: Operator performs explicit rollback
- **WHEN** an operator confirms rollback to the BaoStock/Sina composite
- **THEN** the system SHALL record a valid activation manifest with reason and timestamp
- **AND** it SHALL preserve canonical and source evidence for later restoration

### Requirement: Canonical Quality Summaries Are Deterministic
Full builds, promotions, and targeted merges SHALL derive quality summaries from the complete persisted decision and instrument-status sets using one policy.

#### Scenario: Targeted daily subset is merged
- **WHEN** a promoted canonical series receives a targeted subset update
- **THEN** selection, confidence, agreement, blocked, coverage, and completeness metrics SHALL be recomputed for the merged series
- **AND** stale full-build metrics SHALL NOT be retained as current values

#### Scenario: Every instrument is complete
- **WHEN** every persisted instrument status is complete with events or complete with no events
- **THEN** coverage SHALL be 1.0 and overall completeness SHALL be success

#### Scenario: A source pair is not comparable
- **WHEN** a decision marks a pairwise comparison as `source_incomplete`
- **THEN** reconciliation totals SHALL exclude that pair from compared segments and event totals
- **AND** full builds, promotions, and targeted merges SHALL expose the same reconciliation schema

### Requirement: BaoStock Sina Qualification Describes Factor Path Integrity
The system SHALL qualify the BaoStock/Sina composite by factor-path validity and SHALL NOT claim XDXR event completeness.

#### Scenario: Composite path is eligible
- **WHEN** an instrument's normalized composite factors are positive and finite, its cumulative chain is valid, and provider transitions are bridgeable
- **THEN** the path MAY corroborate another source in canonical selection
- **AND** diagnostics SHALL state that event completeness is not asserted

#### Scenario: Composite path is invalid
- **WHEN** normalization contains an invalid cumulative value or an unbridgeable source transition
- **THEN** the composite SHALL be excluded from source consensus for the affected instrument or segment
- **AND** it MAY remain visible as diagnostic evidence

### Requirement: Canonical Evidence Retention Is Protected
The system SHALL provide previewable retention for obsolete canonical operational records without automatically deleting active production evidence.

#### Scenario: Retention is previewed
- **WHEN** an operator requests retention in dry-run mode
- **THEN** the result SHALL list candidate versions and endpoint-status rows, protected versions, estimated row counts, and reasons
- **AND** no database rows SHALL be deleted

#### Scenario: Retention is confirmed
- **WHEN** an operator sets dry-run false and confirms the exact retention action
- **THEN** the system SHALL delete only unprotected candidates selected by the policy
- **AND** it SHALL preserve the active stable series, activation references, and configured recent staging and benchmark evidence
- **AND** it SHALL preserve non-dominated endpoint coverage intervals needed to prove historical source coverage

#### Scenario: A wider endpoint attempt is incomplete
- **WHEN** a partial or indeterminate endpoint-status row covers the date range of an older complete row
- **THEN** retention SHALL preserve the older complete row
- **AND** only accepted complete coverage MAY dominate complete historical evidence

### Requirement: Operational Entry Points Have Explicit Lifecycles
Canonical factor tasks and one-off governance utilities SHALL disclose whether they are current, deprecated, replayable, or archived.

#### Scenario: Deprecated factor rebuild is listed
- **WHEN** an operator inspects available scheduled tasks
- **THEN** obsolete AkShare-era rebuild entry points SHALL be disabled or clearly rejected as deprecated
- **AND** current CNInfo/TDX rebuild, three-source selection, promotion, and retention tasks SHALL remain unambiguous

#### Scenario: One-off governance script remains in the repository
- **WHEN** a script contains fixed event keys or analysis identifiers
- **THEN** repository documentation SHALL classify it as a replayable governance manifest or archived migration
- **AND** it SHALL NOT appear to be a general production command
