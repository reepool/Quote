## ADDED Requirements

### Requirement: Corporate-action lifecycle is explicit
The system SHALL define one authoritative flow from source observation through resolution, operator review, canonical promotion, factor rebuild, and canonical reads.

#### Scenario: Candidate requires operator review
- **WHEN** automatic resolution cannot safely promote the candidate
- **THEN** the transition result records pending review and prevents canonical promotion

### Requirement: Lifecycle stages have distinct owners
Observation, resolution, operator review, and canonical-factor application SHALL be owned by separate application services with explicit inputs, outputs, and allowed state transitions.

#### Scenario: Resolution service processes normalized evidence
- **WHEN** a provider observation enters resolution
- **THEN** the resolution service may create a decision candidate but cannot bypass operator requirements or directly alter unrelated provider data

### Requirement: Provider logic remains source-specific
CNInfo, TDX, and other provider modules SHALL own transport and source parsing, while application services SHALL consume normalized observations and own cross-stage orchestration.

#### Scenario: CNInfo payload format changes
- **WHEN** source parsing is updated
- **THEN** the application service contract remains based on the same normalized observation type

### Requirement: Canonical promotion has one owner
Only the canonical-factor application service SHALL promote an accepted corporate-action path to the authoritative factor state used by quote queries and backtests.

#### Scenario: Rebuild discovers a different candidate
- **WHEN** a rebuild produces staging evidence that is not approved under current governance
- **THEN** the canonical factor state remains unchanged

### Requirement: Stage execution is idempotent and resumable
Application services SHALL preserve existing identities, checkpoints, decisions, and watermarks so that retries do not duplicate decisions or factor writes.

#### Scenario: Resolution stage is retried after interruption
- **WHEN** the same evidence identity is processed again
- **THEN** the service resumes or returns the existing decision without creating a conflicting duplicate

### Requirement: Scheduler jobs are triggers, not alternate state machines
Existing scheduler jobs SHALL invoke the owning stage service and MUST NOT contain an independent implementation of stage order, decision policy, or canonical promotion.

#### Scenario: Daily corporate-action job runs
- **WHEN** it finds candidates requiring resolution
- **THEN** it submits them through the same resolution service used by operator and API paths

### Requirement: Factor and decision semantics remain equivalent
Extraction MUST preserve current TDX/CNInfo/manual evidence, accepted decisions, canonical tables, factor values, query adjustment behavior, and backtest results.

#### Scenario: Frozen corporate-action fixture is replayed
- **WHEN** the pre-migration and extracted paths process the fixture
- **THEN** canonical events, factor rows, and adjusted quote outputs are equivalent

### Requirement: The extraction baseline includes completed triage behavior
Corporate-action service extraction MUST use the post-`triage-announcement-only-xdxr-candidates` behavior as its baseline and preserve announcement-only modes, provisional case metadata, inactive-watch/reactivation semantics, and associated reports.

#### Scenario: Announcement-only case is replayed during W6 extraction
- **WHEN** the extracted stages process a fixture created after the triage slice
- **THEN** mode behavior, case lineage, inactive-watch/reactivation outcomes, and report fields remain equivalent while canonical promotion ownership stays unchanged

### Requirement: Compatibility paths do not duplicate logic
Retained DataManager and operator methods SHALL delegate to stage services and SHALL be removed after their callers migrate.

#### Scenario: Compatibility review command is used
- **WHEN** an operator invokes the existing command during migration
- **THEN** it reaches the same review service and state transition as the new command path
