## MODIFIED Requirements

### Requirement: Governance reuses fresh master state
The system SHALL avoid repeated upstream instrument-list fetches when local master data and its applicable terminal-reconciliation evidence satisfy the configured freshness policy. Reuse SHALL preserve unresolved or degraded evidence rather than equating recent current-row updates with complete lifecycle verification, and SHALL NOT bypass a known terminal decision becoming effective.

#### Scenario: Master data is already fresh
- **WHEN** a guarded job starts and supported exchange master data and applicable lifecycle checks satisfy the configured freshness window
- **THEN** the guard SHALL return a reused fresh result without fetching the same upstream lists again
- **AND** the result SHALL identify the freshness and lifecycle evidence used and preserve unresolved warnings if any

#### Scenario: Operator forces refresh
- **WHEN** a guarded job is invoked with an explicit force-master-refresh option
- **THEN** the guard SHALL run configured master sync and terminal reconciliation even if freshness evidence is still valid

#### Scenario: Current rows are recent but terminal verification failed or was never done
- **WHEN** recent `instruments.updated_at` values coexist with missing, failed, or expired applicable terminal-verification evidence
- **THEN** the guard SHALL NOT report complete healthy lifecycle verification solely from those timestamps
- **AND** it SHALL refresh when required by policy or report the unmet verification and configured continuation decision

#### Scenario: Reused result includes unresolved terminal candidates
- **WHEN** a reusable governance result still contains unconfirmed terminal candidates or conflicts
- **THEN** the reused result SHALL retain their counts, representative identities, and degraded status
- **AND** the next actual refresh SHALL reevaluate them rather than treating cached unresolved evidence as resolved

#### Scenario: Known terminal decision reaches its effective day
- **WHEN** a cached future decision becomes effective on the Shanghai business date while the ordinary freshness window remains valid
- **THEN** the guard SHALL invalidate the previous not-yet-effective conclusion and reevaluate the due decision before exposing the current universe
- **AND** it SHALL first load applicable production plans and the latest production verification result from the existing discrepancy table using the fixed durable protocol, including after a restart
- **AND** it SHALL apply still-reliable evidence through the existing lifecycle owner or perform the necessary actual refresh
- **AND** it SHALL NOT return the old active state solely because the ordinary freshness duration has not elapsed; failure SHALL retain visible degraded continuation semantics

### Requirement: Governance preserves A-share source authority
The system SHALL use exchange-official sources as primary A-share lifecycle authority, BaoStock as first fallback and cross-check, AkShare as second fallback, and pytdx as diagnostic-only when governance triggers a sync.

#### Scenario: A-share governance sync runs
- **WHEN** the guard refreshes `SSE`, `SZSE`, or `BSE` stock master data
- **THEN** official current and terminal evidence SHALL take precedence for lifecycle decisions
- **AND** BaoStock and AkShare SHALL retain their configured fallback or permitted supplementation roles
- **AND** pytdx SHALL remain diagnostic-only for instrument master status

#### Scenario: BSE uses fallback-only data
- **WHEN** BSE master data is refreshed without usable official lifecycle evidence
- **THEN** the governance result SHALL surface a warning that fallback data does not establish authoritative delisting completion or dates
- **AND** absence of BaoStock rows alone SHALL NOT downgrade otherwise valid official BSE evidence

### Requirement: Delisted instruments are not tradable
The instrument master governance layer SHALL normalize confirmed completed delistings so they are excluded from active and tradable current-universe selection, preserving actual effective-date semantics and historical records.

#### Scenario: Effective delisted date is normalized
- **WHEN** an instrument master upsert has a valid confirmed `delisted_date` on or before the current Shanghai business date
- **THEN** the stored instrument SHALL have `status=delisted`, `is_active=false`, and `trading_status=0`

#### Scenario: Explicit delisting write is normalized
- **WHEN** an official lifecycle write confirms completed delisting, including completion with an unknown exact date
- **THEN** the stored instrument SHALL have `status=delisted`, `is_active=false`, and `trading_status=0`
- **AND** it SHALL preserve an existing valid date or leave the exact date unknown without inventing one

#### Scenario: Historical data remains available
- **WHEN** an instrument is normalized as delisted
- **THEN** the system SHALL NOT delete historical quotes, valuation history, financial facts, shareholder snapshots, or other historical research records as part of that lifecycle update

#### Scenario: Official decision is not effective yet
- **WHEN** a terminal decision has a future effective date or does not yet prove completed delisting
- **THEN** governance SHALL NOT apply the completed-delisting normalization early
- **AND** existing non-terminal suspension or risk semantics SHALL remain distinct from confirmed delisting

### Requirement: Governance is visible in operator reports
The system SHALL include master-governance status in scheduler and Telegram reports for guarded jobs, preserving per-policy and per-exchange terminal-reconciliation detail separately from business-task results.

#### Scenario: Guarded job report is generated
- **WHEN** a guarded job completes or fails
- **THEN** the structured report SHALL include master-governance status, reason, source usage, warnings, and errors
- **AND** applicable terminal evidence, actual deactivations, unconfirmed candidates, future decisions, conflicts, source failures, and write failures SHALL remain visible with bounded examples

#### Scenario: Master governance is degraded
- **WHEN** the guard reports stale, fallback-only, unsupported, failed, incomplete, or unresolved lifecycle verification
- **THEN** the operator-facing report SHALL show that degradation separately from the business job result
- **AND** report merging or freshness reuse SHALL NOT replace that detail with overall success

### Requirement: Financial Maintenance Uses Shared Instrument Master Governance
Current-universe financial maintenance tasks SHALL run shared A-share instrument master governance before resolving active stock targets and SHALL consistently exclude confirmed completed delistings from all current candidate sources.

#### Scenario: Financial full import builds target universe
- **WHEN** `financial_l1_full_import` creates a new current active-stock target manifest
- **THEN** it SHALL evaluate shared master freshness before reading active instruments and use the post-governance local state
- **AND** the task report SHALL include the governance result

#### Scenario: Financial incremental sync scans current universe
- **WHEN** a financial disclosure incremental task maps CNInfo announcement symbols to local instruments
- **THEN** it SHALL use a current-universe instrument set produced after shared master governance
- **AND** if governance fails but continuation is configured, it SHALL report that it continued with existing local master data

#### Scenario: Pending delisting risk is not confirmed delisting
- **WHEN** financial disclosure events indicate suspension, delisting risk, or possible termination
- **THEN** the task SHALL preserve existing pending-risk processing without marking the master record formally delisted absent official terminal evidence

#### Scenario: Reconciliation and state recovery follow the same current scope
- **WHEN** reconciliation enumerates local gaps or incremental/reconciliation maintenance revisits pending, expired, risk, or accepted states
- **THEN** it SHALL restrict every candidate source to the same governed current instrument set
- **AND** a confirmed-delisted instrument SHALL NOT create a repair attempt or a new-run blocker solely because historical state or missing facts remain

#### Scenario: Existing full import resumes a frozen manifest
- **WHEN** full import resumes an already-frozen manifest
- **THEN** the system SHALL preserve that manifest and its original result contract
- **AND** operator documentation SHALL distinguish it from new current-universe selection rather than promising automatic removal of newly delisted targets
