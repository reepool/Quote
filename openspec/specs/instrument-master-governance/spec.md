# instrument-master-governance Specification

## Purpose
Instrument master governance provides a shared pre-run control layer for
current-universe update jobs. It ensures A-share stock master data is fresh
before quote, research, financial, or derived snapshot jobs resolve active
instruments, while preserving historical/backfill semantics and making source
provenance visible to operators.
## Requirements
### Requirement: Shared instrument master governance guard
The system SHALL provide a shared pre-run guard that ensures supported instrument master data is fresh before jobs build a current active-stock universe.

#### Scenario: Guard runs before a current active-universe job
- **WHEN** a guarded current update job starts for one or more supported exchanges
- **THEN** the system SHALL evaluate instrument master freshness before the job resolves active stock instruments
- **AND** the job SHALL use the post-guard local `instruments` state when building its universe

#### Scenario: Quote update uses the shared guard
- **WHEN** normal quote daily update needs pre-run instrument master synchronization
- **THEN** it SHALL invoke the shared instrument master governance guard
- **AND** the system SHALL NOT maintain a separate quote-only master-sync pre-run implementation

#### Scenario: Guard returns structured result
- **WHEN** the governance guard evaluates freshness or runs a sync
- **THEN** it SHALL return status, reason, exchanges, source usage, added counts, deactivated counts, active counts, freshness timestamps, warnings, errors, and elapsed time

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

### Requirement: Governance is market-policy aware
The system SHALL only refresh markets whose master-data policy is explicitly supported and SHALL dispatch supported markets to their own policy implementation.

#### Scenario: Unsupported market is requested
- **WHEN** a guarded job includes a market without an enabled master-data policy
- **THEN** the guard SHALL skip that market with an explicit unsupported-market reason
- **AND** it SHALL NOT apply A-share or HKEX lifecycle rules to that market

#### Scenario: HKEX policy is enabled
- **WHEN** a current HKEX research, quote, financial, disclosure, or derived snapshot job runs after HKEX master policy is enabled
- **THEN** the guard SHALL evaluate HKEX master freshness through the HKEX master policy
- **AND** it SHALL NOT call the A-share `sync_instrument_master()` implementation for HKEX
- **AND** the job report SHALL include the HKEX governance result

#### Scenario: Mixed A-share and HKEX scope is requested
- **WHEN** a guarded current-universe job includes both A-share exchanges and `HKEX`
- **THEN** the guard SHALL dispatch A-share exchanges to the A-share policy and `HKEX` to the HKEX policy
- **AND** it SHALL merge policy results into one structured governance report without losing per-market warnings or errors

#### Scenario: HKEX policy is disabled
- **WHEN** a current HKEX job runs while HKEX master policy is disabled by configuration
- **THEN** the guard SHALL skip HKEX with an explicit disabled or unsupported policy reason
- **AND** the job report SHALL make the skip reason visible when master governance is included

### Requirement: Historical jobs do not silently use current master refresh
The system SHALL keep historical backfill and point-in-time workflows isolated from automatic current master refresh unless explicitly requested.

#### Scenario: Historical backfill starts
- **WHEN** a guarded job is identified as historical backfill or point-in-time rebuild
- **THEN** the guard SHALL skip current master refresh by default
- **AND** the result SHALL include a historical-universe skip reason

#### Scenario: Historical job explicitly requests current refresh
- **WHEN** a historical job explicitly requests current master refresh
- **THEN** the guard SHALL run only the configured current master refresh
- **AND** the result SHALL report that the historical job used current-universe governance

### Requirement: Governance failures are reported with continuation policy
The system SHALL make master-governance failures visible and apply configured continuation behavior.

#### Scenario: Guard fails and continuation is allowed
- **WHEN** master governance fails and the job is configured to continue
- **THEN** the guarded job MAY continue using the existing local universe
- **AND** the report SHALL mark master governance as failed or degraded with the continuation decision

#### Scenario: Guard fails and continuation is not allowed
- **WHEN** master governance fails and the job is configured to fail closed
- **THEN** the guarded job SHALL stop before resolving the active universe
- **AND** the report or raised error SHALL include the master-governance failure reason

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

### Requirement: HKEX Governance Uses Official-Source Policy
The shared governance guard SHALL enforce HKEX-specific source authority when it refreshes HKEX master data.

#### Scenario: HKEX governance refreshes master data
- **WHEN** the guard refreshes HKEX stock master data
- **THEN** official HKEX or HKEXnews evidence SHALL be authoritative for lifecycle fields
- **AND** AkShare, Eastmoney, yfinance, and local quote state SHALL remain supplemental or diagnostic for lifecycle decisions

#### Scenario: HKEX freshness is reused
- **WHEN** HKEX master data is already fresh under the configured freshness window
- **THEN** the guard SHALL reuse the local HKEX master state without external fetches
- **AND** the result SHALL identify the HKEX freshness evidence used

### Requirement: Shared guard dispatches modular governance policies
The shared instrument master governance guard SHALL dispatch explicit governance requirements to market-specific policy modules.

#### Scenario: A-share stock requirement is requested
- **WHEN** the shared guard receives an `a_share_stock` requirement for `SSE`, `SZSE`, or `BSE`
- **THEN** it SHALL dispatch the requirement to the A-share stock policy adapter
- **AND** it SHALL NOT route the requirement through index or HKEX lifecycle logic

#### Scenario: A-share index requirement is requested
- **WHEN** the shared guard receives an `a_share_index` requirement for `SSE` or `SZSE`
- **THEN** it SHALL dispatch the requirement to the A-share index policy adapter
- **AND** it SHALL keep index lifecycle warnings separate from stock master warnings

#### Scenario: HKEX instrument requirement is requested
- **WHEN** the shared guard receives an `hkex_instrument` requirement
- **THEN** it SHALL dispatch the requirement to the HKEX policy adapter
- **AND** it SHALL NOT call A-share stock or A-share index sync code for HKEX

### Requirement: Legacy governance configuration remains compatible during migration
The shared instrument master governance guard SHALL support existing job-name governance configuration until all callers are migrated to explicit requirements.

#### Scenario: New-style requirements exist
- **WHEN** a job has explicit master-governance requirements
- **THEN** the shared guard SHALL use those requirements as the source of truth
- **AND** it SHALL NOT add extra policies solely because the job name appears in a legacy force-refresh list

#### Scenario: Only legacy configuration exists
- **WHEN** a job has no explicit requirements but matches the existing legacy governance configuration
- **THEN** the shared guard SHALL preserve the previous behavior through a compatibility fallback
- **AND** the result SHALL identify that legacy fallback was used

### Requirement: Historical governance isolation applies per requirement
The shared governance guard SHALL apply current-master refresh isolation per governance requirement.

#### Scenario: Historical job has stock and index requirements
- **WHEN** a historical backfill job declares stock and index governance requirements without explicit current-refresh override
- **THEN** each current-master requirement SHALL return a historical skip result
- **AND** no policy SHALL fetch current upstream master data

#### Scenario: Historical job explicitly refreshes one scope
- **WHEN** a historical job explicitly requests current refresh for one scope only
- **THEN** the guard SHALL refresh only that scope
- **AND** other current-master requirements SHALL remain skipped unless separately overridden
