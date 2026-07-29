## ADDED Requirements

### Requirement: Schedule-aware announcement coverage
The system SHALL calculate the daily announcement window independently from the
factor cutoff. In `calendar_daily` mode it SHALL cover the previous natural day
through the current run timestamp. In `trading_day` mode it SHALL cover the
previous completed trading day through the current run timestamp, including every
intervening weekend or exchange-holiday day.

#### Scenario: Calendar-daily overnight coverage
- **WHEN** a calendar-daily task runs during the morning of July 29
- **THEN** announcement discovery includes announcements from July 28 and announcements published after midnight on July 29 up to the run timestamp

#### Scenario: Trading-day long-holiday coverage
- **WHEN** a trading-day task runs after a multi-day exchange holiday
- **THEN** announcement discovery begins on the trading session strictly before the run date and includes the complete holiday interval through the run timestamp

#### Scenario: Tuesday run covers the weekend
- **WHEN** the configured Tuesday-to-Saturday task next runs on Tuesday morning
- **THEN** its calendar overlap includes announcements published after Saturday's run and on Sunday

### Requirement: Completed-session factor cutoff
The system SHALL NOT require quote evidence from an exchange session that has not
completed and been stored locally. The daily factor cutoff SHALL be no later than
the latest common locally available quote date for the requested A-share
exchanges.

The completed date for an exchange SHALL be the minimum latest complete quote date
across its currently tradable stocks. Suspended stocks SHALL NOT hold back this
market cutoff, and stocks first listed after the candidate completed session SHALL
NOT be required to have an earlier quote. Unresolved factor paths for suspended
stocks SHALL remain queued.

#### Scenario: Pre-market daily run
- **WHEN** the task runs before the July 29 market open and local SSE, SZSE, and BSE quotes are complete through July 28
- **THEN** factor rebuilding ends on July 28 and July 29 events are not reported as missing same-day quote evidence

#### Scenario: Quote cutoff is unavailable
- **WHEN** any requested exchange has no verified local quote date
- **THEN** factor rebuilding is deferred and the system does not fall back to the requested current date

#### Scenario: Pre-market listing-day run
- **WHEN** a stock is already active on its listing morning but was not listed by the prior completed session
- **THEN** its absent prior-session quote does not make the entire exchange quote cutoff unavailable

#### Scenario: Stale master tradability flag
- **WHEN** an exited stock still has a master `trading_status=1`, or a suspended stock's latest bounded quote is marked non-trading
- **THEN** that stock does not hold back the exchange quote cutoff

#### Scenario: Event waits for a stale quote feed
- **WHEN** an affected event is newer than the latest common quote cutoff
- **THEN** its instrument remains queued for targeted factor rebuilding until the quote cutoff catches up

#### Scenario: Rebuild remains pending
- **WHEN** any newly affected or previously carried instrument still has a pending CNInfo or TDX factor path
- **THEN** the complete instrument set remains queued regardless of diagnostic sample limits

#### Scenario: Retry without an announcement scan
- **WHEN** a BSE-only or explicit-instrument run has no market announcement scan context and its factor path remains pending
- **THEN** the pending instrument is persisted independently and remains targeted after the rolling event window expires

#### Scenario: Retry queue read fails
- **WHEN** the persisted factor-retry queue cannot be read
- **THEN** the run reports a partial operational result and does not replace or clear the existing retry queue

### Requirement: Relevant announcement candidate selection
The system SHALL select an announcement-driven structured CNInfo refresh candidate
only when the title survives deterministic non-implementation exclusions and
contains both a corporate-action subject and implementation-grade evidence.
Explicit, retry, recent-event, deferred, and rotating-safety candidates SHALL
remain available independently of the title gate.

#### Scenario: Irrelevant disclosures do not trigger refresh
- **WHEN** announcements concern annual reports, board meetings, legal opinions, pledges, guarantees, repurchases, or unrelated share listings
- **THEN** those announcements do not add their instruments to the announcement-driven CNInfo refresh set

#### Scenario: Implemented distribution triggers refresh
- **WHEN** a title identifies an implemented dividend, rights issue, share reform distribution, restructuring capitalization, or compensation-share action
- **THEN** its active SSE or SZSE instrument is selected with an auditable corporate-action announcement reason

#### Scenario: Implemented share cancellation triggers refresh
- **WHEN** a title identifies a completed or implemented repurchase cancellation, treasury-share cancellation, or capital reduction
- **THEN** its active SSE or SZSE instrument is selected without requiring the exact phrase `股份注销`

### Requirement: Conservative cursor and relevant deferred queue
The system SHALL commit an announcement cursor only after a complete provider scan.
The committed cursor SHALL NOT advance beyond the current run timestamp when the
provider returns later-dated records; those records SHALL remain discoverable by a
subsequent run.
It SHALL persist only genuinely relevant unprocessed announcement instruments in
the deferred candidate queue. An incomplete scan or a non-empty relevant deferred
queue SHALL remain operationally partial.

#### Scenario: Complete catch-up advances cursor
- **WHEN** the provider completes the prior-session announcement window within the configured bound
- **THEN** the latest complete cursor is committed and unrelated disclosure instruments are absent from the deferred queue

#### Scenario: Legacy deferred queue survives policy migration
- **WHEN** prior scan state has deferred candidates but predates the current title policy
- **THEN** those candidates are drained once before the existing committed cursor is reused without them

#### Scenario: Provider bound is exhausted
- **WHEN** the provider cannot complete the announcement window within the configured bound
- **THEN** the prior committed cursor is retained and the task reports the announcement scan as partial

#### Scenario: Provider returns a later-dated record
- **WHEN** a complete scan observes an announcement timestamp after the current run timestamp
- **THEN** that announcement is not processed and the committed temporal cursor is capped at the run timestamp without regressing an existing cursor

### Requirement: Source-separated daily readiness
The daily result and operator report SHALL separately expose CNInfo path readiness,
TDX reference-path diagnostics, and CNInfo/TDX reconciliation diagnostics. TDX
pending events or cross-source differences SHALL NOT be labelled as CNInfo
historical unreadiness and SHALL NOT by themselves make a completed CNInfo daily
refresh operationally partial.

#### Scenario: TDX reference issue with complete CNInfo refresh
- **WHEN** CNInfo discovery and refresh complete but an affected instrument has an independent TDX historical path defect
- **THEN** the top-level operational status is successful, CNInfo readiness is successful, and the TDX reference section is partial

#### Scenario: Reconciliation differences remain auditable
- **WHEN** CNInfo-only, TDX-only, or conflicting events are found
- **THEN** their counts remain in the reconciliation section without being added to the CNInfo incomplete-instrument count

### Requirement: BSE source boundary
The system SHALL exclude BSE observations from the CNInfo-derived factor path and
CNInfo readiness because CNInfo is not a supported BSE source. It SHALL continue
TDX refresh and diagnostics for BSE when BSE is requested.

#### Scenario: BSE TDX event is affected
- **WHEN** a BSE instrument is affected by the daily TDX refresh
- **THEN** it may enter TDX reference rebuilding but cannot create a CNInfo pending event or CNInfo endpoint gap
