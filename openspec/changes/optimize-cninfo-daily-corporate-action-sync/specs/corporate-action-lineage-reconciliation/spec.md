## ADDED Requirements

### Requirement: Reconciliation respects active issuer segments
The corporate-action reconciliation layer SHALL compare CNInfo and TDX events
only when the reference event belongs to the active issuer segment represented
by the instrument code.

#### Scenario: Reference event belongs to a predecessor segment
- **WHEN** persisted lineage metadata places a TDX event before the active issuer segment
- **THEN** reconciliation SHALL exclude it from current-issuer unmatched counts while preserving the raw TDX record

### Requirement: Non-continuous transitions do not synthesize factors
The system SHALL NOT create or imply an adjustment factor across a lineage
transition whose metadata declares non-continuous prices and a
`no_synthetic_factor` policy.

#### Scenario: Absorption-merger transition is encountered
- **WHEN** a transition event is on a non-continuous absorption-merger boundary
- **THEN** it SHALL be excluded from comparable event matching and no factor SHALL bridge the two price regimes

### Requirement: Suppressed reference events are auditable
Every lineage-based reference suppression SHALL retain a stable reason,
instrument, event identity, segment boundary, transition type, and factor
policy in reconciliation output.

#### Scenario: Predecessor events are suppressed for 600018.SH
- **WHEN** reconciliation processes the six predecessor events and the 2006-10-26 transition event for `600018.SH`
- **THEN** all seven SHALL appear in suppressed reference output and SHALL NOT contribute to `tdx_only` or partial reconciliation

#### Scenario: Lineage policy is absent
- **WHEN** no explicit lineage metadata authorizes suppression
- **THEN** reconciliation SHALL preserve existing matching behavior

### Requirement: Long-suspension reference events use bounded forward alignment
The corporate-action factor rebuild SHALL align an event without same-day
trading to the first later valid traded quote when the caller explicitly
selects next-observed-trade alignment, and SHALL constrain that search to the
rebuild and instrument lifecycle bounds.

#### Scenario: Suspension rows are absent or carry a zero close
- **WHEN** a TDX event occurs during a long suspension and the first later
  valid traded quote is more than fourteen days after the event
- **THEN** the reference path SHALL use that first later traded date when it is
  no later than the instrument's lifecycle bound

#### Scenario: Conservative caller does not opt in
- **WHEN** a quote-evidence caller does not explicitly select
  next-observed-trade alignment
- **THEN** an unexplained long quote gap SHALL retain the existing bounded
  lookup behavior

### Requirement: Terminal reference events do not block the derived path
The reference factor path SHALL preserve but suppress a TDX event when its
instrument has reached a reviewed terminal lifecycle date and no later traded
quote exists through that date.

#### Scenario: Event occurs after the last traded session before delisting
- **WHEN** the event date is no later than the delisting date and no valid
  traded quote exists from the event through delisting
- **THEN** the event SHALL be reported as
  `terminal_no_post_event_trade`, SHALL remain in raw TDX storage, and SHALL
  NOT create `missing_effective_trade_date` or `prior_event_pending`
