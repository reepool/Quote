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
