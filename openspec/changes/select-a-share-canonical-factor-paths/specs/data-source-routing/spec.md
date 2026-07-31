## ADDED Requirements

### Requirement: Direct Sina A-share factor routing
The AkShare A-share factor adapter SHALL request the direct Sina `hfq-factor` endpoint used
by `stock_zh_a_daily(adjust="hfq-factor")` and SHALL NOT download Tencent or Eastmoney raw
or adjusted price histories to derive factors.

#### Scenario: Sina succeeds
- **WHEN** Sina returns a positive dated cumulative factor series
- **THEN** the adapter emits sparse `sina_hfq_factor` observations for material factor
  changes in the requested range

#### Scenario: Sina is unavailable
- **WHEN** the endpoint raises an error or returns an indeterminate response
- **THEN** the adapter reports the acquisition failure and preserves the prior complete
  governed snapshot

#### Scenario: Valid zero-event window
- **WHEN** the endpoint is valid and the requested window contains no factor change
- **THEN** the adapter records complete zero-event coverage rather than treating it as a
  provider failure

#### Scenario: One request times out
- **WHEN** a Sina factor request exceeds the configured per-instrument timeout
- **THEN** the adapter reports an indeterminate acquisition failure, preserves the prior
  complete snapshot, and allows the checkpointed batch to continue

#### Scenario: Truncated Sina history
- **WHEN** the declared response row count is incomplete or the response lacks an anchor at
  or before the requested instrument lifecycle start
- **THEN** the adapter treats the response as indeterminate and does not certify a complete
  voting path

### Requirement: Incremental factor extraction
The AkShare adapter MUST use a pre-range anchor when extracting a bounded incremental
window and MUST persist sparse factor events rather than daily plateaus.

#### Scenario: Factor changes inside the window
- **WHEN** the cumulative factor differs materially from the preceding anchor value
- **THEN** the adapter emits the adjacent positive factor ratio on the change date

#### Scenario: Provider precision drift
- **WHEN** an adjacent cumulative ratio remains within the configured material-change
  threshold of one
- **THEN** the adapter does not emit a factor event

#### Scenario: No anchor before the window
- **WHEN** a bounded response has no reliable point before the requested start
- **THEN** the adapter does not fabricate the first in-window value as a new event

### Requirement: Explicit Sina snapshot lineage
Every governed Sina factor snapshot SHALL preserve the source profile, requested coverage,
ingestion id, event count, and quality status.

#### Scenario: Snapshot replacement
- **WHEN** a complete Sina refresh succeeds for an instrument and range
- **THEN** selection uses only rows belonging to the new snapshot and does not combine stale
  historical observations

### Requirement: Removed price-ratio providers
The system SHALL contain no active Tencent/Eastmoney A-share price-ratio factor route,
configuration, provider profile, or persisted provider-snapshot state.

#### Scenario: Source scan
- **WHEN** code, configuration, tests, documentation, and governed factor status are audited
- **THEN** no `akshare_tencent_price_ratio_v1`, `akshare_eastmoney_price_ratio_v1`, or
  `akshare_market_price_ratio_snapshot_v1` active implementation artifact remains, and
  database initialization removes rows carrying those exact retired identifiers

### Requirement: Writable BaoStock access-governance state
BaoStock quota state and the cross-process session lock SHALL default to a persistent
project runtime directory rather than a user-home cache that may be read-only.

#### Scenario: User home is read-only
- **WHEN** the service starts with a read-only home filesystem and a writable project data
  directory
- **THEN** BaoStock initializes its access governor under
  `data/runtime/baostock/` and remains available as the configured fallback

#### Scenario: Deployment supplies explicit paths
- **WHEN** absolute quota-state and session-lock paths are configured
- **THEN** the adapter continues to use those explicit paths

#### Scenario: Rolling upgrade from user-cache paths
- **WHEN** a deployment changes from the previous user-cache defaults to project-local
  runtime paths
- **THEN** the governor preserves the larger current-day request count, coordinates the
  legacy session lock, and mirrors state when the legacy path remains writable
