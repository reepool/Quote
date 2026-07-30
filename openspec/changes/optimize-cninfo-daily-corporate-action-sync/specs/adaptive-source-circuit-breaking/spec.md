## ADDED Requirements

### Requirement: Sustained rate-limit density opens a shared circuit
The source-scoped adaptive throttle SHALL open a shared long-cooldown circuit
when rolling 403/429 density remains above the configured threshold while
ordinary pacing is already at or near its maximum.

#### Scenario: Interspersed failures remain dense
- **WHEN** 403/429 responses remain above the density threshold despite occasional successes
- **THEN** the shared source state SHALL impose a jittered 60 to 120 second cooldown on all callers

#### Scenario: Isolated rate limit occurs
- **WHEN** one rate-limit response occurs without sustained rolling density
- **THEN** the existing bounded short-cooldown behavior SHALL apply without opening the long circuit

### Requirement: Circuit recovery is gradual
The adaptive throttle SHALL require sustained stable behavior before closing
the circuit and SHALL reduce request intervals incrementally.

#### Scenario: One request succeeds after a circuit
- **WHEN** the first request after a long cooldown succeeds
- **THEN** the throttle SHALL NOT immediately restore its minimum interval or erase rolling density

#### Scenario: Stable success threshold is reached
- **WHEN** the configured stable-success streak is reached and rate-limit density has fallen
- **THEN** the throttle SHALL recover one pacing step at a time

### Requirement: Circuit behavior is observable and testable
The adaptive throttle SHALL expose cumulative rate-limit responses, adaptive
wait, short cooldowns, circuit trips, and circuit wait, with injectable time
and jitter dependencies for deterministic tests.

#### Scenario: Metrics snapshot is requested
- **WHEN** a caller reads the source throttle metrics after requests
- **THEN** the snapshot SHALL report 403/429 counts and all adaptive and circuit wait counters without resetting shared state
