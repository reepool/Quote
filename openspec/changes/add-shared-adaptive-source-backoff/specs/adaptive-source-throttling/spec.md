## ADDED Requirements

### Requirement: Source-Scoped Adaptive Request Admission
The system SHALL provide a reusable, thread-safe adaptive throttle that coordinates synchronous request admission by logical source key within a process.

#### Scenario: Business paths share one upstream source
- **WHEN** two opted-in business paths request admission using the same source key
- **THEN** both paths SHALL observe the same pacing interval and cooldown state

#### Scenario: Unrelated sources request admission
- **WHEN** callers use different source keys
- **THEN** throttle signals and cooldowns for one key SHALL NOT delay the other key

#### Scenario: Concurrent callers request admission
- **WHEN** multiple threads request admission for one source at the same time
- **THEN** request starts SHALL be reserved in a serialized order using the active source interval
- **AND** state updates SHALL remain available while callers sleep

### Requirement: Throttle Signals Reduce Request Frequency
The adaptive throttle SHALL reduce request frequency when opted-in upstream requests return HTTP 403 or 429 responses.

#### Scenario: Consecutive throttle responses occur
- **WHEN** a source returns consecutive HTTP 403 or 429 responses
- **THEN** the throttle SHALL increase the request interval within configured bounds
- **AND** it SHALL apply progressively stronger bounded cooldown stages

#### Scenario: Throttle responses are interspersed with successes
- **WHEN** the rolling throttle-response density reaches the configured threshold without being strictly consecutive
- **THEN** subsequent requests SHALL continue using reduced request frequency

#### Scenario: Retry-After is supplied
- **WHEN** a throttled response supplies a valid `Retry-After` duration
- **THEN** the throttle SHALL not admit a request before that duration expires
- **AND** the applied duration SHALL remain within the configured maximum cooldown

#### Scenario: Jitter is applied
- **WHEN** a cooldown is calculated
- **THEN** bounded jitter SHALL spread request restarts
- **AND** jitter SHALL NOT shorten a valid upstream `Retry-After` duration

### Requirement: Stable Responses Gradually Restore Throughput
The adaptive throttle SHALL restore request frequency only after sustained stable responses.

#### Scenario: One success follows a throttle response
- **WHEN** a source returns a single successful response after a throttle episode
- **THEN** the throttle SHALL clear the consecutive-throttle count
- **AND** it SHALL NOT immediately reset the request interval to its minimum

#### Scenario: Sustained stable responses occur
- **WHEN** the configured stable-success threshold is reached and rolling throttle density is below the recovery threshold
- **THEN** the throttle SHALL reduce the request interval by one bounded recovery step

#### Scenario: Multiple stable periods occur
- **WHEN** successive stable periods continue
- **THEN** the request interval SHALL approach but SHALL NOT fall below the configured minimum

### Requirement: Adaptive State Is Bounded And Observable
The adaptive throttle SHALL validate policy bounds and expose a consistent state snapshot for operational diagnostics and deterministic tests.

#### Scenario: Invalid policy is configured
- **WHEN** interval, density, factor, window, cooldown, or jitter settings violate supported bounds
- **THEN** policy construction SHALL fail with a clear validation error

#### Scenario: State snapshot is requested
- **WHEN** a caller requests a source throttle snapshot
- **THEN** the snapshot SHALL include current interval, remaining cooldown, recent throttle density, consecutive throttle count, stable success count, wait count, throttle count, and recovery count

#### Scenario: Non-throttle failure occurs
- **WHEN** a request fails without an HTTP 403 or 429 response
- **THEN** the throttle SHALL reset the stable-success streak
- **AND** it SHALL NOT count that failure as anti-crawl evidence

### Requirement: CNInfo Request Paths Share Adaptive State
The CNInfo structured corporate-action and announcement metadata request paths SHALL opt into the same source-scoped adaptive throttle without changing their business result semantics.

#### Scenario: Structured endpoint receives HTTP 403
- **WHEN** an AkShare-backed CNInfo dividend or allotment request receives HTTP 403
- **THEN** the response SHALL update the shared `cninfo` throttle before AkShare payload parsing completes
- **AND** existing bounded endpoint retry and `indeterminate` coverage behavior SHALL remain in effect

#### Scenario: Announcement endpoint receives HTTP 429
- **WHEN** a CNInfo announcement or stock-identity request receives HTTP 429
- **THEN** the response and any valid `Retry-After` duration SHALL update the shared `cninfo` throttle

#### Scenario: CNInfo becomes stable
- **WHEN** successful structured or announcement responses satisfy the stable-success policy
- **THEN** both paths SHALL observe the same gradual recovery in request frequency

#### Scenario: Existing daily task is already running
- **WHEN** the new code is deployed while a daily task process still has the old modules loaded
- **THEN** the running task SHALL continue under its loaded behavior
- **AND** adaptive throttling SHALL take effect after the process or worker reloads
