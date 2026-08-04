## ADDED Requirements

### Requirement: Durable Non-Blocking Stage Queue
The system SHALL persist business-profile acquisition and processing work in durable idempotent stages and SHALL NOT require a discovery run to wait for the downstream backlog to drain.

#### Scenario: Slow semantic backlog does not block discovery
- **WHEN** semantic work remains pending or retryable from prior days
- **THEN** the next daily run SHALL still scan and persist new announcement metadata before claiming downstream work
- **AND** downstream work SHALL be limited by its own item, concurrency, and elapsed-time budgets

#### Scenario: Worker exits during a stage
- **WHEN** a worker exits after claiming an item without acknowledging completion
- **THEN** the item SHALL become claimable after its durable lease expires
- **AND** replay SHALL use the same idempotency and checkpoint identity

#### Scenario: One stage fails
- **WHEN** PDF acquisition, parsing, semantic extraction, or publication fails for one item
- **THEN** only that item SHALL be retried or terminally classified
- **AND** independent items and stages SHALL continue within their budgets

### Requirement: Latest Annual Automatic Evidence Policy
Unattended production SHALL select only the latest available active full annual report for each issuer and its correction or replacement as of the knowledge cutoff.

#### Scenario: New annual report becomes available
- **WHEN** an issuer has an older archived annual report and a newer full annual report is discovered
- **THEN** the newer report SHALL be enqueued for automatic processing
- **AND** the older immutable report SHALL remain available for point-in-time history without being reprocessed as current evidence

#### Scenario: Corrected annual report is discovered
- **WHEN** a full corrected or replaced annual report is discovered for the selected report period
- **THEN** it SHALL supersede an unstarted original work item before download where possible
- **AND** the corrected report SHALL be the active automatic input

#### Scenario: Semiannual or specialist disclosure is discovered
- **WHEN** unattended discovery finds a semiannual report, operating announcement, resource report, contract, capacity change, hedge disclosure, prospectus, or profile-change announcement
- **THEN** the disclosure SHALL remain auditable in the frontier
- **AND** it SHALL NOT be automatically enqueued under the latest-annual policy

#### Scenario: Issuer has no annual report
- **WHEN** a newly listed issuer has no eligible annual report as of the cutoff
- **THEN** its automatic business-profile state SHALL remain not ready
- **AND** the system SHALL NOT silently substitute a prospectus or specialist disclosure

### Requirement: Resumable Peak-Season Discovery
Market-wide discovery SHALL distinguish a complete provider scan from a page-bound partial scan and SHALL persist resumable date-window work until the requested range is covered.

#### Scenario: Filing-season scan reaches page bound
- **WHEN** a market/date window reaches its configured page bound before provider completion
- **THEN** the system SHALL NOT commit that partial scan as the canonical completed watermark
- **AND** it SHALL persist non-overlapping smaller date windows for later bounded scans

#### Scenario: Date window cannot be split further
- **WHEN** a single publication date still exceeds the configured page bound
- **THEN** the system SHALL retain an explicit incomplete discovery-window state
- **AND** the operations report SHALL expose the date, market, and stop reason instead of reporting full coverage

### Requirement: Stage Idempotency, Supersession, and Retry
Every work item SHALL have a deterministic identity, atomic claim, bounded retry policy, and explicit supersession behavior.

#### Scenario: Daily enqueue repeats unchanged frontier data
- **WHEN** the same source-qualified annual report and processing identity are evaluated again
- **THEN** the queue SHALL reuse the existing item without creating duplicate downloads, parses, model calls, or candidate writes

#### Scenario: Retryable failure occurs
- **WHEN** a stage fails with a retryable network, rate-limit, timeout, or transient parser error
- **THEN** the queue SHALL record the reason and schedule bounded exponential backoff
- **AND** it SHALL move to terminal failure after the configured attempt limit

#### Scenario: Older unstarted work is superseded
- **WHEN** a newer active annual input for the same issuer becomes eligible
- **THEN** older pending or retryable work SHALL be marked superseded
- **AND** running or completed immutable evidence SHALL not be deleted or overwritten

### Requirement: Manual Scoped Backfill
The system SHALL provide a manual-only backfill entry point that reuses the production queue and requires explicit scope for historical or non-annual work.

#### Scenario: Operator requests specialist disclosure backfill
- **WHEN** an operator supplies explicit instrument identifiers, a bounded date or report-year range, and allowed specialist document types
- **THEN** the task SHALL discover or select only that scope and enqueue it through the same durable stages

#### Scenario: Broad backfill has no explicit scope
- **WHEN** a manual request asks for historical or specialist processing without instruments or a bounded time range
- **THEN** the task SHALL reject the request before network downloads or model calls

### Requirement: Queue Health and Automated Operations Reporting
Each automatic or manual run SHALL report discovery completeness, enqueue decisions, work progress, backpressure, retries, and queue age without requiring routine human classification.

#### Scenario: Backlog exceeds high-water mark
- **WHEN** a configured stage queue exceeds its high-water mark
- **THEN** the run SHALL preserve discovery and frontier writes
- **AND** it SHALL reduce or pause eligible slow-stage claims and report the applied backpressure reason

#### Scenario: Run completes with deferred work
- **WHEN** bounded work remains after a run
- **THEN** the run SHALL complete successfully with deferred queue counts and oldest age
- **AND** deferred work SHALL remain automatically claimable by later daily runs
