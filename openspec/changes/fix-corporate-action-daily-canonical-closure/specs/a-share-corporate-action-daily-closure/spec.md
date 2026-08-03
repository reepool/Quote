## ADDED Requirements

### Requirement: Quote-composite predecessor readiness is durable and exchange-specific
The system SHALL persist and evaluate successful BaoStock/Sina quote-composite coverage independently for SSE and SZSE before merging a daily canonical factor subset.

#### Scenario: Normal producer advances exchange watermarks
- **WHEN** the normal A-share quote and composite-factor update completes successfully through a trading session for an exchange
- **THEN** the system SHALL persist `a_share_quote_baostock_sina:<exchange>` with `successful_through` equal to that completed session

#### Scenario: One exchange is stale
- **WHEN** one required exchange watermark is earlier than the canonical factor cutoff
- **THEN** the system SHALL defer canonical merge and identify that exchange and cutoff in predecessor diagnostics

#### Scenario: Pre-watermark data is already complete
- **WHEN** a required exchange watermark is absent but local quote coverage and the BaoStock/Sina composite path prove coverage through the required session
- **THEN** the system SHALL accept a labelled compatibility recovery, persist the recovered exchange watermark, and continue canonical evaluation

### Requirement: Retry queues contain only actionable instrument defects
The system MUST distinguish a workflow-level predecessor deferral from an instrument-level factor or merge defect.

#### Scenario: Global predecessor state is unavailable
- **WHEN** canonical merge is deferred only because predecessor readiness is missing or stale
- **THEN** the system SHALL report the workflow blocker and SHALL NOT enqueue every affected instrument as `pending_factor_rebuild`

#### Scenario: Factor derivation fails for selected instruments
- **WHEN** CNInfo or TDX factor derivation, canonical selection, candidate persistence, or targeted merge fails for specific instruments
- **THEN** the system SHALL retain only those actionable instruments in the daily factor retry queue

#### Scenario: A later run succeeds
- **WHEN** a later maintenance run has no actionable instrument defects
- **THEN** the system SHALL clear stale retry markers within the processed scope

### Requirement: Non-XDXR announcement titles are excluded deterministically
The system SHALL exclude deterministic non-XDXR announcements before semantic anomaly governance without excluding genuine distribution implementation notices.

#### Scenario: Financing compensation disclaimer
- **WHEN** a title states that a private placement has no direct or indirect financial assistance or compensation
- **THEN** the system SHALL classify the title as non-XDXR and SHALL NOT create an unmatched special-announcement blocker

#### Scenario: Ordinary cancellation notice
- **WHEN** a title concerns repurchase cancellation, restricted-share cancellation, or the resulting registered-capital change without a distribution implementation pattern
- **THEN** the system SHALL classify the title as non-XDXR

#### Scenario: Genuine implementation notice
- **WHEN** a title contains an annual or interim equity-distribution or profit-distribution implementation pattern
- **THEN** the system SHALL keep it eligible for structured refresh and semantic governance when necessary

#### Scenario: Persisted queue predates the current title policy
- **WHEN** a deferred special announcement was stored under an older title policy
- **THEN** the system SHALL reclassify its title with the current policy before carrying it forward, remove deterministic non-XDXR entries from the deferred candidate queue, and retain entries that remain exceptional or cannot be safely reclassified

#### Scenario: Current policy version is persisted
- **WHEN** the daily scan state is written
- **THEN** the system SHALL persist the active title-trigger policy version rather than a hard-coded historical version

### Requirement: BSE successful empty windows are non-blocking
The system SHALL treat a complete BSE official scan with no matching distribution implementation notice as a successful empty result.

#### Scenario: Weekend window has no matching BSE notice
- **WHEN** the BSE provider completes the requested window without transport, normalization, parse, or persistence errors and returns zero matching records
- **THEN** the BSE stage SHALL be `success` and SHALL NOT cause the overall daily task to become `partial`

### Requirement: Daily reports expose exact blockers
The daily corporate-action report SHALL expose the reason and scope for any canonical or semantic governance deferral.

#### Scenario: Canonical merge is deferred
- **WHEN** canonical incremental maintenance is not merged
- **THEN** the report SHALL include the canonical blocker reason, predecessor readiness, merge gate result, and actionable retry count

#### Scenario: Special announcements remain unmatched
- **WHEN** genuine exceptional announcements cannot be associated with structured events
- **THEN** the report SHALL include the unmatched count and a bounded list of instrument IDs and titles

#### Scenario: All operational stages complete
- **WHEN** CNInfo, BSE, TDX, factor derivation, semantic governance, and canonical maintenance have no operational blockers
- **THEN** the task SHALL report `success` even if cross-source reconciliation retains audit-only differences
