## Purpose
Define consistent source routing for futures data so supported first-hand exchange sources are preferred, normalized through exchange adapters, monitored for quality and anti-crawl failures, and replaced by aggregators only through explicit provenance-preserving fallback rules.
## Requirements
### Requirement: Official Futures Sources Are Preferred
The system SHALL prefer supported first-hand official domestic futures exchange sources before AkShare-style aggregator providers for futures daily bar sync.

#### Scenario: Official source succeeds
- **WHEN** a supported futures series is synced and the official exchange source returns valid rows
- **THEN** the stored bars SHALL use `source_profile=exchange_official`
- **AND** the sync result SHALL count the series as official-source coverage
- **AND** AkShare fallback SHALL NOT be called for that successful series/date request

#### Scenario: Official source fails and fallback is enabled
- **WHEN** the official provider fails, is unsupported for the exchange, or returns no rows for a requested supported series
- **THEN** the sync service SHALL attempt the configured AkShare fallback
- **AND** the sync result SHALL report the official failure or unsupported reason separately from fallback success or failure

### Requirement: Official Futures Payloads Are Normalized
The system SHALL normalize official exchange daily contract payloads into the canonical futures bar field set before storage or continuous-series construction.

#### Scenario: Official contract row is normalized
- **WHEN** an official exchange parser reads a contract daily bar
- **THEN** it SHALL normalize trade date, contract symbol, variety, open, high, low, close, settlement, volume, open interest, amount, currency, unit, source, source interface, parser version, quality flag, and raw payload hash
- **AND** numeric fields SHALL be converted consistently without silently keeping comma-formatted strings

#### Scenario: Unit or field quality is uncertain
- **WHEN** a parser cannot verify a field, unit, or exchange-specific amount convention
- **THEN** the normalized row SHALL include a warning or quality flag
- **AND** readiness SHALL NOT present that row as clean official coverage without the warning

### Requirement: Main Continuous Series Are Constructed Locally From Official Contracts
The system SHALL construct first-stage main-continuous futures bars from official contract rows using a deterministic project-owned rule.

#### Scenario: Main contract is selected
- **WHEN** multiple contracts for the same variety and date are available from the official source
- **THEN** the main-continuous bar SHALL use the contract with the highest open interest
- **AND** ties SHALL be broken by higher volume and then stable contract code ordering
- **AND** the stored bar metadata SHALL include the selected underlying contract and construction method

#### Scenario: No contract rows exist
- **WHEN** no official contract row exists for a requested series/date
- **THEN** the official provider SHALL return a structured empty result
- **AND** the sync service SHALL either fallback or report a missing official source row

### Requirement: Official Source Coverage Is Reported
The system SHALL expose official-source coverage and fallback usage in sync summaries and readiness outputs.

#### Scenario: Readiness is requested
- **WHEN** futures readiness is built
- **THEN** it SHALL include enough local lineage to distinguish official bars from fallback bars
- **AND** it SHALL report warnings when a P0 series has only aggregator fallback coverage

#### Scenario: Sync completes with mixed sources
- **WHEN** a sync run writes some series from official sources and some from fallback sources
- **THEN** the run metadata SHALL include source-selection counts and per-series source status

### Requirement: DCE official browser access is bounded and proxy recoverable
The system SHALL access protected DCE official endpoints with a real browser, SHALL attempt configured authenticated proxy leases after direct challenge or transport failure, and SHALL keep all requests within the authoritative official futures provider.

#### Scenario: Direct DCE browser session succeeds
- **WHEN** the direct headed browser completes the DCE challenge and the requested DCE business endpoint succeeds
- **THEN** the system SHALL reuse that direct session for subsequent DCE requests in the same provider run
- **AND** it SHALL NOT acquire a proxy lease

#### Scenario: Direct egress is challenged or unavailable
- **WHEN** the direct browser receives HTTP 412, an in-page fetch failure, or a bounded transport timeout
- **THEN** the system SHALL acquire a fresh authenticated proxy lease from the configured `akshare_proxy_patch` authorization service
- **AND** it SHALL execute the DCE browser session through a loopback forwarder that supports HTTP absolute-form requests and HTTPS CONNECT
- **AND** it SHALL rotate proxy leases only up to the configured bound

#### Scenario: DCE session is validated
- **WHEN** a browser route passes a lightweight challenge probe
- **THEN** the system SHALL require a successful requested `dayQuotes` or `contractInfo` business response before treating that session as ready

#### Scenario: DCE route diagnostics are reported safely
- **WHEN** direct or proxy DCE attempts succeed, fail, time out, rotate, or circuit-break
- **THEN** the system SHALL record corresponding route metrics
- **AND** it SHALL NOT expose proxy credentials, authorization tokens, or full proxy URLs in logs or results

#### Scenario: A validated proxy session later stalls
- **WHEN** a proxy session has completed a DCE business request and a subsequent request times out or receives challenge evidence
- **THEN** the system SHALL refresh and retry the request once in the same proxy session
- **AND** if that retry fails, a new recovery cycle SHALL receive the configured per-recovery proxy allowance
- **AND** all recovery cycles together SHALL remain within the configured run-wide proxy lease cap

#### Scenario: Consecutive DCE business dates are requested
- **WHEN** the provider reuses a validated DCE session for another business date
- **THEN** it SHALL enforce the configured minimum interval from the previous business-request completion

#### Scenario: A proxy authorization expires
- **WHEN** a DCE browser route receives HTTP 407 or equivalent expired proxy authorization evidence
- **THEN** the provider SHALL invalidate and rotate that proxy route within the configured bounds
- **AND** logs, classifications, and results SHALL contain only a credential-free route summary
