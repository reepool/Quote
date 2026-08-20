## ADDED Requirements

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
