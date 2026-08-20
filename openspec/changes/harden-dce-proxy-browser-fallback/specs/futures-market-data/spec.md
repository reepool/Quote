## ADDED Requirements

### Requirement: Product-page browser fallback is exchange scoped
The system SHALL keep browser-assisted product enrichment scoped to the exchange whose official page or business endpoint is being accessed.

#### Scenario: DCE contract information is enriched
- **WHEN** DCE `contractInfo` or a DCE product page is requested during master governance
- **THEN** the system SHALL use the same validated DCE browser route and session used by the authoritative DCE provider

#### Scenario: Another exchange product page needs fallback
- **WHEN** a CZCE, INE, SHFE, or GFEX product page needs browser-assisted fallback
- **THEN** the fallback SHALL NOT initialize, wait for, or circuit-break on the DCE browser challenge
- **AND** a DCE access failure SHALL NOT delay that exchange's master governance

### Requirement: DCE browser request lifecycle is not orphaned by caller timeout
The market-data sync SHALL let the bounded DCE browser client own timeout and cleanup for official DCE exchange payload requests.

#### Scenario: A DCE route takes longer than the generic source timeout
- **WHEN** a DCE exchange payload request is still rotating bounded browser routes after the generic official-source timeout elapses
- **THEN** the sync SHALL continue awaiting that request until the DCE client succeeds or reaches its own bounded terminal result
- **AND** it SHALL NOT start fallback or queue another DCE date while the prior browser request remains active

### Requirement: Scheduled futures phases preserve DCE route continuity and exchange isolation
The scheduled futures daily task SHALL reuse one provider-scoped DCE browser route across calendar repair, master governance, and price synchronization without invoking its event loop from multiple worker threads.

#### Scenario: Calendar repair validates a DCE route
- **WHEN** scheduled calendar repair completes a DCE business request on a validated browser route
- **THEN** subsequent DCE master governance and price synchronization in that task SHALL borrow the same official provider
- **AND** all synchronous DCE calls SHALL execute on the provider-owned single-worker executor
- **AND** the provider SHALL be closed exactly once after the scheduled task exits

#### Scenario: One exchange's master governance is blocked
- **WHEN** master governance blocks one exchange while another requested exchange remains runnable
- **THEN** the scheduler SHALL exclude only the blocked exchange from price synchronization
- **AND** it SHALL continue the runnable exchange or exchanges
- **AND** the final ingestion result SHALL be `partial`
- **AND** reports and persisted metadata SHALL retain the blocked result's real date range, calendar coverage, counts, route metrics, warnings, and blockers

#### Scenario: Every requested exchange is blocked by master governance
- **WHEN** no requested exchange remains runnable after master governance
- **THEN** price synchronization SHALL NOT start
- **AND** the scheduler SHALL report each original blocked governance result without replacing its diagnostics with zero-valued synthetic data
