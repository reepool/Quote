## MODIFIED Requirements

### Requirement: Formal Annual And Semiannual Reports Are Primary Broker Regulatory Sources
For each periodic-report type enabled by the shared capability, the system SHALL use the shared verified formal asset as the primary source for listed-broker regulatory capital and risk-control facts. Version 1 enables shared annual reports but not shared semiannual reports. Broker ingestion SHALL NOT independently discover, download, or archive a formal report when the shared asset capability supports that document type.

#### Scenario: Formal annual report is selected
- **WHEN** the shared asset service exposes the effective formal annual report for a listed broker and report period
- **THEN** broker ingestion SHALL parse that asset and exclude summaries, audit reports, inquiry letters, reply notices, continuous-supervision reports, and performance-briefing announcements

#### Scenario: Formal annual report is missing locally
- **WHEN** broker ingestion requires an annual report that is not locally available
- **THEN** it SHALL call the shared local-first ensure contract with its network policy
- **AND** it SHALL NOT invoke a broker-owned attachment downloader or archive writer

#### Scenario: Formal semiannual report is selected before shared semiannual rollout
- **WHEN** version 1 has not yet enabled shared semiannual asset management
- **THEN** the existing semiannual path MAY remain temporarily available behind an explicit migration gate
- **AND** it SHALL be retired when the shared semiannual capability is enabled

#### Scenario: Standalone risk-control report exists
- **WHEN** a standalone `风险控制指标报告` exists for the same instrument and report period
- **THEN** it SHALL be treated as supplementary or validation evidence rather than the default primary source

#### Scenario: Annual-report correction becomes effective
- **WHEN** the shared asset service replaces an original broker annual report with a verified correction
- **THEN** broker processing bound to the original asset SHALL be superseded or requeued
- **AND** current broker facts SHALL not silently remain bound to the deleted predecessor attachment

### Requirement: Broker Regulatory Facts Must Use Existing Financial Storage
Parsed broker annual/semiannual regulatory facts SHALL be stored through existing financial numeric-fact APIs, while source-asset identity and parser-processing identity SHALL remain separate.

#### Scenario: Shared annual report is parsed
- **WHEN** a formal annual-report asset is parsed
- **THEN** the system SHALL write a broker processing manifest linked to the shared asset id and `financial_numeric_facts_hot/history` rows with source profile, parser version, content hash, report period, data available date, unit, and diagnostics
- **AND** it SHALL NOT create another physical annual-report archive copy

#### Scenario: Broker parser fails
- **WHEN** the broker parser cannot extract required facts
- **THEN** the broker processing record SHALL be failed or retryable
- **AND** the shared source asset SHALL remain valid and reusable by other consumers

#### Scenario: Duplicate source profiles exist
- **WHEN** both annual-report embedded facts and standalone risk-control report facts exist for the same canonical fact and report period
- **THEN** the system SHALL preserve both processing/source lineages and apply configured source priority for DCF input assembly

### Requirement: Backfill And Incremental Update Must Share The Same Source Rules
Historical backfill and daily incremental update SHALL use the same listed-broker scope gate, shared effective-report selection, embedded parser, financial fact storage, and source priority semantics.

#### Scenario: Historical backfill runs
- **WHEN** broker regulatory backfill is run for configured report periods
- **THEN** it SHALL target only confirmed listed broker scope entries
- **AND** it SHALL request formal annual-report assets from the shared service before supplementary standalone reports

#### Scenario: Incremental asset event is received
- **WHEN** the shared annual-report daily task activates a new effective broker annual-report asset
- **THEN** broker incremental processing SHALL consume the affected asset id and persist facts using the same parser and storage path as backfill

#### Scenario: Financial disclosure daily maintenance completes
- **WHEN** the scheduled `financial_disclosure_incremental_sync` task completes successfully without an annual-report asset event
- **THEN** existing supplementary broker report handling MAY continue according to dependency configuration
- **AND** formal annual-report acquisition SHALL remain owned by the shared asset task
