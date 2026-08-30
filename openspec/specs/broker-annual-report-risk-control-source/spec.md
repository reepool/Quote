# broker-annual-report-risk-control-source Specification

## Purpose
Define the securities-company regulatory fact ingestion source for listed broker DCF inputs. The source uses formal annual and semiannual report PDFs as the primary public disclosure source for broker net capital and risk-control indicators, with standalone risk-control reports only as supplementary gap-fill or validation evidence.
## Requirements
### Requirement: Listed Broker Scope Must Be Evidence-Based
The system SHALL determine broker regulatory fact eligibility from CSRC securities-company registry evidence plus an explicit listed-instrument scope mapping, not from industry classification or company-name heuristics alone.

#### Scenario: Shenwan security member is not sufficient
- **WHEN** an instrument belongs to Shenwan `证券`
- **THEN** broker regulatory fact ingestion SHALL require a matching `listed_broker_dealer_scope` entry before treating the instrument as a listed broker

#### Scenario: Listed broker group is supported
- **WHEN** a listed instrument maps to a licensed securities-company entity through explicit `listed_broker_dealer_scope` evidence
- **THEN** the system SHALL allow broker regulatory fact ingestion and record the licensed entity, scope type, evidence source, and effective date

#### Scenario: Platform or holding company is excluded
- **WHEN** an instrument has Shenwan `证券` membership but lacks confirmed listed broker scope
- **THEN** the system SHALL skip broker regulatory fact ingestion and SHALL NOT route the instrument to broker DCF by default

### Requirement: Formal Annual And Semiannual Reports Are Primary Broker Regulatory Sources
The system SHALL use formal CNInfo annual and semiannual report PDFs as the primary source for listed-broker regulatory capital and risk-control facts.

#### Scenario: Formal annual report is selected
- **WHEN** the source scanner finds multiple announcements containing `年度报告`
- **THEN** it SHALL select the formal annual report PDF and exclude summaries, audit reports, inquiry letters, reply notices, continuous-supervision reports, and performance-briefing announcements

#### Scenario: Formal semiannual report is selected
- **WHEN** the source scanner finds multiple announcements containing `半年度报告`
- **THEN** it SHALL select the formal semiannual report PDF and exclude summaries and non-report announcements

#### Scenario: Standalone risk-control report exists
- **WHEN** a standalone `风险控制指标报告` exists for the same instrument and report period
- **THEN** it SHALL be treated as supplementary or validation evidence rather than the default primary source

### Requirement: Embedded Risk-Control Tables Must Be Parsed Conservatively
The parser SHALL extract broker regulatory facts from embedded annual/semiannual report sections only when the section, current-period column, label, unit, and numeric value can be determined confidently.

#### Scenario: Embedded table contains net capital
- **WHEN** an annual or semiannual report contains a section such as `母公司的净资本及风险控制指标` or `净资本及风险控制指标`
- **THEN** the parser SHALL emit canonical `net_capital` when the current-period value and unit are unambiguous

#### Scenario: Embedded table contains risk ratios
- **WHEN** an embedded risk-control table contains risk coverage ratio, capital leverage ratio, liquidity coverage ratio, or net stable funding ratio
- **THEN** the parser SHALL emit those canonical facts with ratio units and source value text

#### Scenario: Table extraction is ambiguous
- **WHEN** the parser cannot determine the current-period column, source unit, or label-to-value pairing
- **THEN** it SHALL emit diagnostics and SHALL NOT fabricate the missing fact

### Requirement: Broker Regulatory Facts Must Use Existing Financial Storage
Parsed broker annual/semiannual regulatory facts SHALL be stored through existing financial source manifest and numeric fact APIs.

#### Scenario: Embedded report is parsed
- **WHEN** a formal annual or semiannual report embedded table is parsed
- **THEN** the system SHALL write a `financial_source_files` manifest and `financial_numeric_facts_hot/history` rows with source profile, parser version, content hash, report type, report period, data available date, unit, and diagnostics

#### Scenario: Duplicate source profiles exist
- **WHEN** both annual-report embedded facts and standalone risk-control report facts exist for the same canonical fact and report period
- **THEN** the system SHALL preserve both source manifests and apply configured source priority for DCF input assembly

### Requirement: Backfill And Incremental Update Must Share The Same Source Rules
Historical backfill and daily incremental update SHALL use the same listed-broker scope gate, formal-report selection rules, embedded parser, storage path, and source priority semantics.

#### Scenario: Historical backfill runs
- **WHEN** a broker regulatory backfill is run for configured report periods
- **THEN** it SHALL target only confirmed listed broker scope entries and SHALL scan formal annual/semiannual reports before supplementary standalone reports

#### Scenario: Incremental update runs
- **WHEN** new annual or semiannual report announcements appear after the stored watermark
- **THEN** the incremental update SHALL process eligible listed brokers and persist parsed facts using the same parser and storage path as backfill

#### Scenario: Financial disclosure daily maintenance completes
- **WHEN** the scheduled `financial_disclosure_incremental_sync` task completes successfully
- **THEN** the scheduler SHALL trigger `broker_risk_control_incremental_sync` as a financial-data post task
- **AND** the broker task SHALL use the confirmed listed-broker scope gate and write incremental facts to the configured hot financial fact tier

### Requirement: AkShare Must Not Be A Broker Regulatory Fact Source
AkShare common financial indicator APIs SHALL NOT be treated as a source for broker regulatory capital or risk-control facts.

#### Scenario: AkShare financial indicators are available
- **WHEN** AkShare returns common financial indicators for a listed broker
- **THEN** those rows SHALL NOT satisfy `net_capital`, risk coverage, capital leverage, LCR, or NSFR requirements unless a future explicit broker regulatory interface is validated

#### Scenario: AkShare CNInfo disclosure wrapper is used
- **WHEN** AkShare is used for announcement discovery
- **THEN** its results SHALL be converted into the same CNInfo source manifest model and SHALL NOT bypass parser, lineage, or scope-gate requirements

### Requirement: Broker Report Assets Must Use The Shared Announcement Asset Service
Broker risk-control ingestion SHALL obtain formal annual and semiannual report metadata and content only from `research.announcement_assets` and SHALL NOT discover, download, archive, or read those reports through a broker-specific or business-profile legacy path.

#### Scenario: Effective shared report exists
- **WHEN** broker risk-control ingestion requests a report period with a valid effective shared asset
- **THEN** it SHALL parse the shared asset content and preserve its asset identity, source identity, content hash, report period, and availability time in downstream lineage

#### Scenario: Shared report is missing
- **WHEN** no valid effective shared asset exists for the requested instrument and report period
- **THEN** broker risk-control ingestion SHALL report an explicit asset-not-ready result
- **AND** it SHALL NOT invoke a legacy downloader, archive, or manifest fallback

#### Scenario: Corrected report becomes effective
- **WHEN** the shared announcement asset service promotes a valid corrected full report
- **THEN** subsequent broker ingestion SHALL use the corrected effective asset
- **AND** it SHALL NOT select the superseded original through a consumer-specific rule

#### Scenario: Formal semiannual report is requested
- **WHEN** broker risk-control ingestion requests a formal semiannual report period
- **THEN** classification and effective selection SHALL be performed inside `research.announcement_assets`
- **AND** the broker consumer SHALL receive a shared immutable asset or an explicit asset-not-ready result
