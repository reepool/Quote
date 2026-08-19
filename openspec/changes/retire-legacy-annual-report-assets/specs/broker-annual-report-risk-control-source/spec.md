## ADDED Requirements

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
