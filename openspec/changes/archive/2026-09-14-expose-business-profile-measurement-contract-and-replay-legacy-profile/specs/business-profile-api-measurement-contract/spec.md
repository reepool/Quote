## ADDED Requirements

### Requirement: API declares measurement authority
The company business-profile API SHALL identify `company_specific_profile.operating_facts` as the authoritative complete measurement collection, SHALL identify activity values as compatibility projections, and SHALL publish `metadata.source_activity_id` as the operating-fact-to-activity link field.

#### Scenario: Linked current profile
- **WHEN** a company profile contains approved operating facts produced under the linked measurement contract
- **THEN** the response reports all operating facts as linked and exposes the authoritative path and link field

#### Scenario: Historical activity-derived profile remains readable
- **WHEN** approved historical activity-derived operating facts do not contain `metadata.source_activity_id`
- **THEN** the response reports their linkage as unlinked or partially linked without changing profile readiness or omitting the facts

#### Scenario: Standalone operating fact needs no activity link
- **WHEN** an approved operating fact such as customer or supplier concentration was not derived from an activity
- **THEN** the response counts it as standalone and does not treat the missing activity link as a compatibility defect

### Requirement: API exposes company-specific field families
The company business-profile OpenAPI schema SHALL explicitly expose business segments, operating facts, activities, value-chain roles, named supply-chain relationships, commodity exposure facts, and commodity exposures while retaining compatible unknown extension fields.

#### Scenario: Client inspects the OpenAPI schema
- **WHEN** a client reads the schema for the company business-profile response
- **THEN** the company-specific field families and measurement contract are discoverable without relying on an unrestricted root dictionary

### Requirement: Authorized legacy profile uses the current contract
The system SHALL replay `601088.SH` through the existing business-profile production owner and SHALL not use direct database rewriting or a second publication path.

#### Scenario: Representative production replay
- **WHEN** the authorized forced replay of the effective latest annual report for `601088.SH` completes
- **THEN** its approved operating facts link to their source activities, its profile remains ready, and its approved value-chain and commodity-exposure information remains available without duplicate logical facts

### Requirement: Annual refresh uses the effective annual-report lifecycle
The automatic refresh SHALL discover effective annual reports and annual-report corrections from the shared announcement asset layer, enqueue changed effective assets, and advance the durable business-profile stages without requiring per-company manual intervention.

#### Scenario: New annual report becomes effective
- **WHEN** the enabled incremental schedule discovers a newer effective annual report for a listed company
- **THEN** the company is queued for acquire, parse, semantic, and publish processing and its new approved facts become the current point-in-time profile

#### Scenario: Corrected annual report supersedes the full report
- **WHEN** a corrected full annual report is the effective asset for the same company and report period
- **THEN** production uses the correction as the effective source and does not require the superseded full report to be processed again

#### Scenario: Targeted repair is required
- **WHEN** an operator needs to repair a company, historical period, or urgent special disclosure outside the automatic annual scope
- **THEN** the manual backfill command routes the request through the same durable stages and single writer
