## ADDED Requirements

### Requirement: Scoped Data Reads Reject Unknown Query Fields
Unknown query-field validation SHALL apply to quotes/daily, quotes/latest, quotes/coverage, single-period and history financial-statements, technical/summary, valuation/relative, risk-free-rate and financials/pit/cross-section. The accepted field set SHALL derive from actual HTTP parameter declarations, including dependencies and aliases, rather than a separately maintained list. Other endpoints SHALL retain their existing validation contracts.

#### Scenario: Caller supplies a misspelled parameter
- **WHEN** an affected endpoint receives foo=bar or another undeclared query field
- **THEN** it SHALL return HTTP 422 with error_code=unknown_query_parameter and identify every unrecognized field
- **AND** it SHALL NOT return ordinary data success after dropping those fields

#### Scenario: Declared parameters and legitimate repeated lists are supplied
- **WHEN** a request uses declared aliases, dependency-model query parameters, or supported repeated list values
- **THEN** the unknown-field check SHALL permit those fields and SHALL preserve normal value validation
- **AND** declared parameters such as include_local_core=false SHALL NOT be mistaken for unsupported fields because the response is unchanged

### Requirement: Scoped Data Errors Expose Stable Codes With Legacy Detail
Affected data-read endpoints SHALL expose error_code, message, field and hint while preserving existing HTTP status semantics and detail payload shape. Multiple value errors SHALL remain inspectable through an errors collection. Existing annual-report error schemas and endpoints outside the scope SHALL NOT be rewritten.

#### Scenario: Coverage lacks its required date selection
- **WHEN** quotes/coverage receives neither date nor an acceptable start/end range
- **THEN** it SHALL retain HTTP 400, expose error_code=missing_date_range, provide a correction hint and retain its previous detail text

#### Scenario: A value fails normal parameter validation
- **WHEN** a scoped endpoint receives an invalid numeric bound, enum or incompatible view combination
- **THEN** it SHALL return HTTP 422 with error_code=invalid_query_parameter and the offending field or per-field errors
- **AND** it SHALL preserve existing FastAPI validation details without leaking internal stack traces or credentials

### Requirement: Default And Field-Semantics Changes Are Published As Migrations
The implementation SHALL update the API info version, actual OpenAPI response/parameter schemas and the existing API documentation with migration examples for volume aliases, adjusted reference fields, missing turnover, filters, financial views and rate pagination. Existing legacy percentage values SHALL remain documented as compatibility fields with explicit units.

#### Scenario: A consumer migrates from the previous release
- **WHEN** a consumer inspects the changed API schema and migration examples
- **THEN** it SHALL be able to request prior full financial detail with view=full, obtain adjusted volume through volume_adj, and traverse rate history using explicit order and pages
- **AND** the documentation SHALL clearly identify which defaults and field meanings changed
