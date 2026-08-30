# official-financial-source-profiles Specification

## Purpose
TBD - created by archiving change promote-cninfo-data20-financial-source. Update Purpose after archive.
## Requirements
### Requirement: Official Financial Source Profiles Must Be Explicit
The Research Data Engine SHALL manage official structured financial statement ingestion through explicit source profiles that bind exchanges, endpoint families, parser profile, request policy, unit normalization, checkpoint identity, and fallback policy.

#### Scenario: SSE profile is selected
- **WHEN** an operator runs official structured financial validation or backfill for `SSE`
- **THEN** the system SHALL use the SSE `commonQuery.do` source profile with configured endpoint candidates, SSE report-period parameter mapping, coded-field parser aliases, and profile-aware checkpoint metadata

#### Scenario: CNInfo data20 profile is selected
- **WHEN** an operator runs official structured financial validation or backfill with source profile `cninfo_data20` for `SSE`, `SZSE`, or `BSE`
- **THEN** the system SHALL use the CNInfo data20 source profile with configured context resolvers, three statement endpoint candidates, CNInfo period-bucket parsing, 10k-CNY unit normalization, and profile-aware checkpoint metadata

#### Scenario: SSE default source remains exchange-hosted
- **WHEN** an operator runs official structured financial validation or backfill for `SSE` without explicitly selecting `cninfo`
- **THEN** the system SHALL keep `sse_commonquery` as the default source profile and SHALL NOT silently switch SSE production evidence to CNInfo data20

#### Scenario: Unsupported profile is requested
- **WHEN** an operator requests an official source profile that is not configured for the target exchange
- **THEN** the command SHALL refuse execution and report the unsupported exchange/source profile combination before making network requests or storage writes

### Requirement: Official Source Profiles Must Share Storage Semantics
Official source profiles SHALL write through the same financial source manifest, numeric fact, core fact, hot/cold tier, and readiness repository APIs regardless of upstream interface type.

#### Scenario: Structured JSON source succeeds
- **WHEN** an SSE or CNInfo structured JSON payload is parsed successfully
- **THEN** the system SHALL persist source manifests, all numeric facts, derived core facts, parser diagnostics, source mode, source profile, and tier placement using the existing financial storage abstraction

#### Scenario: Source profile differs by exchange
- **WHEN** financial facts from SSE and CNInfo profiles are stored
- **THEN** downstream financial statement APIs and valuation readiness SHALL see a consistent logical model while readiness still exposes source and parser distribution by exchange

#### Scenario: Source labels differ for the same financial concept
- **WHEN** SSE coded fields, CNInfo Chinese row labels, or XBRL tags represent the same standardized financial concept
- **THEN** numeric fact rows SHALL retain the source-native `fact_name` and SHALL also persist `canonical_fact_name`, `canonical_statement_family`, `canonical_semantic`, `canonical_unit`, and `canonical_version` metadata for cross-source querying

#### Scenario: Total-company metric conflicts with parent-attributable metric
- **WHEN** a source provides total owners' equity or total net profit but the canonical core field requires a parent-attributable metric
- **THEN** the total-company row SHALL be stored as a distinct canonical long-form fact such as `equity_total` or `net_income_total` and SHALL NOT silently populate the parent-attributable core field

### Requirement: AkShare Fallback Must Be Controlled By Source Profile Readiness
AkShare financial statements SHALL remain a configured fallback path for missing official data without overwriting higher-priority official facts.

#### Scenario: Official profile has missing facts
- **WHEN** an official source profile cannot provide a required report period or core fact for an instrument
- **THEN** the sync SHALL allow configured AkShare fallback only for the missing period or missing field and SHALL record fallback reason, fallback source, fallback-filled fields, and fallback share

#### Scenario: Official fact exists
- **WHEN** a higher-priority official source fact already exists for an instrument and report period
- **THEN** AkShare fallback SHALL NOT overwrite that fact unless an explicit operator-controlled repair mode is introduced by a later change

### Requirement: Official Source Profiles Must Be Rate Limited
Official structured financial source profiles SHALL define conservative request policy controls so validation and backfill runs remain bounded and recoverable under unknown official endpoint traffic limits.

#### Scenario: Validation run is executed
- **WHEN** a live validation or dry-run backfill is executed against SSE or CNInfo official endpoints
- **THEN** the run SHALL apply configured or explicit timeout, request interval, retry, backoff, batch size, batch timeout, checkpoint path, and maximum scope controls

#### Scenario: Endpoint access changes or throttles
- **WHEN** an official endpoint returns blocked, empty, malformed, timeout, HTTP error, or parser-ineligible responses
- **THEN** the run SHALL record explicit diagnostics and failed instrument-periods without silently switching source priority or claiming readiness

### Requirement: Broker Annual Report Embedded Risk-Control Source Profile
Official financial source profiles SHALL include a broker annual/semiannual embedded risk-control source profile for CNInfo formal report PDFs.

#### Scenario: Annual embedded profile is selected
- **WHEN** broker regulatory backfill targets an annual report period
- **THEN** the source profile SHALL discover and parse the formal CNInfo annual report PDF before attempting standalone risk-control report supplements

#### Scenario: Semiannual embedded profile is selected
- **WHEN** broker regulatory backfill targets a semiannual report period
- **THEN** the source profile SHALL discover and parse the formal CNInfo semiannual report PDF before attempting standalone risk-control report supplements

#### Scenario: Non-formal report is discovered
- **WHEN** a matching announcement title is a summary, audit report, inquiry letter, reply notice, continuous-supervision report, or performance-briefing announcement
- **THEN** the source profile SHALL reject it as a primary broker regulatory source and record the rejection reason

### Requirement: Standalone Risk-Control Reports Are Supplementary
Official financial source profiles SHALL treat standalone broker risk-control reports as supplementary evidence unless a report period cannot be covered by formal annual or semiannual reports.

#### Scenario: Primary embedded facts exist
- **WHEN** formal annual or semiannual embedded facts exist for an instrument and report period
- **THEN** standalone risk-control facts SHALL NOT override the primary facts unless an explicit repair or validation policy is configured

#### Scenario: Primary embedded report is missing
- **WHEN** the formal annual or semiannual report is missing, unparseable, or does not disclose a required broker regulatory fact
- **THEN** the source profile MAY use a standalone risk-control report as a fallback and SHALL record the fallback reason

### Requirement: Official Financial Acquisition Shall Distinguish Partial Success

Official financial source validation and maintenance SHALL distinguish transport success, structured payload parsing, numeric-fact persistence, strict canonical readiness, and missing-fact fallback. A target with valid official manifests or numeric facts but incomplete strict readiness SHALL NOT be labeled as an endpoint transport failure.

#### Scenario: CNInfo returns structured statements but misses parent equity
- **WHEN** CNInfo data20 returns parseable income, balance, and cash-flow JSON for an instrument-period
- **AND** the payload writes valid numeric facts including `equity_total` but not `equity_parent`
- **THEN** the result SHALL count the target as official acquisition/parse success
- **AND** SHALL report `equity_parent` as missing or fallback-required
- **AND** SHALL allow configured THS/Sina fallback to fill only the missing canonical fact

#### Scenario: CNInfo transport or parsing fails
- **WHEN** a CNInfo request returns an HTTP error, empty body, malformed JSON, unsupported payload, or parser failure
- **THEN** the result SHALL count a source failure with a bounded diagnostic containing endpoint class, HTTP status when available, content type, and a redacted response prefix
- **AND** SHALL keep the target eligible for configured fallback

### Requirement: CNInfo Equity Semantics Shall Remain Auditable

CNInfo data20 `所有者权益` SHALL remain mapped to `equity_total` and SHALL NOT silently populate `equity_parent`. The system MAY derive `equity_parent` only from same-context numeric `equity_total - minority_equity`; per-share main indicators SHALL NOT be used for this canonical derivation without a future source contract that defines share-count basis, units, and rounding.

#### Scenario: Total and minority equity are available
- **WHEN** the same CNInfo instrument-period context contains numeric total owners' equity and numeric minority interest in compatible CNY units
- **THEN** the parser or canonical derivation layer SHALL persist a derived `equity_parent` with explicit derivation metadata and source lineage

#### Scenario: Only total owners' equity is available
- **WHEN** CNInfo provides `所有者权益` but no compatible minority-interest fact
- **THEN** the system SHALL persist `equity_total`
- **AND** SHALL leave `equity_parent` missing for fallback/readiness purposes

### Requirement: Sina Fallback Requests Shall Be Bounded and Diagnosable

The Sina financial statement fallback adapter SHALL apply configured request timeout, inter-request interval, bounded retry attempts, and retry backoff at the actual Sina HTTP request boundary. It SHALL classify the response before JSON decoding and SHALL preserve the next configured fallback interface after final failure.

#### Scenario: Sina returns a transient non-JSON response
- **WHEN** the Sina endpoint returns an empty body, malformed JSON, rate-limit response, or transient HTTP status
- **THEN** the adapter SHALL retry within configured limits
- **AND** SHALL emit one compact final diagnostic with status, content type, and response-prefix evidence
- **AND** SHALL allow the provider loop to continue to THS/Eastmoney fallback

#### Scenario: Sina returns valid JSON
- **WHEN** the Sina endpoint returns a successful structured financial response
- **THEN** the adapter SHALL parse the target statement without changing existing report-period, unit, or source-native field semantics
