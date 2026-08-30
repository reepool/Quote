## ADDED Requirements

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
