## MODIFIED Requirements

### Requirement: HKEX Direct Multiplicative Factor Source
The system SHALL use the Sina HKEX qfq-factor endpoint through a project-owned safe response parser as the HKEX AkShare-compatible primary production source for multiplicative adjustment factors and SHALL NOT evaluate upstream response text as Python code.

#### Scenario: Convert qfq factor to backward cumulative factor
- **WHEN** the source returns a valid sparse `qfq-factor` table with a base row and event rows
- **THEN** the system computes each event `cumulative_factor` as `qfq_factor / base_qfq_factor`
- **AND** the system stores only real event dates, not the `1900-01-01` base row

#### Scenario: No-event HKEX symbol
- **WHEN** the source returns only the base row or an otherwise valid table with no event in the requested range
- **THEN** the system returns an empty list
- **AND** the source factory does not call the fallback source

#### Scenario: Source cannot determine factors
- **WHEN** the source call fails due to network error, parser error, missing required columns, non-positive factors, or missing usable base factor
- **THEN** the system returns `None`
- **AND** the source factory may call the configured fallback source

#### Scenario: Missing base row
- **WHEN** the source response does not contain the configured base date
- **AND** `require_base_date` is true
- **THEN** the system returns `None`
- **AND** the system does not silently treat the first real event as the base factor

#### Scenario: Upstream response is not parseable data
- **WHEN** the Sina response is HTML, empty, truncated, oversized, malformed, or not the expected assignment payload
- **THEN** the parser rejects it without `eval`, `exec`, or another code-execution mechanism
- **AND** the source returns `None` for configured fallback

## ADDED Requirements

### Requirement: HKEX factor parser failures are actionable
The HKEX factor adapter SHALL classify source and parser failures with bounded diagnostics that distinguish upstream response defects from project source-code syntax errors.

#### Scenario: Safe parser rejects a response
- **WHEN** response validation or decoding fails
- **THEN** INFO or WARNING logs include provider, endpoint family, symbol, HTTP status when available, response hash, exception type, and stable error code
- **AND** DEBUG logs include a bounded traceback
- **AND** response bodies, cookies, and credentials are not logged

#### Scenario: Python source is valid but upstream text is malformed
- **WHEN** malformed upstream text would previously have raised `SyntaxError` from dependency `eval`
- **THEN** the system reports an upstream parser error instead of the ambiguous message `invalid syntax (<string>, line 1)`
