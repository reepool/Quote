# a-share-corporate-action-daily-input-integrity Specification

## Purpose
Defines data-integrity rules for daily A-share corporate-action factor discovery, per-instrument request windows, failure gates, bounded diagnostics, and deterministic filtering of convertible-bond conversion-price notices.
## Requirements
### Requirement: Daily factor discovery preserves event dates
The system SHALL preserve the event date associated with every discovered A-share ex-dividend symbol and SHALL use that dated evidence when selecting per-instrument factor synchronization work.

#### Scenario: Discovered event falls inside the instrument window
- **WHEN** a discovered event date for an instrument falls inside that instrument's inclusive factor request window
- **THEN** the system SHALL select the instrument for factor synchronization and SHALL pass the instrument's own request window to the factor source

#### Scenario: Discovered event falls outside the instrument window
- **WHEN** a symbol is discovered because its event falls inside another instrument's wider union window but all of its own discovered event dates fall outside its own factor request window
- **THEN** the system SHALL exclude that instrument from factor synchronization and SHALL NOT count it as skipped source data or a download failure

#### Scenario: Discovery date cannot be normalized
- **WHEN** the discovery source returns a matched symbol with a missing or invalid event date
- **THEN** the system SHALL expose discovery uncertainty and SHALL NOT use that row as proof that an empty factor response is successful

### Requirement: Factor failures require in-window evidence
The system MUST distinguish a real missing factor for an in-window known event from a zero-row result caused by requesting a later instrument window.

#### Scenario: In-window known event returns no factor
- **WHEN** an instrument has a normalized discovered event inside its own factor window and the configured factor source returns no usable event coverage for that window
- **THEN** the system SHALL count an actionable factor failure and SHALL keep the exchange predecessor watermark from advancing

#### Scenario: Out-of-window event is not requested
- **WHEN** an instrument has no discovered event inside its own factor window
- **THEN** the system SHALL NOT call the factor source solely because the same symbol appeared elsewhere in the union discovery result

#### Scenario: All selected instruments complete
- **WHEN** every instrument with an in-window discovered event is synchronized successfully and quote persistence has completed through the target session
- **THEN** the factor stage SHALL be successful and the producer SHALL be eligible to advance the exchange-specific BaoStock/Sina predecessor watermark

#### Scenario: Real transport or persistence failure remains blocking
- **WHEN** a selected instrument encounters a factor transport, decode, history-coverage, event-coverage, or persistence failure
- **THEN** the factor stage SHALL remain partial and the producer SHALL NOT advance that exchange's successful-through watermark

### Requirement: Factor synchronization reports explain selection and failure scope
The system SHALL report bounded, machine-readable diagnostics for daily factor discovery, selection, exclusion, and failure decisions.

#### Scenario: Union discovery contains out-of-window symbols
- **WHEN** one or more discovered symbols are excluded because their event dates do not intersect their own factor windows
- **THEN** the result SHALL report the excluded count and a bounded sample containing instrument ID, request window, and discovered dates

#### Scenario: Known in-window event fails
- **WHEN** a selected instrument produces no usable factor for an in-window known event
- **THEN** the result SHALL report the failure class and a bounded instrument sample without requiring debug-level logs

#### Scenario: Watermark remains stale
- **WHEN** factor-stage failures prevent an exchange watermark from advancing
- **THEN** watermark metadata and downstream predecessor diagnostics SHALL identify the factor-stage reason and affected count

### Requirement: Convertible-bond conversion-price notices are not debt-to-equity events
The system SHALL distinguish ordinary convertible-bond conversion-price adjustment notices from actual debt-to-equity restructuring events before XDXR anomaly governance.

#### Scenario: Repurchase cancellation adjusts convertible-bond conversion price
- **WHEN** a title concerns completion of repurchased-share cancellation and adjustment of a convertible-bond conversion price without a distribution implementation pattern
- **THEN** the system SHALL classify it as deterministic non-XDXR and SHALL NOT retain it as an unmatched special corporate-action candidate

#### Scenario: Actual debt-to-equity restructuring notice
- **WHEN** a title describes an actual debt-to-equity swap, restructuring conversion, or shares issued to settle debt and is not merely convertible-bond conversion-price terminology
- **THEN** the system SHALL retain it as an exceptional corporate-action candidate

#### Scenario: Genuine distribution pattern takes precedence
- **WHEN** a title contains both convertible-bond conversion-price language and an explicit equity or profit distribution implementation pattern
- **THEN** the system SHALL keep the announcement eligible for structured refresh or semantic governance

#### Scenario: Persisted false positive is revalidated
- **WHEN** a previously persisted unmatched announcement is re-evaluated under the new title policy and matches only the convertible-bond conversion-price exclusion
- **THEN** the system SHALL remove it from the carried semantic queue without deleting the original announcement evidence
