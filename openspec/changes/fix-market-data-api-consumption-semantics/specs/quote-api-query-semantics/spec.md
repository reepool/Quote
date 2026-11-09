## ADDED Requirements

### Requirement: Adjusted Quote Price Fields Share One Basis
The daily Quotes API SHALL apply the same actual adjustment multiplier to OHLC, `pre_close`, and `change`, SHALL preserve source reference values as `raw_pre_close` and `raw_change`, and SHALL disclose the actual price basis. Source `pre_close` SHALL retain its ex-date reference-price meaning rather than being replaced with a prior raw close.

#### Scenario: Backward adjusted price has an aligned reference
- **WHEN** a raw bar has close 1453, pre_close 1452, change 1 and the backward multiplier is 7.671096
- **THEN** the returned close, pre_close and change SHALL equal their respective raw values multiplied by that multiplier within the declared rounding precision
- **AND** raw_pre_close and raw_change SHALL remain 1452 and 1
- **AND** the source percentage return SHALL remain unchanged

#### Scenario: Unadjusted or ineligible instrument is queried
- **WHEN** adjustment is none or the existing instrument contract does not apply stock adjustment
- **THEN** price values SHALL remain raw, the actual basis SHALL be none, and the factor SHALL be 1

### Requirement: Quote Volumes Distinguish Actual Trading From Adjustment
Daily and latest Quotes API responses SHALL expose `volume_raw` as actual traded shares, `volume_adj` as the volume corresponding to the declared adjustment, and deprecated `volume` as an alias of `volume_raw`. Amount SHALL remain actual transaction currency amount and SHALL carry the instrument currency unit.

#### Scenario: The same bar is requested with different adjustments
- **WHEN** the caller requests none, qfq and hfq for one bar
- **THEN** volume_raw, volume and amount SHALL be identical across requests
- **AND** volume_adj SHALL follow the actual inverse price multiplier with declared integer rounding
- **AND** quote volume statistics SHALL aggregate actual shares

#### Scenario: Caller uses a non-JSON response format
- **WHEN** daily quotes are serialized through a supported CSV or other response branch
- **THEN** the original-versus-adjusted price and volume semantics SHALL match the JSON contract

### Requirement: Forward Adjustment Uses A Disclosed Query Window Anchor
The daily API SHALL retain the qfq default and existing query-window-last-bar anchor within each continuity context, SHALL explain this anchor in OpenAPI and the current API documentation, and SHALL NOT label a legal single-bar factor of 1 as an unsupported adjustment.

#### Scenario: Single date and wide interval differ
- **WHEN** a single historical bar and a wider interval spanning an ex-date are queried with qfq
- **THEN** the single-bar factor SHALL be allowed to equal 1
- **AND** the wider interval SHALL use its last-bar anchor and correctly adjust pre-event prices
- **AND** the documentation SHALL warn that prices from different end dates do not share a guaranteed anchor

#### Scenario: One complete interval is paginated
- **WHEN** a caller changes limit or offset without changing the full requested interval or filters
- **THEN** qfq SHALL be calculated before pagination and the same row SHALL retain the same factor across pages

### Requirement: Quote Percentage Units And Return Definitions Are Explicit
Daily and latest quotes SHALL retain legacy percentage-valued `pct_change`, SHALL expose `pct_change_ratio=pct_change/100`, and SHALL provide machine-readable units and the source-reference return basis. Object envelopes SHALL carry units at their top level, while latest SHALL preserve its list shape and carry units per item.

#### Scenario: A negative percentage is normalized
- **WHEN** a bar's source pct_change is -0.5513 percent
- **THEN** pct_change SHALL remain -0.5513 and pct_change_ratio SHALL equal -0.005513 within serialization precision
- **AND** units SHALL identify percent and ratio separately

### Requirement: Unavailable Turnover Is Not A Valid Zero
The TDX source adapter SHALL represent unavailable turnover as null. API reads SHALL normalize only known TDX placeholder zeros to null, disclose per-field missing reasons, and cap the response quality score at 0.9 when turnover is known missing, without reclassifying a valid OHLCV bar as suspended or incomplete solely for that missing optional field.

#### Scenario: Historical TDX placeholder is read
- **WHEN** a stored pytdx bar contains the known unavailable-turnover zero placeholder
- **THEN** turnover SHALL be null, missing_fields SHALL include turnover and missing_reason SHALL explain source unavailability
- **AND** response quality_score SHALL be no greater than 0.9 and SHALL NOT exceed its previously stored score
- **AND** the read SHALL NOT rewrite the stored row

#### Scenario: Another provider supplies a valid zero
- **WHEN** another provider supplies a legitimate zero-turnover observation
- **THEN** the API SHALL preserve zero and SHALL NOT infer missingness solely from that value

### Requirement: Declared Quote Filters Execute Before Paging And Statistics
Daily queries SHALL execute `include_suspended` and `min_quality_score` in addition to existing filters, using the publicly normalized quality score. All filters SHALL be combined by intersection before pagination and response statistics; inability to evaluate a required filter SHALL produce a structured data-unavailable error rather than unfiltered success.

#### Scenario: Suspended and low-quality rows exist
- **WHEN** a fixture contains 20 normal bars and 21 suspended low-quality bars
- **THEN** include_suspended=false SHALL exclude the 21 suspended bars
- **AND** include_suspended=true alone SHALL retain all 41
- **AND** include_suspended=true with min_quality_score=0.9 SHALL exclude rows below that threshold

#### Scenario: Explicit filters conflict
- **WHEN** include_suspended=false and tradestatus=0 are supplied together
- **THEN** the result SHALL be their empty intersection and SHALL NOT silently relax either filter

### Requirement: Latest Daily Quotes Disclose EOD Timing And Identity
Latest quotes SHALL disclose `quote_type=eod`, `is_intraday=false`, the timezone-aware bar-date label `as_of`, response assembly `computed_at`, persisted record timestamps when available, and `staleness_trading_days`. The endpoint description SHALL begin by identifying the response as the latest stored daily close rather than a live quote. Symbol SHALL resolve from authoritative instrument metadata when absent in the bar. Existing batch_id SHALL remain available with `batch_id_kind=progress_session` and SHALL NOT be described as a quote timestamp or unique ingestion run.

#### Scenario: A prior close is returned during a trading session
- **WHEN** the last stored bar is the prior completed session and the current session has not completed
- **THEN** the response SHALL identify that bar's date, EOD status and non-intraday nature
- **AND** completed-session staleness SHALL be zero, increasing to one if the current session completes without a new bar

#### Scenario: Calendar coverage is unavailable
- **WHEN** authoritative trading-session coverage or cutoff evidence is missing
- **THEN** staleness_trading_days SHALL be null with an explanatory reason rather than a guessed calendar-day count
