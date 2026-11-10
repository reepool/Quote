## ADDED Requirements

### Requirement: Adjusted Quote Price Fields Share One Basis
The daily Quotes API SHALL apply the same actual adjustment multiplier to OHLC, `pre_close`, and `change`, SHALL preserve source reference values as `raw_pre_close` and `raw_change`, and SHALL disclose the actual price basis. Source `pre_close` SHALL retain its ex-date reference-price meaning rather than being replaced with a prior raw close.

#### Scenario: Backward adjusted price has an aligned reference
- **WHEN** a raw bar has close 1453, pre_close 1452, change 1 and the backward multiplier is 7.671096
- **THEN** the returned close, pre_close and change SHALL equal their respective raw values multiplied by that multiplier within the declared rounding precision
- **AND** raw_pre_close and raw_change SHALL remain 1452 and 1
- **AND** the source percentage return SHALL remain unchanged

#### Scenario: Return arithmetic is consistent within declared precision
- **WHEN** the raw reference price is positive and the fixture has close=1453, pre_close=1452 and pct_change=0.0689 percent
- **THEN** abs(close/pre_close-1-pct_change_ratio) after hfq adjustment SHALL be less than 1e-6
- **AND** general inputs SHALL be checked against a rounding budget derived from source-return precision and adjusted-price precision rather than an unconditional exact-equality promise

#### Scenario: Unadjusted or ineligible instrument is queried
- **WHEN** adjustment is none or the existing instrument contract does not apply stock adjustment
- **THEN** price values SHALL remain raw, the actual basis SHALL be none, and the factor SHALL be 1

#### Scenario: The adjustment value is misspelled
- **WHEN** daily quotes receives an adjust value outside qfq, hfq, none and the existing forward/backward aliases after existing normalization
- **THEN** it SHALL return structured 422 with error_code=invalid_query_parameter and field=adjust
- **AND** it SHALL NOT silently fall through to an unadjusted-success response

### Requirement: Quote Volumes Distinguish Actual Trading From Adjustment
Daily and latest Quotes API responses SHALL expose `volume_raw` as actual traded shares, `volume_adj` as the volume corresponding to the declared adjustment, and deprecated `volume` as an alias of `volume_raw`. Amount SHALL remain actual transaction currency amount and SHALL carry the instrument currency unit.

#### Scenario: The same bar is requested with different adjustments
- **WHEN** the caller requests none, qfq and hfq for one bar
- **THEN** volume_raw, volume and amount SHALL be identical across requests
- **AND** volume_adj SHALL follow the actual inverse price multiplier with declared integer rounding
- **AND** quote volume statistics SHALL aggregate actual shares

#### Scenario: Actual volume yields an actual traded average price
- **WHEN** the fixture has raw volume=4284700, amount=6199528448, raw low=1436 and raw high=1457.72
- **THEN** amount/volume_raw SHALL be approximately 1446.90 and lie in that raw price range under none, qfq and hfq

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

### Requirement: Existing Factor Evidence And Technical Traceability Are Preserved
Quote projection changes SHALL preserve the existing factor_metadata dataset, series version, activation, coverage and fallback evidence, and SHALL NOT turn an existing factor availability error into a successful adjusted response. Technical calculation provenance SHALL preserve calc_version, parameter_hash and quote_summary unless the underlying algorithm actually changes.

#### Scenario: Canonical factor availability is invalid
- **WHEN** the existing factor bundle reports an availability error that produces HTTP 409
- **THEN** the public projection SHALL preserve that failure and SHALL NOT return raw prices labelled forward or backward
- **AND** explicitly complete no-event coverage and legal query anchors with factor=1 SHALL remain valid outcomes

### Requirement: Quote Percentage Units And Return Definitions Are Explicit
Daily and latest quotes SHALL retain legacy percentage-valued `pct_change` as a deprecated compatibility alias, SHALL expose unit-bearing `pct_change_pct=pct_change` and decimal `pct_change_ratio=pct_change/100`, and SHALL provide machine-readable units and the source-reference return basis. Object envelopes SHALL carry units at their top level, while latest SHALL preserve its list shape and carry units per item.

#### Scenario: A negative percentage is normalized
- **WHEN** a bar's source pct_change is -0.5513 percent
- **THEN** pct_change and pct_change_pct SHALL both remain -0.5513 and pct_change_ratio SHALL equal -0.005513 within serialization precision
- **AND** units SHALL identify percent and ratio separately
- **AND** actual OpenAPI SHALL mark pct_change deprecated and identify its replacements

### Requirement: Unavailable Turnover Is Not A Valid Zero
The TDX and Tencent source adapters SHALL represent unavailable turnover as null. API reads SHALL normalize only known TDX/Tencent placeholder zeros to null, apply the same missingness policy to newly collected null values, disclose per-field missing reasons, and cap a valid numeric response quality score at 0.9 when turnover is known missing. An unavailable quality score SHALL be represented conservatively as zero with a missing-score reason rather than invented as full quality. Missing optional turnover SHALL NOT by itself reclassify a valid OHLCV bar as suspended or incomplete.

#### Scenario: Historical TDX placeholder is read
- **WHEN** a stored pytdx or tencent bar contains the known unavailable-turnover zero placeholder
- **THEN** turnover SHALL be null, missing_fields SHALL include turnover and missing_reason SHALL explain source unavailability
- **AND** response quality_score SHALL be no greater than 0.9 and SHALL NOT exceed its previously stored score
- **AND** the read SHALL NOT rewrite the stored row

#### Scenario: Newly collected unavailable turnover is already null
- **WHEN** a new pytdx bar has turnover=null and otherwise valid OHLCV with quality_score=1
- **THEN** both daily and latest SHALL report the same turnover missing reason and quality_score no greater than 0.9
- **AND** they SHALL NOT regress to full quality merely because the legacy zero normalization was unnecessary

#### Scenario: Another provider supplies a valid zero
- **WHEN** another provider supplies a legitimate zero-turnover observation
- **THEN** the API SHALL preserve zero and SHALL NOT infer missingness solely from that value

### Requirement: Declared Quote Filters Execute Before Paging And Statistics
Daily queries SHALL execute `include_suspended` and `min_quality_score` in addition to existing filters, using the publicly normalized quality score. All filters SHALL be combined by intersection before pagination and response statistics. include_quality SHALL control presentation rather than whether filter inputs are loaded; inability to evaluate a required filter from actual source data SHALL produce a structured data-unavailable error rather than unfiltered success.

#### Scenario: Suspended and low-quality rows exist
- **WHEN** a fixture contains 20 normal bars and 21 suspended low-quality bars
- **THEN** include_suspended=false SHALL exclude the 21 suspended bars
- **AND** include_suspended=true alone SHALL retain all 41
- **AND** include_suspended=true with min_quality_score=0.9 SHALL exclude rows below that threshold

#### Scenario: Explicit filters conflict
- **WHEN** include_suspended=false and tradestatus=0 are supplied together
- **THEN** the result SHALL be their empty intersection and SHALL NOT silently relax either filter

#### Scenario: Quality presentation is disabled while a quality threshold is active
- **WHEN** min_quality_score=0.95 is supplied with include_quality=false and a known missing-turnover bar has normalized score=0.9
- **THEN** the bar SHALL be excluded exactly as with include_quality=true
- **AND** hiding quality fields SHALL NOT bypass filtering, invent full-quality values, or discard the internally required input columns

### Requirement: Latest Daily Quotes Disclose EOD Timing And Identity
Latest quotes SHALL disclose `quote_type=eod`, `is_intraday=false`, the timezone-aware bar-date label `as_of`, response assembly `computed_at`, persisted record timestamps when available, and `staleness_trading_days`. The endpoint description SHALL begin by identifying the response as the latest stored daily close rather than a live quote. Symbol SHALL resolve from authoritative instrument metadata when absent in the bar. Existing batch_id SHALL remain available; batch_id_kind SHALL identify a proven progress_session or source_batch, be unknown when origin is unproven, and be null when batch_id is absent. A progress identity SHALL NOT be described as a quote timestamp or unique ingestion run.

#### Scenario: A prior close is returned during a trading session
- **WHEN** the last stored bar is the prior completed session and the current session has not completed
- **THEN** the response SHALL identify that bar's date, EOD status and non-intraday nature
- **AND** completed-session staleness SHALL be zero, increasing to one if the current session completes without a new bar

#### Scenario: Calendar coverage is unavailable
- **WHEN** authoritative trading-session coverage or cutoff evidence is missing
- **THEN** staleness_trading_days SHALL be null with an explanatory reason rather than a guessed calendar-day count

#### Scenario: A provider supplied its own batch identity
- **WHEN** a bar retains a provider-supplied batch_id rather than a DataManager progress identity
- **THEN** its original batch_id SHALL remain unchanged and SHALL NOT be relabelled progress_session merely because another stock used that mechanism
- **AND** origin without sufficient evidence SHALL be marked unknown

#### Scenario: Instrument currency and units are explicit
- **WHEN** a daily or latest quote is returned
- **THEN** currency SHALL be the actual instrument currency, with units identifying amount in that currency, prices in currency/share, volumes in share, turnover and percentage returns in percent, factor and ratio return in ratio, and quality in score_0_1
- **AND** deprecated legacy fields SHALL retain correct unit entries alongside new fields

#### Scenario: Suspended-only selection is explicit
- **WHEN** the caller requests tradestatus=0 and include_suspended=true
- **THEN** suspended rows satisfying other filters SHALL be returned
- **AND** documentation SHALL state that the include_suspended=false default otherwise excludes them
