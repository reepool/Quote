## ADDED Requirements

### Requirement: Financial PIT panel materialization
The system SHALL materialize a point-in-time financial panel table `financial_pit_panel` in the financials database, with exactly one row per `(instrument_id, report_period)` built from `financial_facts` joined with `financial_indicator_snapshots`, carrying base statement values, ratio indicators, derived indicators, availability metadata, attribution quality, a reserved delisting flag, and lineage fields.

#### Scenario: Full build covers every ingested instrument-period pair
- **WHEN** a full panel build runs against the current financials database
- **THEN** the system SHALL produce one panel row per `(instrument_id, report_period)` pair present in `financial_facts`
- **AND** each row SHALL carry its `available_at`, `available_at_quality`, `effective_at` when derivable, `panel_version`, and `built_at`

#### Scenario: Rebuild is idempotent
- **WHEN** a panel build runs twice with unchanged source rows and the same panel version
- **THEN** the second build SHALL leave row count and row content unchanged
- **AND** rows whose source `updated_at` did not change SHALL NOT be rewritten

#### Scenario: Sparse early periods are preserved as-is
- **WHEN** a report period before 2023-12-31 contains only sparse coverage (single-digit rows)
- **THEN** the panel SHALL include those rows without fabricating additional instruments
- **AND** cross-section consumers SHALL see them subject to the same availability rules

#### Scenario: Delisting flag is reserved and constant
- **WHEN** any panel row is materialized or returned
- **THEN** `is_delisted` SHALL be present with value `false`
- **AND** the API documentation SHALL state the field is reserved with no data promise for this change

### Requirement: Point-in-time availability derivation
The system SHALL derive each panel row's `available_at` as the maximum of the announcement-date derivation chain result and the last substantive value change date, SHALL derive `effective_at` as the next trading day after `available_at` when the trading calendar covers it (null otherwise), and SHALL record a quality level distinguishing real disclosure dates, disclosure-event dates, statutory-deadline fallbacks, and delayed estimates.

#### Scenario: Real publish date is used when present
- **WHEN** a panel row's `(instrument_id, report_period)` has a non-empty `financial_facts.publish_date`
- **THEN** the derivation chain result SHALL be that publish date with quality `exact`

#### Scenario: Disclosure event date is used when matched
- **WHEN** a row lacks `publish_date` but a matching disclosure event exists in `financial_disclosure_event_state` with a non-empty `announcement_time`
- **THEN** the derivation chain result SHALL be derived from that announcement time with quality `exact_event`

#### Scenario: Statutory deadline fallback never precedes the deadline
- **WHEN** neither `publish_date` nor a matching disclosure event exists
- **THEN** the derivation chain result SHALL be the statutory disclosure deadline (Q1 report -> 04-30, semi-annual -> 08-31, Q3 -> 10-31, annual -> 04-30 of the following year) with quality `statutory_deadline`
- **AND** the resulting `available_at` SHALL NOT be earlier than that deadline

#### Scenario: Delayed filers are pushed later
- **WHEN** a matching delay or unable-to-file-on-time event exists in `financial_disclosure_event_state`
- **THEN** the derivation chain result SHALL be `max(statutory deadline, delay announcement date + grace parameter)` with quality `delayed_estimate`
- **AND** the grace parameter SHALL be calibrated against the measured delayed-filer event hit rate on the 2026Q2/2026Q3 periods before the fallback strategy is accepted

#### Scenario: Substantive value change pushes availability later
- **WHEN** the last `data_change_log` record for the `(instrument_id, report_period)` with `old_hash != new_hash` has a `changed_at` later than the derivation chain result
- **THEN** the row `available_at` SHALL be that `changed_at`
- **AND** the row SHALL set `revised_after_publish = true` and expose `last_value_change_at`

#### Scenario: Non-substantive updates do not push availability
- **WHEN** source rows are re-parsed or re-ingested without any value change (`old_hash = new_hash` or no change-log record with differing hashes)
- **THEN** the row `available_at` SHALL NOT advance due to those updates

#### Scenario: Effective date is the next trading day
- **WHEN** a panel row has a derived `available_at` and the trading calendar covers dates after it
- **THEN** the system SHALL derive `effective_at` as the next trading day strictly after `available_at` using the trading calendar
- **AND** responses SHALL expose both `available_at` (announcement semantics) and `effective_at` (execution semantics) with their meanings documented
- **AND** when the trading calendar does not cover dates after `available_at`, `effective_at` SHALL be null rather than approximated

### Requirement: As-of cross-section query
The system SHALL provide a batch cross-section query that, for a required `as_of` date, returns for each instrument the latest panel row whose `available_at` is on or before `as_of`, with optional report-period, exchange, field-projection, and pagination controls, assembled server-side in a single query path, using `YYYYMMDD` date formatting at the API layer.

#### Scenario: Each instrument returns its latest known period
- **WHEN** the query runs with `as_of = T`
- **THEN** every returned row SHALL satisfy `available_at <= T`
- **AND** for each returned instrument no panel row with a later `report_period` and `available_at <= T` SHALL exist
- **AND** an instrument whose newest filing is not yet available at T SHALL fall back to its latest earlier visible period

#### Scenario: No future data is returned
- **WHEN** a panel row's `available_at` is after the requested `as_of`
- **THEN** the row SHALL NOT appear in the cross-section result for that `as_of`

#### Scenario: Revision-guarded rows fall back before the change date
- **WHEN** a panel row has `revised_after_publish = true` because a substantive change occurred after the original announcement
- **THEN** an as-of query before that change date SHALL NOT return that `(instrument_id, report_period)` row
- **AND** the instrument SHALL instead return its latest earlier visible period
- **AND** cross-section and panel responses SHALL expose `last_value_change_at` and `revised_after_publish` so consumers can detect pushed availability

#### Scenario: Optional lenient revision guard only when thresholds are exceeded
- **WHEN** the measured substantive-change impact exceeds 5% of rows or an average fallback of more than one period
- **THEN** the cross-section endpoint SHALL accept a `revision_guard` parameter with default `strict` and optional `lenient`
- **AND** `lenient` mode SHALL reinstate announcement-based availability while marking every response with `guard=lenient` and an explicit lookahead warning in the API documentation
- **AND** when thresholds are not exceeded the parameter SHALL NOT be implemented

#### Scenario: Optional filters, paging, and response statistics apply
- **WHEN** the query includes `report_period`, `exchange`, `fields`, `limit`, or `offset` parameters
- **THEN** the system SHALL apply them after the per-instrument latest-row selection
- **AND** single-page size SHALL NOT exceed 2000 rows
- **AND** the response SHALL include a `stats` block with the period-lag distribution of returned rows and the attribution-quality distribution

#### Scenario: Universe limitation and completeness are disclosed
- **WHEN** any panel API endpoint returns a response
- **THEN** the response SHALL carry a universe note stating that coverage is the listed-instrument universe only and delisted stocks are not covered
- **AND** the cross-section universe note SHALL include the latest complete report period and, when the newest period is still inside its disclosure window, its incomplete status

### Requirement: Derived indicators with computability boundaries
The system SHALL precompute per-share, year-over-year, and TTM derived indicators at panel build time from cumulative statement values, SHALL compute them only when the required prior-period inputs exist, and SHALL return null outside the computable boundaries.

#### Scenario: YoY boundaries reflect available history
- **WHEN** a panel row's report period is an annual period on or after 2024-12-31, or a quarterly period on or after the corresponding 2025 period
- **THEN** `rev_yoy` and `np_yoy` SHALL be computed against the prior-year same period when that input exists
- **AND** for earlier periods (2023-12-31 and quarterly 2024 periods) these fields SHALL be null

#### Scenario: TTM boundary reflects available history
- **WHEN** a panel row's report period is on or after 2025-03-31
- **THEN** `ttm_revenue` and `ttm_net_income` SHALL be computed as current cumulative plus prior annual minus prior-year same-period cumulative when inputs exist
- **AND** for earlier periods these fields SHALL be null

#### Scenario: Per-share derivation guards the denominator
- **WHEN** `shares_outstanding` is missing or zero
- **THEN** `eps`, `bps`, and `ocfps` SHALL all be null for that row

### Requirement: Attribution quality marking
The system SHALL treat parent-attributable (归母) as the default semantic contract for `net_income` and `equity` (consistent with the AKShare dual-source field alignment), SHALL mark each panel row's `attribution_quality` as `parent`, `mixed`, or `unverified` based on the recorded lineage semantics, SHALL keep values computable regardless of marking, and SHALL expose the distribution through coverage and cross-section statistics.

#### Scenario: Parent-attributable rows are marked parent
- **WHEN** lineage records both `net_income` and `equity` as parent-attributable semantics
- **THEN** the row SHALL be marked `parent`

#### Scenario: Unmarked or fallback lineage is flagged, not nulled
- **WHEN** a row's `net_income` has a value but lineage records no source alias, or a field fell back to a consolidated-total alias
- **THEN** the row SHALL be marked `unverified` or `mixed` respectively
- **AND** its numeric values SHALL still be computed and returned (not null)
- **AND** the marking SHALL be visible in panel rows, cross-section items, and coverage distributions

### Requirement: Null over placeholder values
The system SHALL represent missing numeric values in panel API responses as JSON null and SHALL NOT emit `0`, empty string, `-`, or `"0.00%"` placeholders for missing financial values.

#### Scenario: Missing values are null
- **WHEN** a numeric field has no computable value for a panel row
- **THEN** the API response SHALL contain `null` for that field

#### Scenario: Legitimate zero is distinguishable
- **WHEN** a financial value is genuinely zero in the source
- **THEN** the response SHALL return numeric `0` for that field without converting it to null

### Requirement: Panel raw access endpoint
The system SHALL provide a raw panel access endpoint returning panel rows with paging, optional filters by report period, instrument, and availability date, so a consumer can cache the full panel offline and assemble cross-sections client-side.

#### Scenario: Paged panel dump
- **WHEN** the endpoint is called with `limit` and `offset`
- **THEN** the system SHALL return panel rows in a stable order with rows including all availability-quality, attribution, and lineage fields
- **AND** single-page size SHALL NOT exceed 2000 rows

### Requirement: Panel changes feed endpoint
The system SHALL provide an append-only changes feed endpoint based on `data_change_log` for the financials domain, keyed by the monotonic `sequence_id` cursor, returning affected `(instrument_id, report_period)` keys with change timestamps.

#### Scenario: Cursor is monotonic and replay-safe
- **WHEN** the endpoint is called with a cursor
- **THEN** the system SHALL return change entries with `sequence_id` greater than the cursor using the existing `(domain, sequence_id)` index
- **AND** the response SHALL include `next_cursor`
- **AND** re-consuming the same cursor SHALL return the same entries (idempotent replay)

### Requirement: Panel coverage self-check
The system SHALL provide a coverage endpoint reporting, per report period, actual instrument counts, lifecycle-derived expected counts with the derivation stated, gap counts, reverse discrepancies where rows exist outside the lifecycle universe, the `available_at_quality` distribution, `attribution_quality` distribution, `revised_after_publish` row counts, derived-field non-null rates, completeness markers, and recent source-change row counts.

#### Scenario: Expected counts are lifecycle-derived and disclosed
- **WHEN** the coverage endpoint reports a bulk report period (2023-12-31 onward)
- **THEN** expected counts SHALL be derived from instrument lifecycle fields (stock instruments on SSE/SZSE/BSE listed on or before the period end and not delisted as of the period end)
- **AND** the response SHALL state this derivation explicitly

#### Scenario: Sparse early periods report actual counts only
- **WHEN** the coverage endpoint reports a report period before the first bulk period (2023-12-31) that has only sparse panel rows
- **THEN** it SHALL report the actual row count with a sparse marker
- **AND** it SHALL NOT compute lifecycle expected or gap counts for that period, because they would be meaningless against single-digit actuals

#### Scenario: Reverse discrepancies are exposed
- **WHEN** panel rows exist for instruments whose lifecycle fields exclude them from the expected universe
- **THEN** the coverage endpoint SHALL report the count as a reverse discrepancy
- **AND** the calibration task SHALL attribute these discrepancies before the M1 acceptance

#### Scenario: Completeness markers are time-based
- **WHEN** the coverage endpoint reports a report period
- **THEN** `is_complete` SHALL be true only when the evaluation time is past the statutory disclosure deadline of that period
- **AND** the response SHALL include `latest_complete_period`

#### Scenario: Coverage numbers match the panel
- **WHEN** the coverage endpoint is called for a report period
- **THEN** the reported actual counts and quality distributions SHALL equal the values obtained by querying the panel table directly

### Requirement: Incremental refresh after financial sync
The system SHALL refresh affected panel rows after the financial statement sync pipeline commits new or updated source rows, and after disclosure-event updates that affect availability derivation, within a bounded refresh time, through a single write owner.

#### Scenario: Source upsert triggers panel refresh
- **WHEN** a financial statement sync upserts a source row and its `updated_at` changes
- **THEN** the affected panel row SHALL be refreshed with recomputed values and availability
- **AND** the refreshed row SHALL become visible in the changes feed

#### Scenario: Disclosure event update re-derives availability
- **WHEN** a disclosure event for an `(instrument_id, report_period)` is inserted or updated
- **THEN** the panel row availability derivation SHALL be re-evaluated
- **AND** a quality change (for example `statutory_deadline` to `delayed_estimate`) SHALL be reflected in the panel and coverage outputs

#### Scenario: Refresh stays bounded
- **WHEN** an incremental refresh runs after a routine sync batch
- **THEN** the refresh SHALL complete within 30 seconds

### Requirement: Panel query performance
The system SHALL serve panel queries from the precomputed table without request-time aggregation over the raw fact tables, within the stated latency targets.

#### Scenario: Cross-section latency target
- **WHEN** the cross-section endpoint is called for a page of up to 1000 rows
- **THEN** warm-query P95 latency SHALL be at most 1 second and cold-start at most 3 seconds

#### Scenario: Paged endpoint latency target
- **WHEN** the panel raw access, coverage, or industries as-of endpoint is called for a page of up to 2000 rows
- **THEN** the response SHALL complete within 3 seconds

#### Scenario: Full panel pull target
- **WHEN** a consumer pulls all report periods (12 periods, roughly 5,500 rows each) through the paged endpoints
- **THEN** the total sequential pull SHALL complete within 2 minutes
