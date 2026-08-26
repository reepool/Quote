## Context

The current A-share daily path is `scheduler.tasks.daily_data_update` -> `DataManager.update_daily_data` -> `DataSourceFactory.get_daily_data` -> `DatabaseOperations.save_daily_quotes`. The write owner is already single and should remain so. The integrity problems arise from broad success conditions at the boundaries: an empty list is treated as a successful instrument attempt, an existing target-date row is treated as sufficient coverage regardless of row quality, a missing calendar row is indistinguishable from a confirmed closed day, stock routes do not require end-date coverage, and daily writes bypass the historical quality preparation path.

The factor phase separately discovers ex-dividend symbols through a small set of report-period queries. Its failure state is already partial and retryable, but the period set does not cover all quarterly disclosures.

## Goals / Non-Goals

**Goals:**

- Make daily quote success mean that required coverage was either written or explicitly classified as a legitimate no-quote case.
- Re-fetch an existing target-date bar when it is incomplete or fails the daily OHLC quality checks, while preserving idempotent skipping for valid complete bars.
- Treat missing trading-calendar evidence as unknown/failure, not as a confirmed holiday.
- Apply end-date coverage and stale-source protection to A-share stocks using the existing source-factory routing mechanisms.
- Apply the minimum OHLC and completeness validation required before daily upsert.
- Expand ex-dividend discovery to the report periods needed by the requested event window and preserve partial/retry diagnostics.

**Non-Goals:**

- No new parallel daily update loop, write owner, public API, or storage format.
- No global data-quality framework, schema registry, cache platform, or historical database rewrite.
- No change to the bounded catch-up window policy beyond making unresolved coverage visible.
- No claim that every empty stock response is a transport failure; legitimate suspension/no-quote cases remain explicitly classified.

## Decisions

### 1. Keep the existing list-based source contract and add local outcome classification

`DataSourceFactory.get_daily_data` will continue returning `List[Dict]`. For A-share short windows, the configured backup chain will be attempted when the primary returns an empty result unless the instrument has positive evidence for a legitimate no-quote case. If all eligible sources return empty, DataManager will record an unresolved coverage outcome instead of incrementing success. This avoids a cross-cutting result-object migration while preventing empty source responses from advancing success semantics.

Alternative rejected: changing every source adapter to return a new result object. That would expand the change across all markets without being required for the A-share business path.

### 2. Validate existing target-date rows before deciding to skip

The daily loop will obtain the latest row state needed to distinguish a complete, valid target-date bar from an incomplete or invalid one. A valid complete row keeps the current idempotent skip. Otherwise the normal source window is requested and the fetched payload is validated before upsert.

Alternative rejected: always re-fetching every target-date row. That would add unnecessary source load and remove a useful idempotency property.

### 3. Use explicit calendar evidence states

The scheduler will distinguish a calendar row with `is_trading_day=false` from no row. Missing coverage will use the existing DateUtils fallback where it can provide an explicit answer; if neither database nor fallback can establish the state, the exchange will be marked calendar-unknown and the run will not report a successful daily update for that exchange.

Alternative rejected: changing the existing boolean `is_trading_day` contract to a tri-state return. Existing callers rely on the boolean API, so the distinction will be implemented at the scheduler/DB query boundary.

### 4. Reuse narrow quality rules at the daily write boundary

Daily payloads will pass the existing quote validation semantics for required time/OHLCV fields, positive prices, high/low ordering, and completeness. Derived fields and quality metadata will be populated consistently enough for daily records; invalid rows will not be persisted as `is_complete=True`.

Alternative rejected: routing the entire daily update through the historical downloader. That would couple current updates to historical progress state and add unrelated behavior.

### 5. Enable stock coverage checks through existing routing configuration

The stock route configuration will enable `require_end_date_coverage`, and the existing stale-source circuit-breaker path will apply to stocks with an explicit configured threshold. A source that returns only older dates will be rejected and the next source will be tried.

Alternative rejected: adding a separate stock-only source manager. The source factory already owns route ordering, coverage validation, and stale breakers.

### 6. Expand factor discovery periods without changing factor persistence

The daily ex-dividend discovery query will derive report-period anchors from the target years and include the quarterly periods required to cover first-quarter, interim, third-quarter, and annual plans. Partial period failures will continue returning `None`, resulting in `partial` status and retry diagnostics; an empty but complete discovery remains a valid no-event result.

Alternative rejected: making the daily factor phase full-universe on every run. That would increase API load and is not needed when dated discovery evidence is complete.

## Risks / Trade-offs

- [Risk] Legitimately suspended stocks may produce empty responses on a trading day. -> Mitigation: classify empty outcomes separately, use instrument/trading-status evidence where available, and report unresolved coverage without falsely advancing the quote watermark.
- [Risk] Enabling stock end-date checks may increase fallback requests and rate-limit pressure. -> Mitigation: reuse the existing source chain, stale breaker, per-instrument timeout, and bounded windows; expose fallback and unresolved counters.
- [Risk] Re-fetching bad target-date bars can overwrite a prior value. -> Mitigation: retain the existing semantic upsert/changelog path and only replace rows after the same quality checks used for new rows.
- [Risk] More report-period queries can expose AkShare limits. -> Mitigation: deduplicate period anchors per requested year, keep the query window bounded to daily factor targets, and preserve partial failure diagnostics.

## Migration Plan

1. Add focused unit tests for each acceptance scenario before or alongside implementation.
2. Implement DataManager/source-factory/database/scheduler changes behind the existing configuration keys and report structures.
3. Enable stock coverage settings in `config/03_data.json` and the configuration template.
4. Run targeted unit tests and a mocked daily update representative case covering empty, invalid, stale, and valid rows.
5. Roll back by reverting the code/config change; no schema migration or irreversible data operation is required.

## Open Questions

- The exact stale-source consecutive-result threshold for stocks should follow the production rate-limit profile; the implementation should use an explicit config value rather than a hidden hard-coded number.
- Operator-facing report wording may choose `warning` versus `failed` for a legitimate no-quote classification, but neither state may advance a successful-through coverage watermark without evidence.
