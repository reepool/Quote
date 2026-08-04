## Context

The A-share quote update builds a per-instrument factor window from that instrument's latest local quote and bounded catch-up policy. Phase 2 currently collapses all of those windows into one union of calendar dates, queries Eastmoney's distribution list for symbols over the union, and then calls the Sina cumulative-factor adapter for every matched symbol. The symbol-only result loses the event date that justified selection.

On 2026-08-03, stale or capped instruments widened the union to include 2026-07-29 and 2026-07-30. Seven otherwise current SZSE instruments had events on those dates but individual factor windows beginning on 2026-07-31. Sina correctly returned zero sparse events inside their requested windows, yet Phase 2 classified each empty result as `factor_download_failures`. Quotes were current and the source was reachable, but `a_share_quote_baostock_sina:SZSE` remained at 2026-07-31 and the 03:30 corporate-action workflow correctly deferred Canonical maintenance.

The announcement anomaly trigger has a separate lexical-boundary defect. It recognizes `债转股` as a special restructuring marker through substring matching, so `关于回购股份注销完成调整可转债转股价格的公告` is incorrectly retained even though it only adjusts a convertible-bond conversion price.

The implementation must preserve fail-closed predecessor behavior. A real Sina transport, decode, history-coverage, event-coverage, or persistence failure must still prevent the exchange watermark from advancing.

## Goals / Non-Goals

**Goals:**

- Align discovered ex-dividend events with each instrument's inclusive factor request window before selecting factor work.
- Distinguish out-of-window exclusions from true known-event empty responses and source failures.
- Preserve sufficient bounded diagnostics to explain why an exchange factor stage is `success` or `partial`.
- Exclude convertible-bond conversion-price adjustment notices from XDXR anomaly governance without weakening genuine debt-to-equity restructuring detection.
- Ensure the next successful quote/factor update can advance the exchange predecessor watermark and allow the corporate-action daily Canonical stage to proceed.

**Non-Goals:**

- Change CNInfo, TDX, or BaoStock/Sina raw observations or the three-source Canonical selection policy.
- Treat a source failure as success merely because another factor source has data.
- Backfill or rebuild full-market factors as part of the 03:30 corporate-action task.
- Exclude genuine `债转股`, restructuring, asymmetric distribution, or equity-distribution implementation notices.
- Relax completed-trading-session cutoff rules or advance watermarks during dry runs.

## Decisions

1. **Return dated discovery evidence rather than a bare symbol set.** Ex-dividend discovery will normalize results to a mapping from symbol to one or more event dates. A stock is selected only when its event-date set intersects its own inclusive `[start_date, end_date]` factor window. A compatibility normalizer may accept a legacy set in tests or internal callers, but production discovery must retain dates.

   Alternative considered: query Eastmoney independently for every stock window. This would preserve correctness but multiply external calls and defeat the existing efficient market-wide discovery design.

2. **Keep request windows instrument-specific.** The batch stage will not widen a current stock's Sina factor request merely because another stock requires catch-up. Out-of-window discoveries will increment an exclusion counter and optional bounded sample, but will not be downloaded and will not count as failures.

   Alternative considered: widen every selected stock to the global minimum start date. That would repair the observed symptom but repeatedly download older factor ranges and could hide window construction defects.

3. **Classify empty results using dated evidence.** If an in-window event is known and the factor adapter returns `None`, an empty response, or no event covering the known date after normalization, the instrument remains a real failure. If all discovered dates are outside the instrument window, the instrument is not selected. This maintains the predecessor gate for genuine source publication delays or coverage gaps.

4. **Make diagnostics bounded and machine-readable.** Exchange factor results will include counts such as discovered symbols/events, selected instruments, excluded-out-of-window instruments, synced, skipped, and failed, plus bounded samples with instrument ID, request window, discovered dates, and failure class. Watermark metadata and scheduler output will preserve concise counts; large unbounded lists will remain out of operational state.

5. **Use phrase-level exclusions with positive-pattern precedence.** Titles containing `可转债转股价格`, `调整转股价格`, or equivalent conversion-price adjustment language in the context of repurchase cancellation will be deterministic non-XDXR unless an explicit profit/equity distribution implementation pattern is also present. The actual restructuring phrase `债转股` remains exceptional when it is not part of `可转债转股价格` or equivalent convertible-bond wording.

6. **Do not repair watermarks directly.** Deployment does not manually advance `a_share_quote_baostock_sina:SZSE`. A normal quote/factor producer rerun must demonstrate successful factor processing and advance it through the producer boundary. The subsequent corporate-action daily run then consumes that durable result.

## Risks / Trade-offs

- **[Discovery dates can be malformed or missing]** -> Normalize dates strictly; malformed or missing dates fail closed as discovery uncertainty rather than becoming a false out-of-window exclusion.
- **[Eastmoney and Sina can publish an event on different dates]** -> Keep the existing source-independent factor validation and count an in-window known-event empty result as partial; do not silently shift event dates in this change.
- **[Phrase exclusions can hide mixed-purpose notices]** -> Give genuine distribution implementation patterns precedence and test both positive and negative mixed titles.
- **[Additional diagnostics enlarge watermark metadata]** -> Store only counts and small bounded samples, with no full source payloads.
- **[Existing dirty work overlaps `data_manager.py`]** -> Implementation must isolate only the relevant functions and stage code, preserve unrelated business-profile changes, and stage by exact patch or path only when ownership is unambiguous.

## Migration Plan

1. Deploy the code and restart the application so the quote/factor producer and title classifier load the new policy.
2. Run the normal `daily_data_update` for the latest completed A-share session; verify the SZSE factor stage has no false out-of-window failures and the per-exchange predecessor watermark advances.
3. Run `a_share_cninfo_corporate_action_daily_sync`; verify Canonical is no longer deferred for the old SZSE watermark and `300707.SZ` is absent from unmatched anomaly candidates.
4. Confirm the two 2026-08-04 events (`301297.SZ`, `600675.SH`) are processed only after their completed-session factor cutoff becomes available.
5. Roll back by reverting the code. No schema migration, watermark rewrite, or source-table restoration is required; a stale watermark remains fail-closed until a successful producer run.

## Open Questions

None. The observed log, current source payloads, and stored event windows are sufficient to define the fix.
