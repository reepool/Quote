## 1. Official Calendar Evidence And Publication Timing

- [ ] 1.1 Add validated per-exchange publication timezone/cutoff settings and a daily repair lookback constrained to three through five natural dates in the existing futures configuration loader.
- [ ] 1.2 Update the official futures calendar provider classification so parseable rows prove trading, only weekend or date-specific official closure evidence proves closed, and generic empty/no-data/anti-bot responses remain unresolved.
- [ ] 1.3 Make pre-cutoff current-date empty responses `not_yet_due` and post-cutoff unresolved responses explicit source/data-quality blockers, with the cutoff and request outcome retained in diagnostics.
- [ ] 1.4 Re-probe missing, unresolved, and weak `official_empty_payload` rows in the bounded daily window and allow later positive official rows to repair their stored status and evidence metadata.

## 2. Publication-Aware Target Dates And Gap Repair

- [ ] 2.1 Extend futures trading-day governance to compute each exchange's publication-eligible as-of date and expected latest verified trading date using an injected run clock.
- [ ] 2.2 Merge governed dates lacking persisted exchange/date price coverage into the bounded daily target set, including dates repaired from weakly closed to verified trading.
- [ ] 2.3 Preserve explicit `start/end` inclusive ranges and existing lifecycle exclusions while exposing unresolved recent weekdays as blockers rather than calendar skips.
- [ ] 2.4 Return per-exchange requested range, cutoff, publication as-of date, governed targets, expected latest date, uncovered dates, repaired dates, and unresolved blockers from governance.

## 3. Completeness Status And Operator Reporting

- [ ] 3.1 Add the post-fetch exchange-level completeness check against persisted bars for every required governed target date and expected latest date in the resolved task scope.
- [ ] 3.2 Derive the overall ingestion result from exchange outcomes: success only for complete exchanges, partial for useful work with remaining stale/gapped exchanges, and blocked when calendar governance prevents production work.
- [ ] 3.3 Persist and return actual latest price date, remaining missing dates, repaired dates, lifecycle skips, and blockers for each exchange without treating unchanged existing rows as missing.
- [ ] 3.4 Update scheduled and manual `/run futures_market_data_sync` orchestration and Telegram formatting to display per-exchange target/freshness diagnostics and preserve non-success dry-run or governance outcomes.

## 4. Focused Verification And Recovery

- [ ] 4.1 Add provider/calendar unit tests for weekday empty payloads, explicit closure evidence, pre-cutoff and post-cutoff behavior, and positive evidence repairing `official_empty_payload` rows.
- [ ] 4.2 Add governance tests for exchange-local cutoffs, weekend/holiday resolution, three-to-five-day rolling repair, internal uncovered dates, explicit bounded ranges, and unresolved blockers.
- [ ] 4.3 Add futures sync and scheduler tests proving stale or internally gapped exchanges report partial/non-success, complete unchanged data can report success, and lifecycle skips do not create false blockers.
- [ ] 4.4 Run a representative DCE recovery validation covering empty response, later official rows, calendar correction, 2026-08-12/2026-08-13 price repair, and truthful per-exchange report output.
