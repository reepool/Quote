## 1. Dated Ex-Dividend Discovery

- [ ] 1.1 Refactor A-share ex-dividend discovery to retain normalized event dates per symbol while preserving bounded market-wide API calls.
- [ ] 1.2 Add strict handling for missing or invalid discovery dates and a compatibility boundary for any existing internal test doubles or callers.
- [ ] 1.3 Add unit tests for multi-date, cross-year, duplicate-symbol, invalid-date, and empty-success discovery results.

## 2. Per-Instrument Factor Window Alignment

- [ ] 2.1 Select factor-sync instruments only when a discovered event date intersects that instrument's inclusive factor request window.
- [ ] 2.2 Exclude out-of-window symbol matches without calling the factor source or counting them as skipped/failed downloads.
- [ ] 2.3 Keep in-window empty responses, transport/decode failures, truncated history, missing event coverage, and persistence failures actionable and exchange-watermark blocking.
- [ ] 2.4 Add regression tests reproducing the 2026-08-03 SZSE union-window case and confirming the seven 2026-07-29/30 events do not become false failures for 2026-07-31-starting windows.
- [ ] 2.5 Add positive tests proving a genuine in-window empty factor response still produces `factor_download_failures` and prevents watermark advancement.

## 3. Diagnostics and Watermark Semantics

- [ ] 3.1 Add bounded counts and samples for discovered events, selected instruments, out-of-window exclusions, known-event empty responses, and source/persistence failures.
- [ ] 3.2 Propagate concise factor-stage diagnostics into exchange-specific BaoStock/Sina watermark metadata and corporate-action predecessor reporting.
- [ ] 3.3 Add tests proving a clean producer run advances only the completed exchange session and a real factor failure leaves that exchange watermark stale.

## 4. Convertible-Bond Title Classification

- [ ] 4.1 Add phrase-level deterministic exclusion for repurchase-cancellation notices that only adjust convertible-bond conversion prices.
- [ ] 4.2 Preserve exceptional classification for actual debt-to-equity restructuring and positive precedence for genuine equity/profit distribution implementation titles.
- [ ] 4.3 Revalidate persisted unmatched announcements under the new policy without deleting original announcement evidence.
- [ ] 4.4 Add regression tests for `300707.SZ`, representative real `债转股` notices, mixed distribution titles, and persisted-queue cleanup.

## 5. End-to-End Verification and Delivery

- [ ] 5.1 Run focused daily factor sync, watermark, announcement classification, persisted-queue, scheduler report, and corporate-action Canonical closure tests.
- [ ] 5.2 Validate the OpenSpec change and perform an equivalent dry-run diagnostic proving the observed SZSE stage would be successful with no false factor failures.
- [ ] 5.3 Run `codex review --uncommitted`, evaluate findings for real defects versus over-strict suggestions, fix confirmed defects, and rerun focused tests.
- [ ] 5.4 Commit and push only this change's isolated code, tests, and OpenSpec files, leaving all pre-existing business-profile work untouched.
- [ ] 5.5 After deployment, run the normal `daily_data_update` and then `a_share_cninfo_corporate_action_daily_sync`; verify the SZSE predecessor watermark advances, Canonical is no longer blocked by the old cutoff, and `300707.SZ` is absent from the unmatched anomaly queue.
