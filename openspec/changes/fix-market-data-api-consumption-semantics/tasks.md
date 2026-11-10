## 1. M0 Baseline And Integration Window

- [x] 1.1 Before code edits, inspect current branch/worktree and active runtime jobs; first inventory and safely clean only owned, rebuildable related QuoteBak artifacts after checking references/active use, then map each file to its owner and check overlap with W4/W5/W9 and the two financial performance changes.
- [x] 1.2 Confirm reusable financial scalar-projection and trading-session cutoff/freshness evidence paths; record any implementation adjustment in design without weakening its requirements.
- [x] 1.3 Capture controlled fixtures for a corporate-action interval, raw TDX turnover placeholder, suspended/low-quality bars, financial summary payload size, rate pagination, relative PE outliers, and PIT quality selection; do not save production databases or full API bodies.

## 2. M1 Quote Correctness Vertical Slice

- [x] 2.1 Extend the existing AdjustmentEngine projection call so OHLC/pre_close/change share one multiplier with raw references preserved; keep the canonical source/anchor and reject misspelled daily adjust values instead of silently returning raw success.
- [x] 2.2 Preserve raw shares and adjusted shares separately in the public projection, make volume a deprecated raw-share alias, keep amount in actual currency, and ensure quote statistics aggregate raw shares.
- [x] 2.3 Normalize known pytdx/Tencent unavailable-turnover placeholders to null; apply the same missing reason and bounded quality to new nulls; keep persisted legacy rows unchanged and other providers' legitimate zeros intact.
- [x] 2.4 Implement include_suspended and min_quality_score using the public normalized values before pagination and statistics; missing required filter fields SHALL fail explicitly.
- [x] 2.5 Add quote/latest EOD, bar date, computed time, record timestamps, calendar-backed staleness, authoritative symbol fallback and evidence-based batch kind (progress_session/source_batch/unknown); preserve the existing latest list shape.
- [x] 2.6 Add unit-bearing percentage aliases, deprecation metadata, units and return basis to JSON, latest items and CSV-supported projection; document qfq single-bar behavior and consistent interval anchors.
- [x] 2.7 Add focused HTTP/domain tests for qfq/hfq/none, factor_metadata/409 and technical provenance preservation, shared pagination anchors, close/pre_close return precision, amount/volume_raw raw-price range, invariant raw volume/amount, old/new missing-turnover quality, include_quality=false with thresholds, legitimate provider zeros, filter ordering, EOD staleness, batch classification and serialization.

## 3. M2 Financial Summary And Ratio Projection

- [x] 3.1 Add summary/full views and appropriate response models/OpenAPI branches to the single-period endpoint; summary SHALL read scalar columns without decoding full JSON, while full without fields SHALL retain complete detail.
- [x] 3.2 Add canonical field projection for single-period/history with fields taking precedence over full expansion; preserve metadata and source-layer/remote controls; ensure actual Pydantic HTTP serialization omits unrequested optional keys rather than reinserting defaults.
- [x] 3.3 Expose decimal financial/PIT ratios, PIT percentage aliases and deprecation/units metadata without mutating legacy values, current/quick ratio multiples, per-share units or stored panel/indicator rows.
- [x] 3.4 Expose technical pct_change_ratio as an alias of pct_change_1d and declare adjusted close-to-close return basis.
- [x] 3.5 Test summary below 8192 UTF-8 bytes without full-JSON query/decode, sparse HTTP/OpenAPI key contracts, full-view compatibility, fields/full/raw-statements precedence, history defaults, ratio scaling once, percentage aliases and technical unit compatibility.

## 4. M3 Rate Pagination And Provenance

- [x] 4.1 Implement HTTP order/limit/offset after inclusive date/series/latest-slot filters; return page count, eligible total, boundaries/truncation/continuation with stable same-date source/revision tie ordering.
- [x] 4.2 Update rate response models/OpenAPI for pagination, source_profile, revision_kind, last_observation_date and data_as_of_quality; ensure they survive actual HTTP serialization.
- [x] 4.3 Ensure the static series parent exists before observations (foreign-key constraint), then persist successful fetch time through the existing owner after observation writes; failed/empty runs SHALL preserve the previous successful time and unknown legacy values remain null.
- [x] 4.4 Disclose revision_id latest as an overwrite slot; leave DCF and internal unbounded as-of storage reads unchanged.
- [x] 4.5 Test newest-page default, asc/desc and same-date multi-source traversal, empty/bounded ranges, timestamps after success/failure/empty/partial-write, unknown history and DCF cutoff semantics.

## 5. M4 Research Filters And Error Contract

- [x] 5.1 Exclude invalid observable multiples and invalid denominators only where available; calculate stats on one valid set, report raw peer mean/count, reasons and optional max_pe/max_pb; include effective thresholds in parameter_hash and bump calc_version for the statistical policy change.
- [x] 5.2 Add PIT quality whitelist and exclusion controls after strict latest-visible selection and before paging; add whole-selected-cross-section filter_stats without changing existing page-level stats or universe rules.
- [x] 5.3 Add unknown-query-field rejection derived from actual FastAPI endpoint/dependency parameters for only the scoped read routes; preserve valid aliases, repeated values and dependency parameters.
- [x] 5.4 Add scoped stable codes with field/hint/detail while preserving existing HTTP codes (including manual 400 checks) and annual-report/out-of-scope behavior; define unknown-field validation precedence.
- [x] 5.5 Test negative PE exclusion, raw/filtered diagnostics, threshold opt-in and parameter identity, PIT no-old-row substitution, projection-independent filter_stats, include_snapshot=false rejection and include_local_core=false acceptance, unknown-field vs missing-date precedence, invalid daily adjust 422 vs manual technical adjust 400, valid query acceptance and legacy error shapes.

## 6. M5 API Contract, Regression And Handoff

- [x] 6.1 Update existing OpenAPI response/parameter contracts, API info version and docs/api/restful_api.md with default-change and client-migration examples for adjusted volume, financial view, rate pagination, EOD timing and explicit units.
- [x] 6.2 Run focused financial, quote adjustment/filter, latest quote, risk-free-rate, valuation, PIT and API parameter/error tests using QuoteBak for temporary artifacts.
- [x] 6.3 Run directly relevant API and financial consumer regressions; verify no change to corporate-action factors, PIT availability/effective dates, DCF internal rate semantics or storage format.
- [x] 6.4 Review the implementation diff against the proposal/specs; fix only blocking issues, verify task-start worktree changes remain untouched and stage only attributable implementation files.
- [x] 6.5 Remove owned rebuildable temporary artifacts, confirm no new migration or production write path exists, run strict OpenSpec validation, and archive only after implementation acceptance; update program status if any FR/W9 progress is actually completed.
