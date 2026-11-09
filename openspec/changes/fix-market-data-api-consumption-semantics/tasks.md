## 1. M0 Baseline And Integration Window

- [ ] 1.1 Before code edits, inspect current branch/worktree and active runtime jobs; map each changed file to the current quote, financial, rate, valuation or PIT owner and check overlap with W4/W5/W9, accelerate-financial-readiness-queries and cut-financial-disclosure-read-amplification.
- [ ] 1.2 Confirm reusable financial scalar-projection and trading-session cutoff/freshness evidence paths; record any implementation adjustment in design without weakening its requirements.
- [ ] 1.3 Capture controlled fixtures for a corporate-action interval, raw TDX turnover placeholder, suspended/low-quality bars, financial summary payload size, rate pagination, relative PE outliers, and PIT quality selection; do not save production databases or full API bodies.

## 2. M1 Quote Correctness Vertical Slice

- [ ] 2.1 Extend the existing AdjustmentEngine projection call so OHLC, pre_close and change share the same actual price multiplier while raw pre_close/change remain available; preserve the canonical factor source and query-window anchor.
- [ ] 2.2 Preserve raw shares and adjusted shares separately in the public projection, make volume a deprecated raw-share alias, keep amount in actual currency, and ensure quote statistics aggregate raw shares.
- [ ] 2.3 Normalize known pytdx unavailable-turnover placeholders to null at the source/read boundary, emit missing reason and bounded public quality, and keep persisted legacy rows unchanged.
- [ ] 2.4 Implement include_suspended and min_quality_score using the public normalized values before pagination and statistics; missing required filter fields SHALL fail explicitly.
- [ ] 2.5 Add quote/latest EOD, bar date, computed time, record timestamps, calendar-backed staleness, authoritative symbol fallback and progress-session batch metadata; preserve the existing latest list response shape.
- [ ] 2.6 Add field-path units and return-basis metadata to JSON, latest items and CSV-supported projection; document qfq single-bar behavior and consistent interval anchors.
- [ ] 2.7 Add focused HTTP and domain tests for qfq/hfq/none over controlled factor events, shared pagination anchors, reference-price arithmetic, invariant raw volume/amount, legitimate versus placeholder turnover zeros, filter ordering, EOD staleness and response serialization.

## 3. M2 Financial Summary And Ratio Projection

- [ ] 3.1 Add summary/full views to the single-period financial endpoint; summary SHALL select existing scalar columns and avoid decoding full facts/indicator/statements JSON, while full SHALL retain the current complete response contract.
- [ ] 3.2 Add explicit canonical field projection for single-period and history responses; preserve required metadata, existing requested_canonical_facts source-layer behavior and remote-extension authorization.
- [ ] 3.3 Expose decimal financial ratios and units without mutating legacy percentage values, current/quick ratio multiples, per-share units or stored panel/indicator rows.
- [ ] 3.4 Expose technical pct_change_ratio as an alias of pct_change_1d and declare adjusted close-to-close return basis.
- [ ] 3.5 Test financial summary below 8192 serialized UTF-8 bytes without full-JSON query/decode, field allowlist behavior, full-view compatibility, history defaults, ratio scaling exactly once and technical unit compatibility.

## 4. M3 Rate Pagination And Provenance

- [ ] 4.1 Implement HTTP-level order, bounded default limit and offset after inclusive date/series filters; return page count, eligible total, boundaries, truncation and continuation offset.
- [ ] 4.2 Persist timezone-aware successful fetch time for non-empty rate syncs through the existing rate write owner; do not advance timestamps on failed/empty runs and preserve unknown legacy times as null.
- [ ] 4.3 Disclose revision_id latest as an overwrite slot; leave DCF and internal unbounded as-of storage reads unchanged.
- [ ] 4.4 Test newest-page default, asc/desc traversal without overlap/gaps, empty and bounded ranges, timestamp success/failure behavior, unknown history and DCF cutoff semantics.

## 5. M4 Research Filters And Error Contract

- [ ] 5.1 Exclude non-finite, missing, zero and negative relative valuation multiples; calculate every benchmark statistic on one filtered set and report raw peer mean/count, exclusions and explicit optional positive max_pe/max_pb limits.
- [ ] 5.2 Add PIT quality whitelist and exclusion controls after strict latest-visible selection and before paging; add whole-selected-cross-section filter_stats without changing existing page-level stats or universe rules.
- [ ] 5.3 Add unknown-query-field rejection derived from actual FastAPI endpoint/dependency parameters for only the scoped read routes; preserve valid aliases, repeated values and dependency parameters.
- [ ] 5.4 Add scoped stable error codes with field/hint and legacy detail, preserving status codes and annual-report/out-of-scope error behavior.
- [ ] 5.5 Add tests proving negative PE exclusion, raw-vs-filtered diagnostics, limit opt-in behavior, PIT no-old-row substitution and page-independent filter_stats, foo rejection, valid query acceptance and existing error shape compatibility.

## 6. M5 API Contract, Regression And Handoff

- [ ] 6.1 Update existing OpenAPI response/parameter contracts, API info version and docs/api/restful_api.md with default-change and client-migration examples for adjusted volume, financial view, rate pagination, EOD timing and explicit units.
- [ ] 6.2 Run focused financial, quote adjustment/filter, latest quote, risk-free-rate, valuation, PIT and API parameter/error tests using QuoteBak for temporary artifacts.
- [ ] 6.3 Run directly relevant API and financial consumer regressions; verify no change to corporate-action factors, PIT availability/effective dates, DCF internal rate semantics or storage format.
- [ ] 6.4 Review the implementation diff against the proposal/specs; fix only blocking issues, verify task-start worktree changes remain untouched and stage only attributable implementation files.
- [ ] 6.5 Remove owned rebuildable temporary artifacts, confirm no new migration or production write path exists, run strict OpenSpec validation, and archive only after implementation acceptance; update program status if any FR/W9 progress is actually completed.
