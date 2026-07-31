## 1. AkShare Provider Path

- [x] 1.1 Implement pure aligned-price ratio stabilization and sparse factor-event extraction
- [x] 1.2 Implement Tencent-first and Eastmoney-fallback AkShare A-share factor acquisition with explicit provider profiles
- [x] 1.3 Add provider routing, rounding-noise, structural-failure, and fallback unit tests

## 2. Three-Source Selection

- [x] 2.1 Implement continuity-segment construction from non-continuous lineage transitions
- [x] 2.2 Implement pairwise event-jump and normalized cumulative-path agreement scoring
- [x] 2.3 Implement deterministic CNInfo/TDX/AkShare segment selection and canonical row construction
- [x] 2.4 Add symmetric consensus, special-action, all-conflict, incomplete-CNInfo, and lineage-boundary tests

## 3. Rebuild Integration

- [x] 3.1 Extend the CNInfo factor rebuild with an explicit three-source selection mode and AkShare provider-profile loading
- [x] 3.2 Derive governed special-action dates and lineage segment boundaries without changing source rows
- [x] 3.3 Persist staging candidate and full selection audit evidence with complete write gates
- [x] 3.4 Add DataManager integration tests for dry-run isolation, staging writes, and blocked candidates

## 4. Operator Workflow

- [x] 4.1 Add a manual-only dry-run-first scheduler workflow coordinating optional resumable AkShare backfill and three-source selection
- [x] 4.2 Add task configuration, parameter delegation, bounded reports, and scheduler tests
- [x] 4.3 Document targeted pilot, full-market preview, staging build, interpretation, and rollback commands

## 5. Verification

- [x] 5.1 Run focused provider, selector, DataManager, scheduler, and database regression tests
- [x] 5.2 Run Python compile, diff checks, strict OpenSpec validation, and review all uncommitted changes

## 6. Restore Direct Sina Factor Source

- [x] 6.1 Revise proposal, design, and delta specs from Tencent/Eastmoney price ratios to direct Sina `hfq-factor`
- [x] 6.2 Restore direct Sina A-share factor acquisition with sparse incremental extraction and explicit source profile
- [x] 6.3 Persist and load atomic complete Sina snapshots for three-source selection
- [x] 6.4 Rewire canonical selection, scheduler parameters, and reports to CNInfo/TDX/Sina
- [x] 6.5 Remove Tencent/Eastmoney price-ratio code, configuration, tests, documentation, and exact runtime artifacts
- [x] 6.6 Add and update focused unit, integration, scheduler, and persistence tests
- [x] 6.7 Run regression tests, Python compile, strict OpenSpec validation, and uncommitted-change review

## 7. Use Existing Legacy Composite Third Source

- [x] 7.1 Revise proposal, design, and delta specs from an independent Sina snapshot to the existing BaoStock-plus-Sina legacy composite path
- [x] 7.2 Rewire three-source selection and source coverage from Sina snapshots to local legacy rows without provider downloads
- [x] 7.3 Separate CNInfo and TDX factor-path eligibility from recent endpoint audit intervals
- [x] 7.4 Add exact/shifted event reconciliation counts and factor-difference buckets to bounded selection reports
- [x] 7.5 Remove canonical-task Sina backfill parameters and update configuration, documentation, and operator commands
- [x] 7.6 Add and update selector, DataManager, scheduler, and regression tests
- [x] 7.7 Run focused tests, Python compile, strict OpenSpec validation, uncommitted-change review, commit, and push

## 8. Full-Market Preview Hardening

- [x] 8.1 Bound continuity segments and zero-event coverage to listed/delisted lifecycles
- [x] 8.2 Add guarded low-confidence TDX fallback for completed historical lifecycles only
- [x] 8.3 Change canonical-selection default factor tolerance to 0.1% without overriding explicit values
- [x] 8.4 Move BaoStock quota and session-lock defaults to writable project runtime storage
- [x] 8.5 Add selector, DataManager, scheduler, configuration, and BaoStock regression tests
- [x] 8.6 Run focused tests, full-market dry-run validation, strict OpenSpec validation, and uncommitted review

## 9. Reviewed Historical Source Decisions

- [x] 9.1 Extend proposal, design, and delta specs for reviewed whole-lifecycle overrides, completed-lifecycle TDX conflict fallback, BaoStock-Sina naming, and blocked-first reporting
- [x] 9.2 Allow complete TDX paths to resolve ended lifecycles whose CNInfo path is empty even when the BaoStock-Sina composite conflicts
- [x] 9.3 Add and apply a strictly validated reviewed whole-lifecycle source-override catalog for `000004.SZ` and `600455.SH`
- [x] 9.4 Use `BaoStock_Sina composite` in operator-facing and persisted selection provenance and emit independent bounded blocked decisions
- [x] 9.5 Add selector, catalog, DataManager, and scheduler report regression tests
- [x] 9.6 Run focused tests, Python compile, strict OpenSpec validation, uncommitted review, commit, and push
