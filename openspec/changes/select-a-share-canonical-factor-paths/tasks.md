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
