## 1. Canonical Decision Storage And Migration

- [x] 1.1 Add a normalized canonical selection-decision model, schema initialization, indexes, and bounded serialization helpers.
- [x] 1.2 Add lightweight series-status, per-instrument decision, paged decision, and compact quality database accessors.
- [x] 1.3 Persist decisions separately on candidate build, promotion, and targeted merge while keeping report JSON compact.
- [x] 1.4 Implement an idempotent preview/apply migration that verifies existing JSON decisions before compacting retained reports.

## 2. Production Read And Activation Safety

- [x] 2.1 Change quote factor loading to use lightweight series status and per-instrument decisions without caching full-market reports.
- [x] 2.2 Resolve canonical factor and quality API defaults from active governance and add bounded decision paging.
- [x] 2.3 Align tracked configuration defaults and templates with `canonical / a_share_cninfo_primary_v1` and surface invalid activation without silent fallback.
- [x] 2.4 Isolate activation state in unit tests and restore the daily maintenance suite under promoted production defaults.

## 3. Selection Quality And Summary Consistency

- [x] 3.1 Rename legacy completeness concepts to BaoStock/Sina composite path eligibility and emit explicit path-integrity diagnostics with `event_completeness=not_asserted`.
- [x] 3.2 Centralize canonical decision and coverage summary computation for full builds, promotion, and targeted merge.
- [x] 3.3 Correct coverage, completeness, blocked, low-confidence, historical-single-source, agreement, and reconciliation fields after every write path.
- [x] 3.4 Add regression tests for invalid composite paths, valid corroboration, full summary construction, and targeted summary recomputation.

## 4. Retention And Daily Dependency Governance

- [x] 4.1 Add protected retention preview/apply operations for obsolete staging, benchmark, decision, instrument-status, and endpoint-status records.
- [x] 4.2 Add a manual scheduler task and bounded report formatter for retention, defaulting to dry-run and requiring explicit confirmation for deletion.
- [x] 4.3 Gate promoted canonical daily merge on an explicit successful quote/composite predecessor watermark while preserving resumable CNInfo observations.
- [x] 4.4 Add tests for protected versions, retention confirmation, endpoint-status compaction, predecessor success, and predecessor deferral.

## 5. Operational Consolidation

- [x] 5.1 Disable and document the obsolete AkShare-era rebuild entry point while preserving an explicit compatibility error for direct callers.
- [x] 5.2 Classify fixed-event governance scripts and reusable diagnostics in a lifecycle README and remove directly related unused code.
- [x] 5.3 Align canonical operations, adjustment-factor governance, API, configuration, and scheduler documentation and terminology.
- [x] 5.4 Close verified residual CNInfo async/title OpenSpec rollout tasks with evidence notes and validate all affected OpenSpec changes strictly.

## 6. Verification And Delivery

- [x] 6.1 Run focused database, selection, promotion, API, scheduler, daily maintenance, activation, and retention tests.
- [x] 6.2 Verify on a database copy that decision migration preserves counts, compact quality reads remain bounded, and no source or canonical factor rows change.
- [x] 6.3 Run `codex review --uncommitted`, evaluate findings for scope and correctness, fix confirmed defects, and rerun verification.
- [x] 6.4 Commit and push only this change's code, tests, configuration, OpenSpec, and documentation updates.
