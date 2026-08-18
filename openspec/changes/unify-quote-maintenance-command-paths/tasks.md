## 1. Production Baseline

- [ ] 1.1 Confirm W2 identity/calendar boundaries are accepted and snapshot quote maintenance jobs, commands, APIs, reports, and active-task guards.
- [ ] 1.2 Map daily, target-date, range, historical, and gap-repair inputs, outputs, write tables, factor follow-up, and failure semantics.
- [ ] 1.3 Inventory all gap implementations, Telegram subprocess paths, and production imports from `scripts`/`scripts.dev_validation`.
- [ ] 1.4 Create temporary-database characterization fixtures for A-share, HKEX, index lifecycle, no-data, skip-list, and persistence-failure cases.

## 2. Command Boundary

- [ ] 2.1 Define validated command/result models for daily, target-date, range, historical, and gap-repair operations.
- [ ] 2.2 Implement shared identity, governed-universe, calendar, source-routing, persistence, and reporting ports without changing provider policy.
- [ ] 2.3 Add application-level single-flight identity for equivalent write scopes and test manual/scheduled collisions.

## 3. Gap Repair Vertical Slice

- [ ] 3.1 Move authoritative gap detection, lifecycle filtering, skip policy, segment merge/fill, and persistence verification into one service.
- [ ] 3.2 Preserve and test quote factor follow-up, no-data recording, HKEX/index guards, dry-run, and structured report fields.
- [ ] 3.3 Bind scheduler and API gap paths to the service and compare temporary-database candidates/writes with the baseline.
- [ ] 3.4 Convert CLI and Telegram gap commands to the same service and remove Telegram subprocess execution.
- [ ] 3.5 Convert `smart_fill_gaps.py` and `find_gap_and_repair.py` to thin operator adapters or delete them when no distinct operator contract remains.

## 4. Remaining Maintenance Modes

- [ ] 4.1 Migrate target-date and range backfill entry points to explicit commands with compatible parameters and reports.
- [ ] 4.2 Migrate daily and historical download orchestration while preserving source, master refresh, resume, and factor semantics.
- [ ] 4.3 Move production-required validation helpers out of scripts and add a static test forbidding production imports from `scripts`.
- [ ] 4.4 Reduce migrated DataManager and scheduler methods to documented delegates and delete duplicate business loops.

## 5. Acceptance

- [ ] 5.1 Run quote maintenance, gap governance, Telegram/API command, factor, and scheduler regression suites.
- [ ] 5.2 Compare representative temporary-database row keys, dates, factors, watermarks, and reports across all entry points.
- [ ] 5.3 Verify automatic job configuration and production database files are unchanged; document rollback binding and remaining delegates.
- [ ] 5.4 Update quote maintenance runbook and mark W3 complete in the framework program.
