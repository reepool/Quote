## 1. Publication State And Final Verification

- [ ] 1.1 Add exchange-cutoff-aware provisional/final metadata to normalized futures series and contract bars, including acquisition time, cutoff context, and final verification evidence.
- [ ] 1.2 Extend the existing upsert paths to persist a newly established post-cutoff final verification when price values are semantically unchanged without emitting a semantic price-change record.
- [ ] 1.3 Conservatively classify recent legacy same-day `partial` rows acquired before cutoff as provisional during reconciliation without bulk deleting or rewriting historical rows.

## 2. Business-Semantic Write Accounting

- [ ] 2.1 Classify each series and contract transition before source supersession as new business-date coverage, source upgrade, same-source correction, post-cutoff verified unchanged, or unchanged.
- [ ] 2.2 Return per-row and unique-date semantic counters alongside the existing inserted, changed, unchanged, and changelog counters.
- [ ] 2.3 Preserve fallback delete-marker and official insert audit records while preventing source upgrades from being reported as newly covered dates.

## 3. Reconciliation Completeness And Reporting

- [ ] 3.1 Include publication-eligible provisional dates in the existing bounded daily provider target set even when persisted date coverage exists.
- [ ] 3.2 Extend exchange completeness so finalized latest date, stale provisional dates, current-run finalized dates, and post-cutoff provider blockers determine success or partial/blocked status.
- [ ] 3.3 Update scheduled and manual futures reports to show new business dates, source upgrades, corrections, verified unchanged rows, provisional dates, and finalized coverage by exchange.
- [ ] 3.4 Retain backward-compatible aggregate result fields and ensure report delivery cannot promote a failed provisional reconciliation to success.

## 4. Focused Verification

- [ ] 4.1 Add unit tests for pre-cutoff provisional rows followed by changed final data, identical post-cutoff verification, and failed post-cutoff reconciliation.
- [ ] 4.2 Add storage tests proving fallback-to-official replacement is a source upgrade, not new date coverage, while audit delete/insert records remain intact.
- [ ] 4.3 Add completeness and scheduler/manual report tests proving stale provisional presence cannot satisfy success after cutoff but does not block before cutoff.
- [ ] 4.4 Validate the 2026-08-14 INE/SHFE case or an equivalent fixture through the 21:30 target path, confirming final rows overwrite/verify provisional values without destructive cleanup.
