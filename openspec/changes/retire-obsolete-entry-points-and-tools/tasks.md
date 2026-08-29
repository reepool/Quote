## 1. Final Inventory

- [ ] 1.1 Confirm W1-W7 are complete and record remaining DataManager, ResearchStorageManager, ScheduledTasks, script, command, and document candidates.
- [ ] 1.2 Classify each candidate as production, operator, migration, compatibility, or obsolete with owner and evidence.
- [ ] 1.3 Scan Python imports, config, scheduler job ids, CLI/Telegram commands, docs, tests, external runbooks, and rollback references.
- [ ] 1.4 Record baseline counts for production script imports, Telegram subprocess production paths, compatibility callers, obsolete candidates, and unarchived complete changes.

## 2. Caller And Production Dependency Migration

- [ ] 2.1 Move still-required production functions out of scripts/dev_validation and convert scripts to authoritative service adapters.
- [ ] 2.2 Migrate remaining facade callers to application services/repositories/task adapters and verify replacement equivalence.
- [ ] 2.2a Publish a replacement map and enable deprecation warnings for compatibility aliases with possible external consumers; record the transition-cycle end date.
- [ ] 2.3 Convert retained manual tools to operator status with current runbooks, bounded inputs, and no copied write logic.
- [ ] 2.4 Preserve migration/recovery tools only until their documented rollback window and acceptance close.

## 3. Deletion

- [ ] 3.1 Delete zero-caller DataManager, ResearchStorageManager, ScheduledTasks, API compatibility, and deprecated backup methods.
- [ ] 3.1a Delete only aliases whose deprecation cycle, replacement map, and external/operator evidence checks have passed.
- [ ] 3.2 Delete superseded root probes, obsolete dev-validation scripts, completed migrations, and duplicate operator entry points.
- [ ] 3.3 Move valuable assertions/fixtures from deleted probes into maintained tests before removal.
- [ ] 3.4 Delete superseded framework audit/program-temporary documents after durable current docs/specs contain required rules.

## 4. OpenSpec And Residue Acceptance

- [ ] 4.1 Archive all status-complete changes whose durable specs are current and whose live artifacts have no active dependency.
- [ ] 4.2 Run full repository reference, import-boundary, command-resolution, documentation-link, and relevant regression checks.
- [ ] 4.3 Recalculate residue counts and require zero or an explicit external blocker for each category.
- [ ] 4.4 Update the framework program final matrix, record rollback commits for deletions, and archive this final change when accepted.
