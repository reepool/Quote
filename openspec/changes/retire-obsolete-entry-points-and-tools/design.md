## Context

The repository contains root-level live probes, completed migration scripts, dev-validation modules imported by production, old backup methods, and compatibility facades with many callers. Some are useful operator tools, so deletion must be evidence-based rather than based on age or file size.

## Goals / Non-Goals

**Goals:**

- Establish a complete caller and lifecycle inventory.
- Migrate callers to authoritative application services.
- Delete zero-caller obsolete code and documents.
- Keep necessary operator/recovery tools explicit and documented.
- Archive completed OpenSpec changes after dependency checks.

**Non-Goals:**

- Removing active production, recovery, or in-progress migration capability.
- Creating a permanent legacy directory.
- Refactoring unrelated business behavior while deleting residue.

## Decisions

1. **Use five lifecycle states.** `production`, `operator`, `migration`, `compatibility`, and `obsolete` are recorded for every candidate.
2. **Use evidence before deletion.** Search imports, config, job ids, CLI/Telegram commands, docs, tests, and rollback references; require replacement equivalence.
3. **Delete true obsolete code.** Git history is the archive; retained operational tools remain in clear directories with runbooks.
4. **Remove production script dependencies first.** Functions still needed by production move to formal modules before scripts are classified.
5. **Run cleanup after W1-W7.** Deletion is the final step after canonical paths and service owners are stable.
6. **External consumers get a bounded transition.** Compatibility aliases emit a deprecation warning for one documented release/maintenance cycle, publish the replacement map, then become eligible for deletion after repository and operator evidence is rechecked.

Alternatives rejected: keeping every old file in `legacy/` perpetuates ambiguity; deleting by naming convention risks recovery loss; a generic deprecation framework is unnecessary.

## Risks / Trade-offs

- **[External operator has an undocumented command] ->** Scan docs/logs/config and require a replacement command/runbook before deletion.
- **[Rollback requires a migration tool] ->** Mark it `migration` until the rollback window closes and test restoration.
- **[Deleting tests hides a regression] ->** Move valuable fixtures/assertions into maintained suites before removal.
- **[OpenSpec archive loses active context] ->** Verify status and cross-change references before archive.

## Migration Plan

1. Generate lifecycle and caller inventory.
2. Complete caller migration for each compatibility facade.
3. Move production-needed script logic to formal modules.
4. Delete root probes, obsolete scripts, aliases, and superseded docs with evidence.
5. Archive complete OpenSpec changes and update program matrix.
6. Rollback by restoring deleted files from Git only when a documented replacement is insufficient; do not reintroduce them to the current index without a new lifecycle state.

## Open Questions

- Which manual-only validation scripts are still required for operator release acceptance?
- What retention period is required for one-time migration and recovery tools?
