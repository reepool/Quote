## Context

The repository contains current runbooks, obsolete architecture descriptions, completed change requirements, investigation notes, acceptance replies, and migration records under `docs/`. The current index and development guide still describe an older system. `AGENTS.md`, the framework refactoring program, current code/configuration, and OpenSpec specs have different authority levels and are not consistently linked.

The change is documentation-only. Production code, data, scheduler configuration, public interfaces, and databases are out of scope.

## Goals / Non-Goals

**Goals:**

- Establish one current documentation index and explicit document lifecycle.
- Make current architecture and development entry points match code and configuration.
- Merge same-capability documents without losing valid operational rules.
- Delete historical or superseded documents only after replacement and reference checks.
- Reconcile completed OpenSpec changes with the archive lifecycle.

**Non-Goals:**

- Rewriting every domain runbook in this change.
- Changing production behavior or OpenSpec requirement semantics.
- Preserving every historical document in a permanent archive directory.
- Introducing a documentation database or generated portal.

## Decisions

1. **`docs/README.md` is the only current index.** Individual documents may link to related material, but an unindexed file is not current by default.
2. **Document lifecycle is explicit.** Use `current`, `runbook`, `requirements`, or `historical`; historical material is removed once its valid rules are absorbed by current docs/specs.
3. **Current truth wins.** Code/configuration and OpenSpec specs outrank old Markdown. Conflicts are recorded in the cleanup matrix and resolved before deletion.
4. **Cleanup is manifest-driven.** For each candidate, record replacement document, preserved rules, reference scan result, and deletion decision in the change tasks/evidence. Do not delete by filename pattern alone.
5. **OpenSpec remains the implementation history.** Completed changes are archived through the CLI after verifying no in-progress change depends on their files.

Alternatives rejected: keeping all history under `docs/legacy/` would preserve ambiguity; replacing all docs in one rewrite would risk losing operational knowledge; a generated documentation platform would add infrastructure without a current business need.

## Risks / Trade-offs

- **[Valid runbook is deleted] ->** Require replacement path, command/reference scan, and reviewer sign-off before deletion.
- **[Current code and docs drift again] ->** Add a documentation checklist to OpenSpec completion and require current index updates.
- **[Archiving an active change breaks context] ->** Archive only status-complete changes with no in-progress dependency.
- **[Large merge becomes unreviewable] ->** Consolidate by capability family and keep each change's cleanup matrix bounded.

## Migration Plan

1. Classify all `docs/` files and record candidate families.
2. Add the governance and framework program documents as current authorities.
3. Rewrite `docs/README.md` and the development entry document.
4. Merge one capability family at a time, preserving valid commands and contracts.
5. Delete only candidates whose replacement and reference checks pass.
6. Re-run link/reference scans and OpenSpec status checks.
7. Rollback by restoring the deleted file from Git if a missed dependency is found; do not restore it to the current index without classifying it.

## Open Questions

- Which completed requirements still need a short operator runbook rather than deletion?
- Which OpenSpec complete changes are safe to archive while another in-progress change references their evidence?
