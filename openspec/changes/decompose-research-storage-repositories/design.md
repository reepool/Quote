## Context

`ResearchStorageManager` contains 212 methods, multiple database scopes, cross-domain SQL, schema initialization, migrations, and compatibility helpers. Many services import the entire manager even when they use one table family. Existing financial repository code and coordinated SQLite wrappers provide migration patterns that can be reused without merging databases.

## Goals / Non-Goals

**Goals:**

- Split storage by database/table ownership while preserving behavior.
- Keep connection, transaction, WAL, busy-timeout, and attach/scope semantics explicit.
- Allow application services to depend on narrow repositories.
- Make schema/migration ownership reviewable and testable.

**Non-Goals:**

- A universal repository base with generic CRUD.
- A new ORM or database merger.
- Cross-database transactions or a new storage platform.
- Deleting `ResearchStorageManager` in the first migration.

## Decisions

1. **Use `research/repositories/` with one repository per stable owner.** Initial owners are financials, valuation, industry, shareholders, signals/reports, ingestion metadata, and research database coordination.
2. **Keep a shared connection coordinator.** It owns connection lifecycle and database scope; repositories receive a connection/unit of work instead of opening unmanaged connections.
3. **Split schema by owner using existing migrations.** The manager's table initializer becomes explicit idempotent schema modules/migrations, with no destructive table rebuild.
4. **Retain a delegating ResearchStorageManager.** Existing imports continue to work while new services migrate to repositories; delegates carry removal metadata.
5. **Preserve attach and financial scope behavior.** Any cross-database read remains an explicit operation with tests; repository splitting must not create hidden cross-database writes.

Alternatives rejected: merging all tables would change operational failure domains; a new ORM would add risk; copying SQL into repositories without shared transaction ownership would create a second storage implementation.

## Risks / Trade-offs

- **[Transaction boundaries change] ->** Add repository contract tests for commit/rollback and multi-step operations before rebinding callers.
- **[Migration order changes startup behavior] ->** Run clean/migrated database fixtures and keep additive migration order explicit.
- **[Cross-database joins are missed] ->** Inventory `ATTACH`/scope calls and make them named coordinator operations.
- **[Facade remains forever] ->** Track caller counts and require each repository slice to delete real manager methods.

## Migration Plan

1. Inventory methods by database/table owner and connection scope.
2. Extract coordinator and one low-risk repository with equivalence tests.
3. Migrate services domain by domain, starting with read paths.
4. Move schema creation/migration blocks into owned modules.
5. Convert DataManager and scripts to narrow repository consumers.
6. Remove zero-caller manager methods and update exports.
7. Rollback by retaining the facade delegates and old migration entry point until repository equivalence is accepted; never run two writers for the same table.

## Open Questions

- Which existing financial/valuation scope helpers must be public repository contracts?
- Should repository modules own SQL constants directly or use a small per-domain schema module?
