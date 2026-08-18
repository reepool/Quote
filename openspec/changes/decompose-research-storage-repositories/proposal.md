## Why

`ResearchStorageManager` combines more than two hundred methods, multiple database scopes, cross-domain CRUD, and a multi-thousand-line schema initializer. Its broad interface makes every research service depend on storage behavior it does not own and makes safe review unnecessarily difficult.

## What Changes

- Preserve all existing research database files, table names, query results, transactions, and connection coordination.
- Split storage operations into narrow repositories by database and table owner.
- Move schema creation and additive migrations to explicit owners using the existing migration structure.
- Inject narrow repositories into application services while retaining a temporary delegating `ResearchStorageManager`.
- Record and remove compatibility methods after callers migrate.
- Extract only proven common connection policies; do not introduce a universal CRUD base, ORM migration, or cross-database transaction framework.

## Capabilities

### New Capabilities

- `research-storage-repository-boundaries`: Defines repository ownership, connection and transaction invariants, schema ownership, compatibility delegation, and migration acceptance.

### Modified Capabilities

None.

## Impact

- Affects `research/storage.py`, `research/migrations/`, research services, scripts that construct storage, and storage contract tests.
- Depends on W4 application-service boundaries so consumers can migrate to narrow interfaces.
- Implements W5, FR-07, and FR-11 while preserving database layout and production data.
