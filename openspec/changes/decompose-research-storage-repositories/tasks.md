## 1. Storage Inventory

- [ ] 1.1 Confirm accepted W4 service boundaries and map every ResearchStorageManager method to database, table owner, callers, transaction scope, and migration block.
- [ ] 1.2 Inventory coordinated connection, WAL, busy-timeout, read-only, financial scope, valuation scope, and ATTACH behavior.
- [ ] 1.3 Build clean, migrated, rollback, and concurrent-access temporary-database fixtures for each configured research store.
- [ ] 1.4 Define repository protocols required by current application services without adding unused generic methods.

## 2. Connection And Schema Ownership

- [ ] 2.1 Extract the existing coordinated connection/database-scope behavior into an injectable storage coordinator with equivalence tests.
- [ ] 2.2 Split idempotent schema creation and additive migrations by database/table owner using the current migration structure.
- [ ] 2.3 Verify startup initialization order, clean database creation, existing database migration, and rollback behavior.

## 3. Repository Vertical Slices

- [ ] 3.1 Extract ingestion metadata and one low-risk read repository as the pattern slice; migrate callers and remove duplicate manager SQL.
- [ ] 3.2 Extract industry and shareholder repositories with business-key, history, and transaction equivalence tests.
- [ ] 3.3 Extract valuation repository while preserving compact payload, database scope, and history semantics.
- [ ] 3.4 Extract financial repositories while preserving `financials.db`, source manifests, facts, mappings, and coverage queries.
- [ ] 3.5 Extract reports, sentiment, risk, technical, profile, and risk-free-rate repositories in owner-bounded slices.

## 4. Compatibility And Consumers

- [ ] 4.1 Convert ResearchStorageManager methods for migrated slices to delegates with recorded callers and removal conditions.
- [ ] 4.2 Inject narrow repositories into W4 application services, API reads, scheduler adapters, and retained operator scripts.
- [ ] 4.3 Remove zero-caller manager methods and verify no old/new repository pair writes the same table independently.

## 5. Acceptance

- [ ] 5.1 Run repository, migration, research-domain, API, DCF, and concurrency regression suites against temporary databases.
- [ ] 5.2 Compare schema objects, row keys, query projections, commits/rollbacks, watermarks, and configured database paths with the baseline.
- [ ] 5.3 Review extracted shared code and remove any abstraction that does not delete real duplication for at least two current repositories.
- [ ] 5.4 Update storage architecture and mark W5 complete/remaining delegates in the framework program.
