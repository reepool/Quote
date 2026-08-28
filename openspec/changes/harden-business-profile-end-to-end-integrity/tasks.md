# Tasks

## Persistence and temporal identity

- [x] Fix activity and relationship producer payloads to nest repository-unsupported identity fields in metadata.
- [x] Fix temporal version identity extraction and empty-pointer handling in governance.
- [x] Add end-to-end persistence and conflict regression tests for activities and relationships.

## Business fact retention and publication

- [x] Preserve unresolved named counterparties with explicit resolution status and raw evidence.
- [x] Close stale catalog-pending exceptions when the same semantic relationship later resolves.
- [x] Thread the runtime publication manifest through commodity exposure publication and fail closed on missing/failed gates.
- [x] Protect business-profile history with trusted diagnostic authorization and add API regression tests.

## Units and verification

- [x] Prevent unknown produces dimensions from becoming production-volume facts.
- [x] Preserve activity period basis through conversion, with explicit unknown fallback.
- [x] Run targeted tests, review the complete diff, and verify no pre-existing worktree changes were touched.
