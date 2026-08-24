## 1. API Contract

- [x] 1.1 Add explicit company-specific profile and measurement-contract response models with backward-compatible extension handling.
- [x] 1.2 Project deterministic operating-fact linkage telemetry from both ready and empty business-profile responses.
- [x] 1.3 Add focused resolver, route-model, and OpenAPI regression tests for linked, partially linked, unlinked, and empty profiles.

## 2. Production Replay

- [x] 2.1 Confirm no competing business-profile writer is active and replay `601088.SH` through the authoritative expanded backfill entry point.
- [x] 2.2 Verify `601088.SH` is ready, has linked operating facts, retains roles/relationships/exposures, and has no duplicate logical facts.

## 3. Operations And Closure

- [x] 3.1 Verify and document the configured annual automatic update, correction supersession, queue stages, retry/resume behavior, and manual repair trigger.
- [x] 3.2 Run targeted validation and blocking-defect review, then commit and push only this change's files.
