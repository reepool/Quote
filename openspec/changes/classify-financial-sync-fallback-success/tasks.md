## 1. Source-aware result model

- [x] 1.1 Add final source classification and completed-target source counts to the incremental sync result and persisted run metadata.
- [x] 1.2 Update status derivation so routing diagnostics alone do not downgrade a run whose final targets are complete, while unresolved fallback failures still do.

## 2. Operator reporting

- [x] 2.1 Update the scheduler financial report to display completion status separately from final source classification and identify non-CNInfo fallback collection.
- [x] 2.2 Preserve CNInfo health and fallback warning details in the report without presenting successful fallback collection as a failed task.

## 3. Verification

- [x] 3.1 Add unit tests for CNInfo-only, fallback-only, mixed-source, and unresolved-source status/classification cases.
- [x] 3.2 Run the focused financial disclosure and scheduler report test suites, then perform a diff review for the change.
