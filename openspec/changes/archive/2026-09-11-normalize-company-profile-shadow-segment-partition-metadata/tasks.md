## 1. Contract and fixtures

- [x] 1.1 Add the delta specification and bind the immutable September 11, 2026 batch/result hashes as offline-only inputs.
- [x] 1.2 Add provider-free tests for the closed annual duration aliases, narrower/instant period preservation, exact report-segment heading options, and unchanged substantive conflicts.

## 2. Adapter implementation

- [x] 2.1 Implement the closed annual-duration normalization in the existing adapter helper, use it before partition metadata reconciliation, and retain bounded conflicting values in failure diagnostics.
- [x] 2.2 Extend controlled segment dimension options with exact `报告分部...` headings without accepting ambiguous prefixes or weakening source-label validation.

## 3. Offline validation and closeout

- [x] 3.1 Run focused provider tests, Ruff, immutable batch/hash checks, and an offline failure-class audit; do not call the provider or write the historical batch.
- [x] 3.2 Record the repairable versus retained failure classes, keep `not_authorized`, and commit/push only this change's isolated artifacts.
