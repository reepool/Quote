## 1. Contract and fixture baseline

- [x] 1.1 Freeze the BaoSteel out-of-sample findings, historical authority hashes, and the four semantic targets without changing any existing bundle.
- [x] 1.2 Add offline fixtures for distinct utilization rows, supplier/customer field separation, related-party scope mismatch, and unsupported adjustment subject promotion.
- [x] 1.3 Add focused tests that assert source-native values, Evidence bindings, field IDs, row identity, and subject restrictions.

## 2. Local implementation

- [x] 2.1 Correct occurrence identity to retain measured-object/source-row distinctions without weakening same-occurrence conflict detection.
- [x] 2.2 Correct supplier field binding and keep related-party transaction candidates outside concentration contracts.
- [x] 2.3 Prevent unsupported `consolidated_group` promotion for adjustment rows while preserving row class and values.

## 3. Verification and bounded validation

- [x] 3.1 Run focused Stage 5 tests, relevant acceptance/projection regressions, Ruff, and strict OpenSpec validation.
- [x] 3.2 Confirm historical authority and BaoSteel optimized-run hashes remain unchanged and no production path is reachable.
- [x] 3.3 After all offline checks pass, execute at most one new BaoSteel complete run with a fresh run ID; otherwise retain the typed finding without retry loops.
- [x] 3.4 Record the new run's call success, semantic caveats, report status, research usability, and `production_authorization=not_authorized` without splicing historical output.

## 4. Closure

- [x] 4.1 Review scoped changes, mark the change complete only after the bounded verification is honest, then commit and push only this change.
