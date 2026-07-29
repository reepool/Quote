## 1. Shared Adaptive Throttle

- [x] 1.1 Implement validated adaptive policy, source throttle state, request admission, response feedback, snapshots, and source-keyed registry.
- [x] 1.2 Add deterministic unit tests for interval growth, staged cooldowns, rolling density, Retry-After, jitter, gradual recovery, policy validation, and source isolation.

## 2. CNInfo Integration

- [x] 2.1 Integrate the AkShare-backed CNInfo corporate-action request proxy with shared admission and response feedback while preserving endpoint retries and coverage semantics.
- [x] 2.2 Integrate CNInfo announcement metadata and stock-identity POST requests with the same shared source key.
- [x] 2.3 Add focused CNInfo tests for 403/429 feedback, Retry-After handling, successful responses, and shared state reuse.

## 3. Validation And Delivery

- [x] 3.1 Run focused unit tests, syntax checks, diff checks, and strict OpenSpec validation.
- [x] 3.2 Review all task changes, fix confirmed defects without expanding into unrelated provider migration, and rerun affected tests.
- [x] 3.3 Commit and push only the files changed for this adaptive-throttling task.
