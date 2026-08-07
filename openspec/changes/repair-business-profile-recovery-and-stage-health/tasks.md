## 1. Scope-Safe Recovery

- [x] 1.1 Add complete checkpoint logical-scope inspection and compatibility tests
- [x] 1.2 Restrict stale-scope recovery to the active processing identity and rotate incompatible checkpoints
- [x] 1.3 Supersede obsolete-identity pending and failed work when current replacement work exists

## 2. Queue And Health Semantics

- [x] 2.1 Order stage claims by stage-entry time so freshly recovered work cannot monopolize a bounded batch
- [x] 2.2 Derive daily and backfill status and reason codes from discovery and worker outcomes

## 3. Regression Verification

- [x] 3.1 Add regression coverage for old-model/new-route recovery, mixed pending queues, and truthful degraded reports
- [x] 3.2 Run focused and related business-profile test suites and strict OpenSpec validation
- [x] 3.3 Review all uncommitted changes, fix confirmed findings, and verify only task-owned files are staged
