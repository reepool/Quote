## 1. Persistent Control And Readiness

- [x] 1.1 Add an atomic business-profile backfill progress and targeted stop-request store under the existing checkpoint root
- [x] 1.2 Expose current-phase readiness separately from daily activation readiness
- [x] 1.3 Add focused tests for atomic snapshots, stale stop isolation, and phase-ready evaluation

## 2. Cooperative Queue Draining

- [x] 2.1 Thread an optional stop callback through backfill worker draining and stop before claiming another concurrent item batch
- [x] 2.2 Preserve one-pass defaults, leases, retries, stage budgets, parallel compute, and cooperative single-writer behavior
- [x] 2.3 Add tests for stop-before-claim and stop-after-in-flight-batch behavior

## 3. Continuous Scheduler And Operations

- [x] 3.1 Add validated continuous-loop parameters, cumulative cycle accounting, interruptible idle waits, phase-complete exit, and blocked/no-progress exit
- [x] 3.2 Add a separate manual status/stop control task and compact operational report
- [x] 3.3 Update scheduler defaults and the production runbook with one-pass, continuous, status, stop, and restart commands
- [x] 3.4 Add scheduler and parameter-parser tests for continuous execution and control actions

## 4. Verification And Delivery

- [x] 4.1 Run focused tests, static/JSON checks, and strict OpenSpec validation
- [x] 4.2 Review all uncommitted changes, fix confirmed findings, then commit and push only this change
