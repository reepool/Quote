## 1. Frontier-Bound Acquisition

- [x] 1.1 Add repository accessors that load and validate the active frontier row bound to a durable work item.
- [x] 1.2 Convert the bound frontier row to the existing archive candidate contract and archive it without issuer rediscovery.
- [x] 1.3 Gate acquire-stage acknowledgement on a locally usable identity-matching source manifest and preserve retry behavior on failure.

## 2. Defect Recovery

- [x] 2.1 Add an idempotent repository recovery operation for completed latest-annual work lacking a usable bound manifest.
- [x] 2.2 Resolve only stale missing-document machine-rework exceptions after successful bound acquisition.

## 3. Operator Reporting

- [x] 3.1 Separate enqueued work from worker-completed work in backfill task results.
- [x] 3.2 Normalize single-batch snapshots from `latest_result` for queue-health and rollout-readiness reporting.

## 4. Verification And Rollout

- [x] 4.1 Add focused tests for frontier binding, manifest gates, retry behavior, recovery idempotency, exception reconciliation, and truthful reporting.
- [x] 4.2 Run focused tests, compilation, OpenSpec validation, diff checks, and review; fix confirmed findings.
- [x] 4.3 Execute the narrowly scoped production recovery and verify that only positively identified defective work is requeued.
