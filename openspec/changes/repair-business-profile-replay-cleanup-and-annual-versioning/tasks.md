## 1. Baseline and audit

- [x] 1.1 Add a read-only audit command/report that inventories semantic receipt latest status, work-item status, candidate descendants, approved history, and occurrence collisions by instrument/report/source.
- [x] 1.2 Add runtime/source-revision smoke verification so the running application reports the code identity used for a backfill.
- [x] 1.3 Add fixtures from the production-shaped 002496.SZ activity replay and 300750.SZ anonymous relationship response.

## 2. Stable fact occurrence identity

- [x] 2.1 Implement one normalized occurrence-material builder for activities, operating facts, and relationships using report period, source revision, page/evidence, source row or contract, subject scope, and object.
- [x] 2.2 Make semantic conversion and governance temporal identity consume the same occurrence material and fail closed when required row/contract identity is absent.
- [x] 2.3 Ensure `reuse` reuses an identical occurrence, `replace` creates an explicit successor, and different report periods/contracts/rows remain independent.
- [x] 2.4 Add regression tests for approved-history plus force/reuse replay, same-row idempotence, distinct contract rows, new annual report, and corrected annual report.

## 3. Anonymous relationship semantics

- [x] 3.1 Extend the normalized relationship contract with an explicit ordinary-versus-concentration discriminator derived from the model response and source label.
- [x] 3.2 Accept masked ordinary contract counterparties without `disclosed_share`; require finite `disclosed_share` only for top-five concentration facts.
- [x] 3.3 Add regression tests covering `客户 A(1)`, `前五名客户`, and `前五名供应商` in one response, including mixed valid and invalid rows.

## 4. Error taxonomy and retry behavior

- [x] 4.1 Preserve stable reason codes and retryability from semantic conversion through runtime quality payload and worker queue.
- [x] 4.2 Remove string-based wrapping that turns deterministic business/schema/evidence failures into `gateway_failure`.
- [x] 4.3 Ensure only provider rate-limit, timeout, and transport failures receive gateway backoff; deterministic failures become machine rework without another LLM call.
- [x] 4.4 Add tests for anonymous validation, duplicate/missing verification IDs, evidence failures, unit failures, and real gateway failures with distinct queue dispositions.

## 5. Historical cleanup and lifecycle migration

- [x] 5.1 Define and implement the cleanup classification for rejected receipts, non-reusable conversion pending receipts, legacy identities, failed work items, and candidate descendants.
- [x] 5.2 Execute cleanup transactionally for the selected production scope, preserving approved records, evidence, and review audit; emit a deletion manifest with counts and IDs.
- [x] 5.3 Make replay lookup exclude deleted/unusable receipts and prevent old failed outputs from re-entering `reuse`.
- [x] 5.4 Add an explicit pre-batch gate that blocks broad backfill while identity collisions or unusable receipts remain unresolved.
- [x] 5.5 Make repair converge after legacy machine-approved role duplication by retaining one current role, auditing duplicate rows out of current reads, and ignoring already-held history.
- [x] 5.6 Isolate replay execution state by rotating orphan/stale checkpoints at enqueue and restricting targeted backfill workers to the current invocation's work IDs.
- [x] 5.7 Replace checkpoint quarantine-by-rotation with owned-file deletion; remove retired shadow lifecycle rows, superseded runs/work, and orphan checkpoints while preserving approved records, evidence, review audit, active current work, and current receipts.

## 6. End-to-end acceptance

- [x] 6.1 Deploy code and verify runtime source revision before invoking LLM work.
- [ ] 6.2 Run targeted no-broad-retry replays for 002496.SZ and 300750.SZ and confirm zero temporal collision, zero anonymous-contract misclassification, and zero false gateway retry.
- [ ] 6.3 Validate one new annual report and one same-period correction report for append/successor behavior and approved-history preservation.
- [ ] 6.4 Run the 11-instrument batch only after all gates pass; confirm no terminal failures, no stale failed receipt reuse, and expected publication/reporting status.
- [x] 6.5 Review the full diff and focused test suite, then document remaining non-blocking backlog separately from acceptance failures.
