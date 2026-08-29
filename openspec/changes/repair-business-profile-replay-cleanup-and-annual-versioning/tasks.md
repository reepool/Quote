## 1. Baseline and audit

- [ ] 1.1 Add a read-only audit command/report that inventories semantic receipt latest status, work-item status, candidate descendants, approved history, and occurrence collisions by instrument/report/source.
- [ ] 1.2 Add runtime/source-revision smoke verification so the running application reports the code identity used for a backfill.
- [ ] 1.3 Add fixtures from the production-shaped 002496.SZ activity replay and 300750.SZ anonymous relationship response.

## 2. Stable fact occurrence identity

- [ ] 2.1 Implement one normalized occurrence-material builder for activities, operating facts, and relationships using report period, source revision, page/evidence, source row or contract, subject scope, and object.
- [ ] 2.2 Make semantic conversion and governance temporal identity consume the same occurrence material and fail closed when required row/contract identity is absent.
- [ ] 2.3 Ensure `reuse` reuses an identical occurrence, `replace` creates an explicit successor, and different report periods/contracts/rows remain independent.
- [ ] 2.4 Add regression tests for approved-history plus force/reuse replay, same-row idempotence, distinct contract rows, new annual report, and corrected annual report.

## 3. Anonymous relationship semantics

- [ ] 3.1 Extend the normalized relationship contract with an explicit ordinary-versus-concentration discriminator derived from the model response and source label.
- [ ] 3.2 Accept masked ordinary contract counterparties without `disclosed_share`; require finite `disclosed_share` only for top-five concentration facts.
- [ ] 3.3 Add regression tests covering `客户 A(1)`, `前五名客户`, and `前五名供应商` in one response, including mixed valid and invalid rows.

## 4. Error taxonomy and retry behavior

- [ ] 4.1 Preserve stable reason codes and retryability from semantic conversion through runtime quality payload and worker queue.
- [ ] 4.2 Remove string-based wrapping that turns deterministic business/schema/evidence failures into `gateway_failure`.
- [ ] 4.3 Ensure only provider rate-limit, timeout, and transport failures receive gateway backoff; deterministic failures become machine rework without another LLM call.
- [ ] 4.4 Add tests for anonymous validation, duplicate/missing verification IDs, evidence failures, unit failures, and real gateway failures with distinct queue dispositions.

## 5. Historical cleanup and lifecycle migration

- [ ] 5.1 Define and implement the cleanup classification for rejected receipts, non-reusable conversion pending receipts, legacy identities, failed work items, and candidate descendants.
- [ ] 5.2 Execute cleanup transactionally for the selected production scope, preserving approved records, evidence, and review audit; emit a deletion manifest with counts and IDs.
- [ ] 5.3 Make replay lookup exclude deleted/unusable receipts and prevent old failed outputs from re-entering `reuse`.
- [ ] 5.4 Add an explicit pre-batch gate that blocks broad backfill while identity collisions or unusable receipts remain unresolved.

## 6. End-to-end acceptance

- [ ] 6.1 Deploy code and verify runtime source revision before invoking LLM work.
- [ ] 6.2 Run targeted no-broad-retry replays for 002496.SZ and 300750.SZ and confirm zero temporal collision, zero anonymous-contract misclassification, and zero false gateway retry.
- [ ] 6.3 Validate one new annual report and one same-period correction report for append/successor behavior and approved-history preservation.
- [ ] 6.4 Run the 11-instrument batch only after all gates pass; confirm no terminal failures, no stale failed receipt reuse, and expected publication/reporting status.
- [ ] 6.5 Review the full diff and focused test suite, then document remaining non-blocking backlog separately from acceptance failures.
