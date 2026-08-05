## 1. Baseline And Operator Contract

- [ ] 1.1 Record production observation, canonical revision/current, readiness, blocker and watermark counts without writing the database.
- [ ] 1.2 Add fixture-backed tests for deterministic full-scope selection, instrument/event filters, frozen-universe hashes and checkpoint identity validation.
- [ ] 1.3 Define the manual-only task parameters and structured report fields for dry-run, write, batch size, resume, checkpoint id, instrument ids and source event keys.

## 2. Resumable Historical Projection

- [ ] 2.1 Extend the existing projector with deterministic source-event batching while preserving current changed-event callers and projection semantics.
- [ ] 2.2 Implement an append-safe checkpoint store containing normalized parameters, source-universe hash, batch identities, completed batches, counters and latest watermark.
- [ ] 2.3 Implement dry-run planning and projection counters that do not write canonical rows, checkpoints, change records or watermarks.
- [ ] 2.4 Implement resumable write execution that marks only successful batches complete and reports failed or timed-out batches without mutating raw evidence.
- [ ] 2.5 Add tests proving first-write, unchanged rerun, changed-evidence revision, blocked-event preservation, failed-batch resume and no-write dry-run behavior.

## 3. Scheduler And Reporting

- [ ] 3.1 Register a `manual_only` canonical corporate-action history backfill task without a cron trigger or provider acquisition dependency.
- [ ] 3.2 Route Telegram `/run` and direct scheduler invocation through the same bounded executor and parameter validation.
- [ ] 3.3 Add progress logs and final reporting for considered, ready, blocked, blocker reasons, inserted, unchanged, failed batches, checkpoint, database id and watermark.
- [ ] 3.4 Add scheduler/config/report tests proving no automatic execution, no network request and bounded report delivery.

## 4. Temporary Database Acceptance

- [ ] 4.1 Run the complete relevant unit and API contract suites, including PIT, readiness, pagination, watermark and evidence-preservation tests.
- [ ] 4.2 Copy the production quote database to an explicit temporary path and run a full historical dry-run followed by write execution against the copy.
- [ ] 4.3 Audit the temporary result for source-event coverage, ready/blocked distribution, blocker explanations, non-null lineage, decision availability and unchanged raw-evidence counts.
- [ ] 4.4 Rerun the same temporary scope and prove that it creates no duplicate revisions or watermark changes.

## 5. Production Backfill And Closure

- [ ] 5.1 Verify database health and free space, create a recoverable production database backup, and record its path and hash before any canonical write.
- [ ] 5.2 Execute the production historical projection in resumable batches and retain the completed checkpoint and structured report.
- [ ] 5.3 Run post-write integrity checks and representative historical API queries for ready-only, blocked, `known_at`, pagination and change-cursor behavior.
- [ ] 5.4 Rerun the production scope to prove idempotency, update the historical backfill ledger with final counts and unresolved blockers, and keep later roadmap changes unstarted.
