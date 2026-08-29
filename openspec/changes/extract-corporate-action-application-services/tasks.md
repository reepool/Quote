## 1. Lifecycle Baseline

- [ ] 1.1 Confirm W2 identity boundaries and map observation, resolution, review, promotion, rebuild, validation, and read entry points.
- [ ] 1.2 Document current state tables, business keys, checkpoints, canonical authority, job triggers, operator commands, and result/report consumers.
- [ ] 1.3 Build frozen fixtures covering CNInfo/TDX matches, asymmetric events, unresolved dates, non-factor events, BSE, suspension, and manual decisions.
- [ ] 1.4 Capture baseline canonical event/factor rows and adjusted quote outputs for fixture replay.
- [ ] 1.5 Re-run the baseline inventory against the post-`triage-announcement-only-xdxr-candidates` code and include announcement-only mode, case, inactive-watch, reactivation, and report semantics.

## 2. Stage Contracts

- [ ] 2.1 Define normalized observation and stage transition result contracts with evidence, status, idempotency identity, and next-stage eligibility.
- [ ] 2.2 Implement the observation application service while preserving provider transport/parsing ownership.
- [ ] 2.3 Implement the resolution service with existing automatic, LLM, and governance rules unchanged.
- [ ] 2.4 Implement the operator review service and preserve current approval/rejection/manual-override contracts.
- [ ] 2.5 Implement the canonical factor service as the only owner of selection, promotion, rebuild, and canonical factor publication.

## 3. Entry-Point Migration

- [ ] 3.1 Convert relevant DataManager methods to stage-service delegates in bounded method groups and remove migrated business blocks.
- [ ] 3.2 Rebind only existing scheduler callables to stage services; defer domain handler extraction and final job-resolution changes to W7.
- [ ] 3.3 Rebind API, Telegram, and operator scripts to the same review/canonical services and remove alternate state transitions.
- [ ] 3.4 Add dependency tests that keep provider modules source-specific and prevent application services from importing global facades.

## 4. Acceptance And Cutover

- [ ] 4.1 Replay frozen fixtures and compare decisions, evidence, canonical rows, factor values, and adjusted quote outputs.
- [ ] 4.2 Test retry/resume/idempotency across every stage and verify no duplicate decisions or canonical writes.
- [ ] 4.3 Run corporate-action database, governance, scheduler, API, factor, and backtest regression suites.
- [ ] 4.4 Verify old and new stage implementations are never enabled simultaneously and document rollback bindings.
- [ ] 4.5 Verify cutover occurs while affected jobs are idle, observe the first natural run, and record rollback criteria for canonical and announcement-only outputs.
- [ ] 4.6 Update the corporate-action current architecture/state-flow document and mark W6 complete in the framework program.
