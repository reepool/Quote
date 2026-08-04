## 1. Resource inventory and admission gates

- [ ] 1.1 Add a versioned machine-readable resource catalog schema and typed loader for the five backtest-critical datasets plus existing industry membership/return capabilities.
- [ ] 1.2 Populate catalog entries with existing providers, parent jobs, universe owners, transports, checkpoints, stores, watermarks, APIs, required history, point-in-time fields, and quality thresholds.
- [ ] 1.3 Implement catalog validation that rejects unknown owners and prevents a new full-market provider or standalone cron unless bounded probe evidence approves the route.
- [ ] 1.4 Add fixture-backed catalog tests for `reuse`, `extend_existing`, `new_source_required`, `manual_import_only`, and `unavailable` decisions.
- [ ] 1.5 Add a runtime readiness aggregator that merges catalog decisions with scoped row coverage, quality blockers, latest runs, and database-scoped watermarks.

## 2. Bounded existing-resource probes

- [ ] 2.1 Implement a no-write index probe for current and historical composition/weight capability across existing official CNIndex/CSIndex adapters and installed AkShare routes.
- [ ] 2.2 Implement a no-write security-state probe for current ST state, announcement-derived ST dates, official delisting lifecycle evidence, and all-market price-limit fields in existing quote/provider routes.
- [ ] 2.3 Implement a no-write filing-vintage probe for stable filing ids, correction classification, publication timestamps, attachment hashes, retained versions, and supersession evidence in existing announcement/financial routes.
- [ ] 2.4 Implement a no-write corporate-action and industry capability audit that confirms existing acquisition jobs, evidence stores, as-of APIs, units, coverage, effective-date versus knowledge-time semantics, and watermark ownership.
- [ ] 2.5 Ensure all probes require bounded symbols/indexes and dates, emit request/coverage/field/error evidence, and cannot write production rows or watermarks.
- [ ] 2.6 Run approved bounded probes, review their artifacts, and freeze one catalog route decision plus forward/backfill owner for every required dataset before source-dependent implementation proceeds.

## 3. Shared point-in-time and change infrastructure

- [ ] 3.1 Add additive migrations for resource catalog/run evidence and for append-only index validity, security-interval, price-limit, filing-relationship, filing-parse, and canonical-action revisions without starting data downloads.
- [ ] 3.2 Reuse the temporal-availability normalizer for publication and local-availability timestamps, preserving raw timestamps and exchange timezone lineage.
- [ ] 3.3 Extend domain semantic-hash and change-log contracts to include effective/publication/availability time, source profile, revision, quality, completeness, rule version, and lineage.
- [ ] 3.4 Add database-scoped change readers and cursor validation for the new quote and financial domains without promising cross-database ordering.
- [ ] 3.5 Add migration, idempotency, unchanged-overlap, dry-run-no-write, and database-scoped cursor tests.

## 4. Historical index composition integration

- [ ] 4.1 Implement immutable index snapshot-header/member repositories and append-only validity/continuity revisions with decision availability, evidence, input snapshots, reference/effective dates, weight units, completeness, and instrument-resolution diagnostics.
- [ ] 4.2 Extend the source route approved by task 2.6 to normalize composition/weight snapshots while rejecting current-only responses as historical evidence.
- [ ] 4.3 Integrate changed/due forward snapshots into `index_master_governance_sync` using its provider, freshness, transport, and report context.
- [ ] 4.4 Extend `a_share_daily_data_historical_backfill` with an operator-bounded index-composition scope only when the approved source proves historical coverage.
- [ ] 4.5 Implement the effective-date and `known_at` resolver that independently filters snapshot and validity revisions, plus strict completeness/continuity gates, deterministic pagination, and readiness metrics.
- [ ] 4.6 Add tests for snapshot/validity revisions, later-learned bounds, bounded current snapshots, weight units, missing/unknown rebalances, PIT selection, partial snapshots, resumable backfill, and no duplicate index cron.

## 5. Historical security-state integration

- [ ] 5.1 Implement repositories for immutable security-state/lifecycle events, append-only interval interpretation revisions, and immutable daily price-limit reference revisions.
- [ ] 5.2 Extend shared instrument-master governance to emit idempotent forward observed transitions from persisted accepted state without repeating the master request.
- [ ] 5.3 Link official exchange list and source-neutral announcement evidence to ST, suspension, resumption, delisting-decision, and termination transitions under existing source-authority rules.
- [ ] 5.4 Integrate source-reported price-limit persistence or unresolved-only enrichment into the existing daily quote universe and parent run context according to the approved route.
- [ ] 5.5 If enabled by catalog policy, implement a versioned non-official price-limit rules engine that requires the governed exchange reference price, applicable ex-right/ex-dividend reference-price decision, board, listing-age, ST, regime, tick-size, and rounding inputs.
- [ ] 5.6 Extend governed historical backfill with only the security-state and limit scopes whose probes prove historical availability.
- [ ] 5.7 Implement PIT market-state and price-limit resolvers that filter event, interval, and limit revisions by `known_at`, plus scoped readiness metrics that fail closed on unknown intervals, rule inputs, and source conflicts.
- [ ] 5.8 Add tests for later-learned interval evidence, limit corrections/rule revisions, ex-right/ex-dividend reference prices, rejection of raw-prior-close substitution, forward-only current observations, official authority, pending versus confirmed delisting, interval conflicts, IPO/rule transitions, tick rounding, limit-pool validation-only behavior, and unchanged-state idempotency.

## 6. Financial filing vintages

- [ ] 6.1 Complete additive immutable filing lineage and append-only filing relationship decisions for stable filing identity, content hash, publication/availability time, correction type, predecessor/successor identities, relation evidence/availability, artifact lineage, and parser version.
- [ ] 6.2 Add append-only financial parse revisions and preserve facts per `source_file_id + parse_revision_id`, including parser/mapping/catalog versions and parsed availability, before updating existing latest compatibility projections.
- [ ] 6.3 Implement evidence-based period-semantic classification for instant, single-quarter, YTD, annual, derived-single-quarter, and unknown facts with basis and quality.
- [ ] 6.4 Implement derived single-quarter lineage using explicit input fact identities, derivation version, and maximum input availability.
- [ ] 6.5 Extend `financial_disclosure_incremental_sync` and reconciliation to archive, link, parse, and report filing versions through existing announcement/provider/throttle paths.
- [ ] 6.6 Implement strict and explicitly estimated `known_at` financial-fact resolvers that evaluate filing supersession and parse revisions at the cutoff, with report/fact/semantic filters and full revision lineage.
- [ ] 6.7 Add vintage readiness metrics and tests for pre-correction reads, later parser revisions, append-only relationship corrections, later-discovered supersession, unresolved conflicting filing relationships, missing filing/relationship/parse availability, YTD versus single-quarter classification, derived availability, immutable versions, and latest-projection compatibility.

## 7. Canonical corporate-action projection

- [ ] 7.1 Implement append-only canonical corporate-action projection revisions plus a current compatibility projection with stable event/revision ids, decision availability, input versions, terms, dates, lifecycle applicability, factor decisions, readiness, blockers, and source lineage.
- [ ] 7.2 Implement deterministic projection from existing CNInfo observations, reviewed resolved terms, effective-date evidence, TDX reconciliation, factor governance, and coverage state without mutating evidence rows.
- [ ] 7.3 Hook changed-only projection rebuilds into existing CNInfo daily, TDX weekly, reconciliation, and review/promotion closure paths without network acquisition.
- [ ] 7.4 Emit semantic changes when a new canonical revision changes terms, dates, readiness, factors, coverage, or blockers and suppress unchanged rebuild watermarks.
- [ ] 7.5 Add tests for ready and blocked events, non-factor events, partial dates, source conflicts, lifecycle applicability, late review known-time isolation, stable ids, changed-only revisions, and evidence preservation.

## 8. Scheduler and operational integration

- [ ] 8.1 Extend scheduler configuration validation so approved backtest stages attach to existing parent workflows or dependency-DAG nodes and inherit only explicit runtime scope.
- [ ] 8.2 Add stage-specific timeout, retry, continuation, freshness, and source-pressure controls without introducing an umbrella backtest-data cron.
- [ ] 8.3 Add structured parent reports for reuse decision, inherited scope, provider usage, coverage, inserted/changed/unchanged rows, watermarks, skips, and blockers.
- [ ] 8.4 Add scheduler tests proving no redundant full-market job, no deployment/startup download, historical bounds/checkpoints, degraded-stage visibility, and preservation of existing parent lifecycle behavior.

## 9. Consumer APIs and compatibility

- [ ] 9.1 Add `GET /api/v1/backtest-data/capabilities` with scoped catalog/readiness output and discovery links for existing Shenwan return and membership-as-of APIs, explicitly distinguishing effective-date-only from strict knowledge-time-safe coverage.
- [ ] 9.2 Add the paginated PIT index-constituent, instrument market-state, and daily price-limit endpoints with provenance and coverage diagnostics.
- [ ] 9.3 Add the financial facts as-of endpoint with strict/estimated availability policy, vintage lineage, semantic filters, and explicit exclusion reasons.
- [ ] 9.4 Add the paginated canonical corporate-action endpoint with stable business ordering, readiness filters, `known_at` projection-revision selection, and database-scoped change cursors.
- [ ] 9.5 Preserve existing quote, instrument, financial, industry, and corporate-action endpoint defaults and add contract tests for backward compatibility, page bounds, cursor scope, PIT cutoffs, and fail-closed results.

## 10. Rollout and external-platform acceptance

- [ ] 10.1 Add disabled-by-default rollout flags and operator runbook sections for probes, migrations, forward stages, sample backfills, coverage review, rollback, and manual/unavailable source states.
- [ ] 10.2 Run migration and empty-database smoke tests to prove initialization performs no network crawl and existing APIs remain available.
- [ ] 10.3 Run bounded forward-maintenance dry runs for each reused parent job and verify no repeated universe/source requests, no writes, and complete stage reports.
- [ ] 10.4 Run operator-approved sample backfills only for proven routes and verify checkpoints, pacing, PIT invariants, lineage, completeness, and watermarks before broader rollout.
- [ ] 10.5 Validate external-platform contract fixtures for survivorship-safe index membership, historical state/limit constraints, pre-revision financial facts, canonical actions, industry discovery, pagination, and incremental resume.
- [ ] 10.6 Record final market/date coverage and unresolved gaps in readiness output, enable strict endpoints only for proven scopes, and keep unsupported historical scopes explicitly unavailable.
