## Context

The requested backtest datasets cross the quote, master-data, research, financial, announcement, and corporate-action domains. They are not greenfield acquisition problems:

- `daily_data_update` already refreshes the A-share stock/index universe and quotes after shared master governance.
- `index_master_governance_sync` and the official CNIndex/CSIndex adapters already acquire index metadata, lifecycle evidence, and some official quotes.
- AkShare 1.18.81 already exposes current CSI constituents/weights, a current ST board, dated limit-up/limit-down pools, official SSE/SZSE delisting lists, and CNInfo dividend/allotment adapters. The constituent and ST interfaces explicitly describe current state, while the limit pools contain only securities that hit a limit; none can be assumed to provide complete history or all-market reference prices without a source capability probe.
- `industry_standard_sync` and `industry_index_analysis_sync` already maintain historical Shenwan membership and industry returns. Their read APIs exist and need discovery/readiness integration, not another downloader.
- `financial_disclosure_incremental_sync` already uses source-neutral official announcement acquisition, while `financial_source_files` and long-form numeric facts already carry most of the required filing identity and fact lineage fields.
- CNInfo daily corporate-action maintenance, TDX weekly refresh, effective-date governance, canonical factor selection, and resolved-term review already form the acquisition and governance pipeline. A consumer projection is missing; another event downloader is not.
- The scheduler dependency DAG, shared HTTP transport, adaptive source backoff, database-scoped change logs, temporal availability governance, and governed historical backfill are existing cross-cutting mechanisms.

The external platform needs point-in-time correctness. Effective dates, publication timestamps, local availability timestamps, revision identities, source profile, and quality/completeness must remain separate. Current snapshots cannot be retroactively presented as historical facts, and report periods cannot be used as publication dates.

## Goals / Non-Goals

**Goals:**

- Make resource reuse an enforceable implementation gate rather than an informal preference.
- Reuse existing parent jobs, target universes, transports, checkpoints, stores, and reports wherever cadence and source overlap.
- Deliver point-in-time index composition, security states, financial filing vintages, and canonical corporate-action reads without introducing future leakage.
- Expose existing industry returns and historical industry membership in the same discoverable readiness contract.
- Keep historical work bounded, resumable, dry-run-first, and separate from automatic deployment or startup.
- Preserve existing APIs and compatibility projections while adding explicit point-in-time read paths.

**Non-Goals:**

- Assuming a paid Tushare subscription, token, or new heavyweight dependency.
- Fabricating historical index membership from today's constituents or historical ST intervals from today's security name.
- Treating limit-hit pools as complete daily limit-price reference data.
- Reconstructing financial revisions that no retained filing or upstream archive can supply.
- Replacing the source-neutral announcement service, the corporate-action evidence pipeline, the current industry pipeline, or the scheduler dependency DAG.
- Running full-market downloads automatically during schema initialization, deployment, application startup, or unit tests.
- Guaranteeing pre-2010 Shenwan industry returns or cross-taxonomy historical mappings in this change.

## Decisions

### 1. Admit new acquisition only through a resource capability record

Introduce a versioned backtest-data resource catalog. Each dataset entry records:

- business dataset and point-in-time contract;
- required markets, history start, frequency, key fields, and quality threshold;
- existing providers and their declared capabilities;
- existing parent job, target-universe owner, transport, checkpoint, store, watermark, and read API;
- bounded probe evidence for history depth, date meaning, field units, completeness, permissions, rate limits, and empty/error behavior;
- route decision: `reuse`, `extend_existing`, `new_source_required`, `manual_import_only`, or `unavailable`;
- forward-maintenance and historical-backfill ownership.

The implementation cannot register a new full-market source or standalone scheduled job while the entry lacks probe evidence or identifies an existing owner that can be extended. Runtime readiness combines this static catalog with local row counts, date ranges, unresolved-quality counts, watermarks, and latest successful runs.

Alternative: document reuse decisions only in the proposal. Rejected because the same duplication problem will return when providers drift or another consumer requests similar data.

### 2. Integrate maintenance by parent workflow, not by one new umbrella cron

The steady-state topology is:

```text
shared stock master governance
  -> persist current master
  -> emit ST/suspension/delisting transitions from observed changes

index master governance (invoked directly or by the daily guard)
  -> refresh index metadata/lifecycle when due
  -> refresh current composition snapshot when the source is due/changed

daily_data_update
  -> reuse governed stock universe and trading date
  -> persist quotes
  -> persist source-reported limits or run a bounded unresolved-only enrichment stage

financial_disclosure_incremental_sync
  -> reuse announcement scan/audit
  -> archive immutable filing revision
  -> parse versioned facts and update the latest compatibility projection
  -> existing post-success industry supplements

a_share_cninfo_corporate_action_daily_sync / TDX weekly refresh
  -> maintain existing evidence and factor governance
  -> rebuild changed canonical event projections

industry_standard_sync / industry_index_analysis_sync
  -> unchanged acquisition
  -> readiness/catalog only
```

When a stage needs separate timeout or failure policy, it is represented as a scheduler dependency-DAG child or an explicit stage of the existing parent, not as an independent cron over the same full-market universe. The child inherits exchange, trading date, dry-run, and target scope. Historical work extends `a_share_daily_data_historical_backfill` with explicit scopes and checkpoints.

Alternative: add one new `backtest_data_daily_sync` job that downloads every dataset. Rejected because it would repeat master/quote/announcement requests, mix unrelated failure domains, and create a second owner for the same source state.

### 3. Probe the already-installed source surface before choosing fallbacks

The first implementation phase runs fixture-backed and bounded no-write probes:

- Index composition: inspect existing official CNIndex/CSIndex endpoints and AkShare `index_stock_cons*` / `index_stock_cons_weight_csindex`; confirm whether historical snapshots, rebalance effective dates, publication times, and weights are available. Current-only results are valid for forward accumulation but never historical backfill.
- Security state: inspect current official stock-master names and AkShare `stock_zh_a_st_em`, official exchange announcements, and retained announcement archives for ST implementation/removal dates. Current-only boards seed current state only.
- Price limits: inspect existing daily quote payloads and bounded official/free-source endpoints for all-security `limit_up`/`limit_down`. AkShare limit pools are validation evidence for actual hits, not an all-market limit-price source.
- Delisting: reuse official SSE/SZSE/BSE/HKEX lifecycle sources and source-neutral announcements; fill evidence gaps without weakening confirmed-delisting rules.
- Financial revisions: verify correction/amendment classification, stable filing ids, publication timestamps, attachment hashes, and retained historical filings through the existing announcement providers and official filing profiles.

If no free source provides a required historical dataset, the resource catalog records `manual_import_only` or `unavailable`; production readiness stays false. A paid provider may be added later behind the same provider contract, but this change does not silently introduce a credential requirement.

### 4. Store index composition as immutable dated snapshots

Add an index-composition snapshot header and member rows in `quotes.db`:

- snapshot identity, index instrument id, composition/effective date, announcement/publication time, local `available_at`, source/source profile, source artifact/hash, weight unit and normalization, completeness, and ingestion lineage;
- constituent instrument id and source symbol, weight, inclusion metadata, row hash/version, and quality diagnostics.

The source snapshot is immutable by source identity/content hash. A correction creates a new revision or superseding snapshot; it does not overwrite what was previously known. Separate append-only validity/continuity revisions record `valid_from`, `valid_to_exclusive`, decision `available_at`, basis, input snapshot identities, and evidence. A validity conclusion is established only by official validity evidence, adjacent complete snapshots, or a freshness observation proving that no replacement was published through a bounded date.

The point-in-time resolver first filters snapshots and validity revisions by `available_at <= known_at`, then selects a complete snapshot whose effective and validity intervals contain `as_of_date`. A later adjacent snapshot, freshness observation, correction, or continuity decision cannot retrospectively alter an earlier known-time result. An open-ended or stale snapshot is not carried indefinitely across an expected but unobserved rebalance. If a source reports weights only for a rebalance/reference date, the API labels that date and does not claim daily weights.

Alternative: store only membership intervals. Rejected because weights are snapshot facts, revisions need artifact lineage, and deriving intervals can erase what the provider actually published.

### 5. Separate security events, intervals, and daily price limits

Use three logical datasets in `quotes.db`:

- immutable security-state/lifecycle events for ST, *ST, removal of warning, suspension/resumption, delisting decision, and listing termination;
- append-only status-interval interpretation revisions built only from ordered, accepted events or observed forward transitions, with decision `available_at`, inputs, source, and confidence retained;
- immutable daily price-limit reference revisions keyed by instrument and trading date, with observation/derivation availability.

Current master-name changes can create forward `observed_transition` evidence after deployment, but they cannot invent a historical start date. Announcement-derived transitions carry publication and effective dates separately. A PIT state read either resolves from events available by `known_at` or selects an interval interpretation revision available by that cutoff, so a later announcement cannot rewrite earlier knowledge. Confirmed delisting continues to require official master/list/announcement evidence.

Price-limit revisions use `source_reported` when an upstream source supplies the actual references. If no complete source is available, a configured rules engine may produce `derived_rule` revisions using the governed exchange reference price (including applicable ex-right/ex-dividend reference-price decisions), board, listing age, ST interval, trading regime, and tick-size rounding. Revisions include rule version, input identities, decision `available_at`, and quality status and never masquerade as official values. A later source correction or rules-engine change appends a revision; a raw prior close is not substituted for a required adjusted reference price, and missing or ambiguous state yields no strict-ready derived row.

Alternative: add `is_st`, `limit_up`, and `limit_down` directly to every historical quote row. Rejected because status is interval/event data, limit provenance differs from quote provenance, and revisions would rewrite a very large quote table.

### 6. Use filing artifacts as the financial revision anchor

Complete the existing `financial_source_files` contract instead of replacing it:

- one immutable source file per stable filing identity and content hash;
- `published_at`, normalized `available_at`, filing/revision id, and correction/amendment type;
- retained attachment/archive lineage and parser version.

Add append-only filing relationship decisions with decision id, relation type, predecessor/successor source-file identities, evidence, and decision `available_at`. The existing `supersedes_source_file_id` may remain a current compatibility projection, but PIT resolution never treats that mutable field as historical truth.

Long-form numeric facts already use `source_file_id`, but their current primary key can replace facts when the same filing is parsed again. Add an immutable parse-run identity and append-only fact revision boundary using `source_file_id + parse_revision_id`, with parser version, parsed `available_at`, mapping/catalog versions, and input artifact hash. Existing `financial_facts`, current numeric-fact tables, and core hot tables remain latest compatibility projections and are not used as point-in-time history. A correction or reparse appends filing/relationship/parse-version facts before updating those compatibility projections.

Add explicit `period_semantic` values: `instant`, `single_quarter`, `ytd`, `annual`, `derived_single_quarter`, and `unknown`, plus semantic basis and quality. Duration contexts use source `period_start`/`period_end`; `report_period` or a `quarterly` label alone cannot prove single-quarter versus YTD. A derived single quarter stores input fact identities and uses the maximum input availability date.

The as-of resolver evaluates filing supersession and parse revisions using only relationships and parse results available at `known_at`, then chooses the latest eligible fact revision. A later parser run or later-discovered supersession cannot rewrite an earlier response. If multiple eligible filings conflict for the same fact scope and their relationship is unresolved at the cutoff, strict PIT resolution fails closed instead of choosing one by ingestion or publication order. Unknown filing availability, unknown parse availability, or unknown duration semantics likewise fail strict PIT reads instead of falling back to report deadlines unless the caller explicitly requests an estimated policy.

Alternative: change `financial_facts` primary keys to include a revision. Rejected because the long-form/source-file layer already provides the correct version boundary and the current table is widely used as a compatibility projection.

### 7. Materialize a canonical corporate-action consumer projection inside existing closure

Add append-only `corporate_action_canonical_event_revisions` in `quotes.db`, keyed by stable canonical event id plus projection revision, and a current compatibility projection. Revisions record decision `available_at`, projection version, and input identities/hashes. They are appended only for changed instruments/events by the existing CNInfo daily, TDX reconciliation, review/promotion, and weekly refresh closure paths. No new network acquisition job is introduced.

The projection combines, without mutating source evidence:

- current CNInfo observations;
- reviewed resolved-term overlays and effective-date evidence;
- TDX reconciliation and source asymmetry decisions;
- event/factor eligibility, factor decision, coverage state, and blocking reason;
- announcement, record, ex/effective, payment, and share-arrival dates when known;
- per-share cash, bonus, capitalization, rights terms, currency, and source lineage.

Rows are queryable even when incomplete, but `backtest_ready=true` requires an accepted event, usable effective date, coherent economic terms, lifecycle applicability, and no blocking conflict. Point-in-time reads select the latest projection revision whose decision `available_at <= known_at`; a later review or reconciliation cannot replace the revision visible at an earlier cutoff. Non-factor corporate actions remain canonical events with `factor_effect=false` when supported by reviewed evidence.

Alternative: expose raw CNInfo observations as the canonical API. Rejected because raw evidence intentionally retains partial dates, source asymmetry, and unresolved governance states.

### 8. Use one additive, point-in-time API family

Add bounded endpoints:

- `GET /api/v1/backtest-data/capabilities` for catalog plus runtime readiness;
- `GET /api/v1/indices/{instrument_id}/constituents` with `as_of_date`, optional `known_at`, pagination, and completeness metadata;
- `GET /api/v1/instruments/{instrument_id}/market-state` and `/price-limits` with date/as-of filters and source-quality fields;
- `GET /api/v1/research/company/{instrument_id}/financial-facts/as-of` with `known_at`, report/fact/period-semantic filters, revision lineage, and strict/estimated policy;
- `GET /api/v1/corporate-actions/canonical` with instrument/date/type/readiness filters, optional `known_at` revision selection, and stable pagination.

The capability endpoint advertises existing Shenwan industry-return and membership-as-of endpoints, their coverage, units, and temporal contract. It distinguishes effective-date history from knowledge-time-safe history; an endpoint without retained availability/revision evidence is not advertised as strict `known_at` safe. It does not proxy or duplicate industry data.

All collection endpoints use deterministic business-key ordering, bounded page sizes, and database-scoped change cursors where available. Existing endpoints and defaults remain unchanged.

### 9. Fail closed on coverage and point-in-time ambiguity

Every dataset exposes a readiness summary with target universe, covered universe, date range, missing dates/fields, partial/indeterminate/conflict counts, latest successful watermark, and source-route decision. Strict APIs never forward-fill across an unknown index rebalance, infer an ST interval before first evidence, synthesize an official price limit, or select a filing without a usable availability date.

The resource catalog may declare a dataset usable for a narrower market or date range. Readiness is not a global boolean detached from scope.

### 10. Reuse existing change logs and temporal availability rules

Writes emit `data_change_log` rows in the physical database that owns the dataset. Business keys include snapshot/event/filing identities, and semantic hashes include effective/publication/availability dates, source profile, quality, revision, and lineage. Consumers resume by `database_id + domain + sequence`; no cross-database global ordering is promised.

Publication and availability normalization uses the existing temporal-data-availability policy and exchange timezone rules. Raw timestamps remain available for audit.

## Risks / Trade-offs

- **[Free sources expose only current composition or ST state]** -> Preserve forward snapshots, keep historical readiness false, and support a later configured/manual historical provider without fabricating history.
- **[Daily integration increases runtime or source pressure]** -> Reuse target universes, ETag/content hashes and freshness checks; fetch only due or unresolved scopes; keep independent timeout/failure policy through dependency-DAG stages.
- **[Derived limit prices are wrong around rule changes, IPO days, or ST transitions]** -> Prefer source-reported rows, version the rule catalog, require complete state inputs, validate against limit-hit pools, and label or reject ambiguous derived rows.
- **[Filing corrections cannot be linked reliably]** -> Require stable ids or explicit correction evidence for supersession; otherwise keep both filings and mark the relationship unresolved rather than guessing.
- **[Large historical backfills overload SQLite or upstreams]** -> Use existing scoped checkpoints, chunking, pacing, dry-run evidence, WAL-aware batch writes, and operator-controlled rollout by dataset/index/date/exchange.
- **[Corporate projection diverges from evidence]** -> Build it deterministically from versioned source rows, store projection input hashes, emit changes only on semantic differences, and validate samples against the existing factor/canonical summaries.
- **[One umbrella change becomes too broad]** -> Keep implementation phases independently gated and ordered; no downstream dataset proceeds to production backfill until its source probe, schema, forward path, API, and focused tests pass.

## Migration Plan

1. Add the resource catalog schema and bounded probe scripts; record evidence for existing index, ST, limit, delisting, financial, corporate-action, and industry routes. Freeze each route decision before source-dependent implementation.
2. Add additive database migrations and repository contracts. Migrations create schema only and do not fetch or rewrite historical data.
3. Add readiness/capability output and point-in-time read APIs behind disabled rollout flags; verify empty/local-fixture behavior.
4. Integrate forward maintenance into existing parent jobs and dependency DAG. Run bounded dry-runs, then enable changed-only writes by dataset.
5. Extend the unified historical backfill scopes for datasets whose source probes demonstrate real historical coverage. Execute sample index/instrument/date ranges before broader operator-approved backfill.
6. Reconcile coverage, PIT invariants, source lineage, watermarks, API schemas, and external-platform contract fixtures. Enable strict read readiness only for proven scopes.

Rollback is configuration-first: disable the new dataset stage and read endpoint while leaving additive evidence intact. Existing jobs and APIs continue on their prior paths. Schema rollback does not delete populated audit/history tables; a destructive removal requires a separate reviewed migration.

## Open Questions

- Which existing free/official source can provide historical broad-index compositions and weights with explicit effective and publication dates, beyond AkShare's current-only adapters?
- Does any current quote route expose complete exchange reference limit prices, or must the first release combine a new optional source with a derived, non-official fallback?
- How far back can official announcement acquisition recover ST implementation/removal and financial correction filings without a paid archive?
- Should the first enabled index universe remain the four requested broad indexes or include every index whose source passes the same completeness gate?
- Which strict PIT policy should the external platform use when an old financial filing has a report period but no trustworthy publication/availability timestamp: omit the filing by default, or allow an explicit conservative-estimate mode?
