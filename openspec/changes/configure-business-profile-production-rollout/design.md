## Context

The repository now has a durable `acquire -> parse -> semantic -> publish` queue, latest-annual daily selection, immutable annual-report assets, parallel parse/semantic computation, and a cooperative single SQLite writer. Production switches and scheduler jobs are deliberately disabled, all production fact tables are empty, and the current configuration contains no runtime identities or promotion manifests.

The first production load is different from steady-state daily discovery. It must scan enough historical announcement metadata to see every active issuer's latest available annual report, but it must not enqueue every annual report found in that historical range. The rollout will take many bounded executions and must resume without operator-managed company lists or routine record review.

## Goals / Non-Goals

**Goals:**

- Configure a safe initial structured shadow phase that can be started immediately without approved writes.
- Discover a bounded historical frontier and enqueue one latest active annual report per issuer.
- Reuse the same durable workers, assets, checkpoints, retries, and single-writer contract as daily production.
- Derive runtime identities from the actual parser, schema, catalog, model, verifier, rule, and policy configuration, and reject stale configured identities.
- Represent all later rollout phases in one versioned configuration while keeping promotion phases closed until real benchmark manifests pass.
- Allow repeated bounded manual runs to advance the bootstrap and expose deterministic readiness for switching to the next phase and finally to daily cron.

**Non-Goals:**

- Automatically declaring a semantic benchmark passed without labeled evidence.
- Automatically changing the active phase or enabling daily cron based only on elapsed time.
- Downloading historical annual-report series for every issuer.
- Changing approved history, deleting archived PDFs, or bypassing review and promotion gates.
- Automatically including semiannual or specialist disclosures in the bootstrap.

## Decisions

### Store rollout policy separately from runtime switches

Add `config/business_profile_production_rollout.json` as the versioned deployment plan. It contains the active phase, historical discovery bounds, document types, latest-annual selection policy, ordered phases, field families, promotion requirements, stage budgets, and readiness thresholds. `config/10_research.json` remains the source of runtime safety switches and common defaults; `config/05_scheduler.json` remains the scheduler source.

This avoids repeatedly editing unrelated configuration blocks while preserving the existing configuration ownership boundaries. Embedding every phase directly in the scheduler job was rejected because it would duplicate semantic configuration and make phase changes error-prone.

### Extend manual backfill with latest-annual selection

The manual backfill task accepts `selection_policy`. `expanded` preserves the existing explicit historical/specialist behavior. `latest_annual_only` performs the requested historical metadata discovery, coalesces the frontier by issuer, report period, and active correction, then enqueues only the latest annual work item per issuer.

Using the date-only `expanded` path for the bootstrap was rejected because a two-year discovery range can enqueue multiple annual periods per issuer. Supplying thousands of instrument ids manually was rejected because it is fragile and defeats unattended operation.

### Advance work in bounded repeated executions

One bootstrap invocation commits discovery first, enqueues idempotent latest-annual work, and consumes only configured stage budgets. Remaining work stays in SQLite. Repeating the same task continues from durable queue and split-window state; it does not duplicate downloads, candidates, or LLM calls.

The task remains `manual_only=true` during rollout. This preserves the agreed two-entry architecture and prevents an unfinished phase from silently becoming a permanent scheduler. The operator may invoke it repeatedly through the task manager; after the initial historical frontier is complete, the normal daily task can also drain existing work when explicitly enabled.

### Derive and validate runtime identities

Add a deterministic identity builder using code constants, loaded catalog versions, and the configured `semantic_extraction` LLM profile. Identity keys remain the promotion contract keys: `document`, `section`, `selector`, `parser`, `schema`, `catalog`, `model`, `verifier`, `rules`, and `policy`.

The rollout config uses `runtime_identity_mode=derived`. If explicit identities are supplied for an approved phase, runtime compares them with the derived identities and fails closed on any mismatch. This prevents a code or model upgrade from being mislabeled with an old identity.

### Make phase activation explicit and fail closed

Initial phase `structured_shadow` enables `structured_segments` and `tabular_operating_facts` with promotion disabled. Later phases are declared but disabled until their prerequisites are satisfied:

- `structured_promotion`
- `semantic_shadow`
- `semantic_promotion`
- `derived_publication`
- `daily_incremental`

Promotion phases require complete manifests whose identities match the runtime and whose `enabled` and `benchmark_passed` flags are true. Shadow phases persist candidates, exceptions, and machine rework but cannot create approved records. Derived publication requires approved upstream activities and exposure facts.

Automatic phase advancement was rejected because correctness thresholds, especially semantic precision, cannot be inferred solely from queue completion.

### Keep scheduler states conservative

Enable the business-profile module, discovery, semantic runtime, asynchronous production, reconciliation, and the manual backfill job. Keep global promotion, semantic scheduler gating, and `business_profile_daily_incremental` cron disabled. Network and write kill switches remain open so a manually started shadow task can work; promotion remains disabled by configuration rather than by disabling candidate writes.

## Risks / Trade-offs

- [A historical metadata scan can take many executions] -> Persist split windows and treat incomplete discovery as normal backlog, never as complete coverage.
- [A configured identity can drift from code or LLM settings] -> Derive identities at runtime and fail closed on mismatch.
- [Shadow candidates accumulate before promotion] -> Keep idempotent lineage and monitor candidate/exception counts by field family; replay with matching manifests instead of rewriting rows.
- [Manual-only rollout requires repeated starts] -> Each run is bounded and resumable; no company lists or per-record approvals are required, and the command is stable.
- [Promotion manifests are initially incomplete] -> Represent them as disabled templates and refuse approved writes until real benchmark artifacts exist.
- [Annual-report corrections arrive during bootstrap] -> Latest-active selection and immutable supersession create a new work identity without deleting the prior artifact.

## Migration Plan

1. Add and validate the rollout configuration with `structured_shadow` active.
2. Enable only the runtime prerequisites and manual backfill task; leave promotion and daily cron disabled.
3. Add latest-annual manual selection, derived runtime identities, task parameter parsing, and rollout reporting.
4. Run unit tests and isolated rollout-gate validation without starting network acquisition or production writes.
5. Start the first bounded structured shadow bootstrap manually after deployment.
6. Generate real benchmark manifests and activate later phases one at a time after their readiness gates pass.
7. Enable daily cron only after full-market latest-annual coverage and required publication coverage are complete.

Rollback disables the manual task or business-profile module. Durable frontier, queues, checkpoints, assets, candidates, exceptions, approved records, and audit history remain intact.

## Open Questions

- Real promotion manifests remain intentionally unresolved until production shadow evidence exists; this is a deployment result, not a configuration default that can be safely guessed.
