## Context

The async queue currently maps four queue stages to five semantic-pipeline stages: `semantic` runs extraction and `publish` runs verification followed conditionally by promotion. A targeted expanded run therefore spends nearly all of its time in LLM verification while reporting it as publish time. The global shadow configuration also disables promotion, so a verified work item can be marked completed while its activities remain candidates and no downstream role or commodity rows are produced.

The current operator validation command explicitly selects instruments, uses `selection_policy=expanded`, requests `atomic_activities` and `named_relationships`, and commonly uses `force=true`. Full-market latest-annual rollout must remain on its configured active phase until the bounded validation succeeds.

## Goals / Non-Goals

**Goals:**

- Make targeted expanded semantic validation complete the existing promote, role derivation, exposure-fact, and exposure-publication path automatically.
- Attribute extraction, independent verification, and serial publication to separate durable queue stages.
- Run independent verification calls concurrently through the existing common LLM gateway while preserving target-id checkpoint resume.
- Report actual coverage and publication outcome without warning markers on successful details.
- Increase the resumable semantic token guard to 50,000.

**Non-Goals:**

- Activating full-market daily publication or changing the default `structured_shadow` phase.
- Removing deterministic promotion gates, exact evidence checks, unit governance, product mappings, or exception persistence.
- Adding another queue, downloader, LLM client, publication writer, or database schema.
- Treating work for a different selection policy or field-family identity as obsolete.

## Decisions

### Add a targeted complete-publication rollout phase

When an expanded invocation has explicit instruments and includes `atomic_activities`, the application selects `semantic_complete_targeted` unless the caller supplied another rollout phase. That phase includes activities, relationships, derived roles, exposure facts, and exposure publication and uses explicit manifests bound to the current runtime identities. The existing promotion service remains authoritative and promotes only confirmed records whose deterministic gates pass.

This keeps the user's existing validation command useful while preventing an implicit full-market activation. A separate `publish=true` flag was rejected because it would duplicate the existing rollout-phase contract; globally enabling semantic promotion was rejected because it would change the current market rollout.

### Represent verification as a real queue stage

The durable stage sequence becomes `acquire`, `parse`, `semantic`, `verify`, `publish`. Their semantic-pipeline modes are respectively `plan`, `select`, `extract`, `verify`, and `promote`. Verify inherits semantic worker limits when an older stage-budget config has no explicit verify entry; production config will nevertheless declare it explicitly. Publish concurrency is one, preserving the single-writer requirement.

Completed historical work remains completed. On storage initialization, unfinished legacy `publish` rows whose checkpoints have not completed verify are moved to `verify`; rows with a completed verify artifact remain at publish. This is a narrow state migration, not a data rewrite.

### Verify records in bounded concurrent waves

The runtime first collects pending verification targets in deterministic order and applies all deterministic bypasses. Network-backed targets then run in waves up to `max_concurrency` through one async bridge call. Results are merged in target order, token/error metrics are accumulated per field family, and the stage artifact is written after each wave so interruption or token exhaustion resumes by target id.

The 50,000-token value remains a soft per-field-family, per-stage-run guard: no new wave starts once completed usage reaches the limit. An in-flight wave can finish above the threshold, and every successful result remains durable for the next resume.

### Derive report outcome from business results

The async result exposes execution mode (`candidate_only` or `complete_publication`) and candidate, verified, promoted, role, exposure-fact, and exposure-publication counts. A complete-publication run cannot report end-to-end success if promotion did not run or required publication remains blocked. Candidate-only completion is reported as such rather than as published success.

The progress formatter uses reconciliation coverage when rollout readiness is absent, displays the effective phase and processing-identity scope, and selects notification severity from the task result. Generic report delivery remains bounded and non-blocking.

## Risks / Trade-offs

- [Current identity manifests become stale after parser, catalog, model-route, or policy changes] -> The complete phase fails closed with an explicit identity mismatch; updating the versioned config is safer than silently promoting under changed semantics.
- [Concurrent verification overshoots the soft token guard] -> Start no additional wave after the measured limit and persist all completed in-flight results; the 50,000 limit bounds normal single-report work while preserving throughput.
- [Some confirmed activities cannot map to a commodity] -> Publish supported rows, persist existing machine-rework exceptions for unsupported mappings, and report partial publication rather than discarding unrelated valid facts.
- [Five queue stages affect old checkpoints] -> Migrate only unfinished legacy publish rows based on their persisted completed semantic stages and cover both cases with temporary-database tests.

## Migration Plan

1. Deploy the stage migration, targeted phase, manifests, runtime concurrency, report changes, and 50,000-token config together.
2. Run focused temporary-database and fake-LLM regressions.
3. Re-run the existing targeted `601088.SH` command; it should reuse extraction/verification artifacts where identities match and complete program-owned publication.
4. Rollback restores the prior code/config. Existing `verify` queue rows can be moved back to legacy `publish` only if rollback is required before they complete; no canonical table rollback is needed because writes are versioned and idempotent.

## Open Questions

None for this bounded targeted-publication change. Full-market activation remains a separate operator decision after representative output review.
