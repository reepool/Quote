## Implementation checkpoint (2026-08-28 post-review correction)

The first implementation established the future-write baseline for field-backed shareholder scopes, per-scope merge authority, non-colliding exposure identities, approved-only DCF mappings, relationship report cohorts, and a bounded local repair entry point. Recheck found that the change is not complete: relationship status vocabulary is inconsistent, the configured company-profile providers do not prove full legal names, point-in-time end dates are not uniformly enforced, and repair classifiers can miss historical relationships or alter valid data. Only the verified baseline remains checked below; all remaining acceptance work is reopened.

## 1. Preserve the verified baseline

- [x] 1.1 Keep shareholder scope field-backed, remove first-holder controller inference from future aggregate writes, merge scopes by period/authority, preserve per-scope provenance, normalize dates, isolate per-instrument failures, and retain degraded reporting.
- [x] 1.2 Keep business-profile shareholder reads local-only and project top holders/control history at the requested knowledge cutoff without provider or LLM access.
- [x] 1.3 Keep relationship latest-report cohort selection, same-cohort occurrence preservation, concentration direction/share validation, approved-only profile reads, and candidate fingerprint isolation.
- [x] 1.4 Keep purchase/consumption/consumer exposure identities separate, hedge fact-only behavior explicit, energy-cost classification program-governed, industry defaults non-executable, and DCF roles fail-closed.
- [x] 1.5 Keep one bounded repair service with audit default, explicit apply scope, per-instrument isolation, stable issue IDs, and no network/LLM access.

## 2. Close counterparty authority and state semantics

- [ ] 2.1 Define and enforce the company-profile full-name source contract: PyTDX/BaoStock securities display names populate only short/display semantics, while `legal_name` accepts only a locally governed full-name value with attributable source authority; do not use ticker as an official corporate identifier.
- [ ] 2.2 Make an empty governed entity set a supported state: `named_relationships` continues with evidence-backed `disclosed_name_only` candidates instead of aborting the document or manufacturing an entity.
- [ ] 2.3 Make `identity_status` with values `resolved_entity` and `disclosed_name_only` the canonical relationship contract across production, promotion gates, exception routing, review, successor creation, repository decoding, repair, and API projection; normalize legacy spellings at one compatibility boundary.
- [ ] 2.4 Correct later-resolution behavior so a governed successor updates entity ID, basis, and canonical status together, supersedes the prior disclosed-name occurrence, and closes catalog exceptions only after successful approval.
- [ ] 2.5 Add production-semantic tests where `company_name == short_name`, where the full-name table is empty, where a governed full name resolves, and where legacy/new status spellings cannot disagree or block a valid resolved relationship.

## 3. Unify current/as-of time semantics

- [ ] 3.1 Centralize repository eligibility as knowledge interval plus the record policy's half-open business validity interval, report cutoff, and freshness; apply it to regimes, activities, value-chain roles, relationships, assumptions, exposure facts, and exposures whenever their fields exist.
- [ ] 3.2 Keep relationship latest-cohort/same-cohort occurrence selection after eligibility filtering, and make resolver, approved API, readiness, publication, and DCF consumers use repository current-state results rather than a conflicting second predicate.
- [ ] 3.3 Add cutoff-boundary tests for expired activities, roles, relationships, assumptions, and exposures, plus stale/new report cohorts and historical API reads.

## 4. Make repair evidence-positive and owner-driven

- [ ] 4.1 Replace the negative-evidence controller rule with provenance-aware classification: clear only a proven aggregate/fallback first-holder inference unsupported by eligible official evidence; preserve official-source values without control-history rows and hold ambiguous provenance without writes.
- [ ] 4.2 Audit historical relationship identity using the persisted `resolution_basis` column and nested entity-resolution metadata, including legacy basis/status values; route corrections through the normal review/temporal owner while preserving source evidence and decisions.
- [ ] 4.3 Redefine exposure collision audit from approved facts and predecessor/supersession lineage: report lost or overwritten distinct legs under the legacy key, but accept correctly coexisting purchase/consumption and consumer publications.
- [ ] 4.4 Replay reconstructable exposure successors through the current publication classifier and promotion gates; retain unreconstructable items as reason-coded held gaps without generic system promotion.
- [ ] 4.5 Make audit/apply reports include issue evidence, provenance classification, before/after current projections, stable affected IDs, and explicit `would_change`, `changed`, `unchanged`, `held`, and `failed` counts; audit remains provably zero-write and apply remains idempotent per instrument.
- [ ] 4.6 Add temporary-database tests for official controller without control history, proven aggregate inference, ambiguous provenance, legacy relationship basis locations, valid multi-action exposure coexistence, true legacy collision, repeated apply, partial failure isolation, and immutable evidence/history.

## 5. End-to-end acceptance and rollout

- [ ] 5.1 Resolve the current relationship regression suite failures by aligning implementation and tests with the canonical status contract; do not weaken the explicit `disclosed_name_only` approval rule to satisfy stale expectations.
- [ ] 5.2 Run focused shareholder, company-profile provider, relationship, temporal, exposure publication, resolver, review, API, scheduler, and DCF suites; all failures in these owned paths must be classified and blocking regressions fixed.
- [ ] 5.3 On a copied database, run audit before apply and verify sampled official/aggregate controllers, short-name/full-name relationships, cross-year cohorts, ended records, purchase/consumption legs, legacy publications, and industry-only DCF contexts against before/after APIs and valuation lineage.
- [ ] 5.4 Permit production apply only after copied-database evidence shows no official controller deletion, no valid relationship/exposure loss, no network/LLM access, and idempotent second apply; otherwise keep production in audit-only mode.
- [ ] 5.5 Verify `/run business_profile_backfill`, scheduler job IDs, public API routes, database paths, approved-only defaults, and configured source authority remain compatible and no second write owner is introduced.
- [ ] 5.6 Update the current runbook with canonical relationship statuses, full-name authority, audit-only safety, apply prerequisites, backup/rollback, held semantics, and copied-database evidence; archive the change only after every unchecked task and strict OpenSpec validation pass.
