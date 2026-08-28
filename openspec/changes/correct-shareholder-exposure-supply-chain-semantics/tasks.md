## Implementation checkpoint (2026-08-28 recheck)

The current code already contains useful corrections that must be preserved: field-backed shareholder scope checks, removal of first-holder controller inference, per-scope authority selection, BSE alias collision rejection, separate purchase/consumption/consumer exposure identities, fact-only publication gaps, company-only executable mappings, cost/revenue cycle direction, and explicit `disclosed_name_only` review.

The change is not complete. The tasks below are the remaining end-to-end work after the recheck. A checked task means the revised requirement and its focused tests are complete, not merely that an earlier partial implementation exists.

## 1. Complete the shareholder write path

- [x] 1.1 Use one report-date normalizer for provider payloads, per-scope comparison, content hashing, persisted report dates, and readiness; prove equivalent compact/ISO dates do not produce changes.
- [x] 1.2 Reuse the full-sync canonical `instrument_id`/symbol/exchange response guard in incremental and force-merge paths, and validate provider-returned identity fields when the upstream response exposes them.
- [x] 1.3 Isolate efinance and other sequential aggregate-provider exceptions per instrument, record the failed instrument/source attempt, and continue the remaining exchange batch.
- [x] 1.4 Fix bounded incremental candidate ordering so `max_candidates > 0` cannot fail and deterministically keeps missing-required-scope candidates before newest time-based candidates.
- [x] 1.5 Make merged top-level source/source-mode/raw provenance agree with the per-scope selections by explicitly representing a primary or composite snapshot; preserve attributable raw provenance for every selected scope.
- [x] 1.6 Preserve application `degraded` semantics through scheduler and Telegram adapters as warning/partial completion, without returning or displaying unqualified success.
- [x] 1.7 Add focused provider, merge, incremental, hash, readiness, and scheduler tests for response mismatch, one-instrument failure, equivalent dates, bounded ordering, composite provenance, and degraded reporting.

## 2. Complete the local shareholder read boundary

- [x] 2.1 Identify and document the existing local snapshot/control-history query owner used by shareholder and company-research APIs; add only the narrow missing query projection rather than a second acquisition path.
- [x] 2.2 Project top holders, actual controller, and control method at the requested knowledge cutoff from eligible local records with source, report date, and availability date; return an explicit local-data gap when absent.
- [x] 2.3 Add API/query tests that fail if company-profile shareholder reads instantiate a provider, access CNInfo/another remote source, or invoke an LLM.

## 3. Complete relationship identity and current-state semantics

- [x] 3.1 Build counterparty entities from governed local company full names and official identifiers at the cutoff; never assign `instruments.name`, ticker history, or short names to `legal_name`; retain only explicitly approved aliases.
- [x] 3.2 Reverse the production resolver regression test: a unique securities short name remains unresolved, a governed full legal name resolves, and a clearly disclosed external name remains `disclosed_name_only` without requiring a global enterprise directory.
- [x] 3.3 Expand deterministic anonymous aggregate classification for real top-customer/top-supplier labels before production, while keeping generic `关联方` unclassified unless evidence supplies the missing counterparty/direction semantics.
- [x] 3.4 Implement two-level temporal identity: stable business lineage excludes evidence/source-row occurrence, latest eligible report cohort wins, and contract/evidence-independent row discriminators preserve multiple occurrences within that cohort.
- [x] 3.5 Apply explicit half-open validity and the configured relationship freshness window consistently in repository as-of selection, resolver output, API current/history projection, and readiness.
- [x] 3.6 Ensure approved profile and DCF reads exclude candidates by default while separately requested diagnostic APIs can still return them without changing executable fingerprints.
- [x] 3.7 Add tests for local full-name resolution, unique short-name non-resolution, explicit disclosed-name approval, aggregate labels with omitted anonymous flag, generic related-party ambiguity, consecutive reports, same-period contracts, evidence-derived row keys, explicit end dates, and stale relationships.

## 4. Complete exposure publication and DCF isolation

- [x] 4.1 Make DCF assembly request an approved-only business profile (`include_candidates=False`) and prove candidate-only changes leave profile/model/executable/valuation fingerprints unchanged.
- [x] 4.2 Enforce recognized `economic_role` at every executable DCF consumer and return an input gap for absent/unknown roles rather than defaulting to revenue; remove or correct any reachable legacy fallback.
- [x] 4.3 Verify canonical assumption aliases are resolved before identity/selection and make conflicting synonymous values fail closed without last-write-wins behavior.
- [x] 4.4 Route legacy exposure componentization through the current publication classifier and promotion gates; do not allow direct generic system promotion to create an approved executable successor.
- [x] 4.5 Retain industry defaults only as non-executable diagnostics and retain fact-only gaps until a local mapping/rule replay succeeds without another extraction LLM call.
- [x] 4.6 Add focused exposure/resolver/DCF tests for distinct purchase/consumption legs, multiple consumers, assumption conflicts, unknown roles, candidate fingerprint isolation, legacy gate enforcement, industry-only context, revenue/cost cycles, and ambiguous multi-series inputs.

## 5. Implement one bounded local audit and repair flow

- [x] 5.1 Add one repair application service with `audit` and `apply`; expose it through a thin operator adapter with dry-run default, explicit instrument/all scope, and no duplicated merge/review/publication logic.
- [x] 5.2 Audit shareholder snapshots for inferred controller fields, unsupported scope labels, mixed/noncanonical periods, retained lower-authority scopes, incoherent top-level provenance, and readiness differences; emit stable IDs and before/after projections.
- [x] 5.3 In apply mode, delegate shareholder reconstruction to the corrected local merge/query owners, clear unsupported control fields, preserve attributable raw provenance, and mark unreconstructable scopes incomplete without remote acquisition.
- [x] 5.4 Audit and repair short-name entity resolutions, evidence-split relationship lineages, stale/concurrently-current report cohorts, and explicit disclosed-name status through normal review/temporal transitions while retaining all evidence history.
- [x] 5.5 Audit and replay collided or incorrectly promoted exposure publications from approved local facts/mappings through the corrected publisher, including legacy component successors and fact-only gaps.
- [x] 5.6 Report industry-only contexts previously labeled governed and verify subsequent DCF assembly excludes them; do not persist/delete ephemeral DCF outputs as a substitute for correcting input assembly.
- [x] 5.7 Make audit provably read-only and apply transactional per instrument, idempotent, reason-coded, and explicit about `changed`, `unchanged`, `held`, `deleted_duplicate`, and `failed`; preserve source evidence/valid history, delete only proven unreferenced machine-derived duplicates, and perform no network or LLM access.
- [x] 5.8 Add copied/temporary-database tests covering audit zero writes, repeated apply, partial failure isolation, raw/evidence retention, no remote/LLM calls, and stable repair IDs.

## 6. End-to-end verification, cleanup, and rollout

- [x] 6.1 Run focused shareholder, local profile query, relationship, temporal, exposure publication, resolver, API, scheduler, and DCF suites; resolve blocking regressions only.
- [x] 6.2 On a copied local database, run repair audit then bounded apply for representative controller/top-holder, external counterparty, short-name, cross-year relationship, purchase/consumption, energy-cost, legacy publication, and industry-only DCF cases; compare before/after APIs and valuation lineage.
- [x] 6.3 Verify existing scheduler job IDs, `/run business_profile_backfill`, shareholder/profile/exposure API routes, approved-only defaults, database locations, and configured source authority remain compatible and no second write owner or implicit remote shareholder path exists.
- [x] 6.4 After repair cohorts show no remaining consumers, remove the superseded one-off legacy migration path or reduce it to the bounded repair service adapter; do not retain a parallel long-term legacy implementation.
- [x] 6.5 Update current operator documentation with audit/apply, backup, rollback, warning/degraded interpretation, and result fields; record intentionally retained historical rows and archive this change only after every revised requirement is verified.
