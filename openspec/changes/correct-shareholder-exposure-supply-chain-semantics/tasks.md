## 1. Shareholder snapshot truth vertical slice

- [ ] 1.1 Add field-backed scope helpers that validate holder count/report date, coherent exchange-appropriate top holders, and actual ownership/control fields without trusting `coverage_scope` labels.
- [ ] 1.2 Remove first-top-holder controller inference from aggregate providers and preserve their returned top-holder data without populating `control_owner_*`.
- [ ] 1.3 Replace snapshot first-wins merging with per-scope selection by normalized report period, configured provider authority for equal periods, and deterministic completeness tie-breakers; persist selected scope source and period.
- [ ] 1.4 Normalize provider report dates before comparison/hash, isolate per-instrument provider exceptions, and reject returned security identities that do not match the requested canonical instrument.
- [ ] 1.5 Make incremental discovery reject ambiguous BSE aliases, retain missing-scope priority, and order time-based candidates newest first when bounded.
- [ ] 1.6 Rework shareholder readiness to count the per-instrument intersection of all required field-backed scopes and preserve degraded/failure status in scheduler and Telegram reports.
- [ ] 1.7 Add shareholder provider, merge, incremental, readiness, and scheduler tests for top-holder-only ownership, newer cross-source data, same-period official precedence, mixed dates, response mismatch, BSE alias collision, isolated failure, and degraded reporting.

## 2. Local shareholder read boundary

- [ ] 2.1 Identify the existing local shareholder snapshot/control-history query owner used by shareholder and company research APIs, and expose any missing narrow read method without adding a remote or LLM fallback.
- [ ] 2.2 Ensure any company-profile projection that requests top holders, actual controller, or control method reads only eligible local records at the knowledge cutoff and reports an explicit local-data gap when absent.
- [ ] 2.3 Add query/API tests that fail if shareholder profile reads instantiate a provider, call CNInfo, call another remote source, or invoke an LLM.

## 3. Supply-chain relationship vertical slice

- [ ] 3.1 Stop generating approved aliases from unique securities short names or ticker history; keep exact governed legal identities and explicitly approved aliases as the only resolved-entity inputs.
- [ ] 3.2 Add explicit relationship identity status for `resolved_entity` and evidence-backed `disclosed_name_only`, and require review decisions to preserve that distinction without requiring a global enterprise catalog.
- [ ] 3.3 Separate relationship occurrence ID from stable temporal lineage, exclude evidence ID from lineage, retain contract/source-row occurrence discrimination, and supersede prior eligible annual occurrences.
- [ ] 3.4 Deterministically classify anonymous customer/supplier concentration from label plus direction, expand real anonymous aggregate labels, and fail closed on contradictory semantics.
- [ ] 3.5 Normalize named and anonymous disclosed shares from raw numeric value/unit in program code, require a finite fraction, and reject ambiguous or out-of-range values.
- [ ] 3.6 Include activities and relationships in temporal readiness/current selection and exclude candidates from approved DCF/profile input hashes while retaining explicit diagnostic reads.
- [ ] 3.7 Add tests for external named counterparties, short-name non-resolution, explicit disclosed-name approval, later entity resolution, consecutive annual reports, same-period multiple contracts, relationship end dates, concentration direction conflicts, and percent conversion.

## 4. Commodity exposure and DCF vertical slice

- [ ] 4.1 Extend exposure lineage/predecessor identity with source fact/action class and canonical role so purchase and consumption, or other distinct legs, cannot supersede one another.
- [ ] 4.2 Canonicalize assumption aliases before lookup and include `consumer_id` in assumption-bearing publication identity; fail closed on conflicting synonymous assumptions.
- [ ] 4.3 Resolve feedstock versus energy cost through deterministic action/product catalog rules, keep unsupported hedges and unknown roles fact-only, and never default an absent role to revenue.
- [ ] 4.4 Make fact-only a terminal non-retryable publication gap that remains open and can be locally replayed after mapping/rule approval without another extraction LLM call.
- [ ] 4.5 Make publication gates derive current/candidate and semantic proof from approved component provenance instead of constants, and route legacy component migration through the same publication classifier/gates.
- [ ] 4.6 Separate industry-default mappings from executable company mappings in the resolver and make automatic DCF accept only current `approved_company_business_profile` sources with active cutoff-eligible series.
- [ ] 4.7 Replace first-diagnostic cycle selection with explicit governed role/materiality/spread selection and implement opposite economic normalization for revenue and cost legs; fail closed for mixed ambiguity.
- [ ] 4.8 Add exposure/resolver/DCF tests for purchase plus consumption, multiple consumers, assumption aliases, energy inputs, unsupported hedges, fact-only replay, industry-only context, candidate exclusion, revenue high-cycle, cost high-cycle, and ambiguous multi-series inputs.

## 5. Local audit and repair

- [ ] 5.1 Add one bounded dry-run-default operator command over the existing domain services with explicit instrument/all scope and apply flag; do not duplicate merge, review, publication, or DCF logic in the adapter.
- [ ] 5.2 Audit local shareholder snapshots for unsupported controller inference, false scope labels, mixed periods, lower-authority retained scopes, and readiness differences; emit stable IDs and before/after projections.
- [ ] 5.3 In apply mode, rebuild shareholder scopes from local raw snapshots and control history, clear unsupported inferred fields, and mark unreconstructable scopes incomplete without remote acquisition.
- [ ] 5.4 Audit and repair short-name relationship resolutions and concurrent cross-year current occurrences from persisted evidence/semantic records, preserving history and using normal review/supersession transitions.
- [ ] 5.5 Audit and replay collided exposure publications and incorrectly closed fact-only gaps from approved local facts/mappings through the corrected publisher, preserving component evidence lineage.
- [ ] 5.6 Report industry-only contexts previously labeled governed and verify corrected DCF assembly excludes them; do not persist or delete ephemeral DCF outputs as a substitute for fixing input assembly.
- [ ] 5.7 Add temporary-database tests proving audit has no writes, apply is transactional per instrument and idempotent, raw evidence is retained, and no repair path performs network or LLM access.

## 6. End-to-end verification and rollout

- [ ] 6.1 Run the focused shareholder, business-profile activity/relationship, exposure publication, resolver, API, and DCF unit/integration suites and resolve only blocking regressions introduced by this change.
- [ ] 6.2 On a copied local database, run repair audit then apply for representative controller/top-holder, external counterparty, cross-year relationship, purchase/consumption, energy-cost, and industry-only DCF cases; compare before/after API and valuation lineage.
- [ ] 6.3 Verify existing scheduler job IDs, `/run business_profile_backfill`, shareholder/profile/exposure API routes, and default approved-only behavior remain compatible and no second write owner or implicit remote shareholder path was added.
- [ ] 6.4 Review the complete change against all three capability specs, document corrected versus intentionally retained historical records, and update operator/current documentation with dry-run, apply, backup, rollback, and result interpretation.
