## Context

The immutable refined replay produced 1,348 accepted research facts with complete Evidence traceability, but manual review found 19 wrong chapter-source selections, three over-broad regime legal-empty interpretations, one generic material-input relationship, and two critical regime contradictions. The existing owner chain is sufficient: `ShadowEvidencePlanner` chooses source scopes and `CommonGatewaySemanticProvider` normalizes bounded extract results before the existing Stage 5 validator and verifier.

This change corrects those current-result defects without another provider run. It must preserve the frozen twenty-report manifest and both historical batches, retain the same six chapters and field IDs, and keep production authorization closed.

## Goals / Non-Goals

**Goals:**

- Route each shadow chapter to a source section that can own a chapter-level positive or legal-empty conclusion.
- Reject broad `business_regime=not_applicable` conclusions when Evidence only addresses statistical calibre or affirmatively reports a consolidation/control change.
- Reject a generic cost category as a named material input unless the cited Evidence identifies a specific procured or consumed item.
- Prove the correction provider-free against all 23 noncritical and two critical reviewed findings from the refined replay.

**Non-Goals:**

- Tuning prompts beyond the two closed semantic guards, changing models, output budgets, timeouts, repair sequencing, or gateway behavior.
- Running an LLM, rewriting either shadow batch, changing Gold, or claiming scale readiness from provider-free checks.
- Adding a generalized routing engine, ontology, confidence system, policy registry, new semantic field, or production writer.

## Decisions

### 1. Encode closed chapter ownership in the existing shadow planner

The planner will retain its current chapter configuration and section selector but distinguish strong owning signals from incidental text matches. Overview requires a principal-business or business-model passage with direct main-business wording; segments require a segment/revenue-cost disclosure rather than a parent income statement; material inputs require procurement/material/risk wording; counterparties require a governed customer/supplier section; and regime requires an explicit business-change, consolidation-scope, control-change, or restructuring disclosure.

Bare occurrences of `主营业务`, `客户`, `供应商`, `营业收入`, `在建工程`, or `合并` inside financial-statement notes, balance-sheet variance commentary, generic risk text, or unrelated tables do not own chapter-level legal-empty coverage. Candidate scope ranking will prefer owner-bearing ranges and planning will fail before provider access when a required chapter has no owner.

Alternative rejected: create a new selector service. The existing selector already supplies page text, section keys, table signatures, and reasons; the defect is the local admission/scoring policy.

### 2. Keep positive source recall and legal-empty authority separate

A selected page may contain a useful fact without being authoritative for a whole chapter. The planner may retain a source-positive scope when it contains the requested fact, but it must not use a non-owning page to conclude that the chapter is `not_disclosed` or `not_applicable`. Required owner checks remain provider-free and report-local.

Alternative rejected: accept every lexical match and rely on the LLM verifier. The reviewed replay proved that complete traceability does not prevent a semantically wrong legal-empty conclusion when the wrong section is selected.

### 3. Add two narrow normalization guards before Pydantic acceptance

For `extract_business_regime`, normalized source text and returned coverage are checked together. A `not_applicable` result is invalid when the cited Evidence affirmatively marks consolidation-scope/control change as applicable or describes an inclusion, establishment, acquisition, transfer, or equivalent change. A principal-business statistical-calibre checkbox can close only that narrow question and cannot by itself close the broader regime field. The guard rejects the response through the existing schema/repair path; it does not synthesize an event or mutate coverage.

For `extract_material_inputs`, a returned Relationship whose name is only a generic category such as `材料`, `原材料`, `燃料`, or `能源` is invalid unless the same Evidence directly identifies that expression as a specific named procured or consumed input. Explicit items such as electricity, steam, iron ore, coal, natural raw materials, or synthetic raw materials remain eligible when source-supported.

Alternative rejected: silently drop invalid candidates. Silent removal would hide a model error and could produce a false legal-empty result.

### 4. Bind validation to the archived 25 findings without adding sample rules to runtime

Focused fixtures will load the archived review package/outcomes and classify the already reviewed errors into four closed families: 19 routing, three statistical-calibre scope errors, two regime contradictions, and one generic input. The corrected planner will rebuild the same twenty-report plan without provider calls. The audit will require every formerly wrong routing scope to be replaced by an owner-valid scope or a typed pre-provider failure, and every semantic fixture to be rejected by the corresponding pure guard. Correct reviewed examples remain accepted by regression tests.

The audit records source artifact hashes, counts by family, unresolved findings, provider calls equal to zero, and `production_authorization=not_authorized`. It does not predict report usability or precision.

### 5. Keep repair/output-budget stabilization separate

The refined replay also exposed five output-limit overruns and provider rejection on repair paths. Those are execution-stability P2 defects, but changing them here would obscure whether correctness improvements came from source routing or transport behavior. A following change will address only bounded output/repair behavior, after this change is archived.

## Risks / Trade-offs

- **[Owner rules become too strict and miss a valid disclosure]** → Required chapters fail before provider access; conditional chapters retain legal-empty only when an owning disclosure was actually checked. Add signals only from the frozen reviewed corpus.
- **[A phrase looks like a change but is only accounting language]** → The regime contradiction guard requires an applicability/change combination or a closed event phrase, not a bare occurrence of `合并`.
- **[Generic terms are valid named inputs in some report]** → Permit them only when the same bounded Evidence explicitly treats the term as the named procured or consumed item; otherwise leave the candidate unresolved.
- **[Provider-free success is mistaken for production readiness]** → Audit explicitly reports no provider calls and retains research-only authorization; another complete replay remains separately authorized only after execution-stability fixes.

## Migration Plan

1. Add owner-aware selection/ranking and narrow semantic guard fixtures.
2. Rebuild the frozen twenty-report plan provider-free and generate the 25-finding correction audit outside historical batches.
3. Run focused tests, scoped Ruff, `git diff --check`, and strict OpenSpec validation.
4. Archive and push this correction. Rollback removes only the new rules/tests/audit; historical plans and batches remain unchanged.

## Open Questions

None. Output-budget and repair-path behavior is intentionally deferred to the next change.
