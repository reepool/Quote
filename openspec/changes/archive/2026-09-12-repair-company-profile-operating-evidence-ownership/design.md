## Context

The September 11 Evidence-role replay completed all twenty shadow reports without a
transport failure, but seventeen operating-quantity scopes still failed local schema
validation. The dominant shape was not truncation: a direct table page and its bounded
context were assigned every field inferred from the combined scope, then the Stage 5
request builder copied every Evidence item across every field. Context-only pages could
therefore appear to own capacity, production, sales, or inventory fields they did not
support, and unsupported legal-empty coverage caused fail-closed rejection.

The existing shadow planner owns page selection, `Stage5EvidencePreparer` owns prepared
scope construction, and `ManufacturingMaterialsProfileSliceService` owns semantic
requests. This change keeps that chain and corrects only field ownership at those
existing boundaries.

## Goals / Non-Goals

**Goals:**

- Infer operating fields only from field-specific text on direct candidate pages.
- Preserve bounded adjacent pages as interpretation context without letting them add or
  own numeric fields.
- Carry explicit field bindings into prepared Evidence and make Stage 5 respect those
  bindings.
- Preserve legacy Stage 5 behavior for scopes whose Evidence has no explicit bindings.
- Prove the correction provider-free against the frozen twenty-report inputs and all
  seventeen previously failed operating scopes.

**Non-Goals:**

- No LLM call, prompt/schema relaxation, dynamic token implementation, timeout change,
  model comparison, historical bundle mutation, Gold change, or semantic acceptance
  change.
- No generalized Evidence-ownership registry, new execution service, production writer,
  Stage 6, approved table, scheduler/backfill, commodity exposure, value chain, or DCF.

## Decisions

1. **Separate field inference from scope context.** For operating scopes, field IDs are
   derived from `candidate_pages` only and from the existing field-specific text
   patterns. Section keys, selector reasons, anchor terms, and adjacent context do not
   add an operating numeric field. This retains the existing selector while removing
   the confirmed source of over-expansion.
2. **Bind direct Evidence at preparation time.** The shadow preparer expands a direct
   page into one `PreparedEvidence` item per actually supported scope field. Pages that
   are present only for table continuation or interpretation remain unbound
   (`field_id=None`). Evidence IDs and source text remain unchanged.
3. **Respect explicit bindings with a legacy fallback.** `_field_bound_evidence` filters
   and returns explicit bindings when any are present. Only a scope with no explicit
   binding at all uses the historical page-by-field expansion, preserving approved
   Stage 5 fixtures and non-shadow callers.
4. **Keep legal-empty validation fail closed.** The correction removes false ownership;
   it does not allow context-only Evidence to support legal-empty coverage or weaken the
   provider/local validators.
5. **Use a provider-free comparison audit.** Rebuild the plan and prepared scopes from
   the frozen manifest/PDF artifacts, compare affected field sets and bindings, and
   record retained positive capacity/quantity owners. The authoritative replay bundle
   remains immutable.
6. **Defer token adaptation.** Observed extract/verify sizes support a later bounded
   tiering policy, but all over-budget warnings succeeded and all relevant failures were
   schema failures. Token tuning is therefore not part of this causal fix.

## Risks / Trade-offs

- **[A valid operating field appears only on a continuation page]** → table-context
  fixtures must prove that a continuation forming the same direct table is retained as
  an owner only when its own text contains the field-specific signal; otherwise it stays
  context-only and cannot create unsupported coverage.
- **[Field-specific matching becomes too strict]** → provider-free comparison must retain
  known capacity and production/sales/inventory tables, and focused positive fixtures
  cover these source shapes.
- **[Explicit and legacy Evidence are mixed]** → once any explicit binding exists, only
  explicitly bound active fields are owners; unbound items remain context-only. This is
  deliberate for shadow scopes and tested separately from the all-unbound legacy path.
- **[The plan improves but report readiness remains hold]** → close this change on the
  provider-free causal proof; a later bounded replay may measure semantic impact, but is
  not authorized here.
