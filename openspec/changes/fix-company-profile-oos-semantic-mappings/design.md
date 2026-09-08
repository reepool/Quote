## Context

The BaoSteel out-of-sample run proved that the selected model and bounded execution parameters can complete the Stage 5 workflow. Its remaining hold findings are local semantic mapping issues: occurrence identity collapses rows with different measured objects, supplier disclosures enter a customer field contract, related-party tables contain facts outside the concentration scope, and an adjustment row is promoted without affirmative group wording.

## Goals / Non-Goals

**Goals:**

- Make the four observed mappings source-faithful and reusable across annual-report layouts.
- Keep all changes local to existing Stage 5 owners and preserve source-native values, units, periods, and Evidence.
- Prove each correction with offline fixtures and one bounded validation run only after the fixtures pass.

**Non-Goals:**

- Changing the LLM model, output budgets, gateway deadlines, or response schemas.
- Re-running historical four-report bundles or modifying Gold expectations.
- Creating a general ontology, confidence service, parser, database, or production publication path.

## Decisions

1. **Occurrence identity includes the source row/measured object.** The existing logical slot and physical anchor remain necessary, while the source row or measured object disambiguates equal-valued rows such as `铁` and `坯材`. No numeric value is changed and unrelated rows are never merged.
2. **Field binding follows the disclosed counterparty direction.** Supplier purchase amounts and shares are assigned to the supplier field family when the source heading says supplier/procurement. Customer and supplier facts remain independently projectable, subject to existing subject restrictions.
3. **Related-party transactions stay out of concentration scopes.** The existing scope contract remains narrow. Related-party transaction facts are preserved as candidates or blocked with an explicit scope reason; they are not discarded or relabelled as customer/supplier concentration.
4. **Adjustment rows retain values but not unsupported group scope.** `row_class=consolidation_adjustment` remains source-derived. The adapter keeps `subject_scope=unclear` unless the cited Evidence contains affirmative group wording or an allowed same-report reconciliation.
5. **Validation is offline first, then one complete run.** Fixtures and focused regression tests establish behavior without invoking an LLM. Only one new run ID may be used after those checks; historical bundles remain immutable.

## Risks / Trade-offs

- **[More distinct occurrences may increase candidate count]** → Use source row/measured object only when Evidence distinguishes the row; preserve existing duplicate guards for the same physical occurrence.
- **[Supplier facts may reveal a missing package field]** → Keep them in research review with explicit supplier semantics; add a separate field-family change only if later requirements need publication.
- **[Adjustment rows remain unclear]** → This is safer than an unsupported consolidated promotion and keeps the source fact available for bounded research use.

## Migration Plan

No production migration is required. Existing bundles are read-only. New code is covered by fixtures, then a new isolated run may be produced under a fresh run ID. Rollback is a code revert; no database or approved-table state changes.

## Open Questions

- Whether a future contract should add a dedicated related-party transaction chapter is deferred until a separate business requirement exists.
