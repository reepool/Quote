## Context

The frozen external shadow replay contains 29 extract calls whose provider response reached local semantic adaptation but failed as `candidate_schema_invalid`. The report bundle persists request identity, prepared Evidence, trace identity, and the bounded local cause, but not the original provider payload. The observed causes form five families: context-only legal-empty coverage, contradictory or too-narrow business-regime legal empty, BusinessOverview text/Evidence mismatch, and ambiguous counterparty direction.

Today `_expand_extract_response` and final Pydantic validation are call-wide: one `ValueError` or item validation error propagates through `_execute`, records a failed trace, and discards all sibling items. The existing Stage 5 workflow already knows how to leave a requested field unresolved when no candidate or legal-empty coverage remains, so the smallest correct change is inside the current provider adapter and trace model.

## Goals / Non-Goals

**Goals:**

- Preserve independently valid sibling extract items from a schema-parseable response.
- Reject each unsupported item without rebinding Evidence, changing facts, or weakening existing semantic validators.
- Persist a bounded typed diagnostic for every locally rejected item.
- Leave an affected required field unresolved when isolation leaves no valid result for it.
- Prove the five observed mixed-response families without a provider call and inventory all 29 historical failure shapes.

**Non-Goals:**

- Recover or reconstruct the unavailable historical raw provider payloads.
- Change Evidence plans, prompts, models, timeouts, retries, partitions, token budgets, Gold, or source-review decisions.
- Convert a rejected item into a repaired or accepted fact.
- Rerun or mutate the frozen twenty-report bundle.
- Authorize production, Stage 6, approved storage, scheduler/backfill, commodity exposure, value-chain publication, or DCF.

## Decisions

1. **Keep one authoritative adapter path and add an isolated normalization result.** The provider-facing extract path will return normalized data plus rejected-item diagnostics from the existing `stage5_provider` adapter. The strict normalization helper remains available for direct contract tests. This avoids a second extraction service or replay engine.

2. **Only item-local failures are recoverable.** A valid response root, request identity, and JSON/schema envelope remain mandatory. Candidate/coverage expansion, known business-regime coverage checks, counterparty direction mapping, and final single-item Pydantic validation may reject one item. Root parse errors, request-id mismatch, and unbounded/unknown call-level failures still fail the call.

3. **Reuse the validators; do not sanitize invalid facts.** Each retained item passes the same expansion and Pydantic models used by the strict path. The adapter never changes a source value, unit, subject, period, actor, relationship direction, or Evidence ID to make an item pass. Known invalid business-regime legal-empty coverage is removed while any valid event sibling remains unchanged.

4. **Let the workflow derive unresolved coverage.** Rejected items are not replaced with fabricated legal-empty results. If no valid candidate or coverage remains for an active field, the existing workflow derives `unclear`/`required_result_missing` and keeps the scope or report on hold. A valid sibling for the field may still satisfy ordinary field coverage, except where an existing request-level guard independently requires an additional result.

5. **Extend traces compatibly.** `Stage5ProviderCallTrace` receives a default-empty tuple of typed rejected-item diagnostics. Each diagnostic contains the source item path, optional field ID, `candidate_schema_invalid`, and a bounded causal message. Successful calls with rejected items add a warning; historical bundles without the field continue to validate.

6. **Treat the 29-row replay as a shape audit, not raw-response recovery.** The provider-free artifact will hash and inventory every historical failed trace, classify it into the five observed families, and run equivalent mixed-response fixtures. It will explicitly state that historical salvage counts cannot be known because raw payloads were not persisted.

7. **Separate adaptive output budgets.** Output budget tiering is reasonable for large segment/table scopes, but the 29 failures are not size-correlated and six over-budget responses were schema-valid. Threshold derivation and request-time tier selection therefore belong to a later isolated change.

## Risks / Trade-offs

- **Risk: a dropped extra item can make a field appear complete through another valid item.** → Retain typed diagnostics and source-review visibility; do not mark the dropped item accepted. Existing coverage rules remain authoritative for field completion.
- **Risk: isolating arbitrary exceptions could hide a call-level defect.** → Recover only bounded `ValidationError`, `TypeError`, and `ValueError` failures at declared item boundaries; envelope and identity failures remain fatal.
- **Risk: trace schema extension breaks old bundles.** → Use a default-empty backward-compatible field and validate representative historical reports.
- **Risk: equivalent fixtures overstate historical recovery.** → Report only fixture retention and 29/29 shape classification; label actual historical candidate salvage as unavailable.

## Migration Plan

1. Add typed trace diagnostics with a default empty value.
2. Add item-isolated extract normalization inside the existing provider adapter.
3. Add provider-free mixed-response regressions and the frozen historical-shape audit.
4. Run focused and compatibility tests; do not call the LLM.
5. Archive the change after strict validation. Rollback is a normal revert; no stored bundle or production data is migrated.

## Open Questions

None for this change. Dynamic token thresholds remain intentionally deferred until their own trace-derived proposal.
