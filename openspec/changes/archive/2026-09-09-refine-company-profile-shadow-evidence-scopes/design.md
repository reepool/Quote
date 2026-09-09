## Context

The first twenty-report shadow batch used the existing PDF artifact, section selector, Stage 5 semantic owner, verifier, and immutable batch store successfully, but the planner copied one chapter-wide field tuple into every selected scope. That made a revenue table responsible for segment identity, revenue, cost, and margin even when only some columns were present, and made customer-only pages responsible for supplier coverage. The resulting request contract created 127 `required_coverage_missing` reviews, duplicated field-bound Evidence, and inflated model output without indicating that the source documents lacked the underlying facts.

The authoritative execution chain remains `ShadowEvidencePlanner` → `Stage5EvidencePreparer` → `ManufacturingMaterialsProfileSliceService`. This change refines the first step and the provider-free audit only. It does not introduce another semantic loop or change the field, Evidence, verification, projection, or production-authorization owners.

## Goals / Non-Goals

**Goals:**

- Assign each generated scope only the existing Stage 5 fields positively supported by that scope's governed section keys, table signatures, anchors, headers, and source text.
- Preserve all mandatory report-level chapter fields while allowing optional fields to be absent from a request when the selected Evidence provides no source signal for them.
- Keep an owning table header, unit, footnote, and required continuation pages together inside the existing three-page scope bound.
- Produce a provider-free replay audit comparing the frozen v1 plan with the refined plan using exact field/scope counts and field-bound Evidence payload size.
- Leave the September 9, 2026 provider-bearing batch and all historical bundles immutable.

**Non-Goals:**

- Changing semantic objects, field IDs, subject rules, verifier blockers, report acceptance policy, Gold, or human-review decisions.
- Running an LLM, repairing the first shadow batch, or claiming that provider-free improvements prove report usability or semantic precision.
- Adding a confidence service, schema registry, generalized planner framework, new PDF/OCR engine, or production write/promotion path.
- Retrying the three observed provider-unavailable scopes or tuning gateway timeout/output limits in this change.

## Decisions

### 1. Use a closed field-signal table inside the existing shadow planner

Each existing field receives a small set of source-native signals appropriate to its chapter: governed section keys, known table-signature IDs, header/anchor terms, and bounded text patterns. The planner evaluates these signals per selected scope, not across the whole chapter. It then emits the stable chapter-order subset whose signals are present.

Required chapter fields (`business_overview_source`, `segment_dimension`, and `business_regime`) must have at least one owning scope. If direct field evidence cannot be established, planning fails before provider execution instead of silently deleting the requirement. Conditional fields are included only when their source signal is present. The one-field material-input chapter remains unchanged.

Alternative rejected: ask the LLM to choose fields. That would spend provider calls before the request contract is known and could make coverage depend on model output. Alternative rejected: add new table-specific field IDs. The observed problem is assignment granularity, not a missing semantic object.

### 2. Keep report-level completeness separate from scope-local extraction

The union of emitted scope fields is the chapter's planned coverage surface. A field can appear in multiple scopes only when each scope independently contains a positive source signal, such as separate customer and supplier tables using the shared `counterparty_relationship` field. A scope is not populated with unrelated chapter fields merely to make the union complete.

The existing report benchmark continues to aggregate resolved required coverage across all scopes in a chapter. Scope-local schema, verification, Evidence, or prohibited-inference failures remain blockers; this change does not turn them into caveats. Provider-free audit checks that every required chapter field has an owner and that every emitted field has a recorded positive signal.

Alternative rejected: suppress every `required_coverage_missing` review after execution if another scope succeeded. That would rewrite semantic results and could hide a genuinely incomplete source occurrence. Correct assignment should prevent the mechanical mismatch before the request is sent.

### 3. Bind continuation context before choosing bounded scopes

Scope construction treats pages containing continuation markers as belonging to a table-context group. A continuation page must retain its preceding owning page; markers requiring the next page must retain that page as well. Header, unit, and footnote detection runs over the complete group. If the owning context cannot fit the existing three-page request bound or is not present in governed selected pages, planning emits `table_context_incomplete` before provider invocation.

Alternative rejected: append pages after model output. That would make Evidence selection answer-dependent and would invalidate the frozen page hashes.

### 4. Measure mechanical request reduction without provider access

The replay audit loads the immutable v1 manifest/plan, builds a new versioned plan from the same hash-bound PDFs, prepares both plans through the existing preparer, and compares:

- report, chapter, scope, page, and Evidence traceability counts;
- field assignments per scope and total field/scope pairs;
- field-bound Evidence copies and serialized character volume;
- scopes with unsupported emitted fields or missing required field ownership;
- table-context validation outcomes.

The audit must show zero unsupported assignments, zero missing required owners, 100% Evidence traceability, and a material reduction in field-bound Evidence copies/characters relative to v1. It records the exact change rather than predicting a semantic-pass percentage. The old plan, batch manifest, report files, and readiness audit are read-only inputs.

### 5. Defer provider-bearing validation to a separately authorized full run

This change ends after the provider-free replay and focused verification. A later change may freeze a new unseen cohort or explicitly authorize one complete replay with a new run identity. It must not run targeted scopes or splice old/new candidates. `production_authorization=not_authorized` remains the only allowed authorization state.

## Risks / Trade-offs

- **[A narrow signal misses a valid disclosure]** → Required fields fail planning; conditional omissions are surfaced in the audit and later unseen validation rather than guessed. Add only signals demonstrated by the frozen corpus.
- **[A broad term assigns the wrong field]** → Require chapter-specific conjunctions/table signatures and test negative examples such as revenue tables without margin and customer pages without supplier facts.
- **[Long tables exceed the three-page bound]** → Fail with `table_context_incomplete`; do not silently truncate or expand the request contract.
- **[Provider-free size reduction does not improve semantic outcomes]** → Report it only as a mechanical improvement. Semantic usability and precision require a later complete provider-bearing validation.
- **[Changing shared Stage 5 models reinterprets old plans]** → Keep existing plan schemas loadable and avoid required model fields; version only the new generated shadow plan and audit.

## Migration Plan

1. Add and test scope-specific field derivation and table-context grouping in the current shadow planner.
2. Generate a new versioned Evidence plan and provider-free replay audit from the frozen manifest without modifying v1 artifacts.
3. Run focused unit tests, provider-free preparation, Ruff, and strict OpenSpec validation.
4. Preserve both old and new artifacts for comparison. Rollback is removal of the new plan/audit and reversion of the local planner change; no production or provider output is modified.

## Open Questions

None. Provider model selection, transport tuning, semantic policy, and production promotion remain explicitly outside this change.
