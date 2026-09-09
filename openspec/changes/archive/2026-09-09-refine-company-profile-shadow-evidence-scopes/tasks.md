## 1. Scope-specific field planning

- [x] 1.1 Add a closed chapter/field signal map and derive each generated scope's stable field subset from its own governed section reasons, headers, anchors, and source text.
- [x] 1.2 Enforce positive support for every emitted field and at least one owning scope for every required chapter field before provider execution.
- [x] 1.3 Preserve historical Stage 5/OOS plans and the immutable shadow v1 plan while versioning newly generated shadow plans.

## 2. Bounded table context

- [x] 2.1 Group continuation pages with their owning header/unit/footnote context before selecting the final one-to-three-page request ranges.
- [x] 2.2 Emit the existing typed `table_context_incomplete` failure when required context is absent or cannot fit the existing scope bound.

## 3. Provider-free replay audit

- [x] 3.1 Add a strict provider-free comparison artifact for field/scope pairs, per-scope field counts, field-bound Evidence copies/characters, required-field ownership, unsupported assignments, table context, and Evidence traceability.
- [x] 3.2 Generate a refined versioned Evidence plan and replay audit from the frozen twenty-report manifest without modifying the v1 plan or provider-bearing batch.
- [x] 3.3 Confirm the replay has zero unsupported assignments, zero missing required owners, 100% Evidence traceability, and a material field-bound Evidence reduction without making any LLM call.

## 4. Verification and closure

- [x] 4.1 Add focused tests for source-specific field subsets, required-field failure, customer/supplier separation, table continuation ownership, replay identity, and legacy compatibility.
- [x] 4.2 Run focused pytest, provider-free preparation/replay, scoped Ruff, `git diff --check`, and strict OpenSpec validation.
- [x] 4.3 Review only current-task changes, fix blocking defects, publish the measured result with `production_authorization=not_authorized`, and do not start a semantic rerun or Stage 6.
