## Why

The first twenty-report shadow batch proved that the existing semantic workflow can extract 1,310 source-traceable research facts, but its automatic planner assigned every selected scope the full field set of its chapter. Pages that supported only one table or disclosure were therefore judged as if they had to satisfy the entire chapter, creating 127 `required_coverage_missing` findings, oversized requests, and excessive human review. The next production-readiness step is to make Evidence scopes describe only the fields their source pages can actually support while preserving the existing report-level chapter contract.

## What Changes

- Derive a closed, scope-specific Stage 5 field subset from governed section keys, table signatures, source anchors, and hints instead of copying the complete chapter field tuple into every scope.
- Aggregate required coverage across all scopes in a report chapter so one scope is not required to prove fields assigned to another scope; genuine report-level omissions remain blockers.
- Preserve the owning table header, unit, footnote, and required continuation pages in the same bounded scope before provider execution.
- Add a provider-free replay audit over the frozen twenty-report inputs to measure field/Evidence alignment, request-size reduction, and the expected reduction in mechanically induced coverage findings.
- Keep the existing semantic objects, verifier blockers, subject policy, Gold expectations, historical bundles, and production authorization unchanged. The prior shadow batch remains immutable and is not reinterpreted.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-manufacturing-materials-shadow-batch`: Refine automatic Evidence planning so each bounded scope carries only source-supported fields and report-level chapter coverage is evaluated across those scopes.
- `company-profile-bounded-semantic-workflow`: Require generated scope field assignments and table context to be source-bound before provider invocation without changing the semantic field contract or verification owner.

## Impact

- Affects the existing `research/company_profile/shadow_evidence.py` planner, the Stage 5 report-level coverage aggregation needed by generated shadow scopes, focused tests, and versioned provider-free replay artifacts.
- Reuses the current PDF artifacts, disclosure templates, selector, Stage 5 models, semantic service, verifier, projection, and shadow operator. It does not add a parser, field family, LLM client, retry loop, confidence platform, or production writer.
- No LLM call is authorized until the new contract and provider-free replay are complete. Any later semantic validation must use a new full cohort/run identity and must not splice or mutate the September 9, 2026 shadow batch.
