## Why

The optimized BaoSteel run completed all 18 LLM calls, but the research result remained held because four source-backed semantic facts were mapped incorrectly or blocked by an over-broad scope. These are real generalization defects in occurrence identity, field binding, request-scope separation, and subject handling; they should be corrected before another out-of-sample validation.

## What Changes

- Preserve distinct table occurrences when measured objects differ, even when values, periods, and Evidence pages are identical.
- Bind supplier purchase amounts and supplier concentration shares to supplier field families rather than customer concentration.
- Keep related-party transaction amounts, purchase/service ratios, and customer/supplier concentration as separate governed disclosure families.
- Preserve consolidation-adjustment rows and values while refusing unsupported promotion to `consolidated_group`.
- Add offline fixtures and focused regression tests for all four findings before any new LLM run.
- Run at most one new complete out-of-sample validation after the local fixes pass; retain research-only status and production non-authorization.

## Capabilities

### New Capabilities

- `company-profile-oos-semantic-mappings`: Defines bounded semantic mapping corrections discovered during out-of-sample validation.

### Modified Capabilities

- `company-profile-common-semantic-model`: Clarifies occurrence identity and source-preserving supplier/adjustment semantics.
- `manufacturing-materials-profile-research-contract`: Separates supplier, customer, and related-party field families and their allowed scope.

## Impact

- Affects Stage 5 adapter/service field mapping, occurrence conflict detection, Evidence-bound candidate expansion, and focused tests.
- Reuses the existing semantic service, projection, bundle store, and LLM route; no new gateway, parser, database, or production path is introduced.
- Existing four-report authority and BaoSteel run bundles remain immutable; no historical result is rewritten or spliced.
