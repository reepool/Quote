## Why

A real shadow segment scope now reaches Scorpio successfully but is rejected because the model shortens the only complete Evidence heading `报告分部的财务信息` to the ambiguous prefix `报告分部`. The validator is correct to reject that value; the bounded adapter should instead bind an unambiguous complete heading directly from controlled Evidence so model wording variance does not discard otherwise supported segment facts.

## What Changes

- Detect whether a prepared segment-financial scope contains exactly one supported, complete segment table heading.
- When that heading is unique, remove free-form `dimension` generation from the compact model row schema and bind the Evidence-derived heading locally during expansion and partition reconciliation.
- Preserve `adjustment` as the dimension for `consolidation_adjustment` rows.
- Retain fail-closed behavior when Evidence has no complete heading, multiple supported headings, an incomplete prefix, or any existing row, Evidence, period, subject, duplicate-cell, or coverage conflict.
- Prove the behavior provider-free against the frozen September 11 failure artifact before allowing one new-identity single-scope Scorpio validation.
- Keep historical bundles, Gold, production consumers, Stage 6, approved writes, scheduler/backfill, commodity exposure, value-chain publication, and DCF unchanged and unauthorized.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-bounded-semantic-workflow`: segment extraction may bind a unique complete table-owning dimension from controlled Evidence instead of requiring the model to reproduce it.
- `company-profile-manufacturing-materials-shadow-batch`: the confirmed shadow failure class is closed provider-free and, only after that proof, with one bounded external scope validation.

## Impact

- Affected code: `research/company_profile/stage5_provider.py` and focused provider tests.
- Affected contract: compact `extract_segment_financials` request/merge/expansion behavior only when controlled Evidence yields one unique complete heading.
- No public API, stored production schema, semantic vocabulary, Gold expectation, historical bundle, provider routing, or production authorization changes.
