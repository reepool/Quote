## Why

The authoritative refined twenty-report shadow replay completed only 15 of 20 reports: five provider calls failed, four responses ended in terminal schema failures, five successful extracts exceeded the requested 20,000-token budget (maximum 40,673), and one successful call took 259,482 ms. Evidence routing is now corrected provider-free, so another cohort replay would be wasteful until the existing gateway and Stage 5 adapter have bounded, tested behavior for repair requests, oversized segment output, and truthful output-budget diagnostics.

## What Changes

- Make schema-repair requests preserve a provider-acceptable structured-output payload while remaining one bounded repair attempt under the original execution deadline.
- Distinguish a complete locally valid JSON response whose reported usage exceeds the requested output budget from a genuinely truncated, unparsable, or schema-invalid response; keep both outcomes explicit in diagnostics.
- Permit deterministic splitting only for a real oversized `extract_segment_financials` scope, using stable report-local partitions and one request lineage, then validate and combine the partition results locally without lowering fields, Evidence, schema, verification, or subject requirements.
- Preserve typed provider, transport, deadline, parse, and schema failures without adding unbounded retries or increasing timeouts.
- Prove the behavior with bounded fixture and integration tests before authorizing one separate new-ID twenty-report replay.
- Keep all outputs research-only with `production_authorization=not_authorized`; do not open Stage 6, approved writers, scheduler/backfill, commodity exposure, value-chain publication, or DCF.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `common-llm-gateway`: Clarify provider-compatible repair payloads and output-budget/truncation diagnostics under the existing bounded retry and deadline contract.
- `company-profile-bounded-semantic-workflow`: Allow deterministic report-local partitioning of only oversized segment-financial scopes while preserving the original semantic and verification contracts.
- `company-profile-manufacturing-materials-shadow-batch`: Require provider-free and bounded integration evidence for execution stability before a separate full-cohort replay can be admitted.

## Impact

- Affected code: `utils/llm/client.py`, `research/company_profile/stage5_provider.py`, and the existing Stage 5/shadow execution owner if partition orchestration is required there.
- Affected tests: common LLM gateway tests and manufacturing/materials Stage 5 provider/workflow tests using fake transports and frozen oversized-scope fixtures.
- No public API, database, scheduler, production configuration, semantic vocabulary, Gold expectation, model route, token budget, timeout, or immutable historical bundle is changed by this change.
