## Why

Company-profile semantic fallback calls are reaching the shared LLM gateway, but provider output limits are not enforced reliably and local validation failures are collapsed into generic reasons. A single malformed or unsupported row can therefore discard a whole field-family result without enough telemetry for automatic retry or diagnosis.

## What Changes

- Record safe request, usage, finish-reason, budget, schema, and evidence-validation diagnostics for company-profile semantic attempts.
- Add bounded company-profile extraction behavior that distinguishes empty output, provider timeout, JSON/schema failure, and row-level evidence failures.
- Preserve individually valid structured rows while routing only invalid rows or incomplete families to machine rework; never weaken exact-evidence gates.
- Add regression tests for provider budget overruns, timeout responses, empty rows, and mixed valid/invalid structured rows.

## Capabilities

### New Capabilities

- `business-profile-llm-acceptance`: Auditable, bounded, evidence-gated acceptance of structured company-profile LLM results.

### Modified Capabilities

## Impact

- `research/business_profile_semantic_extraction.py` and `research/business_profile_semantic_runtime.py`.
- Shared LLM response diagnostics in `utils/llm` without changing the public request API.
- `config/11_llm.json` only where a profile-specific bound is required.
- Company-profile unit tests and semantic checkpoint metadata; no raw prompts, responses, credentials, or database schema changes.
