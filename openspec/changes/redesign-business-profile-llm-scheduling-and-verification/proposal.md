## Why

The current business-profile backfill already performs joint extraction, but verification still expands each report into many record-level LLM calls. A recent ten-company run made 105 verification requests, waited as long as an hour for queue admission, and degraded under rate limits even though only a small number of reports were being processed. The workflow needs one coherent concurrency authority, durable stage boundaries, and a verification contract that uses deterministic program checks first and LLM assistance only for genuinely ambiguous records.

## What Changes

- Make the common LLM gateway the single authority for in-flight requests across all business-profile workers and providers.
- Keep acquisition, parsing, extraction, validation, verification, and publication as resumable durable queue stages.
- Keep one report-level joint extraction request for activities and named relationships, and include the other requested field families in the same bounded report context where feasible.
- Replace per-record and follow-up verification calls with at most one batched verification request per report and field family for records classified as ambiguous.
- Make deterministic validation evaluate schema, evidence membership, issuer/period, numeric and unit semantics, conflicts, duplicates, and catalog versions for every extracted record.
- Make the program, rather than model confidence, decide `validated`, `verified`, `held`, or `rejected` publication state.
- Scope exception backlog and recovery gates to the current instrument, report, field family, and processing identity so one company cannot block unrelated work.
- Use a bounded admission timeout, provider execution deadline, retry-after aware backoff, circuit breaking, and durable retry scheduling; classify contract errors separately from gateway congestion.
- Add a versioned offline evaluation set of local annual-report section artifacts and expected labels for repeatable regression and throughput measurement.
- Preserve existing public API, CLI, queue, and database paths unless implementation requires a compatible additive field.

## Capabilities

### New Capabilities

- `business-profile-llm-processing`: Defines the efficient, resumable extraction, deterministic validation, batched ambiguity verification, publication, retry, and offline evaluation contract.

### Modified Capabilities

None. Existing gateway and business-profile behavior is consolidated through the new capability contract without intentionally changing public API shapes.

## Impact

- Affects `research/business_profile_semantic_runtime.py`, `research/business_profile_semantic_pipeline.py`, and `research/business_profile_async_production.py`.
- Affects shared LLM orchestration in `utils/llm/orchestration/` and provider settings in `config/13_llm.json`.
- Adds local evaluation fixtures and tests; no live provider calls are required by tests.
- Existing durable work items and published records remain readable; in-flight work is resumed or reclassified through compatible stage identities.
