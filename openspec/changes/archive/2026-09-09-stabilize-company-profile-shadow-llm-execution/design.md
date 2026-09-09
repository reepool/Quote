## Context

The refined twenty-report shadow replay used the existing report-local semantic owner, Gemini logical route, 20,000 extract-token budget, 18,000 verify-token budget, 300-second request deadline, and bounded repair behavior. It produced five `provider_unavailable` calls, four terminal schema failures, five successful extracts above the requested output budget (maximum 40,673 tokens), and a maximum successful latency of 259,482 ms. The later provider-free change closed the reviewed Evidence-routing and regime defects, so execution stability is now the next P2 blocker before another cohort replay.

The common gateway owns transport, structured-output parsing, schema repair, deadlines, and generic diagnostics. The Stage 5 provider owns compact company-profile prompts and local expansion; the Stage 5 semantic service remains the sole extract/repair/verify workflow owner. This change must preserve those owners and must not create a shadow-only semantic loop.

## Goals / Non-Goals

**Goals:**

- Make the one existing schema-repair attempt acceptable to OpenAI-compatible `json_object` providers without weakening local validation.
- Classify complete valid responses that exceed the requested output budget separately from provider truncation or invalid output.
- Reduce structurally high-cardinality segment responses through deterministic field partitions, preserving every requested field and the same Evidence.
- Keep physical provider-call accounting, typed failures, deadlines, and immutable request lineage truthful.
- Prove the behavior with fake-transport tests, frozen real-scope fixtures, and one bounded provider integration probe before any twenty-report replay.

**Non-Goals:**

- Changing Gemini/Grok/GLM selection, model names, token budgets, timeouts, retry counts, temperature, Gold, semantic enums, Evidence selection, or verification policy.
- Retrying a successful oversized response, stitching historical runs, or importing candidates from another model.
- Adding a generalized chunking framework, confidence platform, provider-specific client, scheduler path, or second semantic owner.
- Running the twenty-report cohort or granting production authorization in this change.

## Decisions

1. **Repair uses one appended user correction envelope.** The gateway keeps the original structured-output mode and schema instruction, but the repair payload appends one `user` message containing the local validation error and the complete prior response. It does not append a `system` message after an `assistant` response. This preserves the existing one-repair limit and shared deadline while avoiding a role sequence rejected by some OpenAI-compatible providers. A repair response is still parsed and validated against the unchanged local schema.

2. **Output-budget status follows response validity, not usage alone.** A response that parses and validates locally is returned as success even when provider-reported output usage exceeds the requested field; it receives the explicit warning `provider_output_budget_exceeded_valid_response`. A response whose finish reason indicates a token limit and whose content cannot be parsed or validated is classified as `response_truncated`, preserving finish reason, requested budget, observed usage when present, and attempt lineage. Missing usage does not fabricate an over-budget result. No successful response is automatically retried merely because the provider ignored its advertised limit.

3. **Only high-cardinality segment extraction is partitionable.** Before a segment extract call, the Stage 5 adapter estimates source cardinality from numeric occurrences in the bound Evidence. A scope is eligible only when the task is `extract_segment_financials`, at least two output-bearing metric fields are unresolved, and the frozen threshold is met. Eligible requests are partitioned by metric field while repeating `segment_dimension` and the identical report-local Evidence. Other chapter tasks and low-cardinality segment scopes remain one call.

4. **Partitions merge before full model expansion.** Each physical response uses a derived request ID and the minimal schema for its metric subset. Compact segment rows are merged by source dimension, source label, period, subject fields, and Evidence IDs; cell maps must be disjoint and conflicts fail closed. Coverage is merged only for the partition's fields. The combined compact response is then normalized once and validated as the original `ExtractResponse`, preventing duplicate Segment objects or cross-partition semantic reconciliation.

5. **Physical calls stay visible under one logical extract.** Stage 5 traces may contain consecutive partitioned `extract` calls for one logical workflow extract, with partition index/count and parent request ID. Scope validation collapses consecutive trace call types when comparing them with the semantic workflow's logical `extract/repair/verify` sequence. Provider-call budgets are consumed immediately before every physical gateway call, including partitions and repairs, so the batch ceiling remains real.

6. **Long latency remains diagnostic, not a reason to extend deadlines.** Existing attempt and total deadlines remain unchanged. Successful latency is retained as observed; attempt timeout, total deadline, DNS, HTTP/provider, parse, truncation, and schema failures remain separately typed. This change adds no retry loop and no timeout increase.

7. **Validation precedes replay.** Unit tests use fake transports. Frozen real-scope tests use archived shadow Evidence but no provider. One bounded live integration probe verifies that an intentionally invalid first `json_object` response can reach the corrected repair request and return schema-valid JSON through the configured Gemini route. The probe records only safe hashes, model, latency, usage, warnings, and typed errors. Passing it authorizes a later separate change/run to perform one new-ID twenty-report replay; it does not itself authorize that replay or production.

## Risks / Trade-offs

- **[Partitioning increases physical calls]** → Limit it to high-cardinality segment extraction, keep a fixed metric partition count, and charge every call to the existing batch budget.
- **[The numeric-cardinality threshold is an imperfect output predictor]** → Freeze it from the actual oversized segment fixtures, test both eligible and non-eligible scopes, and do not generalize partitioning to other chapters.
- **[A row differs across partitions]** → Require identical row identity fields and disjoint cells; any conflict becomes a typed schema failure rather than a guessed merge.
- **[Provider reports inflated reasoning/output usage for valid JSON]** → Preserve the valid response and explicit warning; do not treat reported usage alone as truncation.
- **[A live repair probe is affected by DNS/provider availability]** → Record the typed infrastructure result and do not substitute a twenty-report run, different model, or relaxed payload.
