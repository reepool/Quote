## Context

Company-profile structured fallback already uses the common `LlmClient`, the same provider coordinator, JSON parsing, and schema validation as corporate-action analysis. The production failure is downstream: the provider can exceed requested completion budgets, structured rows are validated as one atomic batch, and runtime exception handling retains only a broad reason code. Corporate-action analysis demonstrates the desired pattern of bounded prompts, independent validation, and persisted safe call metadata.

## Goals / Non-Goals

**Goals:**

- Keep transport, authentication, rate limiting, concurrency, and provider handling in the shared LLM gateway.
- Preserve exact-evidence, numeric, unit, issuer, and report-period gates.
- Accept valid rows from a mixed structured response and route rejected rows to deterministic machine rework.
- Persist safe diagnostics sufficient to distinguish provider, schema, empty-output, and exact-evidence failures.
- Make budget-overrun and timeout behavior observable and resumable.

**Non-Goals:**

- Do not accept unsupported facts or weaken promotion rules.
- Do not persist raw prompts or raw model responses.
- Do not change corporate-action extraction behavior or database schemas.
- Do not solve provider-side token accounting by truncating JSON responses.

## Decisions

1. The shared gateway remains generic. Workload-specific limits stay on `LlmRequest` and semantic policy objects; business validators remain outside `utils.llm`.
2. Structured company-profile validation returns accepted rows plus bounded rejection diagnostics. Top-level JSON/schema/scope failures remain atomic; row-local evidence and numeric failures are isolated.
3. A mixed response succeeds only for its accepted rows and records `partial_row_rejection`. A response with no accepted rows remains machine rework, preserving fail-closed behavior.
4. Runtime passes an audit sink to the extractor and persists sanitized diagnostics with rework exceptions. Diagnostics include identifiers, usage, finish reason, warnings, failure category, and bounded row error codes/messages, but no raw content.
5. Provider output-budget overrun remains a warning in the common gateway because some reasoning providers count hidden tokens. Company-profile policy treats the warning as an efficiency diagnostic while schema and row bounds remain the correctness boundary.

## Risks / Trade-offs

- [A provider can keep charging beyond the requested token budget] -> Track overrun explicitly, keep requests smaller, and retain the option to select a stricter workload profile without coupling business code to a provider.
- [Partial acceptance could hide systemic model errors] -> Persist rejection counts and reasons and keep the field family in machine rework when no valid row survives.
- [Diagnostic payloads can grow] -> Bound row diagnostics and exclude raw prompts, sections, and responses.
- [Existing checkpoints lack detailed diagnostics] -> New retries populate the new metadata; no destructive migration is required.

## Migration Plan

Deploy code and tests without a database migration. Existing retryable semantic work resumes normally and writes richer checkpoint/exception metadata on its next attempt. Rollback restores the previous atomic rejection behavior; persisted extra metadata remains backward-compatible JSON.

## Open Questions

None for implementation. Provider/model replacement remains an operational decision after measuring the new diagnostics.
