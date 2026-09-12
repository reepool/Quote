## Context

The authoritative fast-MVP run completed all physical calls, but several model responses
contained empty arrays or omitted results for active required fields. The compact JSON
schemas allowed those responses, so the common gateway correctly recorded them as
successful and could not use its existing `schema_validation_error` failover policy.
The workflow later emitted `required_result_missing`, after provider selection had ended.

The existing common LLM client already owns retries, pool selection, finite failover,
deadlines, and lineage. The company-profile adapter owns the compact response schemas and
the full local Pydantic expansion. The fix belongs at that schema boundary.

## Goals / Non-Goals

**Goals:**

- Require a candidate or permitted legal-empty result for every active non-optional
  field before an extract response is accepted by the gateway.
- Reuse the gateway's existing schema-validation failover across the four eligible
  providers.
- Preserve optional-field omission and all existing semantic/Evidence verifier rules.
- Prove the fix with provider-free tests and one small unseen-report canary.

**Non-Goals:**

- No semantic retry after verifier rejection or unsupported inference.
- No new gateway, retry loop, confidence engine, Gold rule, or output splicing.
- No twenty-report replay, historical-bundle mutation, Stage 6, or production write.

## Decisions

1. **Express completeness in the provider response schema.** A small helper derives
   active non-optional fields from the existing checklist. Each compact schema uses
   JSON-Schema `contains`/`anyOf` constraints, or a non-empty candidate-or-coverage
   constraint for a single-field specialized response. This makes omission an existing
   `schema_validation_error` inside `LlmClient.complete`, where bounded failover already
   exists. An adapter-level second call was rejected because it would duplicate routing
   and could select the same source without shared lineage.

2. **Candidate or legal-empty coverage are equally complete.** A model is never forced
   to invent a fact. If the checklist permits `not_disclosed`, `not_applicable`, or
   `unclear`, that coverage can satisfy the field. `observed` remains derived only from
   accepted candidates.

3. **Optional fields remain optional.** Completeness constraints apply only to checklist
   items whose active requirement level is not `optional`. This preserves sparse scopes
   while closing the real required-result gap.

4. **Semantic invalidity remains outside provider failover.** A response that includes a
   field but is later rejected for Evidence, metric, subject, or prohibited inference is
   still a semantic result. This change does not cycle models to obtain a preferred
   interpretation.

5. **Validate narrowly.** First replay the known empty/partial payload shapes through
   schema validation with no provider calls. Then run one previously unseen report with
   the current four-model/default-group/dynamic-token baseline and a new immutable ID.
   The first complete canary result is final even if `hold` or `failed`.

## Risks / Trade-offs

- **[A provider has a correct partial fact but omits another required field]** → The whole
  physical response is rejected and another model may be tried; failed-attempt candidates
  are not spliced, preserving one authoritative response.
- **[A schema family cannot express exact field ownership]** → Use the narrowest safe
  candidate-or-coverage constraint and retain full local validation; do not introduce a
  new response model solely for perfect schema expressiveness.
- **[The unseen canary still holds]** → Close with the exact typed blocker; do not tune
  prompts, budgets, or models inside the same run.

## Migration Plan

1. Add provider-free completeness constraints and focused regression tests.
2. Run focused tests, Ruff, compile checks, and strict OpenSpec validation.
3. Freeze one unseen report and its existing local Evidence plan, then run once with a
   new identity through `semantic_extraction`.
4. Publish the immutable result as research-only. Rollback is removal of the schema
   constraints; no stored or production data is migrated.
