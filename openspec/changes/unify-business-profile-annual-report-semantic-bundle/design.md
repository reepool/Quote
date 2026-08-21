## Context

The production pipeline already resolves the management-discussion chapter, caches one page artifact per PDF during a run, persists selected-section artifacts, and promotes field families independently. Its semantic phase nevertheless creates different selections and LLM requests for `atomic_activities` and `named_relationships`. Both requests typically contain overlapping descriptions of the issuer's business, products, operating model, customers, and suppliers.

The active rollout remains `structured_shadow`; this change prepares the later semantic phases without bypassing their readiness gates. Annual-report files are supplied only by `research/announcement_assets`.

## Goals / Non-Goals

**Goals:**

- Select the annual-report subsections that carry the highest-value business and supply-chain evidence.
- Reuse one content-addressed selected-section bundle and one validated LLM response across both semantic field families.
- Persist the model JSON before local conversion so retries are deterministic and do not spend more model calls.
- Keep source labels, values, and units unchanged in model output; keep normalization, calculations, roles, and exposures in program logic.
- Preserve independent field-family completion, promotion, and rollback behavior.

**Non-Goals:**

- Sending a whole annual report to the LLM.
- Combining deterministic structured-table fallback with the narrative semantic request.
- Allowing the LLM to assign canonical product IDs, value-chain roles, commodity directions, normalized units, confidence, or calculated values.
- Changing the active rollout phase, public commands, shared announcement-asset ownership, or database schema.

## Decisions

### Use one explicit semantic input family

Introduce an internal `annual_report_semantic_bundle` selection family containing the union of relevant subsection keys for activities and relationships. The selected artifact remains bounded by the resolved management-discussion chapter and the existing page and character limits. Both output field-family work items reference the same content-addressed artifact.

This is preferred to merging two independent LLM payloads after selection because a single ranked page budget must choose among overlapping chapters consistently. It also avoids creating a new queue stage or public field family.

### Expand the common annual-report subsection vocabulary

The common disclosure template will recognize:

- report-period industry context;
- principal business, products, uses, and application areas;
- operating and profit models;
- revenue/cost and main-business analysis;
- production, sales, inventory, procurement, order, and cost information;
- major customers and suppliers.

Industry-specific templates continue to add their existing resources, reserves, projects, and operating tables. Matching remains deterministic and chapter-scoped; aliases locate evidence but do not become extracted facts.

### Return both atomic output families in one closed JSON schema

The joint request returns `activities[]` and `relationships[]` together. Each accepted row contains a Chinese semantic summary, source-native labels/objects, raw values and units where disclosed, and one or more supplied `evidence_span_ids`. Local code resolves those IDs to immutable page text and rejects unsupported identifiers.

The existing single-family extractor remains callable for compatibility tests and narrowly scoped tools, but production semantic work uses the joint family. The prompt and schema receive a new version so an old single-family response cannot be mistaken for a joint result.

### Persist once and project independently

The joint response is stored in the existing semantic-artifact repository under the internal input family and exact document hash, selected evidence scope, request hash, prompt version, and schema version. An in-run cache prevents the sibling work item from repeating repository work; durable replay covers restarts and retries.

For an `atomic_activities` work item, runtime conversion consumes only `activities[]`. For a `named_relationships` work item, it consumes only `relationships[]`. Each then follows its existing verification, exception, run, and bundle persistence path. Value-chain roles and commodity exposure remain deterministic downstream derivations from approved atomic activities.

### Preserve bounded automated recovery

A joint response may legitimately have an empty `relationships[]`, while an annual report with no activity remains a missing-context condition under existing rules. The runtime therefore preserves field-family-specific empty-result semantics even though both arrays came from one response. Failed local conversion does not invalidate or discard the persisted model response.

Independent verification is also resumable at target granularity. A token limit is a soft, per-field-family, per-stage-run guard checked before the next network request; it is not a per-call output limit or a lifetime cap for the company. Each partial verify artifact retains completed target decisions. A retry reuses those decisions, starts a fresh bounded batch for unfinished targets, and keeps cumulative token and call totals observable. The guard remains enabled so a provider or retry defect cannot create unbounded spend.

## Risks / Trade-offs

- [A joint request has a larger output schema] -> Keep the existing input/output bounds, deduplicate pages, and cap each output array independently.
- [A relevant subsection uses an unrecognized heading] -> Keep table signatures, industry templates, context pages, and the existing bounded missing-context expansion.
- [One field family succeeds while the other fails] -> Persist the joint response first and retry only the failed field-family conversion.
- [Concurrent workers race on the same report] -> Use content-addressed `INSERT OR IGNORE` persistence and an in-run cache; duplicate validated receipts converge to one exact artifact identity.
- [Prompt/schema change invalidates old semantic completion] -> Only semantic-phase identities change; PDF page assets and selected source data remain reusable.

## Migration Plan

1. Deploy the template, selector, extraction-contract, replay, and runtime changes while retaining `structured_shadow` as the active phase.
2. Run focused selection, semantic extraction, artifact replay, and runtime tests with representative annual-report subsection layouts.
3. When the normal rollout reaches `semantic_shadow`, run a bounded batch and confirm one joint LLM call per document plus two independently completed field families.
4. Roll back by reverting code and prompt identity; stored joint artifacts remain immutable audit data and no canonical records or PDF assets need deletion.

## Open Questions

None. Rollout promotion remains a separate operator decision based on existing readiness gates.
