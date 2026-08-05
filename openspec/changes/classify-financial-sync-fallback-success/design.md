## Context

The incremental sync already records per-target write outcomes and source-routing counters. Its status derivation currently treats any routing diagnostic as a degraded run, even when fallback facts are successfully persisted and all targets become ready. The scheduler report also exposes counts but does not state which source supplied the final data.

## Goals / Non-Goals

**Goals:**

- Derive completion status from final collection and data-quality outcomes, not from an official-source preference miss alone.
- Preserve official-source diagnostics so operators can see CNInfo gaps, fallback use, and unresolved source problems.
- Expose a stable source classification for the final run: `cninfo`, `fallback`, `mixed`, or `none`.
- Keep existing metadata keys and report fields compatible.

**Non-Goals:**

- Changing source priority, parser mappings, or fallback selection.
- Reclassifying unresolved missing facts as successful.
- Rewriting historical ingestion-run status values.

## Decisions

1. **Separate completion from source health.** A run is successful when there are no failed writes, blocking gaps, mapping-policy gaps, pending unresolved repairs, or scan errors. Source-routing errors alone do not downgrade the run when every attempted target is ready after fallback.

2. **Compute source classification from outcomes.** The service will classify the final run from successful official and fallback counts. `mixed` is used when both contribute; `fallback` is used when fallback supplies at least one completed target and official source supplies none of the completed targets. Unresolved or empty runs use `none`.

3. **Keep diagnostics visible.** The report will continue showing CNInfo attempts/readiness and routing warnings, but render them as source-health notes when final collection succeeded. Unresolved routing errors remain warnings and still contribute to degraded status when they leave targets incomplete.

4. **Test at service and scheduler layers.** Unit tests will cover status derivation, source classification, and rendered report wording without network calls.

## Risks / Trade-offs

- [Risk] A source outage may be less prominent when fallback succeeds. → Keep an explicit source-health warning and source classification in the report.
- [Risk] Older runs lack source classification metadata. → Derive `unknown`/`none` safely when fields are absent and preserve existing counters.
- [Risk] A fallback can write partial facts. → Continue using existing readiness/blocker counts; successful status is only allowed after final readiness checks pass.
