## Context

The daily task already has two reliable layers: a deterministic title filter
selects instruments to refresh, and official CNInfo structured endpoints supply
ordinary dividend and allotment values. A separate, mature workflow archives
official documents, classifies titles with an LLM, extracts semantic facts,
applies deterministic gates, and writes only governed analysis or resolution
overlays.

The missing piece is orchestration. Daily source changes and reconciliation
results are not converted into bounded inputs for the semantic workflow.
Running the semantic workflow for every scanned announcement would add cost,
latency, and model risk without improving ordinary structured distributions.

## Goals / Non-Goals

**Goals:**

- Detect daily anomalies with deterministic, auditable reason codes.
- Keep ordinary `structured_complete` events on the non-LLM fast path.
- Reuse the current document, title-classification, semantic extraction,
  verification, resume, and auto-promotion implementation.
- Preserve unmatched special announcements for later source-event association.
- Separate execution health from expected semantic review workload.
- Rebuild affected factors again when a validated overlay is promoted.

**Non-Goals:**

- Treating TDX economic terms as CNInfo values.
- Creating synthetic CNInfo source observations from announcements.
- Sending every announcement or every unchanged event to an LLM.
- Automatically approving ambiguous asymmetric or restructuring economics.
- Switching or promoting the production canonical factor source.

## Decisions

### 1. Select anomalies after structured refresh and source reconciliation

The task builds reason sets from four bounded sources:

- refreshed rows with `partial_*` quality;
- rows associated with exceptional implementation titles;
- newly inserted or changed rows involved in a material CNInfo/TDX conflict;
- exceptional implementation announcements for which the current structured
  refresh has no associable event.

The first three produce existing CNInfo `source_event_key` values and can enter
the document-resolution workflow. The last remains an instrument-level deferred
anomaly until CNInfo publishes an associable structured row. This avoids
fabricating source events while ensuring the stock is retried on later runs.

Alternative considered: analyze every selected announcement. This was rejected
because most announcements merely trigger a refresh and do not need semantic
interpretation.

### 2. Reuse explicit event-key support in the existing workflow

Explicit anomaly event keys are allowed through special-action discovery even
when the structured row is complete. Discovery performs LLM title
classification, archives implementation-grade documents, and semantic analysis
uses the existing schema and deterministic gates. Existing review and resume
filters prevent repeated model work.

Alternative considered: add a second daily-specific LLM schema and storage
tables. This would duplicate lineage, validation, and promotion behavior.

### 3. Never overwrite raw source observations

Daily semantic results retain the existing contract:

- `corporate_action_observations` remains official CNInfo structured evidence;
- model output is versioned in `corporate_action_llm_analyses`;
- validated corrections use reviewed resolution overlays;
- TDX contributes only reconciliation triggers and date evidence under existing
  policies.

### 4. Bound semantic work independently from source refresh

The daily task has a separate anomaly event cap and existing pipeline
concurrency controls. Excess event keys and unmatched announcements are returned
as deferred workload and retained as future refresh candidates.

### 5. Keep operational status separate from semantic readiness

LLM transport, document, persistence, or orchestration failures make the daily
operation partial. A successfully analyzed event that still needs human review
sets semantic readiness to partial but does not by itself make source freshness
fail. Reports expose both states.

### 6. Rebuild after a governed promotion

The ordinary targeted rebuild runs before conflict selection. If semantic
governance promotes any validated overlay, the task repeats the targeted rebuild
for anomaly instruments so the same run reflects the governed result. A
non-promoted analysis cannot change factors.

## Risks / Trade-offs

- [CNInfo publishes a special announcement before a structured row] -> retain a
  deferred unmatched anomaly and retry the instrument; do not invent an event.
- [A special title rule is too broad] -> the event cap and LLM title classifier
  provide a second relevance gate.
- [A novel special title is missed] -> incomplete structured quality and
  reconciliation conflicts remain independent triggers; title markers are
  versioned and tested.
- [LLM is unavailable] -> preserve structured data and factors, report semantic
  execution partial, and retry on the next run.
- [Historical conflicts reappear for an affected stock] -> select only events
  anchored in the daily window or explicitly changed by the current refresh.

## Migration Plan

1. Deploy the selector, configuration, reporting, and tests with canonical
   promotion disabled as today.
2. Reload the scheduler service so the new parameters and orchestration load.
3. Observe one scheduled run and verify ordinary events show zero LLM calls.
4. Verify a fixture or live special event enters the existing governed pipeline.
5. Roll back by disabling `anomaly_llm_enabled`; structured daily maintenance
   remains unchanged.

## Open Questions

None. Announcement-only semantic extraction can be added later if the project
introduces a governed event identity independent of structured CNInfo rows.
