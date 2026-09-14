## Context

The current structured semantic path persists the LLM response before program-owned unit conversion. That is the correct authority boundary, but conversion currently snapshots runtime rules once, raises on the first unresolved row, and only schedules a later retry after a rule is auto-approved. The latest production run proved the consequences: correct rules for `艘` and an unsafe rule for `万重箱` were both announced as enabled while their originating work items still entered machine rework; `T/KL` and `套/项` blocked otherwise usable rows.

PDF page artifacts are already shared between the two structured field families within a process, but a new PDF still requires a full pypdf extraction. Raising thread concurrency from four to eight filled all eight slots without improving end-to-end throughput. Any next parser change therefore needs same-byte, same-document evidence rather than comparisons between different annual reports.

The separate `establish-shared-announcement-asset-management` change is actively taking ownership of annual-report discovery, downloading, revision selection, archive paths, and consumer access. This design must not modify that ownership boundary. A future shared asset may supply a different local path, but the business-profile parser remains content-hash bound and domain-owned.

## Goals / Non-Goals

**Goals:**

- Convert a semantic response again immediately after a newly auto-approved rule becomes effective, using the persisted response and a fresh rule overlay.
- Preserve source values and units for unresolved rows while allowing independently valid rows to proceed.
- Make deterministic known classifiers bypass the unit-proposal LLM.
- Quarantine proposals whose base-token meaning is not mechanically governed.
- Correct the known `万重箱` interpretation using a versioned deterministic mass conversion and supersede the unsafe runtime rule through the existing reconciliation lifecycle.
- Produce repeatable parser performance and fidelity evidence over identical cached PDF bytes.

**Non-Goals:**

- No annual-report discovery, download, correction winner, archive, backup, deletion, or shared-asset API work.
- No DataManager, scheduler, or announcement-asset file changes while those paths are dirty in another change.
- No production default parser-engine or concurrency change without benchmark evidence.
- No LLM-authored arithmetic, dimensional conversion, or direct publication of an unknown unit.

## Decisions

### Retry conversion inline from the persisted artifact

When conversion raises `UnitResolutionPendingError`, the runtime persists or reuses the semantic artifact, registers the unit proposal, and examines the final rule state. If the rule is effective, it reloads `overlay_rules()` and reruns deterministic conversion over the same persisted rows in the same stage invocation. This replay performs zero extraction LLM calls, does not increment the work attempt, and records an explicit inline-replay event and metrics.

The registry will keep durable replay scheduling for crash recovery and later operator corrections. Inline success may therefore coexist with an idempotent queued replay signal; the worker must resolve its current result as converted and avoid machine rework.

Alternative: wait for the next backfill batch. Rejected because it wastes a batch slot and makes successful unit notifications contradict the work result.

### Separate row conversion outcome from document conversion outcome

Structured operating rows are converted independently. A row with an unresolved unit remains in the immutable semantic response and receives a bounded conversion-pending diagnostic keyed by row identity, raw value, raw unit, evidence, and reason. It is excluded from canonical production facts until resolved. Other rows are converted and published normally.

An artifact is marked `conversion_pending` while any row remains unresolved, but this state alone does not create document-level machine rework. A later effective rule replays the artifact and requeues the owning completed work item without another extraction call. Non-unit exceptions and failed numeric reconciliation retain their existing fail-closed behavior.

Alternative: create a canonical record with null normalized fields. Rejected because production repositories and promotion gates should not treat an unresolved measurement as a canonical fact.

### Treat LLM unit proposals as hypotheses, not proof of a base token

The proof engine must verify that every dimensional primitive claimed by a proposal is lexically and dimensionally grounded in a governed token or an already effective runtime rule. Magnitude prefixes may be recomputed programmatically. An unknown classifier cannot borrow `classifier:件` merely because the model says it is count-like; such a proposal is quarantined with `unproved_source_token`.

Known classifiers `项` and `艘` are added to the deterministic catalog. `重箱/重量箱` is not a classifier: it is governed as the float-glass weight-case unit, with one weight case equal to 0.05 metric tonne and one ten-thousand weight case equal to 500 metric tonnes. The static catalog version changes, and the existing deterministic reconciliation supersedes the unsafe runtime overlay.

Alternative: require a second LLM agreement. Rejected because model agreement does not prove a physical conversion.

### Benchmark before changing parser defaults

A read-only benchmark accepts an explicit allowlist of existing local PDF paths or manifests and a fixed concurrency matrix. Each trial reads identical bytes and runs in an isolated temporary artifact root so cache hits cannot contaminate extraction timing. It records wall time, per-document CPU/wall time, throughput, peak concurrency, process RSS where available, parser warnings, page count, normalized text hash, page hashes, heading count, extraction errors, and fidelity mismatches against the pypdf baseline.

The first comparison covers pypdf concurrency 4/6/8. Optional process execution or PyMuPDF is reported only when available and must not become a runtime dependency or production default in this change. A candidate is eligible for later rollout only when page count and governed normalized-text fidelity pass and throughput improves materially on the same corpus.

Alternative: infer performance from consecutive production batches. Rejected because annual-report size and structure vary too much for a controlled comparison.

## Risks / Trade-offs

- [Static unit catalog version changes while the old queue still uses a whole-pipeline identity] -> Do not resume full-market backfill until the shared-asset consumer cutover and stage-scoped identity work are complete; use focused tests and artifact replay only.
- [Partial row acceptance may hide unresolved facts] -> Persist explicit row-level diagnostics, include unresolved counts in readiness, and replay automatically when a rule becomes effective.
- [The weight-case convention could be applied outside float glass] -> Bind aliases narrowly to `重箱/重量箱`, preserve source unit and evidence, and keep the conversion program-owned and versioned.
- [Thread/process benchmarks can disturb the host] -> Require explicit paths, bounded document/concurrency limits, temporary output, sequential matrix execution, and no production writes.
- [Other-session conflicts] -> Do not edit dirty announcement-asset, DataManager, scheduler, acquisition, archive, or shared configuration files.

## Migration Plan

1. Add deterministic unit definitions and proof guards with unit tests.
2. Add inline replay and row-level pending conversion tests using temporary databases and persisted semantic artifacts.
3. Reconcile the unsafe `万重箱` rule only through the normal deterministic rule reconciliation in a later controlled run; do not directly mutate production rows in this change.
4. Add and run the read-only parser benchmark on a bounded cached corpus.
5. Keep production parse concurrency at the benchmark-supported value of four; any future increase requires new same-corpus fidelity and throughput evidence.
6. After the shared asset change completes consumer tasks 7.1, 7.2, 7.4, 8.3, and 8.4, integrate stage-scoped processing identity and rerun the benchmark against shared assets.

Rollback consists of reverting code/catalog changes before production replay. Persisted semantic artifacts and quarantined rules remain valid audit evidence; no source PDF or canonical fact is deleted.

## Open Questions

- Whether PyMuPDF preserves enough reading order and table text fidelity for selected annual-report chapters will be decided by benchmark evidence, not by this design.
- The final production parser concurrency remains deliberately unset until same-corpus measurements are available.
