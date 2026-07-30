## 1. CNInfo Endpoint Targeting

- [x] 1.1 Add normalized endpoint-target helpers that merge profile evidence, expand uncertain targets, and preserve legacy profile-complete calls
- [x] 1.2 Make retry, recent-event, announcement, and safety-sweep candidate discovery retain CNInfo source profiles
- [x] 1.3 Route structured backfill through endpoint targets and add one bounded final transient recovery pass
- [x] 1.4 Add unit tests for target merging, endpoint-specific retry, uncertain routing, final recovery, and compatibility

## 2. TDX Reference Refresh

- [x] 2.1 Add explicit targeted/full/auto TDX refresh mode resolution and bounded rotating target selection
- [x] 2.2 Configure normal daily execution for targeted refresh and a periodic full-reference sweep while preserving manual full mode
- [x] 2.3 Add tests for mode selection, target deduplication, rotation bounds, and reference-only status behavior

## 3. Adaptive Rate-Limit Circuit

- [x] 3.1 Extend the shared adaptive throttle with sustained-density circuit opening, jittered long cooldown, and gradual recovery
- [x] 3.2 Expose rate-limit, adaptive-wait, short-cooldown, circuit-trip, and circuit-wait metrics
- [x] 3.3 Add deterministic tests for dense interspersed 403/429 responses, isolated failures, shared blocking, and slow recovery

## 4. Issuer-Lineage Reconciliation

- [x] 4.1 Suppress predecessor and non-continuous transition reference events from current-issuer reconciliation using persisted lineage metadata
- [x] 4.2 Preserve auditable suppression reasons and verify no synthetic factor crosses a no-synthetic-factor boundary
- [x] 4.3 Add generic lineage tests and a `600018.SH` regression for six predecessor events, one transition event, and 22 comparable matches

## 5. Reporting And Investigation

- [x] 5.1 Add CNInfo endpoint counts, final retry outcomes, TDX effective mode/scope, limiter metrics, and per-stage/LLM durations to structured results and reports
- [x] 5.2 Keep CNInfo primary readiness, TDX reference readiness, and reconciliation status distinct in overall task reporting
- [x] 5.3 Document the CNInfo bounded bulk-endpoint investigation and keep unverified endpoints out of production

## 6. Validation

- [x] 6.1 Run focused and regression tests, validate OpenSpec artifacts, and execute a targeted `600018.SH` dry-run
- [x] 6.2 Review all uncommitted task changes, reassess findings for scope and severity, and fix every confirmed defect

## 7. Long-Suspension TDX Reference Alignment

- [x] 7.1 Add opt-in, lifecycle-bounded next-observed-trade quote alignment while preserving conservative default callers
- [x] 7.2 Suppress terminal no-post-event-trade TDX rows from the derived path with auditable reconciliation evidence
- [x] 7.3 Add database, factor-governance, and rebuild integration regressions for long suspension, lifecycle caps, terminal events, and downstream chain recovery
- [x] 7.4 Run focused/full-market validation, validate OpenSpec, review the uncommitted diff, and fix every confirmed defect
