## Context

The shared PDF module already owns one named-profile router, an ordered `pypdfium2 -> pypdf` native chain, supervised native workers, page-level OCR recovery, GPU-to-CPU fallback, page cache identities, and isolated PaddleOCR workers. Commit `8eac643` added a process-local successful GPU probe cache, a configurable 60-second cold-start timeout, and diagnostic logging.

The remaining defect is sequencing. `resolve_profile()` and `build_router()` call `_require_gpu_canary_approval()`, which also calls the live worker probe. This occurs before `PdfRouter.parse()` knows whether native extraction succeeds, whether OCR is disabled by request policy, or whether all OCR pages are cache hits. A GPU runtime dependency therefore blocks unrelated native work. The 2026-08-28 HKEX prolonged-suspension daily run exposed this coupling.

This change modifies the existing `shared-pdf-processing` and `shared-pdf-page-recovery-contract` capabilities. It does not create a new routing layer or alter business persistence. Framework governance mapping is FR-15: all affected callers remain on the shared technical capability.

The delta is based on the completed `optimize-shared-pdf-native-and-gpu-routing` contract, including supervised native-worker isolation and bounded widths `1`, `2`, `4`, `6`, `8`, and `10`. That prerequisite change and its predecessor PDF changes must be reconciled/archived before this follow-up is archived. This change modifies only probe timing, related fallback/result semantics, and the remaining named-profile caller gap; unchanged native routing, isolation, rendering, runtime-version, and parallelism requirements remain authoritative.

## Goals / Non-Goals

**Goals:**

- Allow an approved GPU profile to resolve and build its router without starting or probing the OCR worker.
- Keep native-only requests independent of live GPU readiness without changing caller code or public result models.
- Probe only when at least one uncached physical page is about to enter GPU OCR.
- Preserve fail-closed GPU qualification, version-matched configured CPU fallback, original budgets, diagnostics, candidates, cache identity, and provenance.
- Move the remaining official-index named-profile consumer onto `build_router(request.profile)`.
- Preserve `8eac643` successful-probe caching, cold-start timeout configuration, and detailed diagnostics.

**Non-Goals:**

- Changing `PdfParseRequest`, `PdfProfile`, `PdfDocumentResult`, or page artifact schemas.
- Automatically enabling OCR for callers such as HKEX that request `ocr_mode="none"`/`native_first`.
- Changing HKEX partial-source lifecycle decisions or any business write owner.
- Adding background health services, negative-health persistence, a new worker protocol, another PDF router, or a new capability.
- Importing CUDA Paddle in Quote or allowing implicit full-document OCR.

## Decisions

### 1. Split static approval from live readiness

`resolve_profile()` will continue to resolve the named profile, bind environment-supplied worker commands/cache paths, reject unknown/disabled profiles, and validate the GPU approval flag/report/corpus/checks. The approval validator will not invoke a subprocess. A missing command or unhealthy live runtime is not a static profile-definition failure; it becomes a typed OCR runtime failure only if OCR is selected.

`build_router()` will enforce the same static approval rule for production GPU profiles but will not probe. Its existing evaluation-only approval bypass remains an approval bypass, not an implicit health check.

Alternative rejected: pin native-only business callers to `pdfium_native`. That duplicates routing policy in consumers and prevents the same profile from supplying bounded recovery when a caller explicitly requests OCR.

### 2. Put lazy readiness at the existing Paddle adapter boundary

The runtime guard will execute from `PaddleOcrAdapter.extract_pages()` (or its external-worker path) only after it receives a non-empty page set. `PdfRouter` already applies native selection, OCR policy, page/queue limits, and cache lookup before calling the adapter, so this placement naturally avoids probes for `native_first`, `ocr_mode="none"`, native-usable pages, empty OCR target sets, and complete OCR cache hits.

The adapter will use the existing versioned `--probe` command and will never import Paddle. Successful probe results remain cached process-locally by worker command, runtime, model-cache path, and device. Failures remain retryable on a later OCR request and retain their diagnostic text. No new health registry is introduced.

Alternative rejected: probe in `PdfRouter` as soon as `allow_ocr` is true. That would still probe when native recovery succeeds or every selected OCR page is served from cache.

### 3. Treat probe failure as an existing pre-inference fallback class

A failed/missing GPU probe will produce `_OcrWorkerError("ocr_runtime_unavailable", diagnostic)` or an equivalent internal typed error and enter the existing configured fallback path. When the version-matched isolated CPU fallback is configured, the same already-selected pages may run there within the remaining original budget. A successful CPU candidate records `ocr_primary_runtime_failed`, `fallback_from_runtime`, and `fallback_reason`.

If fallback is disabled, disallowed, unavailable, or out of budget, each affected page returns diagnostic `ocr_runtime_unavailable`, page quality status `ocr_unavailable`, `selected_method=none`, and the retained native candidates. Other usable native pages remain selected and the document is explicitly partial. Under `force_ocr`, a usable native candidate remains diagnostic provenance but is not silently reselected after OCR failure.

Alternative rejected: introduce `ocr_runtime_unhealthy` without updating page status handling. The existing fallback allowlist and document failure taxonomy already use runtime-unavailable semantics; reusing them avoids an incompatible third spelling and prevents probe failure from being mislabeled `ocr_empty`.

### 4. Charge readiness to the original OCR document budget

The router will pass the remaining effective document budget into OCR execution without changing the public request type. Probe timeout is the smaller of `QUOTE_PDF_GPU_PROBE_TIMEOUT_SEC` and that remaining budget. Probe, rendering, primary invocation, and configured CPU fallback share one monotonic deadline; no stage resets it. Completed/cached pages remain retained when the budget expires.

The capability probe used by offline canary evaluation remains separately bounded by its evaluator budget and explicitly invoked by evaluation code.

### 5. Keep canary evaluation explicit

Production profile resolution validates committed approval evidence but does not prove current liveness. Offline/runtime canary functions will explicitly call the worker probe and continue to fail their canary when CUDA, model health, protocol, or version checks fail. `allow_unapproved_gpu_canary=True` continues to permit evaluation of a candidate before approval; router construction itself stays side-effect free.

This preserves two distinct questions:

```text
profile approval:  Is this configuration authorized?  (resolve/build)
runtime readiness: Can selected OCR pages run now?      (adapter/canary)
```

### 6. Preserve business interfaces and close one direct-router gap

HKEX, business-profile, announcement-classification, and broker-risk-control callers retain their current request construction. Their default native behavior changes only by no longer waiting for GPU readiness.

`OfficialIndexLifecycleParser.extract_pdf_text()` currently combines a default-profile request with a direct `PdfRouter()` instance, bypassing shared adapter construction. It will use the same `request = PdfParseRequest(...)` then `build_router(request.profile).parse(request)` pattern already used by other native consumers. The intentional corporate-action custom adapter remains out of scope because it supplies an explicit custom profile and OCR adapter rather than selecting a named shared profile.

## Risks / Trade-offs

- [An approved GPU profile can resolve while the worker is down] -> This is intentional for native availability; actual OCR remains fail-closed with typed diagnostics and configured bounded fallback.
- [A cached successful probe can outlive a worker failure] -> The actual OCR subprocess invocation still detects startup/crash/protocol failures and follows the same typed fallback policy; this change does not claim the cache is permanent readiness.
- [Concurrent first OCR requests can contend on the probe] -> Retain the existing lock and successful-result cache; do not add a distributed health/cache service in this change.
- [Probe time consumes a large part of a short OCR request] -> Cap it by the remaining request deadline and return an explicit budget/runtime diagnostic rather than resetting budgets.
- [A new diagnostic spelling could bypass fallback/status handling] -> Standardize on existing `ocr_runtime_unavailable`/`ocr_unavailable` semantics and add exact status/provenance tests.
- [Removing the one-off proposal loses useful production context] -> First migrate accepted requirements to these specs and current runbooks; Git history preserves the incident note.

## Migration Plan

1. Confirm `optimize-shared-pdf-native-and-gpu-routing` and its predecessor PDF deltas are reconciled/archived without losing native-worker isolation, GPU/CPU runtime, renderer, or parallelism requirements.
2. Add regression tests that demonstrate the current eager-probe failure and required native-only behavior.
3. Split static approval validation from live readiness and make router construction side-effect free.
4. Add lazy adapter readiness with existing cache/timeout/diagnostic behavior, budget accounting, and configured fallback.
5. Normalize page/document status and provenance for probe failures.
6. Migrate the official-index caller and run targeted consumer regressions, including an HKEX text fixture under an approved GPU profile with a failing worker.
7. Update current shared-PDF/runbook documentation, absorb and remove the one-off proposal note, then run PDF and affected consumer regressions.

Rollback is a normal source rollback to the prior shared module; no data, schema, environment variable, profile name, or business caller migration requires reversal.

## Open Questions

None. The approval/readiness split, fallback ownership, diagnostic vocabulary, budget ownership, and caller compatibility are fixed by the existing contracts and this change.
