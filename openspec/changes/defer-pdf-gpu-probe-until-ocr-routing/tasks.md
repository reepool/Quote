## 0. Reconcile the Existing PDF Capability Baseline

- [ ] 0.1 Confirm `optimize-shared-pdf-native-and-gpu-routing` and its predecessor PDF changes are reconciled/archived first; verify their native-worker isolation, renderer, version-matched GPU/CPU runtime, and bounded parallelism requirements remain in the current capability baseline before applying this follow-up delta.

## 1. Lock the Regression Contract

- [ ] 1.1 Add a failing shared-PDF test proving that valid GPU approval plus a missing/failing worker does not block `resolve_profile()`, default `PdfParseRequest`, `build_router()`, or native text extraction and does not invoke `--probe`.
- [ ] 1.2 Add selective/force OCR tests proving that only a non-empty uncached OCR target set triggers the live probe and that complete OCR cache hits do not probe.
- [ ] 1.3 Add result-contract tests for `ocr_runtime_unavailable`/`ocr_unavailable`, retained native pages/candidates, partial document status, and force-OCR `selected_method=none` without changing serialized fields.

## 2. Separate Approval from Runtime Readiness

- [ ] 2.1 Refactor GPU canary validation so `resolve_profile()` and `build_router()` enforce approval flag/report/corpus/profile checks but perform no subprocess/runtime health probe; preserve the evaluation-only approval bypass semantics.
- [ ] 2.2 Move the existing successful-probe cache, lock, configurable cold-start timeout, and detailed failure diagnostic behind a lazy Paddle adapter readiness call without importing CUDA Paddle in Quote.
- [ ] 2.3 Keep explicit evaluator/canary capability probing fail-closed for CUDA, model, version, protocol, and cache-health failures even though router construction is side-effect free.

## 3. Preserve Recovery, Budget, and Provenance Semantics

- [ ] 3.1 Invoke lazy GPU readiness only after native routing, policy limits, and OCR page-cache lookup produce uncached OCR pages.
- [ ] 3.2 Convert missing/unhealthy GPU readiness to the existing `ocr_runtime_unavailable` pre-inference failure class and route it through the configured version-matched CPU fallback allowlist.
- [ ] 3.3 Make probe, render, GPU invocation, and CPU fallback share the original remaining document deadline; cap probe timeout by both configured cold-start timeout and remaining budget.
- [ ] 3.4 Normalize page/document status and provenance so unavailable recovery is not mislabeled `ocr_empty`, successful CPU fallback records the primary GPU failure/reason/runtime, and completed native/cached pages survive.

## 4. Close the Shared-Router Caller Gap

- [ ] 4.1 Migrate `OfficialIndexLifecycleParser.extract_pdf_text()` from direct `PdfRouter()` construction to `build_router(request.profile)` while preserving its public input/output.
- [ ] 4.2 Add caller regressions for the official-index path and HKEX text PDF under an approved GPU profile with a failing worker; confirm existing business callers need no request-interface changes.
- [ ] 4.3 Confirm the explicit corporate-action custom profile/adapter remains outside global GPU-profile inheritance and no other named-profile production caller constructs a direct router.

## 5. Documentation and Final Verification

- [ ] 5.1 Update `shared_pdf_processing.md`, `pdf_ocr_worker_runbook.md`, and deployment guidance to distinguish static approval from lazy runtime readiness and document native-only behavior during GPU outages.
- [ ] 5.2 Absorb accepted incident requirements and remove `pdf_gpu_probe_gate_proposal_20260828.md` after current specs/runbooks contain the authoritative rules.
- [ ] 5.3 Run strict OpenSpec validation, shared PDF tests, HKEX/official-index tests, business-profile PDF tests, announcement-classification tests, and broker-risk-control PDF tests.
- [ ] 5.4 Review the implementation for public model/config compatibility, absence of eager probe subprocesses, bounded fallback, single shared router ownership, and preservation of pre-existing worktree changes; then commit and push only this change's files.
