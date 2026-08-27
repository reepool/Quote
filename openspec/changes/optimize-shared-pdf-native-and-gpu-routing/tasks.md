## 1. Reconcile Existing Contracts and Evidence

- [x] 1.1 Confirm final shared models and map ordered native engines, budgets, cache and provenance onto the existing router; document canonicalization/rebase ownership boundaries.
- [x] 1.2 Create the separate read-only native-promotion manifest.
- [x] 1.3 Add verified 600036 pages 1, 2, 19, 41, 51-62, 195, 350 and source-form gold.
- [x] 1.4 Add bounded gold for 000717, 001322, and mixed 002376 native/OCR cases.
- [x] 1.5 Version corpus/gold identity and document Hong Kong, encrypted, and AcroForm/XFA limitations.

## 2. Deliver the Independently Releasable PDFium Native Slice

- [x] 2.1 Add direct `pypdfium2==5.13.0` and one-open-per-request PDFium extraction with typed failures.
- [x] 2.2 Implement validated ordered native chain with grouped failed-page fallback and per-page short circuit.
- [x] 2.3 Make page quality authoritative; mixed classification cannot force OCR and numeric-only residue cannot pass CJK policy.
- [x] 2.4 Preserve native attempts and bind chain/config/versions into cache identity.
- [x] 2.5 Add native routing, quality, ordering, fallback, cache and one-open contract tests.
- [x] 2.6 Run expanded PDFium versus pypdf evaluation and record fidelity/latency gates.
- [x] 2.7 Activate PDFium-first with pypdf rollback; verify 600036正文 is native-selected without OCR.

## 3. Retire Inspector from Production Native Routing

- [x] 3.1 Inventory inspector native, OCR, classification and evaluation roles.
- [x] 3.2 Remove inspector from production profiles and native routing while preserving non-native roles.
- [x] 3.3 Retain the package with documented non-native ownership because supported roles remain.

## 4. Align the Authoritative OCR Runtime and Renderer

- [x] 4.1 Record Paddle/PaddleOCR 3.3.1/3.7.0, CUDA 11.8, PDFium 5.13.0, GRID P4 and comparative-only lab stack.
- [x] 4.2 Extend the existing PaddleOcrAdapter with one versioned image-only worker protocol and CPU/GPU command profiles.
- [x] 4.3 Render PDFium PNG inputs once per OCR batch at profile DPI; workers never open PDFs or depend on PyMuPDF.
- [x] 4.4 Add isolated worker capability/model-health probe and fail-closed protocol/runtime validation.
- [x] 4.5 Add worker crash, malformed response, timeout, empty result and cache/isolation tests; hardware SIGILL canary remains environment-gated.

## 5. Implement GPU-First with Version-Matched CPU Fallback

- [x] 5.1 Add PDFium-first native, CPU OCR, GPU canary, and OCR-disabled/rollback configuration profiles.
- [x] 5.2 Implement configured GPU-to-CPU fallback with shared original budgets, deadlines and page whitelist.
- [x] 5.3 Enforce bounded force_ocr semantics and reject empty target pages.
- [x] 5.4 Bind renderer, runtime/device, model and inference configuration into provenance/cache identity.
- [x] 5.5 Add integration coverage for force_ocr, selective recovery, worker fallback and no OCR on usable native pages.

## 6. Expanded Canary, Documentation, and Rollout

- [x] 6.1 Run PDFium-rendered GPU and CPU OCR evaluations on the expanded corpus after isolated GPU/CPU environments are available.
- [x] 6.2 Issue the new profile/corpus/renderer/runtime-bound GPU approval only after all gates pass.
- [x] 6.3 Verify native 600036 and mixed negative-OCR behavior; OCR execution remains page/budget bounded with no implicit full-document path.
- [x] 6.4 Enable GPU-first only after new approval and exercise rollback in a host with a healthy worker.
- [x] 6.5 Update shared PDF documentation, profile references, acceptance references and inspector role runbook.
- [ ] 6.6 Run final integrated regressions and archive after GPU milestones are delivered.

## 7. Isolate Native PDFium and Preserve Safe Parallelism

- [ ] 7.1 Add a versioned native-worker protocol for PDFium text extraction, PDFium rasterization, and pypdf fallback; accept only hash-bound read-only PDF input and serializable page results.
- [ ] 7.2 Implement one supervised `spawn`-based native worker pool owned by the shared PDF module, with configurable pool width, queue bound, worker task/restart limit, per-page/document deadlines, signal/exit capture, and no nested PDFium threads.
- [ ] 7.3 Route `PdfiumNativeAdapter`, `PypdfNativeAdapter`, and OCR batch rendering through the native worker boundary while preserving existing `PdfParseRequest`, page ordering, cache identity, provenance, and fallback semantics.
- [ ] 7.4 Convert native worker crashes (`SIGTRAP`/`SIGSEGV`/`SIGABRT`), non-zero exits, timeouts, malformed responses, and missing pages into typed diagnostics; preserve completed pages and bound retries.
- [ ] 7.5 Add focused unit/integration tests for worker protocol validation, signal/timeout containment, worker replacement, completed-page preservation, no nested pool creation, and OCR image handoff.
- [ ] 7.6 Add a read-only multi-process benchmark varying native pool width (at least 1, 2, and 4 where capacity permits) over the frozen corpus and 603268.SH/002496.SZ; run at least 20 crash-isolation rounds and record throughput, P95/tail latency, memory, queue wait, crash/restart counts, parent liveness, and page preservation.
- [ ] 7.7 Select and document the highest safe tested width; use a conservative fallback width when no parallel setting passes, and update deployment/runbook configuration without changing business callers.
- [ ] 7.8 Re-run expanded native/OCR regressions and confirm the Quote service does not restart when a native worker crashes; then complete task 6.6 and archive only after all capability deltas are reconciled.
