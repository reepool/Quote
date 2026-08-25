## 1. Reconcile Existing Contracts and Evidence

- [ ] 1.1 Confirm the final implemented models from `shared-pdf-processing` and `shared-pdf-page-recovery-contract`; map ordered engines and isolated runtimes onto the existing router, candidate, budget, cache, and provenance owner without adding another PDF entry point, and record how their unarchived capability baselines will be canonicalized/rebased before this change archives without taking ownership of company-profile tasks.
- [ ] 1.2 Create a separate read-only native-promotion manifest rather than reusing `pdf_page_recovery_acceptance_manifest.json` unchanged; preserve all source PDFs and production data.
- [ ] 1.3 Add 600036.SH physical pages 1, 2, 19, 41, 51-62, 195, and 350 with verified source-form gold, including `招商銀行`,正文 Chinese headings, key numbers, page continuity, and selected table/read-order checks; do not use simplified cover text as gold.
- [ ] 1.4 Use bounded read-only probes to add non-empty Chinese/numeric/read-order gold for 000717.SZ, scanned-page Chinese/numeric gold for 001322.SZ, and both native-unusable and PDFium-native-usable negative-OCR pages for 002376.SZ.
- [ ] 1.5 Version the new corpus/page/gold identity and document current limitations for Hong Kong annual reports, encrypted PDFs, and AcroForm/XFA documents.

## 2. Deliver the Independently Releasable PDFium Native Slice

- [ ] 2.1 Add direct `pypdfium2==5.13.0` dependency and implement a PDFium native adapter that opens each PDF once per adapter request, extracts assigned physical pages in stable order, and returns deterministic text/hash, page count, version, timing, and typed failures.
- [ ] 2.2 Replace `native_engine`/`alternate_native_engine` router branching with a validated ordered native engine tuple that sends failed pages to each later adapter in one grouped adapter attempt and short-circuits per page at the first usable candidate.
- [ ] 2.3 Update the technical quality gate so document-level `mixed` never triggers OCR, and Chinese-expected pages with numeric/ASCII residue but insufficient Chinese/script evidence cannot become usable native candidates.
- [ ] 2.4 Preserve every native attempt's actual engine/version, text hash, quality, usability, diagnostics, and elapsed time; include chain order/config and engine versions in cache identity and serialization.
- [ ] 2.5 Add unit/contract tests for malformed mapping, normal Chinese/English expected-script behavior, scanned/empty pages, mixed-page OCR suppression, table/numeric reading order, out-of-range pages, stable ordering, grouped fallback extraction, and one-open-per-adapter-request behavior.
- [ ] 2.6 Run PDFium versus `pypdf` on the expanded native-promotion manifest and record every fidelity, false-routing, compatibility, table/read-order, and latency gate; promote only if all mandatory accuracy/compatibility gates pass before latency ranking.
- [ ] 2.7 Canary and activate the PDFium-first native profile with `pypdf` fallback and a configuration-only `pypdf`-first rollback; verify 600036.SH target正文 pages are native-selected and do not invoke OCR.

## 3. Retire Inspector from Production Native Routing

- [ ] 3.1 Inventory `PdfInspectorNativeAdapter`, `PdfInspectorOcrAdapter`, `detect_pdf_bytes`, evaluator profiles, production configuration, supported operator tools, tests, and direct imports as distinct inspector roles.
- [ ] 3.2 Migrate production native callers, retire `pdf_inspector_paddleocr` and inspector `alternate_native_engine` wiring, and update focused profile/router tests without changing still-supported evaluation/classification/OCR behavior.
- [ ] 3.3 Remove `pdf-inspector` from `requirements.txt` only if every supported role is empty; otherwise retain the package with a documented non-native owner and propose later retirement separately rather than silently uninstalling it.

## 4. Align the Authoritative OCR Runtime and Renderer

- [ ] 4.1 Record Paddle/PaddleOCR 3.3.1/3.7.0, model/inference configuration, CUDA 11.8, PDFium 5.13.0, and GRID P4 as the authoritative runtime baseline; mark the 2.6.2/2.7.3 PyMuPDF lab stack as comparative-only evidence.
- [ ] 4.2 Extend `PaddleOcrAdapter` with one versioned worker protocol and separate isolated GPU (`paddlepaddle-gpu==3.3.1`) and CPU (`paddlepaddle==3.3.1`) environments outside Quote conda; do not add a parallel OCR adapter or business loop.
- [ ] 4.3 Refactor the Quote-side rendering stage to open PDFium once per OCR batch and render both GPU and CPU inputs at profile-bound 150 DPI (`scale=dpi/72`) by default; workers receive equivalent rendered images and never open PDFs or import PyMuPDF.
- [ ] 4.4 Implement worker protocol validation and capability/model-health probes for runtime/model/inference versions, device, driver/CUDA, compute capability, memory, persistent writable cache, and process health; Quote must not import CUDA Paddle for probing.
- [ ] 4.5 Add tests that reproduce/fail closed on worker crash, `SIGILL`/invalid optimization configuration, startup/model failure, malformed response, timeout, empty OCR result, non-writable cache, and isolation from the known Quote conda `libz`/`inflateReset2` conflict.

## 5. Implement GPU-First with Version-Matched CPU Fallback

- [ ] 5.1 Add versioned PDFium-first GPU, CPU-only, and OCR-disabled profiles; treat the old `pypdf_paddleocr_gpu_canary` approval/profile as migration-only runtime evidence, not approval for the new profile.
- [ ] 5.2 Implement configured GPU-to-CPU fallback classes through the existing adapter while sharing the original page/request budgets, deadlines, target-page whitelist, and completed partial results.
- [ ] 5.3 Fix `force_ocr` behavior: validate the PDF/page count, retain at most the first native diagnostic candidate, skip later native fallbacks, OCR every valid explicit target page, and reject an empty target set.
- [ ] 5.4 Extend provenance and cache identity with renderer/version/DPI/image configuration, runtime/device, Paddle/PaddleOCR, model, and inference config; prove GPU, CPU, old profile, and new profile cache entries cannot collide.
- [ ] 5.5 Add integration tests for healthy GPU selection, permitted pre-inference CPU fallback, default no CPU retry after GPU page timeout, exhausted-budget behavior, partial-page retention, bounded `force_ocr`, and no OCR after usable PDFium text under selective recovery.

## 6. Expanded Canary, Documentation, and Rollout

- [ ] 6.1 Run new PDFium-rendered GPU and CPU evaluations on the expanded hash-bound corpus; record identical input identity, Chinese/numeric/table/read-order fidelity, candidate selection, typed failures, cache behavior, per-page/document P50/P95, and resource usage.
- [ ] 6.2 Issue a new profile/corpus/renderer/runtime/config-bound approval only if GPU-first passes all gates; do not reuse the old narrow `pypdf_paddleocr_gpu_canary` approval and do not use the PyMuPDF 15.18-second lab result as the production SLA.
- [ ] 6.3 Verify 600036.SH is recovered natively on the expanded正文 set, native-usable mixed pages never enter OCR, scanned pages use bounded GPU OCR, CPU fallback stays within the same budget, and no report receives implicit full-document OCR.
- [ ] 6.4 Enable GPU-first OCR only after the new approval, and exercise rollback to CPU-only/OCR-disabled plus `pypdf`-first native routing without caller API changes.
- [ ] 6.5 Update `docs/development/shared_pdf_processing.md`, current profile/config documentation, acceptance/canary references, and runbook so only the isolated-worker CUDA model is current; record worker/cache and GRID licence owners and delete superseded profile/CUDA instructions after migration.
- [ ] 6.6 Run focused shared-PDF and integrated consumer regressions, review for duplicate execution paths or unrelated scope, resolve blocking findings, and archive the completed change after the native and GPU milestones are both delivered.
