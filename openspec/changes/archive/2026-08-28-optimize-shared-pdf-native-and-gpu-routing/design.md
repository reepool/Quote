## Context

Two in-flight changes already own the shared PDF behavior. `establish-shared-pdf-processing-and-engine-selection` defines `shared-pdf-processing` and `pdf-engine-evaluation`; `business-profile-selective-pdf-recovery` adds `shared-pdf-page-recovery-contract`. The current implementation reflects those contracts with `native_engine` plus one `alternate_native_engine`, `pypdf_paddleocr`, `pdf_inspector_paddleocr`, and `pypdf_paddleocr_gpu_canary`. This change modifies those contracts rather than adding parallel capabilities.

The first upstream change has completed implementation artifacts but has not been archived into canonical `openspec/specs`; the second still contains company-profile tasks outside the PDF team's ownership even though its shared interface slice is implemented. These source specs are the review baseline for this change. Before this change is archived, the three modified capability baselines must exist canonically or this delta must be rebased after the owning changes are archived/split; the PDF work does not absorb unfinished company-profile tasks.

For the shared capabilities named in this change, this delta is the authoritative implementation direction. Earlier baseline wording that permits inspector as a native profile or places PDFium rendering in the Quote parent is historical context and MUST NOT be used to implement the production route after this change is accepted.

The 600036.SH 2025 annual report provides the concrete native failure. On 17 sampled pages, `pypdf` and `pdf-inspector` each produced only one usable page and 15 mapping errors; `pypdfium2` produced 16 usable pages with no mapping errors. PDFium and PyMuPDF text had mean similarity 0.9993, while PDFium's 0.31-second extraction was effectively equal to `pypdf`'s 0.30 seconds and avoided PyMuPDF's AGPL/commercial licence.

There are also two GPU evidence sets. The committed Quote canary reports PaddlePaddle GPU 3.3.1, PaddleOCR 3.7.0, Python 3.11.15, PDFium 5.13.0, CUDA 11.8, and GRID P4 passing four hash-bound one-page cases. The external lab used Paddle GPU 2.6.2, PaddleOCR 2.7.3, PP-OCRv4, and PyMuPDF rendering; it measured 15.18 seconds for 14 pages but required disabling an IR fusion path and reproduced a `libz`/`inflateReset2` crash when mixed into Quote conda. The committed canary proves the 3.3.1/3.7.0 runtime can execute on this P4, but its narrow gold does not approve the new PDFium-first routing architecture or establish the lab timing as a production SLA.

On 2026-08-27, the production service was terminated by `SIGTRAP` from `libpdfium.so` while the business-profile parse stage ran four concurrent in-process PDFium calls. The same two reports completed in single-threaded trials and reproduced the trap with four threads, without OOM evidence. This is a native-runtime safety failure: lowering a thread count is only a mitigation, not process isolation, and systemd restart cannot preserve the interrupted batch.

## Goals / Non-Goals

**Goals:**

- Reconcile the new work with the existing shared PDF capabilities and current documentation.
- Promote PDFium only from expanded正文, normal, mixed, scanned, numeric, and read-order evidence.
- Keep `pypdf` as a bounded native fallback while retiring inspector from the production native chain.
- Use one authoritative 3.3.1/3.7.0 OCR runtime family in isolated, version-matched GPU and CPU workers.
- Fix renderer, recovery-policy, cache, provenance, and fallback semantics before implementation.
- Isolate every production PDFium operation (text extraction and rasterization) from the Quote parent process while retaining bounded multi-process parallelism.
- Measure safe native worker parallelism on representative crash corpus and record the selected pool setting; do not infer it from the business-profile or LLM executor width.
- Allow the native slice to merge and run independently before GPU production enablement.

**Non-Goals:**

- Adding PyMuPDF, deploying the 2.6.2/2.7.3 lab stack, or treating its timing as a production SLA.
- Removing `pdf-inspector` while evaluation, classification, or OCR callers still require it.
- Implementing company-profile TOC/section state machines or table reconstruction.
- Supporting encrypted PDFs, AcroForm/XFA extraction, or establishing Hong Kong annual-report gold in this change; these remain declared corpus limitations, not inferred successes.
- Building a remote OCR platform, a new PDF entry point, or a new cache/database service.
- Treating a lower thread count, systemd restart, or an OCR-only child process as sufficient native crash isolation.

## Decisions

### 1. Modify the existing capabilities and profiles

The new delta specs target `shared-pdf-processing`, `shared-pdf-page-recovery-contract`, and `pdf-engine-evaluation`. The production chain becomes an ordered tuple rather than `native_engine` plus `alternate_native_engine`. Versioned replacement profiles will represent PDFium-first native-only, PDFium-first CPU OCR, and PDFium-first GPU OCR. Existing profile names remain only for a bounded migration window and are then removed with explicit rollback mappings.

This avoids a third PDF routing contract and removes contradictory language that still permits switching production to `pdf_inspector_paddleocr`.

### 2. Use page-level PDFium-first native selection

Each native adapter opens the PDF once per request/adapter attempt and extracts all pages assigned to it in ascending physical-page order. PDFium receives the requested page set first. Only pages that fail the technical gate are passed together to `pypdf`; the router does not reopen a 31 MB document separately for every page.

The quality decision is page-level. A document label such as `mixed` is diagnostic metadata only and cannot override a usable page candidate. For a Chinese-expected profile, numeric/ASCII residue without the configured Chinese/script evidence cannot be selected merely because it contains important-looking numbers. Empty text, mapping errors, suspicious scripts, replacement/control glyphs, insufficient configured-language evidence, and extraction failures advance the page to the next native adapter.

The first usable candidate short-circuits later native work. All attempts retain engine/version, text hash, status, diagnostics, elapsed time, and actual selected method. Chain order/configuration and engine versions enter the cache identity.

### 3. Make PDFium a direct production dependency

`pypdfium2==5.13.0`, the version in the committed GPU evidence and current environment, becomes an explicit dependency rather than an OCR renderer's transitive dependency. PyMuPDF remains excluded because PDFium produced equivalent text on the known failure without a new licensing constraint.

### 4. Expand native promotion evidence instead of reusing the old manifest unchanged

The existing acceptance manifest is retained for page-recovery regression but is insufficient for parser promotion. A separate versioned native-promotion manifest will bind the same four file hashes and add verified gold:

- 600036.SH: physical pages 1, 2, 19, 41, 51-62, 195, and 350. Required evidence includes the actual traditional cover text `招商銀行`, `3.1总体经营情况分析`, the GDP/140/5.0% evidence on page 19, `3.6分部经营业绩`, `零售金融业务`, and table numbers including `90,676` on page 41, and `3.10业务运作`/`874.17` plus page continuity and selected table/read-order checks across pages 51-62. Gold not already verified by the lab handoff is added only after a read-only probe.
- 000717.SZ: at least one representative正文/table page with manually verified Chinese heading, numeric evidence, and reading order; empty gold is not a passing baseline.
- 002376.SZ: one truly native-unusable page and at least one page that PDFium can recover natively. The latter is a negative OCR gold: neither document-level `mixed` classification nor another page's failure may send it to OCR.
- 001322.SZ: verified scanned-page Chinese/numeric gold and the expected native-unusable outcome before OCR.

Promotion is lexicographic: technical/gold fidelity first, compatibility and false-routing second, latency third. The normal baseline cannot regress; tables must retain verifiable numeric reading order, although markdown/table reconstruction remains out of scope.

### 5. Adopt 3.3.1/3.7.0 as the sole production OCR runtime family

The authoritative production candidate is PaddlePaddle/PaddleOCR 3.3.1/3.7.0 because the committed canary ran it successfully on GRID P4. The 2.6.2/2.7.3 experiment remains comparative evidence for speed and isolation hazards only; no implementation task builds or deploys it.

Both GPU and CPU OCR run in isolated workers with the same PaddleOCR version, model identity, inference configuration, and input bitmaps. The GPU environment installs `paddlepaddle-gpu==3.3.1`; the CPU environment installs `paddlepaddle==3.3.1`. This keeps output comparisons meaningful while preserving distinct device/runtime cache identities.

The existing `PaddleOcrAdapter` is extended to manage the worker protocol; no parallel OCR adapter or business entry point is introduced. Quote performs capability checks by invoking the worker's probe command. It never imports a CUDA Paddle package into the Quote process. The reproduced `inflateReset2` conflict is therefore a production isolation invariant. Worker startup records the exact inference/IR configuration used by the approved 3.3.1/3.7.0 canary; an unrecorded optimization path or `SIGILL` fails closed rather than silently applying the 2.7 workaround to a different runtime.

### 6. Fix rendering to PDFium input parity

The isolated native worker renders selected physical pages with `pypdfium2==5.13.0` at the profile's configured DPI, initially 150 DPI (`scale=dpi/72`). It opens the PDFium document once for the requested OCR batch and passes rendered image payloads/references to both GPU and CPU workers. The Quote parent and OCR workers do not open PDF files or call PDFium for this operation, and workers do not import or depend on PyMuPDF, even if PaddleOCR packaging declares it transitively.

DPI, renderer name/version, color/image configuration, and model configuration enter cache and canary identities. Because the lab used PyMuPDF at 2.0x, its 15.18-second result is comparative evidence only; production latency gates are established from PDFium-rendered expanded canaries.

### 7. Define recovery and fallback precisely

`native_first` runs the ordered native chain and never creates OCR work. `selective_recovery` invokes OCR only for explicit target pages on which every configured native engine fails; a document-level class cannot create OCR work. `force_ocr` requires non-empty `target_pages`, runs the first native engine for PDF/page-count validation and diagnostic provenance, retains that candidate, skips later native fallbacks, and sends every valid target page to OCR regardless of native usability.

The production OCR order is GPU then CPU. CPU fallback is allowed by configured typed failure class and shares the same page/request deadlines and page count; it cannot reset budgets or expand target pages. By default only pre-inference unavailability, capability, startup, or model-health failures fall back. A GPU page timeout/inference failure remains typed unless explicitly enabled and enough original budget remains. Completed pages survive partial failure.

### 8. Treat the old GPU approval as runtime evidence, not new-profile approval

The existing `pypdf_paddleocr_gpu_canary` approval remains historical proof that 3.3.1/3.7.0 can execute on the P4. It cannot approve the PDFium-first profile because its corpus hash, profile, pages, native routing, renderer/config identity, and gold scope differ. The new GPU profile requires a new approval artifact over the expanded manifest, PDFium-rendered inputs, worker isolation, CPU fallback, cache separation, selective routing, and no-full-document-OCR checks.

Native PDFium promotion has its own gate and can merge before GPU worker/canary completion. For 600036.SH, successful PDFium native extraction is expected to eliminate OCR on those pages; OCR recovery from the old page-2 canary cannot be counted as proof of the new native architecture.

### 9. Isolate native PDF processing while preserving safe parallelism

PDFium text extraction and PDFium rasterization are native operations and MUST execute in a supervised worker process outside the Quote parent. The worker protocol accepts content-hash-bound PDF bytes or a read-only asset reference, requested physical pages, profile/configuration identity, and explicit deadlines; it returns only validated serializable page results and diagnostics. A worker is never allowed to write business data.

The shared module owns one bounded native worker pool per process/service, using `spawn` (or an equivalent clean-process start method). The pool may process multiple documents concurrently. Each worker handles one assigned document attempt at a time and uses PDFium and `pypdf` serially, with one PDF open per adapter attempt and no nested PDFium thread pool. Pool width, queue bound, worker task limit, per-page/document deadlines, and restart budget are configuration values. The parent detects non-zero exit, signal (including `SIGTRAP`), timeout, malformed output, and missing pages, converts them to typed diagnostics, reaps the worker, and retries only within a bounded policy.

OCR workers continue to receive rendered PNGs only. The native worker performs the authoritative PDFium rendering once per OCR batch; GPU and CPU OCR workers never open PDFs and never load PDFium. Thus both native extraction and OCR preparation are protected from a PDFium crash in the Quote parent, while OCR runtime isolation remains a separate boundary.

The safe default pool width is conservative and must be promoted by canary. A read-only benchmark compares native worker widths 1, 2, 4, 6, 8, and 10 (subject to host capacity), measures throughput, P95/tail latency, memory, crash rate, queue wait, and completed-page preservation, and selects the highest width that has zero parent-process exits and no unexplained page loss over the required corpus. The benchmark must include the known 603268.SH and 002496.SZ reports and run at least 20 rounds for the crash-isolation gate. Throughput-optimal width and highest crash-safe width are recorded separately.

## Risks / Trade-offs

- [PDFium reading order regresses normal/table PDFs] -> Require verified normal and table numeric/read-order gold before promotion; retain `pypdf` rollback.
- [Chinese quality rule rejects legitimate English/HK pages] -> Apply configured language/script expectations, not a global CJK requirement; record HK coverage as a current corpus limitation.
- [The formal GPU canary overstates production readiness] -> Reuse it only for runtime qualification and require a new profile/hash/config-bound expanded canary.
- [CPU and GPU outputs drift] -> Use version-matched isolated workers and identical PDFium-rendered input; preserve device-specific cache identity and compare hashes/gold in canary.
- [GPU package or IR path crashes Quote] -> Keep all Paddle imports inside workers, probe out-of-process, record inference flags, and fail closed on worker crash/SIGILL.
- [Removing inspector breaks non-native tools] -> Inventory native, classification, evaluator, and OCR adapter uses separately; remove only production native routing in this slice and uninstall only if every supported role is empty.
- [GPU issues delay the native fix] -> Merge the PDFium native slice under its independent acceptance gate before GPU enablement work.

## Migration Plan

1. Reconcile the three existing capability contracts and current PDF documentation; ensure the upstream capability baselines can be canonicalized/rebased without taking ownership of company-profile tasks; create the expanded native-promotion manifest without changing production defaults.
2. Add direct PDFium dependency/adapter, page-quality rules, ordered chain, cache/provenance changes, and focused tests. Run the native corpus and promote PDFium only if all gates pass.
3. Canary and activate the PDFium-first native profile. Retain a `pypdf`-first rollback profile. This milestone is independently releasable.
4. Inventory and remove inspector from production native profiles; retain documented non-native uses or schedule their separate retirement before uninstalling the package.
5. Extend the shared PDF module with a supervised native worker protocol/pool. Route PDFium extraction and OCR rasterization through it; keep worker-internal parsing serial and parent-level concurrency bounded.
6. Run the native crash-isolation canary and multi-process parameter benchmark, select a safe pool width, and verify that worker signals/timeouts become typed page/document diagnostics without Quote restarts.
7. Extend `PaddleOcrAdapter` for versioned isolated GPU/CPU workers and native-worker-rendered inputs using the authoritative 3.3.1/3.7.0 runtime family.
8. Run the expanded GPU/CPU canary, issue a new approval for the new profile, then activate GPU-first OCR. Roll back to CPU-only or OCR-disabled configuration without changing callers.
9. Remove superseded profile names and contradictory CUDA/native-isolation instructions after callers/configuration migrate; archive the change.

## Open Questions

- Which persistent service paths and deployment owner will manage the two isolated worker environments and model cache?
- Who owns GRID vWS licence renewal and health monitoring?
- After native profile removal, do `PdfInspectorOcrAdapter`, `detect_pdf_bytes`, and inspector evaluation remain supported, or should their retirement be proposed separately?

The authoritative runtime, CPU fallback ownership, and renderer are no longer open: they are respectively Paddle/PaddleOCR 3.3.1/3.7.0, an isolated version-matched CPU worker, and Quote-side PDFium 5.13.0 at profile-bound DPI.
