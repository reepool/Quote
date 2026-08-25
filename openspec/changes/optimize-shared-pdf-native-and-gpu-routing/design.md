## Context

The shared PDF module already owns page-scoped native extraction, selective OCR recovery, technical quality gates, candidate provenance, cache identity, and budgets. Its current native profiles default to `pypdf` and use `pdf-inspector` as an alternate. On 17 sampled pages from the 600036.SH 2025 annual report, both paths failed 15 malformed-CMap pages, while `pypdfium2` recovered 16 usable pages with no mapping errors in approximately the same extraction time as `pypdf`. PyMuPDF produced equivalent text slightly faster but is not installed and introduces AGPL/commercial licensing constraints.

The GRID P4 lab run also established that PP-OCRv4 GPU inference can process 14 selected pages in 15.18 seconds versus an estimated 274 seconds on CPU. The proven Paddle 2.6 GPU stack conflicts with native libraries in the Quote conda environment, and PaddleOCR's default IR fusion caused `SIGILL` on this Pascal GPU. Production OCR must therefore preserve the shared router contract while isolating the GPU runtime.

This change depends on the request, page, candidate, cache, budget, and ownership semantics defined by the in-flight `business-profile-selective-pdf-recovery` change. It does not transfer company-profile TOC or section decisions into the shared module.

## Goals / Non-Goals

**Goals:**

- Select the production native default using reproducible accuracy, compatibility, and latency evidence, with `pypdfium2` as the promotion candidate.
- Recover from parser-specific native failures through a bounded ordered chain before considering OCR.
- Retire `pdf-inspector` from the native path after confirming that no production caller remains.
- Provide GPU-first PaddleOCR through an isolated worker and configurable CPU fallback without changing caller-facing page-recovery semantics.
- Keep every parser/device attempt auditable and cache-safe.
- Preserve selective page budgets and prohibit accidental full-document OCR.

**Non-Goals:**

- Adding PyMuPDF or resolving its licence for production use.
- Implementing company-profile TOC discovery, section boundaries, page-label correction, or business quality gates.
- Making OCR the default for PDFs whose native text passes technical quality gates.
- Installing CUDA Paddle in the Quote conda environment, building a remote OCR platform, or adding a new database.
- Reconstructing tables into markdown, HTML, or JSON in this change.

## Decisions

### 1. Use an ordered native adapter chain with quality-gated short circuiting

A profile will resolve to an ordered tuple of registered native adapter names, initially targeting `("pypdfium2", "pypdf")`. The router will invoke adapters in order for each requested physical page, record each candidate, and stop after the first candidate that passes the existing shared technical quality gate. A parser exception, empty result, mapping error, or other unusable candidate advances only that page to the next adapter.

This is preferred over hard-coded `primary_adapter`/`alternate_adapter` branches because two real adapters now require the same selection semantics and because the change removes a real duplicate fallback path. It remains a small shared router feature, not a generic plugin platform.

Alternatives considered:

- Keep `pypdf` first and use PDFium only on detected errors: rejected as the target default because detection can miss semantically corrupted text and adds work on PDFs PDFium already handles.
- Run every parser and score all outputs: rejected because it multiplies latency without a current business need; the first technically usable candidate is sufficient.
- Use `pdf-inspector` as alternate: rejected because it failed the same malformed mapping and was approximately 17 times slower than PDFium on the sampled extraction workload.

### 2. Promote PDFium only through a frozen-corpus gate

`pypdfium2` is the intended primary, but promotion is conditional on a reproducible benchmark over four bound classes: 600036.SH mapping-corrupt, 001322.SZ scanned, 002376.SZ mixed, and 000717.SZ normal text. The evaluation will compare `pypdfium2` and `pypdf` on identical physical pages and record text/hash, Chinese and numeric fidelity, required phrase recall, page order, table/read-order checks, failure classification, and elapsed time.

Selection is lexicographic: technical usability and gold fidelity first, compatibility/regression second, latency third. PDFium may become the default only if it recovers the 600036 gold, does not regress the normal baseline, does not turn scanned pages into false usable native text, and stays within the agreed native latency gate. The benchmark manifest and result are versioned, while production assets remain read-only.

PyMuPDF is excluded despite its 0.17-second sample time because PDFium produced effectively equivalent text (mean similarity 0.9993), is already installed, and avoids a new AGPL/commercial dependency.

### 3. Keep native candidate and cache provenance engine-specific

Each adapter attempt will carry method, engine name/version, text hash, quality status, semantic usability, elapsed time, and diagnostics. The selected method will identify the actual native engine rather than only saying `native_text`. Cache identity and serialized results will include the ordered chain/config version and selected/attempted engine versions so that changing the primary or fallback order cannot reuse an incompatible page result.

The existing page-level cache backend remains the persistence owner. No new cache service is introduced.

### 4. Run GPU PaddleOCR as an isolated local worker

The production GPU profile will invoke a version-pinned local worker process/environment through a narrow request/response protocol. The initial P4-compatible baseline is Paddle GPU 2.6.2, PaddleOCR 2.7.3, PP-OCRv4, CUDA 11.8, and the tested setting that disables the incompatible IR fusion path. The Quote process owns PDF selection, rendering/input handoff, budgets, cancellation, selection, cache calls, and final result assembly; the worker owns only model lifecycle and inference.

The protocol will accept a bounded page job with physical page number, image/input reference, OCR settings, and deadline. It will return schema version, page number, text, text hash, confidence, quality diagnostic, elapsed time, engine/model/device/runtime versions, and worker diagnostics. Protocol output is validated before it can become a candidate.

A local worker is preferred over installing CUDA Paddle into Quote because the lab reproduced a native `libz` conflict/SIGSEGV in the shared conda environment. It is preferred over a new remote service because the current requirement is one local GPU and the existing module already supports subprocess timeout boundaries.

### 5. Make GPU primary and CPU fallback explicit

OCR runtime order is profile configuration, with the production target `("gpu", "cpu")` and a CPU-only profile available for rollback or machines without a qualified GPU. Before accepting work, the GPU worker must report driver/CUDA/device, compute capability, free memory, model load health, runtime versions, and the GRID licence state when available.

CPU fallback is attempted only when enabled and the GPU failure code is allowed by policy. The default fallback set covers worker unavailable, capability check failure, startup failure, and model health failure. A page that already consumed its deadline or document OCR budget is not retried. Per-page GPU inference errors/timeouts remain typed failures by default, avoiding silent duplicate work; an operator may explicitly enable those retry classes only within the same effective budgets.

GPU and CPU candidates use distinct device/runtime/model cache identities. Successful pages from a partial batch are retained when another page fails.

### 6. Preserve the shared recovery contract and one production path

For `native_first`, the native chain runs and no OCR work is created. For `selective_recovery`, only requested pages for which every configured native adapter fails can enter OCR. For bounded `force_ocr`, all explicitly requested pages may enter OCR; an empty `target_pages` remains invalid. Mode and profile budgets continue to use the smaller effective limit and apply across GPU plus CPU attempts, not independently per runtime.

All callers continue through `research.document_processing.pdf`; the isolated worker is an adapter implementation, not a business entry point.

## Risks / Trade-offs

- [PDFium reading order or table text regresses PDFs that `pypdf` handles] -> Gate promotion on the normal and mixed frozen corpus, retain `pypdf` in the rollout chain, and make profile rollback a configuration change.
- [A quality detector accepts plausible but wrong PDFium text] -> Use corpus gold for Chinese phrases, numbers, page order, and table/read-order checks; accuracy gates take precedence over latency.
- [The isolated GPU stack drifts from the proven lab environment] -> Pin worker dependencies and model assets, report all runtime versions, and require the production-host canary before enabling the GPU profile.
- [P4 IR optimization or native libraries crash the worker] -> Disable the reproduced incompatible fusion path, keep the crash outside Quote, return a typed worker failure, and allow configured CPU fallback.
- [GPU fallback doubles latency] -> Share one effective deadline/page budget across runtimes and restrict default fallback to pre-inference availability/health failures.
- [GPU cache entries are reused by CPU or changed models] -> Include device class, runtime, engine, model, and parser-config versions in deterministic cache identity.
- [Removing `pdf-inspector` breaks an unknown caller] -> Search production profiles/imports first, migrate real callers to the ordered chain, run focused integration tests, and remove the dependency only when the caller inventory is empty.
- [GRID vWS licence expiry or GPU contention disables production OCR] -> Surface licence/device health in capability reports and retain an explicit CPU-only rollback profile.

## Migration Plan

1. Add the PDFium adapter, ordered native-chain contract tests, and frozen-corpus benchmark without changing the production default.
2. Run the four-class native benchmark; record the decision artifact. If PDFium fails a mandatory accuracy/compatibility gate, keep `pypdf` primary and fix or reject the adapter before proceeding.
3. When gates pass, canary `pypdfium2 -> pypdf`, compare provenance/cache behavior, then make that chain the production default. Rollback restores the previous profile order.
4. Remove `pdf-inspector` profiles/imports and dependency after the production caller inventory and regression suite pass.
5. Package the pinned GPU worker outside Quote, add protocol/capability tests, and validate on GRID P4 with the tested IR setting and writable model cache.
6. Canary GPU-first OCR on the frozen scanned/mixed/mapping-corrupt samples under existing page budgets. Verify output fidelity, P95 latency, typed failures, and CPU fallback without full-document OCR.
7. Enable the GPU-first production profile. Rollback selects the CPU-only OCR profile or disables OCR recovery without changing business callers.

## Open Questions

- The persistent location and deployment owner for the isolated worker environment and model cache must be chosen during implementation from the host's writable service paths; it must not be `/tmp` or the Quote conda environment in production.
- The GRID vWS licence renewal/monitoring owner must be recorded in the OCR runbook before production enablement.
