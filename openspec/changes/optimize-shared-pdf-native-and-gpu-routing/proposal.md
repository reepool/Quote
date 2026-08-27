## Why

The existing shared PDF contracts default to `pypdf`, still describe `pdf-inspector` as a native profile/alternate, and qualify GPU OCR with a one-page-per-document canary. The 600036.SH lab evidence shows that `pypdfium2` recovers malformed-CMap Chinese text at native speed while the current two native paths fail, and that GPU OCR is materially faster than CPU. A 2026-08-27 production incident then showed that concurrent in-process PDFium calls can trigger an `int3`/`SIGTRAP` in `libpdfium.so` and restart the whole Quote service. The contracts and acceptance evidence must therefore reconcile engine selection, native crash isolation, parallel execution, and OCR boundaries before further rollout.

## What Changes

- Add a first-class, directly pinned `pypdfium2` native adapter and an ordered native engine chain, with `pypdfium2 -> pypdf` as the promotion target after expanded frozen-corpus gates pass.
- Make page-level technical quality authoritative: a document-level `mixed` classification cannot force OCR, and numeric/ASCII residue that fails configured Chinese-script evidence cannot be selected as usable native text.
- Expand the native promotion corpus beyond the existing page-recovery manifest: 600036.SH must cover正文 pages 19, 41, and 51-62 in addition to TOC/diagnostic pages; the normal and mixed cases require verified Chinese/numeric/read-order gold and a mixed-page negative OCR case.
- **BREAKING**: retire `pdf_inspector_paddleocr` and `pdf-inspector` as production native engine choices after caller migration; inventory and preserve any still-required evaluation, classification, or OCR-only use instead of silently uninstalling the package.
- Adopt the committed PaddlePaddle GPU 3.3.1 + PaddleOCR 3.7.0 + GRID P4 canary stack as the only production runtime baseline. Treat the separate Paddle 2.6.2/OCR 2.7.3 lab environment as comparative evidence, not a second deployment target.
- Extend the existing `PaddleOcrAdapter` subprocess boundary into isolated, version-matched GPU and CPU workers. The supervised native worker renders identical PDFium 5.13.0 page images at configured DPI; OCR workers do not open PDFs or depend on PyMuPDF.
- Move both PDFium text extraction and PDFium rasterization behind a supervised native worker boundary. Native workers remain parallel at the pool level, but each worker uses PDFium and `pypdf` serially; a native crash, signal, hang, or protocol error must not terminate Quote.
- Add a bounded multi-process concurrency benchmark and crash-isolation canary so the pool can select the highest safe parallelism for the actual host and corpus instead of assuming that the business thread-pool width is safe for PDFium.
- Replace the old one-page GPU approval for the new routing profile with an expanded, profile/hash/config-bound canary. GPU OCR becomes the configured production OCR runtime only after that canary passes; CPU remains an explicit isolated fallback.
- Preserve selective recovery and budgets. `force_ocr` validates the document and retains the first native diagnostic candidate, but it does not run later native fallbacks or allow native success to suppress OCR for explicit target pages.
- Deliver native parser promotion independently of GPU worker readiness so OCR packaging cannot block the immediately useful native correction.
- Keep native worker promotion independently releasable from GPU OCR activation; native isolation is required even when OCR is disabled.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `shared-pdf-processing`: Replace primary/alternate and inspector-profile semantics with an ordered PDFium-first native chain, page-level quality precedence, direct PDFium dependency, and the existing adapter's isolated OCR worker model.
- `shared-pdf-processing`: Also require supervised isolation for PDFium extraction/rasterization and a bounded, configurable multi-process native worker pool with serial work inside each worker.
- `shared-pdf-page-recovery-contract`: Clarify `force_ocr`, renderer, GPU/CPU fallback, shared budgets, cache/provenance, and worker-safe capability probing.
- `pdf-engine-evaluation`: Strengthen native promotion gold, identical-render comparisons, mixed-page negative OCR checks, and invalidate the old narrow canary as approval for the new profile.

## Impact

- Affected shared code: `research/document_processing/pdf/` adapters, profiles, router, quality gate, evaluator, cache identity, capability reporting, and tests.
- Affected configuration: ordered native engine lists; new PDFium-first CPU/GPU profiles; staged retirement of `pypdf_paddleocr`, `pdf_inspector_paddleocr`, and `pypdf_paddleocr_gpu_canary` after replacements and rollback mappings exist.
- Affected dependencies: add direct `pypdfium2==5.13.0`; retain `pypdf`; remove `pdf-inspector` only if the full native/evaluation/classification/OCR inventory becomes empty.
- Affected operations: native PDFium/pypdf workers run outside the Quote request process; isolated CPU and CUDA OCR environments both use Paddle/PaddleOCR 3.3.1/3.7.0 and a persistent writable model cache; the Quote conda process never imports a GPU wheel or calls PDFium directly for production parsing/rendering.
- Affected documentation/evidence: update `shared_pdf_processing.md`, the acceptance manifest, GPU canary artifacts, and profile runbook so only one CUDA deployment model remains current.
- Company-profile logic continues to own TOC/section selection, printed-to-physical page correction, and business quality. Native promotion can merge before GPU enablement and does not introduce a second PDF execution path or database.
