# Shared PDF Processing

`research.document_processing.pdf` is the technical owner for PDF bytes to
ordered page results. Domain modules retain announcement classification,
table interpretation, evidence gates, and database writes.

## Profiles

- `pdfium_native`: production native profile, ordered `pypdfium2 -> pypdf`.
- `pypdf_native`: configuration-only rollback profile.
- `pdfium_paddleocr_cpu`: PDFium-first native chain with an isolated CPU OCR
  worker for explicit page recovery.
- `pdfium_paddleocr_gpu`: canary-only GPU worker profile. It is rejected unless
  a new expanded PDFium-rendered approval and a healthy isolated worker exist.

`pdf-inspector` is no longer a production native engine. It remains installed
only for existing evaluator/classifier/OCR roles until those roles have a
separate retirement decision.

For a canary, set `QUOTE_PDF_ENGINE_PROFILE` to a validated profile name. An
unset value resolves to `pdfium_native`; unknown or disabled values fail closed.

OCR is page-addressable and lazy. Native pages do not render or load an OCR
model. Configure `max_ocr_pages`, `max_document_seconds`, `render_dpi`, batch
size, queue size, and concurrency through `PdfResourceLimits`. Production
PDFium text extraction, pypdf fallback, and PDFium rasterization run in a
supervised native worker pool outside the Quote parent. The pool may process
multiple documents in parallel, but each worker uses PDFium serially and does
not create a nested PDFium thread pool. Worker signal exits, including
`SIGTRAP`, hard timeouts, malformed responses, and missing pages become typed
diagnostics; completed pages are retained. The pool width is selected by a
read-only corpus canary, not copied from the business or LLM executor width.
Real Paddle workers remain terminable child processes and reuse their model
session instead of reloading it for every page in the request. Missing OCR
runtimes and timeouts produce typed `ocr_unavailable`/`ocr_deferred`/
`ocr_timeout` results.
Custom page renderers used with hard timeouts must be picklable by Python's
`spawn` multiprocessing context; injected in-process OCR test sessions may opt
out of hard timeouts explicitly.

Callers select pages with one-based physical `target_pages`. `native_first`
never creates OCR work, `selective_recovery` sends only unusable target pages
to OCR, and `force_ocr` requires a non-empty page set. Mode and profile budgets
are combined using the smaller limit. The default mode budgets are 5 pages / 180
seconds for `toc_probe`, 20 pages / 900 seconds for `section_extract`, and 8
pages / 600 seconds for `table_extract`.

Quote's native environment has direct `pypdfium2==5.13.0` and `pypdf`, but
production PDFium calls are dispatched through the native worker boundary. OCR
workers outside Quote use matched `paddlepaddle==3.3.1` /
`paddleocr==3.7.0` versions for both GPU and CPU. `pdf-inspector==1.17.0`
remains only for non-native consumers. Production workers should
set `PADDLE_PDX_CACHE_HOME` or `ocr_model_cache_dir` in the selected profile to
a persistent writable model-cache directory (for example
`/var/cache/quote/paddlex`). The adapter rejects a configured non-writable
directory and records a warning/provenance marker when it has to use a
temporary development fallback.

## CUDA canary

The 2026-08-25 read-only frozen-corpus run validated a GRID P4-8Q (8 GiB,
compute capability 6.1) with driver 535.183.06 and the CUDA 11.8 build of
`paddlepaddle-gpu==3.3.1`. The isolated environment used the same
`paddleocr==3.7.0`, `pdf-inspector==1.17.0`, and `pypdfium2==5.13.0` versions as
Quote. It passed all four hash-bound cases: GPU document P95 was 20.16 seconds
and OCR-page P95 was 4.37 seconds, versus CPU 69.60 and 61.34 seconds. Both
runs had 100% case success and recovered the confirmed 600036.SH Chinese,
numeric, and TOC gold. The reports are:

- `pdf_page_recovery_cpu_evaluation_20260825.json`
- `pdf_page_recovery_gpu_evaluation_20260825.json`
- `pdf_page_recovery_gpu_canary_approval_20260825.json`

The expanded PDFium-first canary completed on 2026-08-26 with the same four
hash-bound cases. Its GPU report is
`pdfium_paddleocr_gpu_evaluation_20260826.json` and its approval is
`pdfium_paddleocr_gpu_canary_approval_20260826.json`. All four cases passed;
GPU document P95 was 11.59 seconds and OCR-page P95 was 1.93 seconds. The
matching CPU worker report is `pdfium_paddleocr_cpu_evaluation_20260826.json`;
it passed the same gold with OCR-page P95 41.66 seconds. 600036.SH, 000717.SZ,
and mixed 002376.SZ pages selected native text and did not enter OCR.

This artifact proves runtime viability only; it does not approve
`pdfium_paddleocr_gpu`. A new approval must bind the expanded corpus,
PDFium-rendered image hashes, profile, model, and inference configuration.
Quote must never import CUDA Paddle to probe it. Configure worker commands with
`QUOTE_PDF_GPU_OCR_WORKER` and `QUOTE_PDF_CPU_OCR_WORKER`; the adapter sends
PNG images only, so workers never open PDFs or depend on PyMuPDF.

The deployment-specific GPU worker must install the official CUDA 11.8 wheel from
`https://www.paddlepaddle.org.cn/packages/stable/cu118/`. Validate it before
starting Quote:

```bash
python -c "import paddle; paddle.utils.run_check(); print(paddle.device.cuda.device_count())"
```

On this host `/dev/nvidia*` is not recreated automatically. Deployment must run
the following as a host startup/udev/container-device step before the Quote
worker starts; application code must not create device nodes:

```bash
nvidia-modprobe -u -c=0
nvidia-smi
```

After the device, GPU Paddle runtime, and persistent writable model cache are
available, enable only the explicit canary:

```bash
export QUOTE_PDF_ENGINE_PROFILE=pdfium_paddleocr_gpu
export QUOTE_PDF_GPU_CANARY_APPROVED=1
export QUOTE_PDF_GPU_CANARY_REPORT=/path/to/pdfium_paddleocr_gpu_canary_approval_20260826.json
export QUOTE_PDF_OCR_CACHE_DIR=/var/cache/quote/paddlex
```

The approval report is corpus-, renderer-, runtime-, and gate-bound. A
missing/failed report, CPU-only Paddle installation, unavailable worker, or
invisible GPU causes profile resolution to fail closed; there is no automatic
GPU rollout. The known lab Paddle 2.6.2/2.7.3 `inflateReset2`/IR failures are
comparative evidence and are not a production fallback path.

Business-profile extraction forwards `target_page_numbers` to the shared
router. The selected engine profile is part of the artifact parameter hash,
so switching from `pypdf_native` to `pdfium_native` cannot reuse an artifact
created by the other profile. Each page artifact records the selected
`extraction_method`, OCR confidence, and engine/model provenance.

## Evaluation

Use `research.document_processing.pdf.evaluation.load_manifest` with an
explicit, SHA-256-bound JSON manifest such as
`evaluation_manifest.example.json`. The evaluator is read-only and never
discovers or downloads assets. It reports per-profile success rate, OCR page
count, and P50/P95 latency; benchmark reports must keep source text out of
shared artifacts.

The evaluator also records Chinese/numeric exact-match, heading/table
evidence, confidence coverage, low-quality recall, OCR time share, pages per
second, P50/P95 tail latency, queue wait, model warm-up, CPU and RSS deltas.
`run_bounded_canary` limits cases/pages and returns a fail-closed status before
any wider rollout. `probe_ocr_components` records worker-owned PP-OCR and
PP-Structure availability without importing CUDA Paddle into Quote.

The native promotion manifest is
`pdf_native_promotion_manifest_20260826.json`, with its read-only result in
`pdf_native_promotion_evaluation_20260826.json`. PDFium-first passed the
expanded native Chinese, numeric, heading, table/read-order, and mixed-page
negative-OCR checks; pypdf remains the rollback baseline.

The supervised native parallelism canary on 2026-08-27 compared 1, 2, 4, 6,
8, and 10 workers over six hash-bound local reports (the four frozen cases plus
603268.SH and 002496.SZ), pages 1, 2, 19, and 41, for 20 rounds per width
(480 page requests total). Every width completed all requested pages with zero
worker restarts, zero typed crash/timeout/protocol diagnostics, and a live
parent process. Width 4 had the best measured document throughput; width 10
was the highest crash-safe tested ceiling. Use 4 as the production starting
width (`native_max_concurrency=4`) and treat 10 as an upper canary limit until
larger page mixes are measured. The raw report is
`pdf_native_parallel_benchmark_20260827.json`.

The mandatory 600036.SH fixture is classified as
`viewer_readable_native_mapping_corrupt`. Its non-empty but unrelated-script
text must not be promoted as usable native evidence. The local corpus covers
native/text, scanned, mixed, and this mapping-corrupt class. The archived
001322 copy has a readable text layer despite its scanned source label, so its
forced-OCR gold does not claim native failure for every page.

## Handoff boundary

The shared PDF interface has no known remaining PDF-team integration gaps.
Business-profile ownership begins after page recovery: TOC/section selection,
printed-to-physical page correction, bounded recovery state transitions,
business evidence gates, scanned/mixed numeric and table gold, and the
600036.SH business-profile canary remain consumer responsibilities. The shared
module must not turn those workflows into implicit full-document OCR.

## Rollback

Select `pypdf_native` (or the consumer's retained compatibility path) in
configuration. No source PDF or production fact is modified by parsing or
evaluation.
