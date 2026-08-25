# Shared PDF Processing

`research.document_processing.pdf` is the technical owner for PDF bytes to
ordered page results. Domain modules retain announcement classification,
table interpretation, evidence gates, and database writes.

## Profiles

- `pypdf_native`: native diagnostics only.
- `pypdf_paddleocr`: pypdf native first, optional alternate-native recovery,
  then page-selective PaddleOCR. This is the initial integration profile.
- `pdf_inspector_paddleocr`: shadow/evaluation profile; it is not enabled by
  default until the same-corpus gates pass.
- `pypdf_paddleocr_gpu_canary`: explicit CUDA canary. It is rejected unless
  both a passing frozen-corpus approval report and a visible CUDA Paddle
  runtime are present.

For a canary, set `QUOTE_PDF_ENGINE_PROFILE` to a validated profile name. An
unset value resolves to `pypdf_native`; unknown or disabled values fail closed.

OCR is page-addressable and lazy. Native pages do not render or load an OCR
model. Configure `max_ocr_pages`, `max_document_seconds`, `render_dpi`, batch
size, queue size, and concurrency through `PdfResourceLimits`. When
`max_concurrency` is greater than one, the PaddleOCR adapter uses bounded
worker sessions; each worker reuses its model session instead of reloading it
for every page in the request. Real Paddle workers run in terminable child
processes so `max_page_seconds` and the document deadline are hard limits;
completed pages survive another page's timeout. Missing OCR runtimes and
timeouts produce typed `ocr_unavailable`/`ocr_deferred`/`ocr_timeout` results.
Custom page renderers used with hard timeouts must be picklable by Python's
`spawn` multiprocessing context; injected in-process OCR test sessions may opt
out of hard timeouts explicitly.

Callers select pages with one-based physical `target_pages`. `native_first`
never creates OCR work, `selective_recovery` sends only unusable target pages
to OCR, and `force_ocr` requires a non-empty page set. Mode and profile budgets
are combined using the smaller limit. The default mode budgets are 5 pages / 180
seconds for `toc_probe`, 20 pages / 900 seconds for `section_extract`, and 8
pages / 600 seconds for `table_extract`.

Quote's CPU environment has `paddlepaddle==3.3.1`, `paddleocr==3.7.0`,
`pdf-inspector==1.17.0`, and `pypdfium2` installed. Production workers should
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

Quote's normal environment deliberately remains on the CPU package and the
default profile remains `pypdf_native`. A deployment-specific GPU environment
must replace, not coexist with, the CPU Paddle package and install the official
CUDA 11.8 wheel from
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
export QUOTE_PDF_ENGINE_PROFILE=pypdf_paddleocr_gpu_canary
export QUOTE_PDF_GPU_CANARY_APPROVED=1
export QUOTE_PDF_GPU_CANARY_REPORT=/path/to/pdf_page_recovery_gpu_canary_approval_20260825.json
export QUOTE_PDF_OCR_CACHE_DIR=/var/cache/quote/paddlex
```

The approval report is corpus- and gate-bound. A missing/failed report, CPU-only
Paddle installation, or invisible GPU causes profile resolution to fail closed;
there is no automatic GPU rollout.

Business-profile extraction forwards `target_page_numbers` to the shared
router. The selected engine profile is part of the artifact parameter hash,
so switching from `pypdf_native` to `pypdf_paddleocr` cannot reuse an artifact
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
any wider rollout. `probe_ocr_components` records local availability for
PP-OCR, PP-Structure, pdf-inspector OCR, and Tesseract/OCRmyPDF without model
downloads.

The mandatory 600036.SH fixture is classified as
`viewer_readable_native_mapping_corrupt`. Its non-empty but unrelated-script
text must not be promoted as usable native evidence; alternate-native recovery
is attempted before governed OCR. The local corpus now covers native/text,
scanned, mixed, and this mapping-corrupt class. The scanned and mixed probe
pages were established with bounded read-only probes; numeric/table gold for
those reports remains a business-evaluation follow-up and was not guessed by
the shared module.

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
