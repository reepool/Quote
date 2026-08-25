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

For a canary, set `QUOTE_PDF_ENGINE_PROFILE` to a validated profile name. An
unset value resolves to `pypdf_native`; unknown or disabled values fail closed.

OCR is page-addressable and lazy. Native pages do not render or load an OCR
model. Configure `max_ocr_pages`, `max_document_seconds`, `render_dpi`, batch
size, queue size, and concurrency through `PdfResourceLimits`. When
`max_concurrency` is greater than one, the PaddleOCR adapter uses bounded
worker sessions; each worker reuses its model session instead of reloading it
for every page. Missing OCR runtimes produce typed `ocr_unavailable`/`ocr_deferred`
results.

Quote's CPU environment has `paddlepaddle==3.3.1`, `paddleocr==3.7.0`,
`pdf-inspector==1.17.0`, and `pypdfium2` installed. Production workers should
set `PADDLE_PDX_CACHE_HOME` or `ocr_model_cache_dir` in the selected profile to
a persistent writable model-cache directory (for example
`/var/cache/quote/paddlex`). The adapter rejects a configured non-writable
directory and records a warning/provenance marker when it has to use a
temporary development fallback.

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
is attempted before governed OCR. The local corpus currently has native/text
based and this mapping-corrupt class; scanned/table gold labels remain an
explicit evaluation gap when no archived fixture is available.

## Rollback

Select `pypdf_native` (or the consumer's retained compatibility path) in
configuration. No source PDF or production fact is modified by parsing or
evaluation.
