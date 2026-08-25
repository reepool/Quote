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
size, queue size, and concurrency through `PdfResourceLimits`. Missing OCR
runtimes produce typed `ocr_unavailable`/`ocr_deferred` results.

## Evaluation

Use `research.document_processing.pdf.evaluation.load_manifest` with an
explicit, SHA-256-bound JSON manifest such as
`evaluation_manifest.example.json`. The evaluator is read-only and never
discovers or downloads assets. It reports per-profile success rate, OCR page
count, and P50/P95 latency; benchmark reports must keep source text out of
shared artifacts.

The mandatory 600036.SH fixture is classified as
`viewer_readable_native_mapping_corrupt`. Its non-empty but unrelated-script
text must not be promoted as usable native evidence; alternate-native recovery
is attempted before governed OCR.

## Rollback

Select `pypdf_native` (or the consumer's retained compatibility path) in
configuration. No source PDF or production fact is modified by parsing or
evaluation.
