## Context

The repository already has a shared official-announcement retrieval and immutable asset layer, but PDF interpretation is still distributed across business modules. The main native path is `pypdf`; company-action documents expose an injectable OCR protocol, while business-profile artifacts independently track page hashes, low-text pages, and OCR-required diagnostics. Broker risk-control, official-index, HKEX, and announcement-classification code still calls `PdfReader` directly.

The target workload is official listed-company announcements and annual reports. Results are evidence, not merely search text: page numbers, source hashes, extraction method, warnings, and quality status must remain available to downstream validators and LLM evidence gates. Scanned and mixed PDFs are expected, but the project must not OCR every page or silently treat an OCR failure as an empty disclosure.

The 600036.SH 2025 annual report is a mandatory pathological fixture. It opens as readable in a normal viewer, but the current `pypdf` quality path accepts only 9 pages as usable native text and flags 341/350 pages with suspicious glyph mappings. Samples contain valid Unicode characters from unrelated scripts rather than replacement characters, and the current heading index has zero matches. This is a text-layer mapping failure, not proof that the page is image-only.

This change is intentionally cross-cutting. It establishes a shared technical owner for PDF bytes-to-page-results while preserving domain owners for announcement classification, financial facts, corporate actions, business-profile semantics, and database writes. Existing uncommitted changes in announcement-asset/config/business-profile files are outside this change and must not be overwritten.

## Goals / Non-Goals

**Goals:**

- Define one versioned page-level PDF result contract usable by all current production consumers.
- Provide configurable native and OCR engine profiles for `pypdf`, PaddleOCR PP-OCR/PP-Structure, and `pdf-inspector`.
- Route OCR selectively using native-text quality, font-decoding diagnostics, explicit page targets, and configured policy.
- Keep OCR from becoming an unbounded pipeline bottleneck through warm workers, page batching, bounded concurrency, backpressure, and per-document/page budgets.
- Preserve raw PDF bytes, content hashes, page hashes, provenance, confidence, warnings, and fail-closed outcomes.
- Evaluate native and OCR components using identical database-referenced announcement PDF bytes before selecting primary and fallback engines, including cold-start and warm-throughput measurements.
- Migrate all discovered production PDF call sites to the shared execution path without moving business semantics into the shared module.

**Non-Goals:**

- No replacement of announcement discovery, attachment retrieval, annual-report archive ownership, or shared asset APIs.
- No generic table-to-financial-fact parser, semantic classifier, LLM gateway, or repository layer in the PDF module.
- No mandatory Docling, MinerU, Marker, or Unstructured integration.
- No production engine/default change until the evaluation report passes configured fidelity and resource gates.
- No direct mutation of production database facts or source PDF bytes as part of benchmark execution.

## Decisions

### Shared module owns technical page extraction, not business interpretation

Create a narrow package under `research/document_processing/pdf/` (final path may be adjusted to match the repository) with immutable request/result dataclasses, adapter protocols, router, and diagnostic helpers. The shared result contains document identity and page-level text; consumers remain responsible for selecting evidence, parsing domain tables, and writing their own artifacts/facts.

Alternative: make `research/business_profile_pdf_artifacts.py` the global parser. Rejected because its artifact schema and heading logic are business-profile-specific, and using it for HKEX or corporate-action semantics would create an accidental domain dependency.

### Stable contract separates native extraction, OCR, and routing

The public contract has three concepts:

1. `PdfParseRequest`: verified bytes/content hash, target pages, engine profile, OCR policy, parser configuration version, and resource limits.
2. `PdfPageResult`: one-based page number, text, extraction method, quality status, optional confidence, optional blocks/coordinates/tables, warning/diagnostic codes, text hash, and page-result hash.
3. `PdfDocumentResult`: page count, ordered pages, status, engine versions, input/parameter hashes, timing/resource diagnostics, and document-level failures.

The router first obtains native diagnostics, then sends only pages that are empty, low quality, glyph-decoding-error, explicitly requested, or otherwise selected by a configured policy to OCR. Native text remains the preferred result when it passes the quality policy; OCR may be recorded as a replacement or supplemental result with explicit provenance.

The quality policy SHALL distinguish at least three cases: (a) usable native text, (b) an actually textless/image page, and (c) a visually rendered page whose native text mapping is corrupt. Mapping corruption is detected from a combination of parser warnings/ToUnicode evidence, suspicious script distribution or replacement/control characters, native text density, and disagreement with an alternate native engine or OCR sample. A viewer-readable page with corrupt mapping SHALL never be classified as `not_disclosed` merely because its extracted text is non-empty or its heading matcher misses.

Alternative: expose only a `str` or Markdown result. Rejected because downstream evidence gates require page identity, exact text hashes, extraction method, and failure distinction.

### Alternate-native recovery precedes OCR when mapping corruption is suspected

For a page with a suspected `ToUnicode`/CMap mapping error, the router first attempts the configured alternate native adapter (initially `pdf-inspector` when available) because it may recover the embedded text without OCR loss. If alternate-native output does not pass the same quality policy, the page is sent to PaddleOCR. The result records every attempt, selected text source, and disagreement diagnostics. This keeps OCR bounded while handling PDFs that are visually readable but semantically undecodable through one library.

Alternative: treat any non-empty `pypdf` output as usable native text. Rejected because the 600036.SH sample demonstrates legal Unicode mojibake with zero reliable Chinese headings. Alternative: OCR every page that has any text-layer warning. Rejected because it wastes resources and loses embedded text when an alternate native parser can recover it.

### Engine profiles are configuration, not business branching

Add a configuration section for named profiles such as `pypdf_paddleocr`, `pdf_inspector_paddleocr`, and a native-only diagnostic profile. A profile selects native adapter, OCR adapter, routing policy, page/concurrency limits, model/runtime versions, and rollout state. Business modules request a profile or capability intent; they do not import an engine or branch on vendor names.

The initial production-safe profile retains `pypdf` as native baseline and uses PaddleOCR PP-OCR/PP-Structure for OCR. `pdf-inspector` is enabled first in evaluation/shadow mode and can become the native primary only after the same-corpus gate passes. The selected primary and fallback are recorded as configuration evidence, not inferred at runtime from timing.

Alternative: make `pdf-inspector` the immediate default because its upstream benchmark is fast. Rejected because it is a new project, its OCR path requires PDFium/ONNX/model runtime, and the upstream corpus does not prove Chinese announcement accuracy or current artifact compatibility.

### OCR adapter is page-addressable and auditable

PaddleOCR integration renders or receives only the requested page, reuses bounded model sessions, and returns text, coordinates/tables when available, confidence, model version, elapsed time, and warning codes. Missing native runtime/model, low confidence, render failure, and empty OCR output are distinct outcomes. OCR artifacts are content-hash and page-hash bound and never overwrite the source PDF.

OCR execution is a resource-governed stage rather than an unconstrained function call. The profile defines render resolution, maximum pages per request, maximum OCR concurrency, batch size, queue wait budget, per-page execution budget, per-document OCR budget, model warm-up policy, and CPU/RSS/GPU limits. A worker loads a model once and reuses it for bounded batches; it must not reload a model for every page. The router applies backpressure when the OCR queue is full and returns an explicit deferred/bounded outcome instead of allowing unbounded memory growth. Native-only pages bypass all OCR rendering and model work.

The component matrix must distinguish at least: PaddleOCR PP-OCR text recognition, PaddleOCR PP-Structure/PP-StructureV3 for layout/table pages, `pdf-inspector` selective OCR when its OCR runtime is available, and a lightweight Tesseract/OCRmyPDF baseline when operationally available. PP-Structure is not run on every page by default: it is selected only when table/layout output is requested or a profile explicitly opts in. A production profile may therefore use different OCR components for plain text pages and table-heavy pages.

`pdf-inspector` OCR may be supported as an optional adapter if its Python/native runtime is available, but the first implementation may use its detection/native extraction with PaddleOCR as the shared OCR backend. This keeps the OCR contract stable while runtime compatibility is evaluated.

### Evaluation reads database-referenced announcement assets but remains read-only

The benchmark accepts explicit local archive paths or a verified manifest derived from the announcement/asset database. It must verify PDF signatures and SHA-256 before running, use the same ordered bytes for every engine/profile, isolate derived outputs per trial, and enforce document/page/concurrency/time limits. It may read database metadata but cannot discover, download, mutate queues, write production artifacts, or change production configuration.

Evaluation reports include native/OCR fidelity, character and numeric/date accuracy against a labeled gold subset, page/heading/table evidence, cold-start and warm latency P50/P95, OCR pages per second, end-to-end documents/pages per minute, queue wait time, CPU/RSS/GPU/model-load metrics, model warm-up amortization, warnings, failure rates, and reproducibility hashes. Results must report native baseline time, native-plus-routing time, OCR-only page time, and full-document time so the OCR penalty is visible. A faster candidate is ineligible if it violates configured page/hash/evidence fidelity or fail-closed rules.

Selection uses a constrained Pareto decision, not a single accuracy ranking. A component must first pass accuracy/evidence floors; among passing candidates, the evaluator records whether it is preferred for plain text, table/layout, CPU-only, or GPU-enabled profiles based on tail latency and resource budgets. If no candidate meets both the accuracy floor and throughput/budget floor, production remains on the previous profile and the report identifies the blocking trade-off.

### Migration is vertical and compatibility-preserving

Migrate in this order: CNInfo corporate-action documents, business-profile page artifacts, broker risk-control extraction, then official-index/HKEX/announcement classification. Each slice keeps its existing domain parser and tests, replaces only PDF acquisition/extraction calls, and adds adapter-contract regression tests. Direct engine imports are removed only after all consumers use the shared path and the old behavior has a named fallback or is no longer needed.

No new shared abstraction is added to `data_manager.py`, `scheduler/tasks.py`, `api/routes.py`, or other governed facades. The PDF module is called by existing application/domain services, not by a new parallel entry point.

## Risks / Trade-offs

- **Chinese OCR misreads dates, decimals, or stock codes** → Require labeled numeric/date exact-match gates, confidence/warnings, and fail-closed evidence validation; keep native text preferred.
- **A viewer-readable page has a corrupt ToUnicode/CMap map but no replacement characters** → Detect script-distribution and cross-engine disagreement, preserve a `native_text_mapping_error` diagnostic, try alternate-native extraction, then selective OCR; never infer no disclosure from the bad text layer.
- **Tables and cross-page reading order differ by engine** → Preserve raw page text and coordinates, compare table/heading metrics in the benchmark, and do not make Markdown the authoritative evidence representation.
- **PaddleOCR/PDFium/ONNX runtime and model deployment is heavy** → Pin versions, cache models explicitly, bound concurrency/RSS, support offline installation, and keep OCR optional for native-only deployments.
- **OCR is materially slower than native extraction and creates backlog** → Measure cold/warm and end-to-end cost separately, route only affected pages, warm/reuse model workers, batch where supported, cap OCR queue/concurrency, apply backpressure, and expose deferred/budget-exceeded outcomes instead of unbounded retries.
- **One OCR component is accurate but too slow or resource-heavy for all pages** → Use profile specialization: PP-OCR for plain text, PP-Structure only for table/layout pages, `pdf-inspector` selective routing where validated, and retain a lightweight fallback for constrained workers.
- **`pdf-inspector` changes rapidly or lacks a compatible wheel** → Keep it behind an adapter and shadow/evaluation profile; retain `pypdf` as fallback until production gates pass.
- **Business consumers silently depend on current whitespace or page omission behavior** → Add consumer-specific golden fixtures, preserve extraction method/status semantics, and classify behavior changes as migration failures rather than normalizing them away.
- **Benchmark corpus is biased or contains sensitive documents** → Use explicit local manifests, redact/report only metrics and hashes, keep raw text out of shared reports, and stratify native/scanned/mixed/garbled documents.
- **Concurrent work modifies the same business-profile or announcement-asset files** → Keep the shared module and adapters in new paths, coordinate each consumer slice, and do not revert or overwrite pre-existing worktree changes.

## Migration Plan

1. Freeze current direct PDF call sites and create a bounded, hash-verified corpus from database-referenced announcement assets, explicitly including the 600036.SH 2025 annual report fixture when available.
2. Implement the shared contract, diagnostics, engine profile loader, native `pypdf` adapter, and no-op/native-only path with compatibility tests.
3. Implement PaddleOCR PP-OCR/PP-Structure adapter and selective routing; add optional `pdf-inspector` native/detection adapter and bounded OCR worker/backpressure controls.
4. Run engine/profile evaluation, including the OCR component matrix and cold/warm throughput report, and publish a versioned report. Keep current production defaults until fidelity, accuracy, latency, and resource gates pass.
5. Migrate consumers in the stated vertical order, retaining domain artifact schemas and evidence gates; run focused and cross-consumer regression suites after each slice.
6. Select and record primary/fallback engine profiles only after the evaluation gate; enable OCR rollout with bounded canary scope and rollback to the previous profile.
7. Remove direct business-level PDF engine imports and obsolete compatibility paths once the consumer inventory is zero and the shared contract is proven.

Rollback is configuration-first: disable the new profile or OCR route and select the retained native fallback. Consumer code remains compatible during migration; no source PDF or canonical fact is deleted. A failed evaluation blocks default switching but does not block retaining the shared module for further adapter work.

## Open Questions

- Whether `pdf-inspector` Python OCR runtime is deployable on every Quote worker without a separate native image remains an evaluation task.
- The final OCR model variant, page resolution, and concurrency limits depend on measured Chinese announcement accuracy and host resources.
- The exact final package path and config key names can be chosen during implementation as long as the public contract and profile semantics remain stable.
- Whether broker risk-control and HKEX parsers need coordinate/table output beyond page text must be confirmed by their migration fixtures.
