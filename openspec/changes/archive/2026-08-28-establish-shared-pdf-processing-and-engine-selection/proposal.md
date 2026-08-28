## Why

PDF handling is currently split across company-action, business-profile, broker-risk, index, HKEX, and announcement-classification code. The project has shared attachment retrieval and archive ownership, but no shared page-level parser/OCR contract; `pypdf` is used directly and scanned PDFs are diagnosed but not reliably processed.

The 600036.SH 2025 annual report demonstrates a more subtle failure: the PDF is visually readable, but `pypdf` extracts pages through a broken `ToUnicode`/CMap mapping and returns legal Unicode from unrelated scripts. Treating this as usable native text can hide headings and facts; treating it as an empty document is also incorrect.

The project now needs one auditable PDF capability for listed-company announcements: native extraction first, selective OCR for non-native pages, configurable engine selection, and evidence-based engine evaluation before any production default is chosen.

## What Changes

- Add an independent shared PDF processing module with page-level results, input/parameter hashes, parser versions, quality diagnostics, OCR provenance, and bounded execution controls.
- Detect visually readable PDFs whose text layer has a broken `ToUnicode`/CMap mapping, distinguish mapping corruption from an actually scanned page, and route affected pages through a validated alternate native engine or OCR.
- Support configurable native/OCR engine profiles using `pypdf`, PaddleOCR PP-OCR/PP-Structure, and `pdf-inspector`; keep engine selection out of business-specific parsers.
- Add a read-only benchmark and evaluation workflow over explicit local copies of database-referenced announcement PDFs, comparing fidelity, OCR accuracy, cold/warm latency, throughput, resource use, queue behavior, and failure behavior for each OCR component/profile.
- Treat OCR as a bounded, potentially bottlenecking stage: warm model sessions, selective page routing, batched pages where supported, backpressure, per-document/page budgets, and explicit native-versus-OCR cost metrics are required.
- Record an evidence-backed primary and fallback engine decision without changing production defaults before the evaluation gate passes.
- Migrate every production PDF call site to the shared module while preserving each domain's own classification, table semantics, evidence validation, and database ownership.
- Retain compatibility/fallback behavior during migration and remove direct business-level PDF engine imports after all consumers pass regression gates.

Non-goals:

- No replacement of announcement discovery, attachment retrieval, immutable archive ownership, or shared annual-report asset APIs.
- No generic financial semantic parser, LLM orchestration, fact approval, or database write owner in the PDF module.
- No mandatory Docling, MinerU, Marker, or Unstructured integration in this change.
- No production engine/default or OCR rollout based only on upstream benchmarks or uncontrolled production timing.
- No assumption that a PDF viewer's visual readability proves that extracted native text is semantically usable.
- No OCR component becomes production default merely because it has the highest recognition score; accuracy, throughput, tail latency, resource cost, and failure behavior must be evaluated together.

## Capabilities

### New Capabilities

- `shared-pdf-processing`: Versioned, page-level native extraction, selective OCR routing, diagnostics, provenance, and configurable engine adapters.
- `pdf-engine-evaluation`: Read-only same-document evaluation of configured engines against explicit database-referenced announcement corpora, with accuracy, performance, resource, and failure evidence.
- `pdf-parser-consumer-migration`: One shared PDF execution path for all current production consumers, with domain-owned parsing semantics and migration/compatibility gates.

### Modified Capabilities

None. Existing business capabilities keep their domain requirements; this change modifies their implementation dependency to use the new shared PDF capability.

## Impact

- New shared code under `research/document_processing/pdf/` (or the final equivalent module path), configuration for engine profiles, and focused unit/integration/benchmark tests.
- Adapters for `pypdf`, PaddleOCR PP-OCR/PP-Structure, and optional `pdf-inspector`; native runtimes and OCR model caches must be explicitly versioned and bounded.
- Migration of current direct PDF call sites in CNInfo company actions, business profiles, broker risk control, official index lifecycle, HKEX suspension reports, and announcement classification.
- Existing page artifact schemas and evidence gates remain compatible; new OCR artifacts may add provenance/confidence fields without overwriting raw PDF bytes.
- No database schema or production data mutation is required for the module itself; evaluation may read database metadata and explicitly archived PDFs but writes only bounded reports and temporary artifacts.
- The 600036.SH 2025 annual report is a required regression/evaluation sample when its archived PDF is available: announcement `ann_e9a7df3862148a4699fd3a36284fe1c7`, content hash `abe612a273468072b176dd51ea460c1e1596f8ca729cbc6db3fa28ba9a57ea79`, 350 pages, and observed `pypdf` glyph-mapping diagnostics on 341 pages.
