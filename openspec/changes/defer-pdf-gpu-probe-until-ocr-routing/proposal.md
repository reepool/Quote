## Why

When `QUOTE_PDF_ENGINE_PROFILE=pdfium_paddleocr_gpu`, resolving the profile or constructing its router currently launches the isolated GPU worker probe before the request has selected any OCR pages. A transient GPU cold-start or health failure therefore blocks native-only PDF extraction even though `ocr_mode="none"` and `native_first` can complete entirely through PDFium/pypdf; this caused the 2026-08-28 HKEX prolonged-suspension PDF to be dropped during daily maintenance.

## What Changes

- Separate static GPU canary approval from live OCR worker readiness: profile resolution continues to validate the approval report, corpus hash, rollout state, and profile definition and binds configured worker commands/cache paths, but command absence or live worker health is evaluated only when OCR is required.
- Construct the shared router without probing GPU health. Probe only after native routing and page-cache lookup have produced a non-empty set of uncached pages that will actually enter GPU OCR.
- Preserve the successful-probe process cache, configurable cold-start timeout, worker-only CUDA isolation, and detailed probe diagnostic introduced by `8eac643`.
- Route a GPU probe failure through the existing typed GPU-to-CPU fallback policy and original request budget. Preserve successful native pages and candidates; expose an explicit unavailable/partial result when recovery cannot run.
- Keep `PdfParseRequest`, `PdfProfile`, `PdfDocumentResult`, page artifact fields, named profiles, environment variables, cache identity, and business caller inputs unchanged.
- Migrate the remaining official-index direct `PdfRouter()` construction to the existing `build_router(request.profile)` path so it receives the same isolated native and optional OCR behavior as other consumers.
- Add regression coverage for native-only operation with a missing/failing GPU worker, lazy probe timing, cache-hit bypass, fallback/provenance, force-OCR failure, and the HKEX/official-index shared-router paths.
- Update the current OCR worker runbook and shared PDF documentation. Absorb the accepted rules from `pdf_gpu_probe_gate_proposal_20260828.md`; remove that one-off requirements note after implementation so it does not become a second current contract.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `shared-pdf-processing`: Make live GPU capability probing an OCR-execution gate rather than a profile-resolution/router-construction gate while retaining static canary approval and isolated runtime safety.
- `shared-pdf-page-recovery-contract`: Define page/document results, budgets, fallback, cache-hit behavior, and provenance when the lazily probed GPU runtime is unavailable.

## Impact

- **Change dependency:** this is a follow-up delta to `optimize-shared-pdf-native-and-gpu-routing`; that completed change and its predecessor PDF capability deltas MUST be reconciled/archived first, then this change is applied and archived, so the lazy-probe wording cannot overwrite or discard the native-worker isolation and parallelism contract.
- **Authoritative owner:** `research.document_processing.pdf`; this change does not introduce another PDF router, worker protocol, cache, or business-specific parser.
- **Primary code:** `profiles.py`, `adapters.py`, and `core.py`, plus focused tests and current PDF runbooks.
- **Caller cleanup:** `data_sources/official_index_source.py` is migrated from direct router construction; HKEX, business profile, announcement classification, and broker risk-control callers retain their existing interfaces.
- **Production invariants:** no CUDA Paddle import in Quote, no implicit full-document OCR, no unapproved GPU profile, no changed scheduler/API/config/database contract, no source-PDF mutation, and no change to business write ownership.
- **Governance:** satisfies framework program FR-15 by keeping all affected domains on the single shared PDF execution path; it does not broaden the framework-refactoring program.
- **Non-goals:** changing HKEX partial-source lifecycle policy, automatically enabling OCR for HKEX requests that still specify `ocr_mode="none"`, background health infrastructure, a new observability platform, or a new PDF capability.
