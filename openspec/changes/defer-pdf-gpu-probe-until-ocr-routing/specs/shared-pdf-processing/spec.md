## MODIFIED Requirements

### Requirement: Engine selection is profile-driven
The shared module SHALL select an ordered native adapter chain and an ordered OCR runtime policy through named configuration profiles, including adapter/runtime versions, routing policy, resource limits, rollout state, and fallback profile; business consumers SHALL NOT branch on vendor-specific engine imports. The replacement production profiles SHALL use PDFium first with `pypdf` as native fallback. `pdf_inspector_paddleocr` SHALL be retired as a production native profile after caller migration, and the old `pypdf_paddleocr_gpu_canary` approval SHALL NOT authorize a replacement PDFium-first profile. Resolving or constructing an approved GPU profile SHALL validate static approval evidence but SHALL NOT start or probe the live OCR worker.

#### Scenario: Caller selects a configured profile
- **WHEN** a business service requests a named PDF engine profile
- **THEN** the module SHALL resolve the ordered native adapters, OCR runtimes, routing policy, and limits from configuration
- **AND** an unknown, disabled, statically incomplete, unapproved, or retired profile SHALL fail before PDF processing
- **AND** a missing or currently unhealthy OCR worker SHALL NOT make an otherwise approved profile statically incomplete

#### Scenario: Approved GPU profile is resolved while runtime is unavailable
- **WHEN** the GPU approval report, corpus identity, rollout checks, and profile definition are valid but the worker command is absent, failing, or slow
- **THEN** `resolve_profile()`, default `PdfParseRequest` construction, and `build_router(profile)` SHALL complete without invoking the worker probe
- **AND** runtime availability SHALL be evaluated only if the request later produces uncached OCR work

#### Scenario: Profile is changed without consumer code changes
- **WHEN** an operator changes from the retained `pypdf` rollback profile to a validated PDFium-first profile
- **THEN** the same consumer request contract SHALL invoke the newly configured ordered adapters
- **AND** the result SHALL record the effective profile, chain order, and engine/runtime versions

#### Scenario: Inspector native profile is retired
- **WHEN** production callers and configuration have migrated from `pdf_inspector_paddleocr`
- **THEN** the profile and inspector native routing SHALL be removed
- **AND** evaluation, classification, and inspector OCR uses SHALL be inventoried independently before the package dependency can be removed

### Requirement: Shared PDF processing is bounded and side-effect controlled
The shared module SHALL enforce configured page, OCR concurrency, elapsed-time, queue wait, per-page/per-document OCR budget, memory/model-cache, and output-size limits; parsing SHALL not mutate announcement records, source PDFs, business facts, or queues. Each native adapter SHALL open a submitted PDF at most once per request/adapter attempt and extract all pages assigned to that adapter without reopening the document for each page. PDFium text extraction and PDFium rasterization SHALL execute in a supervised native worker outside the Quote parent process. Live OCR readiness checks, rendering, inference, and configured fallback SHALL share the request's original effective document deadline.

#### Scenario: Request exceeds a configured limit
- **WHEN** a request exceeds page, concurrency, elapsed-time, or resource bounds
- **THEN** the module SHALL stop or reject the request with a typed bounded-execution diagnostic
- **AND** it SHALL preserve already-produced page results as explicitly partial rather than successful complete output

#### Scenario: Same content is parsed repeatedly
- **WHEN** the same content hash and parameter hash are processed more than once
- **THEN** the module SHALL produce equivalent page identities and hashes
- **AND** any cache reuse SHALL remain content/profile/version bound

#### Scenario: OCR queue reaches its bound
- **WHEN** pending OCR work reaches the configured queue, concurrency, or time budget
- **THEN** the module SHALL apply backpressure and return an explicit deferred or budget-exceeded outcome for affected pages
- **AND** it SHALL not grow an unbounded in-memory queue, reload the model per page, or silently drop OCR work

#### Scenario: Native pages bypass all OCR cost
- **WHEN** a request uses `ocr_mode=none` or `native_first`, or every relevant page passes native quality checks under a non-`force_ocr` policy
- **THEN** the module SHALL NOT probe an OCR runtime, render an OCR image, invoke an OCR model, or wait for OCR worker readiness
- **AND** the page result SHALL expose enough timing data to prove the native path avoided OCR work

#### Scenario: Cached OCR pages bypass runtime readiness
- **WHEN** every page selected for OCR is served by a valid profile/version-bound page cache entry
- **THEN** the module SHALL return the cached page results without probing or invoking the OCR worker

#### Scenario: Fallback adapter receives multiple failed pages
- **WHEN** several requested pages fail the first native engine
- **THEN** the router SHALL pass the failed page set to the fallback adapter in one adapter attempt
- **AND** per-page selection semantics SHALL NOT cause the fallback adapter to reopen the PDF once per page

### Requirement: OCR execution reuses bounded warm workers and selects components by need
The module SHALL extend the existing `PaddleOcrAdapter` boundary to reuse bounded, versioned worker sessions and batching. Production PaddleOCR SHALL run in isolated CPU or GPU worker environments rather than importing a GPU runtime into the Quote process. Profiles SHALL select PP-OCR or PP-Structure based on requested page needs; inspector OCR and other experimental components SHALL remain evaluation-only until separately accepted. Live GPU readiness SHALL be checked lazily at this adapter boundary only for a non-empty uncached OCR page set.

#### Scenario: Plain text OCR page is processed
- **WHEN** a selected uncached page requires text OCR but no table/layout structure
- **THEN** the profile SHALL lazily ensure the configured PP-OCR runtime is ready and use that worker without invoking PP-Structure
- **AND** the result SHALL record worker/runtime, model warm-up, batch, device, probe, and inference timing

#### Scenario: Table page is processed
- **WHEN** a selected page requires table or layout coordinates
- **THEN** the profile SHALL route it to PP-Structure or an equivalent accepted structure component
- **AND** structure processing SHALL be limited to requested/table-likely pages rather than all document pages by default

#### Scenario: Warm worker processes multiple pages
- **WHEN** multiple OCR pages are processed under one profile
- **THEN** the module SHALL reuse a bounded model/session and batch pages where the component supports batching
- **AND** it SHALL report cold-start, warm-up amortization, per-page, and batch timings separately

#### Scenario: Quote probes a GPU runtime for selected OCR work
- **WHEN** at least one uncached page is about to enter GPU OCR or an evaluator explicitly requests a capability check
- **THEN** the module SHALL invoke the isolated worker's versioned capability probe
- **AND** the Quote process SHALL NOT import a CUDA Paddle package for liveness detection
- **AND** the probe timeout SHALL NOT exceed the configured cold-start timeout or the remaining effective document/evaluation budget

#### Scenario: Successful readiness is reused
- **WHEN** an equivalent worker command, runtime, model-cache path, and device has already passed its process-local probe
- **THEN** subsequent OCR work in that process SHALL reuse the successful readiness result without repeating cold-start probing
- **AND** a failed probe SHALL retain its diagnostic and remain retryable by a later OCR request

## ADDED Requirements

### Requirement: Named-profile consumers use the authoritative shared router
Production consumers that resolve a named shared PDF profile MUST construct adapters through `build_router(profile)` or an equivalent authoritative shared-module factory. They MUST NOT combine a named/default `PdfParseRequest` profile with a separately constructed direct `PdfRouter()` that bypasses the configured native isolation or OCR policy. Explicit custom adapters MAY retain a direct router only when they also provide an explicit custom profile and do not claim named-profile behavior.

#### Scenario: Official index PDF uses the shared profile
- **WHEN** the official-index lifecycle parser extracts text through the default or configured shared profile
- **THEN** its request and router SHALL use the same resolved profile and authoritative adapter factory
- **AND** no public official-index parser input or output SHALL change

#### Scenario: Explicit custom corporate-action adapter remains isolated from named profiles
- **WHEN** a corporate-action parser supplies its own explicit profile and custom OCR adapter
- **THEN** it MAY retain the direct custom router boundary without inheriting the global GPU profile
