## MODIFIED Requirements

### Requirement: Engine selection is profile-driven
The shared module SHALL select an ordered native adapter chain and an ordered OCR runtime policy through named configuration profiles, including adapter/runtime versions, routing policy, resource limits, rollout state, and fallback profile; business consumers SHALL NOT branch on vendor-specific engine imports. The replacement production profiles SHALL use PDFium first with `pypdf` as native fallback. `pdf_inspector_paddleocr` SHALL be retired as a production native profile after caller migration, and the old `pypdf_paddleocr_gpu_canary` approval SHALL NOT authorize a replacement PDFium-first profile.

#### Scenario: Caller selects a configured profile
- **WHEN** a business service requests a named PDF engine profile
- **THEN** the module SHALL resolve the ordered native adapters, OCR runtimes, routing policy, and limits from configuration
- **AND** an unknown, disabled, incomplete, or retired profile SHALL fail before PDF processing

#### Scenario: Profile is changed without consumer code changes
- **WHEN** an operator changes from the retained `pypdf` rollback profile to a validated PDFium-first profile
- **THEN** the same consumer request contract SHALL invoke the newly configured ordered adapters
- **AND** the result SHALL record the effective profile, chain order, and engine/runtime versions

#### Scenario: Inspector native profile is retired
- **WHEN** production callers and configuration have migrated from `pdf_inspector_paddleocr`
- **THEN** the profile and inspector native routing SHALL be removed
- **AND** evaluation, classification, and inspector OCR uses SHALL be inventoried independently before the package dependency can be removed

### Requirement: Native extraction precedes selective OCR
Except for explicit `force_ocr`, the module SHALL prefer the first usable page candidate from the configured ordered native engine chain and SHALL route only explicit target pages for which every configured native candidate is empty, below the configured quality/script threshold, affected by glyph-decoding/extraction or text-mapping diagnostics, or otherwise unusable to OCR. Page-level quality SHALL take precedence over document-level labels such as `mixed`.

#### Scenario: Text-based PDF does not need OCR
- **WHEN** a native engine produces page text that passes the configured technical and expected-script quality policy
- **THEN** the module SHALL return a native page result identifying the selected engine without invoking later native engines or OCR for that page
- **AND** the result SHALL record that OCR was not required

#### Scenario: Mixed PDF contains affected pages
- **WHEN** a document is classified as `mixed` but only a subset of requested pages fails every native engine
- **THEN** the module SHALL invoke OCR only for the failed requested subset
- **AND** natively usable pages SHALL retain their selected native engine and text hash regardless of the document classification

#### Scenario: Viewer-readable PDF has a broken ToUnicode mapping
- **WHEN** a PDF renders readable text in a viewer but a native adapter returns suspicious script distribution, replacement/control glyphs, ToUnicode/CMap warnings, or insufficient configured-language evidence
- **THEN** the module SHALL classify the affected page as `native_text_mapping_error` or an equivalent typed diagnostic even when the extracted string is non-empty
- **AND** it SHALL attempt the next configured native adapter before OCR under `native_first` or `selective_recovery`
- **AND** it SHALL not classify the page as usable native text or infer `not_disclosed` from corrupted text

#### Scenario: Numeric residue is not usable Chinese evidence
- **WHEN** a Chinese-expected page contains important-looking ASCII/numeric fragments but no Chinese/script evidence required by the profile
- **THEN** the technical gate SHALL mark the candidate unusable rather than selecting the surviving numbers as semantic evidence

#### Scenario: All native recovery fails
- **WHEN** every configured native adapter fails the quality gate for an explicit target page under `selective_recovery`
- **THEN** the router SHALL send only that page to the configured OCR runtime
- **AND** the result SHALL retain every native attempt and diagnostic in provenance

### Requirement: Shared PDF processing is bounded and side-effect controlled
The shared module SHALL enforce configured page, OCR concurrency, elapsed-time, queue wait, per-page/per-document OCR budget, memory/model-cache, and output-size limits; parsing SHALL not mutate announcement records, source PDFs, business facts, or queues. Each native adapter SHALL open a submitted PDF at most once per request/adapter attempt and extract all pages assigned to that adapter without reopening the document for each page.

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

#### Scenario: Native pages bypass OCR cost
- **WHEN** a page passes native quality checks under a non-`force_ocr` policy and does not request table/layout OCR
- **THEN** the module SHALL not render the page or invoke an OCR model
- **AND** the page result SHALL expose enough timing data to prove the native path avoided OCR work

#### Scenario: Fallback adapter receives multiple failed pages
- **WHEN** several requested pages fail the first native engine
- **THEN** the router SHALL pass the failed page set to the fallback adapter in one adapter attempt
- **AND** per-page selection semantics SHALL NOT cause the fallback adapter to reopen the PDF once per page

### Requirement: OCR execution reuses bounded warm workers and selects components by need
The module SHALL extend the existing `PaddleOcrAdapter` boundary to reuse bounded, versioned worker sessions and batching. Production PaddleOCR SHALL run in isolated CPU or GPU worker environments rather than importing a GPU runtime into the Quote process. Profiles SHALL select PP-OCR or PP-Structure based on requested page needs; inspector OCR and other experimental components SHALL remain evaluation-only until separately accepted.

#### Scenario: Plain text OCR page is processed
- **WHEN** a selected page requires text OCR but no table/layout structure
- **THEN** the profile SHALL use the configured PP-OCR worker without invoking PP-Structure
- **AND** the result SHALL record worker/runtime, model warm-up, batch, device, and inference timing

#### Scenario: Table page is processed
- **WHEN** a selected page requires table or layout coordinates
- **THEN** the profile SHALL route it to PP-Structure or an equivalent accepted structure component
- **AND** structure processing SHALL be limited to requested/table-likely pages rather than all document pages by default

#### Scenario: Warm worker processes multiple pages
- **WHEN** multiple OCR pages are processed under one profile
- **THEN** the module SHALL reuse a bounded model/session and batch pages where the component supports batching
- **AND** it SHALL report cold-start, warm-up amortization, per-page, and batch timings separately

#### Scenario: Quote probes a GPU runtime
- **WHEN** the shared module checks GPU availability or model health
- **THEN** it SHALL invoke the isolated worker's versioned capability probe
- **AND** the Quote process SHALL NOT import a CUDA Paddle package for liveness detection

## ADDED Requirements

### Requirement: PDFium is a first-class native and rendering dependency
The production dependency manifest SHALL directly pin `pypdfium2==5.13.0` for native extraction and OCR rendering. The PDFium native adapter SHALL return one-based physical pages, deterministic text/hash output, page count, engine version, elapsed time, and typed open/page/extraction failures without performing OCR.

#### Scenario: PDFium recovers a malformed mapping page
- **WHEN** PDFium extracts usable Chinese text from a page whose `pypdf` output is a mapping error
- **THEN** the PDFium candidate SHALL be selected as native text without OCR

#### Scenario: PDFium cannot recover a scanned page
- **WHEN** PDFium opens a scanned page but returns no text that passes the quality gate
- **THEN** it SHALL return an unusable native candidate rather than treating rendered image content as native text
