## ADDED Requirements

### Requirement: Shared PDF processing exposes a versioned page-level contract
The shared PDF module SHALL accept verified PDF bytes or an equivalent content-hash-bound source and SHALL return a versioned document result containing ordered page results, document status, engine versions, input hash, parameter hash, and diagnostics.

#### Scenario: Native PDF is parsed successfully
- **WHEN** a caller submits a valid PDF with a configured native engine
- **THEN** the module SHALL return the page count and one result per processed page with one-based page numbers, text, extraction method, quality status, text hash, and page-result hash
- **AND** the document result SHALL identify the native engine and parser configuration version

#### Scenario: PDF input is invalid or malformed
- **WHEN** the submitted bytes fail PDF signature, page-tree, encryption, or parsing validation
- **THEN** the module SHALL return a typed document failure with diagnostics and input hash
- **AND** it SHALL NOT return the document as a successful empty-text result

### Requirement: Engine selection is profile-driven
The shared module SHALL select native and OCR adapters through named configuration profiles, including adapter versions, routing policy, resource limits, rollout state, and fallback profile; business consumers SHALL NOT branch on vendor-specific engine imports.

#### Scenario: Caller selects a configured profile
- **WHEN** a business service requests a named PDF engine profile
- **THEN** the module SHALL resolve the native adapter, OCR adapter, routing policy, and limits from configuration
- **AND** an unknown, disabled, or incomplete profile SHALL fail before PDF processing

#### Scenario: Profile is changed without consumer code changes
- **WHEN** an operator changes the selected profile from `pypdf_paddleocr` to a validated `pdf_inspector_paddleocr` profile
- **THEN** the same consumer request contract SHALL invoke the newly configured adapters
- **AND** the result SHALL record the effective profile and engine versions

### Requirement: Native extraction precedes selective OCR
The module SHALL prefer usable native text and SHALL route only pages that are empty, below the configured quality threshold, affected by glyph-decoding/extraction or text-mapping diagnostics, or explicitly selected by the request/policy to an alternate native adapter or OCR adapter.

#### Scenario: Text-based PDF does not need OCR
- **WHEN** native extraction produces text that passes the configured quality policy
- **THEN** the module SHALL return `native_text` page results without invoking the OCR adapter for those pages
- **AND** the result SHALL record that OCR was not required

#### Scenario: Mixed PDF contains affected pages
- **WHEN** native diagnostics identify only a subset of pages as empty, low quality, or glyph-decoding-error
- **THEN** the module SHALL invoke OCR only for that subset
- **AND** unaffected pages SHALL retain their native extraction method and text hash

#### Scenario: Viewer-readable PDF has a broken ToUnicode mapping
- **WHEN** a PDF renders readable text in a viewer but the configured native adapter returns suspicious script distribution, replacement/control glyphs, ToUnicode/CMap warnings, or a material disagreement with an alternate native sample
- **THEN** the module SHALL classify the affected page as `native_text_mapping_error` or an equivalent typed mapping diagnostic even when the extracted string is non-empty
- **AND** it SHALL attempt the configured alternate native adapter before OCR when that adapter is available
- **AND** it SHALL not classify the page as a successful usable-native page or infer `not_disclosed` from the corrupted text

#### Scenario: Alternate native recovery fails for a mapping-corrupt page
- **WHEN** the alternate native adapter cannot produce text that passes the configured quality policy
- **THEN** the router SHALL send only that page to the configured OCR adapter
- **AND** the page result SHALL retain the mapping diagnostic and record the alternate-native and OCR attempts in provenance

### Requirement: OCR results preserve provenance and quality outcomes
An OCR adapter SHALL be page-addressable and SHALL return text, extraction method, confidence when available, model/runtime version, timing, and warnings; OCR output SHALL remain bound to the source content hash and page number.

#### Scenario: OCR succeeds for a selected page
- **WHEN** the OCR adapter recognizes a selected page
- **THEN** the page result SHALL use `extraction_method=ocr`, include OCR provenance and confidence or an explicit unavailable-confidence diagnostic
- **AND** its text and page-result hashes SHALL be reproducible for the same input, profile, and model configuration

#### Scenario: OCR runtime is unavailable or low quality
- **WHEN** rendering, model loading, recognition, or configured quality checks fail
- **THEN** the page SHALL receive a typed OCR failure or low-quality status and warning
- **AND** the module SHALL NOT silently convert the page to a successful empty disclosure

### Requirement: Shared PDF processing is bounded and side-effect controlled
The shared module SHALL enforce configured page, OCR concurrency, elapsed-time, queue wait, per-page/per-document OCR budget, memory/model-cache, and output-size limits; parsing SHALL not mutate announcement records, source PDFs, business facts, or queues.

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
- **WHEN** a page passes native quality checks and does not request table/layout OCR
- **THEN** the module SHALL not render the page or invoke an OCR model
- **AND** the page result SHALL expose enough timing data to prove the native path avoided OCR work

### Requirement: OCR execution reuses bounded warm workers and selects components by need
The module SHALL support warm model/session reuse and bounded batching, and SHALL allow profiles to select PP-OCR, PP-Structure, `pdf-inspector` OCR, or a lightweight fallback based on page requirements and resource policy.

#### Scenario: Plain text OCR page is processed
- **WHEN** a selected page requires text OCR but no table/layout structure
- **THEN** the profile SHALL be able to use a text-focused OCR component such as PP-OCR without invoking PP-Structure
- **AND** the result SHALL record model warm-up, batch, and inference timing

#### Scenario: Table page is processed
- **WHEN** a selected page requires table or layout coordinates
- **THEN** the profile SHALL be able to route it to PP-Structure or an equivalent structure-capable component
- **AND** structure processing SHALL be limited to requested/table-likely pages rather than all document pages by default

#### Scenario: Warm worker processes multiple pages
- **WHEN** multiple OCR pages are processed under one profile
- **THEN** the module SHALL reuse a bounded model/session and batch pages where the component supports batching
- **AND** it SHALL report cold-start, warm-up amortization, per-page, and batch timings separately

### Requirement: Business semantics remain outside the shared PDF module
The shared module SHALL expose technical page evidence only; business consumers SHALL retain ownership of document selection, table interpretation, financial semantics, LLM evidence validation, and database writes.

#### Scenario: Business consumer receives shared page results
- **WHEN** a company-action, business-profile, broker-risk, index, HKEX, or classification service invokes shared PDF processing
- **THEN** it SHALL receive page-level evidence through the common contract
- **AND** its existing domain parser and fact/promotion gates SHALL remain the authority for business meaning
