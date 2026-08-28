## ADDED Requirements

### Requirement: All production PDF consumers use the shared execution path
The migration SHALL inventory and route every production PDF parser call site through the shared PDF processing contract, including CNInfo company actions, business profiles, broker risk control, official index lifecycle, HKEX suspension reports, and announcement classification.

#### Scenario: Consumer inventory is complete
- **WHEN** migration planning or validation scans production modules for PDF engine imports or page extraction calls
- **THEN** every discovered call site SHALL be mapped to a shared-module adapter/consumer path or an explicitly approved non-production exception
- **AND** the inventory SHALL identify an owner, migration slice, and removal condition

#### Scenario: Migrated consumer parses a PDF
- **WHEN** a migrated consumer needs PDF text or page evidence
- **THEN** it SHALL invoke the shared module using a configured profile
- **AND** it SHALL not instantiate `PdfReader`, PaddleOCR, PDFium, ONNX Runtime, or `pdf-inspector` directly

### Requirement: Domain parsing semantics remain behaviorally owned by each consumer
The migration SHALL preserve existing domain-specific selection, normalization, table interpretation, evidence validation, database ownership, and fail-closed rules while replacing only the technical PDF extraction dependency.

#### Scenario: CNInfo company-action evidence is migrated
- **WHEN** the CNInfo pipeline receives shared native/OCR page results
- **THEN** page numbers, extraction methods, quality statuses, text hashes, and OCR warnings SHALL remain available to semantic extraction and verification
- **AND** unresolved or low-quality OCR pages SHALL retain existing manual/fail-closed behavior

#### Scenario: Business-profile artifact is migrated
- **WHEN** business-profile extraction consumes shared page results
- **THEN** its existing artifact identity, heading index, low-text/OCR-required diagnostics, and promotion evidence requirements SHALL remain compatible
- **AND** the shared module SHALL not implement business-profile heading or field semantics

#### Scenario: Business-profile report has visually readable but corrupt native text
- **WHEN** a business-profile annual report such as the 600036.SH fixture is flagged with a native text mapping diagnostic
- **THEN** section discovery SHALL treat affected pages as requiring alternate-native recovery or governed OCR rather than as absent disclosure
- **AND** promotion/evidence gates SHALL reject only when recovered text is unavailable or low quality, not merely because the first native parser's `ToUnicode` mapping failed

#### Scenario: Specialized table or report parser is migrated
- **WHEN** broker-risk, index, HKEX, or announcement classification code consumes shared results
- **THEN** its existing row/order/title semantics SHALL remain in that domain module
- **AND** a technical extraction change that alters a governed result SHALL block migration until its fixture and evidence gate are updated explicitly

### Requirement: Migration is staged with compatibility and rollback gates
The migration SHALL proceed in vertical slices ordered by CNInfo company actions, business profiles, broker risk control, then index/HKEX/classification consumers; each slice SHALL pass focused regression tests before the next slice removes direct engine usage.

#### Scenario: A migration slice passes
- **WHEN** a consumer slice passes adapter-contract, golden-output, error, and performance tests
- **THEN** the slice SHALL be allowed to select the shared profile in its configured environment
- **AND** the previous parser behavior SHALL remain available as a named fallback until the next release gate completes

#### Scenario: A migration slice fails
- **WHEN** a consumer slice changes evidence hashes, page selection, table semantics, or failure classification unexpectedly
- **THEN** the slice SHALL keep the previous path active and report the blocking mismatch
- **AND** it SHALL not remove the direct dependency or change the production default

### Requirement: Direct PDF engine imports are removed after migration
After all production consumers pass migration gates, production business modules SHALL contain no direct imports or instantiation of configured PDF engines; compatibility code SHALL be one-way delegation with an explicit removal condition.

#### Scenario: Repository residue check runs
- **WHEN** the final migration validation scans production code
- **THEN** direct `pypdf`, PaddleOCR, PDFium/ONNX, and `pdf-inspector` usage outside shared adapters SHALL be zero except documented non-production tooling
- **AND** the result SHALL list any remaining exception and its removal condition

### Requirement: Migration preserves production ownership and public behavior
The migration SHALL not change announcement discovery, attachment/archive ownership, database paths or schemas, scheduler job identity, public API/CLI contracts, or financial time semantics unless a separate approved change explicitly requires it.

#### Scenario: Shared module is enabled for a migrated consumer
- **WHEN** the consumer switches from its direct parser to the shared path
- **THEN** the same source asset identity, page references, business output contract, and write owner SHALL be preserved
- **AND** no second writer, alternate archive, or parallel business execution path SHALL be introduced
