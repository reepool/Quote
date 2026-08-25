## ADDED Requirements

### Requirement: Ordered native engine chain
The shared PDF module SHALL allow each parser profile to configure a non-empty ordered native engine chain and SHALL attempt engines independently per requested physical page. It SHALL stop at the first candidate that passes the shared technical quality gate and SHALL not execute later native engines for that page.

#### Scenario: Primary native engine succeeds
- **WHEN** the first configured native engine returns text that passes the technical quality gate
- **THEN** the module selects that candidate and does not invoke later native engines for the page

#### Scenario: Primary native engine fails quality
- **WHEN** the first configured native engine returns an exception, empty text, mapping error, or other semantically unusable result
- **THEN** the module records that candidate and attempts the next configured native engine for the same page

#### Scenario: All native engines fail
- **WHEN** every configured native engine produces an unusable candidate for a page
- **THEN** the module selects no native text and exposes the page for OCR only when the request recovery policy permits it

### Requirement: PDFium native adapter
The shared PDF module SHALL provide a `pypdfium2` native-text adapter that uses one-based physical page semantics, returns deterministic page text and hash, reports page count and typed failures, and does not perform OCR.

#### Scenario: Malformed ToUnicode mapping is recoverable through PDFium
- **WHEN** PDFium extracts usable Chinese text from a page whose `pypdf` candidate is classified as `native_text_mapping_error`
- **THEN** the PDFium candidate is eligible for selection as native text without scheduling OCR

#### Scenario: Scanned page has no usable native text
- **WHEN** PDFium opens a scanned page but returns no text that passes the technical quality gate
- **THEN** the adapter reports an unusable native candidate rather than classifying rendered image content as native text

### Requirement: Engine-specific selection provenance
The shared PDF module SHALL preserve, for every attempted native engine, the engine name and version, method, text hash, quality status, semantic usability, elapsed time, and typed diagnostics. The selected page result SHALL identify the actual engine that supplied the selected text.

#### Scenario: Fallback native candidate is selected
- **WHEN** the first native engine fails and a later engine passes the technical quality gate
- **THEN** the result identifies the later engine as selected and retains the failed first-engine candidate provenance

### Requirement: Native routing cache identity
The deterministic page cache identity SHALL include the ordered native chain or its configuration version and all native engine versions that can affect the selected result. A change in engine order, configuration version, or relevant engine version MUST produce a distinct cache identity.

#### Scenario: Production primary changes
- **WHEN** a profile changes its native chain from `pypdf -> pypdfium2` to `pypdfium2 -> pypdf`
- **THEN** a page parsed under the new order does not reuse an incompatible result cached under the old order

### Requirement: Evidence-gated primary engine promotion
The production primary native engine SHALL be selected using a versioned, hash-bound evaluation covering mapping-corrupt, scanned, mixed, and normal text PDFs. Mandatory text fidelity and compatibility gates SHALL take precedence over latency, and the module SHALL keep the current production primary until the candidate passes every mandatory gate.

#### Scenario: PDFium passes the frozen corpus
- **WHEN** `pypdfium2` recovers the 600036.SH required Chinese and numeric gold, preserves the normal-text baseline and requested page order, does not falsely accept scanned pages, passes mixed/table/read-order checks, and meets the native latency gate
- **THEN** the production profile may promote `pypdfium2` to the first native engine and retain `pypdf` as the configurable fallback

#### Scenario: Candidate is faster but less accurate
- **WHEN** a native engine has lower elapsed time but fails a mandatory fidelity or compatibility gate
- **THEN** it is not promoted to production primary

### Requirement: Remove ineffective pdf-inspector native routing
Production native profiles SHALL NOT route through `pdf-inspector` after all real callers have migrated to the ordered native chain. The `pdf-inspector` dependency SHALL be removed when no remaining production or supported operator caller requires it.

#### Scenario: Caller inventory is empty
- **WHEN** production imports, profiles, configuration, and supported operator tools no longer reference `pdf-inspector` and focused regression tests pass
- **THEN** its native profiles, adapter wiring, and package dependency are removed

#### Scenario: A real caller remains
- **WHEN** caller inventory finds a supported path that still requires a capability unique to `pdf-inspector`
- **THEN** dependency removal is blocked and that caller and migration condition are documented without keeping `pdf-inspector` in the default native chain
