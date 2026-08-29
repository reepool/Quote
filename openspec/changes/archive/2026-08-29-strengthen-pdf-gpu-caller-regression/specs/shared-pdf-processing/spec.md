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

#### Scenario: Native-only business caller uses an approved GPU profile while its worker is unavailable
- **WHEN** an HKEX or official-index caller parses a text PDF with a valid approved GPU profile and a GPU worker that cannot become ready
- **THEN** the caller SHALL return its existing native-text business outcome without invoking the GPU worker probe, renderer, OCR worker, or CPU fallback
- **AND** the caller's public input and output contract SHALL remain unchanged

#### Scenario: Profile is changed without consumer code changes
- **WHEN** an operator changes from the retained `pypdf` rollback profile to a validated PDFium-first profile
- **THEN** the same consumer request contract SHALL invoke the newly configured ordered adapters
- **AND** the result SHALL record the effective profile, chain order, and engine/runtime versions

#### Scenario: Inspector native profile is retired
- **WHEN** production callers and configuration have migrated from `pdf_inspector_paddleocr`
- **THEN** the profile and inspector native routing SHALL be removed
- **AND** evaluation, classification, and inspector OCR uses SHALL be inventoried independently before the package dependency can be removed
