## ADDED Requirements

### Requirement: Deterministic bounded evidence catalog
The system SHALL generate a request-local catalog of stable, bounded evidence spans from the selected annual-report sections before invoking semantic extraction.

#### Scenario: Selected sections contain paragraphs and table rows
- **WHEN** a semantic request is prepared from immutable selected sections
- **THEN** the system emits bounded paragraph or table-row spans with stable identifiers, exact normalized text, section identity, page number, document offsets, and hashes derived locally

#### Scenario: Request budget is exhausted
- **WHEN** another complete evidence span would exceed the configured request character or span limit
- **THEN** the system excludes that span without offering a truncated or coordinate-inconsistent span to the model

### Requirement: Semantic response references evidence identifiers
The system SHALL require the LLM to return extracted semantic fields and one or more `evidence_span_ids` and SHALL prohibit model-supplied quotes, pages, offsets, and governed hashes.

#### Scenario: Model extracts a supported fact
- **WHEN** the model identifies an activity, relationship, structured segment, or operating fact supported by the catalog
- **THEN** it returns the normalized semantic fields and the unique catalog span identifiers that support the fact

#### Scenario: Provider returns the former coordinate contract
- **WHEN** a model response includes quote, page, section-offset, or hash fields not allowed by the closed schema
- **THEN** the response fails schema validation and remains resumable machine rework

### Requirement: Exact local evidence binding
The system SHALL resolve every model-selected span identifier locally into governed evidence records and SHALL fail closed on any unresolved or incompatible reference.

#### Scenario: Multiple spans jointly support a fact
- **WHEN** all referenced identifiers exist in the request catalog and their exact texts jointly support the required fields
- **THEN** the system emits exact evidence records with locally derived document identity, page, section, normalized offsets, quote, quote hash, and section hash

#### Scenario: Unknown or ambiguous span reference
- **WHEN** a response references an identifier that was not uniquely present in the request catalog
- **THEN** the affected row is rejected with a bounded stable diagnostic and no fuzzy text fallback is attempted

#### Scenario: Evidence does not support a required field
- **WHEN** resolved span text does not support the row's disclosed numeric value, unit, entity name, relationship direction, issuer, or report period as applicable
- **THEN** the affected row is rejected and cannot pass publication or promotion gates

### Requirement: Partial row isolation and strict promotion
The system SHALL retain independently valid rows from a mixed response while preserving strict publication gates and machine rework for rejected rows.

#### Scenario: Response mixes valid and invalid span references
- **WHEN** at least one row resolves to compatible exact evidence and another row has an invalid reference or unsupported field
- **THEN** the valid rows are retained, invalid rows are recorded as bounded diagnostics, and only governed supported facts can advance

#### Scenario: No row survives local binding
- **WHEN** every model row fails span resolution or field compatibility checks
- **THEN** the field family remains incomplete and is routed to resumable machine rework

### Requirement: Versioned resumability and observable progress
The system SHALL version the span-based semantic contract and expose safe catalog, resolution, request, accepted-row, and rejected-row metrics without invalidating reusable annual-report assets.

#### Scenario: Offset-contract checkpoint exists
- **WHEN** semantic work resumes after deployment of the span contract
- **THEN** the new runtime identity supersedes the old semantic checkpoint while reusing the immutable downloaded report and selected-section assets

#### Scenario: Long-running backfill processes a semantic batch
- **WHEN** one or more LLM requests or local span resolutions complete or fail
- **THEN** progress diagnostics report actual request counts, catalog and resolution counts, accepted and rejected rows, and stable failure categories without storing raw prompts, raw responses, credentials, or unbounded filing text
