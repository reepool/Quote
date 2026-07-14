## ADDED Requirements

### Requirement: Research Daily Writes Emit Or Declare Change Semantics
Research-domain scheduled writes SHALL either emit changelog records for inserted and materially changed rows or explicitly declare that the workflow is read-only, diagnostics-only, or unchanged-only.

#### Scenario: Shareholder incremental snapshot changes
- **WHEN** shareholder incremental sync writes a changed normalized snapshot
- **THEN** the research data engine SHALL emit a shareholder-domain change record linked to the ingestion run

#### Scenario: Diagnostic job reads only
- **WHEN** a research diagnostic job only reads local data and writes no persisted result
- **THEN** the job SHALL declare no changelog emission is expected

### Requirement: Derived Research Outputs Preserve Input Lineage
Derived research outputs such as valuation history, technical snapshots, risk snapshots, and DCF inputs SHALL record input hashes, source watermarks, or equivalent lineage so consumers can distinguish source-data changes from recalculation-only changes.

#### Scenario: Valuation history row is recomputed
- **WHEN** valuation history is recomputed for the same instrument and as-of date
- **THEN** the stored output SHALL identify the calculation version and input lineage
- **AND** the changelog SHALL classify a material output change only when the derived semantic hash changes

### Requirement: Research Change Queries Do Not Affect Existing Read APIs
Research changelog surfaces SHALL be read-only additions and SHALL NOT change existing `/api/v1/research/*` default responses.

#### Scenario: Existing financial facts query
- **WHEN** a caller queries existing financial fact or valuation APIs without changelog parameters
- **THEN** the response SHALL follow the pre-existing research API contract
