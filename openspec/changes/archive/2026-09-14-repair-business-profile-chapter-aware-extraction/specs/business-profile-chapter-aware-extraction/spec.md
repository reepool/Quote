## ADDED Requirements

### Requirement: Annual-report analysis is scoped by report structure
The system SHALL locate the management discussion and analysis chapter from the annual report table of contents or a verified major-heading fallback before selecting business-profile pages.

#### Scenario: Table of contents identifies the chapter
- **WHEN** a verified annual report contains a table-of-contents entry for management discussion and analysis and a following major chapter
- **THEN** the system limits business-profile selection to the inclusive page range between those chapter boundaries
- **AND** records the source, confidence, and page range of the outline decision

#### Scenario: Table of contents is unavailable
- **WHEN** the table of contents is absent, malformed, or cannot be mapped to artifact pages
- **THEN** the system searches for the actual management-discussion heading and following major heading
- **AND** reports that heading fallback was used

#### Scenario: No reliable outline exists
- **WHEN** neither the table of contents nor actual major headings produce a reliable chapter boundary
- **THEN** the system uses a bounded low-confidence fallback
- **AND** does not report high-confidence chapter coverage

### Requirement: Relevant pages are selected within a bounded chapter scope
The system SHALL rank and cluster governed heading aliases, table signatures, and structured hints within the resolved chapter scope and MUST keep the selected page set within the configured page budget.

#### Scenario: Repeated terms exceed the raw page budget
- **WHEN** broad aliases occur on more pages than the configured maximum
- **THEN** the system prioritizes table signatures and substantive heading clusters
- **AND** selects the highest-value bounded context windows instead of failing solely because the raw hit count is large

#### Scenario: Table of contents repeats business terms
- **WHEN** a table-of-contents page or an out-of-scope chapter contains the same business terms as the management discussion
- **THEN** those occurrences do not become selected evidence pages

#### Scenario: No governed page can be selected
- **WHEN** no governed heading, table signature, structured hint, or bounded fallback page can support a required field family
- **THEN** the system records explicit selector machine rework
- **AND** does not treat the field family as successfully extracted

### Requirement: Structured extraction precedes bounded LLM analysis
The system SHALL extract headings, numeric tables, units, totals, and ratios deterministically when possible and SHALL constrain any LLM analysis to selected sections with immutable page and quote evidence.

#### Scenario: Native table is structurally usable
- **WHEN** selected annual-report pages contain a governed table whose rows and columns can be parsed deterministically
- **THEN** the system emits typed records with source page, section, unit, and table signature lineage
- **AND** does not invoke the LLM for those deterministic values

#### Scenario: Narrative wording requires semantic normalization
- **WHEN** selected narrative describes products, applications, business models, value-chain activities, or commodity relationships that deterministic rules cannot normalize
- **THEN** the system may invoke the configured LLM only on bounded selected snippets
- **AND** every accepted semantic output cites an allowed page and quote hash

#### Scenario: Table layout is ambiguous
- **WHEN** native text preserves the relevant page but not a reliable table column structure
- **THEN** the system keeps deterministic parsing fail-closed
- **AND** routes only the affected selected page snippets to semantic fallback or machine rework

### Requirement: Durable completion requires usable business-profile output
The system MUST NOT acknowledge a durable parse, semantic, or publish stage as complete when the stage contains blocking selector/parser machine rework, has no required selected evidence, or otherwise represents an evidence-free partial result.

#### Scenario: Stage status is success but output is partial
- **WHEN** a stage returns outer status `success` while its quality summary reports blocking machine rework or zero required output
- **THEN** the durable work item remains retryable at the affected stage
- **AND** downstream stages are not acknowledged

#### Scenario: Shadow mode has evidence-backed output
- **WHEN** promotion is disabled but the current shadow stage has selected evidence, valid deterministic or semantic artifacts, and no blocking machine rework
- **THEN** the stage may advance without requiring canonical promotion
- **AND** telemetry distinguishes shadow output from published canonical facts

#### Scenario: Evidence-backed publish completes
- **WHEN** all required stages have usable evidence-backed output and no blocking machine rework
- **THEN** the work item may reach completed status
- **AND** worker-completed telemetry counts it separately from enqueued and asset-only work

### Requirement: Evidence-free completions can be recovered without redownload
The system SHALL provide an idempotent recovery operation for completed latest-annual work proven to contain selector/parser machine rework or zero usable semantic output while preserving a valid bound annual-report asset.

#### Scenario: Recover evidence-free completion
- **WHEN** a completed latest-annual work item has a valid bound manifest but its stage history proves that no pages or usable records were produced
- **THEN** recovery resets it to the earliest affected processing stage
- **AND** preserves the PDF, manifest, prior checkpoint path, stage history, and recovery audit metadata

#### Scenario: Preserve valid completion
- **WHEN** a completed work item has evidence-backed stage output and no blocking machine rework
- **THEN** recovery leaves the work item unchanged

#### Scenario: Repeat recovery
- **WHEN** recovery is run after an evidence-free item has already been requeued
- **THEN** the item is not reset or counted again

### Requirement: Operator reporting distinguishes asset and semantic coverage
The system SHALL separately report annual-report asset coverage, selected-section coverage, evidence-backed semantic completion, effective publication, and blocking machine-rework reasons.

#### Scenario: PDF exists but semantic extraction is empty
- **WHEN** an annual-report asset is valid but no selected evidence or semantic records exist
- **THEN** asset coverage increases
- **AND** semantic and publication coverage do not increase

#### Scenario: Bounded batch completes evidence-backed work
- **WHEN** a bounded batch produces selected pages and usable structured or semantic output
- **THEN** the report includes outline source, selected-page count, deterministic and semantic output counts, and worker completion
