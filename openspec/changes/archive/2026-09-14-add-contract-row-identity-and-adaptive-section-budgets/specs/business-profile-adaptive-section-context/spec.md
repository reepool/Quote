## ADDED Requirements

### Requirement: Section page budgets are derived from report structure
The system MUST derive the effective page budget from the requested field-family budget, the bounded chapter outline, table continuation pages, and the configured global safety maximum; it MUST NOT impose a universal fixed 12-page cap.

#### Scenario: A short chapter is selected
- **WHEN** the target chapter spans fewer pages than the effective safety budget
- **THEN** the selector MUST return the complete relevant chapter context including required table continuation pages

#### Scenario: A long chapter is selected
- **WHEN** the target chapter spans more pages than the effective safety budget
- **THEN** the selector MUST partition the chapter into deterministic bounded windows instead of failing with a fixed-page-bound error

### Requirement: Adaptive windows preserve semantic continuity
Each adaptive window MUST preserve physical page order, high-value heading/table anchors, adjacent context, and table continuation pages; the result MUST expose window index/count and the budget reason.

#### Scenario: A table crosses a page boundary
- **WHEN** a governed table starts near the end of one page and continues on later pages
- **THEN** the selector MUST include the continuation pages in the same window when the effective budget permits, or place them in adjacent ordered windows with continuation diagnostics

#### Scenario: Multiple windows are produced
- **WHEN** a chapter cannot fit in one bounded window
- **THEN** windows MUST be ordered by physical page number and MUST NOT duplicate or omit a page within the chapter scope

### Requirement: Character and token safety budgets remain authoritative
Adaptive page selection MUST still enforce configured character, token, and document-level budgets; request-level limits MUST NOT bypass those safety budgets.

#### Scenario: A page budget fits but character budget does not
- **WHEN** adding a page would exceed the configured character or token budget
- **THEN** the selector/runtime MUST stop or split before that page and MUST record a typed budget diagnostic

#### Scenario: Caller supplies a page override
- **WHEN** a caller supplies an explicit field-family page budget
- **THEN** the effective limit MUST be the smallest applicable request, chapter, and global safety limit, and the decision MUST be recorded in diagnostics

### Requirement: Adaptive selection remains compatible with immutable artifacts
Every selected window MUST use the existing selected-section artifact store and content-addressed identity, and replay MUST be able to distinguish selector-budget changes from source-document changes.

#### Scenario: Selector budget changes
- **WHEN** the same annual-report asset is selected with a different adaptive budget
- **THEN** a new immutable selection artifact identity MUST be created while the original artifact remains readable for semantic-result reuse validation
