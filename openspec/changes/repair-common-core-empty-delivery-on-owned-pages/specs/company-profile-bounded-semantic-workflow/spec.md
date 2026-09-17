## MODIFIED Requirements

### Requirement: Deterministic extraction precedes bounded LLM fallback
The application workflow MUST first accept deterministic results for source structures that satisfy the active task contract and MUST invoke semantic fallback only for unresolved cells, headers, narrative meaning, subject, period, or event boundaries. Deterministic input includes already structured fixture or table candidates and, for the common-core path, official owned page excerpts that already state the active checklist field. Common-core MUST NOT treat a missing LLM provider as failure when such an owned excerpt already satisfies the field. This change MUST NOT implement PDF selection, OCR, table parsing, or page-layout recovery. A deterministic and LLM candidate for the same physical occurrence MUST be reconciled through one validator rather than published twice.

#### Scenario: Standard revenue cost margin table is complete
- **WHEN** the table parser provides complete row labels, metric headers, units, cells, and anchors
- **THEN** the workflow creates deterministic Segment and Measurement candidates without an extract-model call
- **AND** only unresolved semantic fields, if any, can enter repair or review

#### Scenario: Multi-page header is ambiguous
- **WHEN** deterministic parsing cannot bind a continued page to the owning header
- **THEN** the bounded extract fallback receives the complete continuation Evidence and active checklist
- **AND** it returns source-native candidates or `unclear`, not a guessed canonical value

#### Scenario: Owned official excerpt already states the checklist field
- **WHEN** common-core has selected a readable owned excerpt that already states the active checklist field and no LLM provider is configured
- **THEN** the workflow accepts the deterministic page-grounded candidate
- **AND** it MUST NOT record `provider-unavailable` for that field
