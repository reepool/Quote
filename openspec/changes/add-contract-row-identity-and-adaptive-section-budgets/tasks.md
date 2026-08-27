## 1. Contract and row identity

- [x] 1.1 Extend the structured operating-row schema and normalization path with optional contract reference, row ordinal, and evidence-derived `source_row_key` fields while preserving raw values and units.
- [x] 1.2 Update deterministic table extraction and semantic operating-fact construction so record ids, fact scopes, and metadata include the row key and table/page provenance.
- [x] 1.3 Add ambiguity grouping for same-subject rows and isolate ambiguous groups from unrelated facts during persistence and publication.

## 2. Targeted reasoning review

- [x] 2.1 Add a closed JSON stronger-model review request for ambiguous row groups using the existing LLM gateway and configurable `gpt-5.6-terra` default.
- [x] 2.2 Validate review row-key coverage, prohibit numeric/evidence mutations, and define timeout/schema/inconclusive fallback that preserves all rows as candidates.
- [x] 2.3 Integrate targeted review into the structured semantic flow with bounded retries, audit diagnostics, and no review call for non-ambiguous groups.

## 3. Adaptive section context

- [x] 3.1 Add an adaptive page-budget input and derive effective limits from outline chapter span, field-family defaults, request overrides, and global character/token safety budgets.
- [x] 3.2 Update selection to preserve anchors, adjacent context, and cross-page tables, returning deterministic ordered windows with budget metadata instead of raising the fixed 12-page error.
- [x] 3.3 Wire runtime selection and artifact persistence to carry window identity, page budget diagnostics, and replay compatibility across selector-budget changes.

## 4. Verification and migration

- [x] 4.1 Add focused tests for duplicate product labels with distinct contract rows, approved legacy replay, stronger-review outcomes, and unrelated-row isolation.
- [x] 4.2 Add selector/runtime tests for short chapters, long chapters, table continuation, dynamic overrides, and character/token budget stops.
- [x] 4.3 Run the representative polysilicon-shaped synthetic canary and targeted regression suite; document observed row-review and adaptive-window metrics.
- [x] 4.4 Review the complete change, mark all requirements verified, and prepare the change for archival without modifying pre-existing worktree files.
