## ADDED Requirements

### Requirement: The stage-4 index is read-only
The change MUST record the archived material-input, operating-quantity, and segment-financial observations as independent rows. Each row MUST keep its chapter, plan, report set, recall, accuracy, critical numeric errors, gate, and archive path. The change MUST NOT recompute those metrics and MUST NOT write a later result back into an earlier replay.

#### Scenario: A later slice pass does not replace an earlier failure
- **WHEN** the index lists a chapter that has both a failed observation and a later slice pass
- **THEN** both rows remain visible with their own plans and gates
- **AND** the later pass is not described as a recount of the earlier replay

### Requirement: There is no aggregate stage-4 gate
The change MUST state that no aggregate `expansion_gates_met=true` exists for Stage 4. `scale_quality_claim_allowed` MUST remain false and `production_authorization` MUST remain `not_authorized`. Publication, closure, mode, and processing identity MUST remain unchanged. The change MUST NOT authorize production, a scale-quality claim, the six-chapter package, or another replay.

#### Scenario: Local passes stay local
- **WHEN** a reader sees the material-input 23/23, the operating-quantity 38/38, and the segment-financial 132/132
- **THEN** each true gate stays limited to its own plan, report set, and chapter
- **AND** those three results are not combined into one Stage 4 pass

### Requirement: The next chapter is only a dossier assessment
After scope approval, the only next business assessment MUST be `extract_counterparties_and_concentration`. The assessment MUST use dossiers on the approved report list. It MUST require at least three reports, coverage of SZSE, SSE, and BSE, and at least two disclosure forms. If that bar is not met, the assessment MUST stop and MUST NOT add an issuer outside the approved list. This change MUST NOT implement the chapter or run a replay. A later implementation MUST be a separate change opened only after that dossier review passes.

#### Scenario: The sample bar fails
- **WHEN** the approved reports do not supply the required count, exchanges, or disclosure forms for the candidate chapter
- **THEN** the assessment stops
- **AND** no report outside the approved list is added
- **AND** no Python implementation or replay starts

#### Scenario: Scope approval does not start implementation
- **WHEN** the 1.1 review accepts this hold
- **THEN** Python, enqueue, and replay remain unchanged
- **AND** the six-chapter package stays inactive
