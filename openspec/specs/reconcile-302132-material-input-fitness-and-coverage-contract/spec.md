# reconcile-302132-material-input-fitness-and-coverage-contract Specification

## Purpose

This reconciliation asks whether a complete `extract_material_inputs` read of `302132.SZ` with no named material is sample-unsuitable or lawful coverage. The closed result is coverage-only suitable. A full review that records evidenced `not_disclosed` or `not_applicable` completes the chapter without creating a `material_input` fact. The archived assessment treated the absence of a deliverable fact as sample unsuitability; that archived text stays unchanged. Coverage-only suitable allows only a later, separate four-report successor scope change. It does not create that change, does not add `302132.SZ` to the three-report material-input result, and does not produce aggregate `expansion_gates_met=true`. The Stage 4 aggregate stays hold. `production_authorization` stays `not_authorized` and `scale_quality_claim_allowed` stays false.

## Requirements

### Requirement: The reconciliation reads only the named contract texts
Before the 1.1 review passes, the change MUST only contain its scope documents. The comparison MUST read only the manufacturing-materials requirements, the current `manufacturing-materials-stage4-minimum-slice` spec, the archived `302132.SZ` material-input dossier, and the archived assessment proposal, design, spec, and tasks. It MUST NOT modify those archived files, MI-1, MI-2, the aggregate ledger, replay, or source-review. It MUST NOT choose between retained unsuitable and coverage-only suitable, create a successor scope, change Python, enqueue, or replay.

#### Scenario: Scope review does not choose fitness
- **WHEN** the 1.1 review has not yet accepted this change
- **THEN** no reconciliation verdict is recorded
- **AND** the archived unsuitable conclusion remains unchanged
- **AND** no successor scope is created

### Requirement: Two suitability definitions stay side by side
After scope approval, the comparison MUST state both definitions without choosing one. The first definition says suitable requires at least one bindable named `material_input` fact in the report. The second definition says suitable requires only a complete review that records evidenced `not_disclosed` or `not_applicable` coverage where no named material is disclosed. The comparison MUST record that procurement mode, cost composition, related-party procurement, and supplier totals were read, and that no source-native named material was found. It MUST ask whether that result means the chapter cannot be assessed or means the chapter is lawfully not disclosed. It MUST keep `not_disclosed` distinct from `extraction_failed` and MUST NOT turn a coverage status into an observed material fact.

#### Scenario: A complete read with no named material is not yet classified
- **WHEN** the dossier shows the checked locations and no named material input
- **THEN** both suitability definitions remain visible
- **AND** the comparison does not yet say the report is unsuitable or coverage-only suitable

### Requirement: The verdict chooses one definition
The 2.2 review recorded coverage-only suitable. A complete review with evidenced `not_disclosed` or `not_applicable` and no named material completes the chapter without a `material_input` fact. The archived assessment treated that lawful coverage as sample unsuitability, and that archived text MUST remain unchanged. Coverage-only suitable MUST allow only a later, separate four-report material-input successor scope change. This change MUST NOT implement that successor, enqueue, or replay. The result MUST NOT add `302132.SZ` to MI-2, add an issuer outside the approved list, reopen the aggregate gate, create aggregate `expansion_gates_met=true`, or start restricted promotion, the six-chapter package, scale quality, or production. `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false.

#### Scenario: Retained unsuitable does not open a successor
- **WHEN** a review keeps unsuitable
- **THEN** the reason includes both the named-fact sample bar and its relation to the `not_disclosed` contract
- **AND** the Stage 4 aggregate stays hold
- **AND** no successor scope is created

#### Scenario: Coverage-only suitable authorizes only a later scope card
- **WHEN** the 2.2 review records coverage-only suitable
- **THEN** the archived dossier and historical replays stay unchanged
- **AND** the only permitted follow-up is a separate successor scope change
- **AND** this change still does not enqueue or replay
