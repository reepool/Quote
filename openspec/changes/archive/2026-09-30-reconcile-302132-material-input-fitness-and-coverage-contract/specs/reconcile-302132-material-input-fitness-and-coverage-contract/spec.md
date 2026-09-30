## ADDED Requirements

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
The 2.2 review MUST record exactly one result after the comparison. Retained unsuitable MUST state why the aggregate sample bar requires every included report to have a named material fact and why that bar does not violate the upstream `not_disclosed` contract. That result MUST keep the Stage 4 aggregate on hold and MUST NOT open a material-input successor. Coverage-only suitable MUST state that the archived assessment treated lawful coverage as sample unsuitability. It MUST NOT rewrite the archived dossier or any historical replay. It MUST allow only a later, separate four-report material-input successor scope change. This change MUST NOT implement that successor, enqueue, or replay. Neither result MUST add `302132.SZ` to MI-2, add an issuer outside the approved list, reopen the aggregate gate, or start restricted promotion, the six-chapter package, scale quality, or production. `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false.

#### Scenario: Retained unsuitable does not open a successor
- **WHEN** the 2.2 review keeps unsuitable
- **THEN** the reason includes both the named-fact sample bar and its relation to the `not_disclosed` contract
- **AND** the Stage 4 aggregate stays hold
- **AND** no successor scope is created

#### Scenario: Coverage-only suitable authorizes only a later scope card
- **WHEN** the 2.2 review corrects the interpretation to coverage-only suitable
- **THEN** the archived dossier and historical replays stay unchanged
- **AND** the only permitted follow-up is a separate successor scope change
- **AND** this change still does not enqueue or replay
