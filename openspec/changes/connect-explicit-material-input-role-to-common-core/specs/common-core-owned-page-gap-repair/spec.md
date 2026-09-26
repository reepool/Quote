## MODIFIED Requirements

### Requirement: Repair publishes a successor identity without overwriting predecessors
The current published processing identity MUST be `{"rules":"company_profile_common_core.v1","owned_page_facts":"v8","material_input_facts":"v1"}`. `owned_page_facts` MUST remain `v8`. The owner MUST NOT force-replay or overwrite a work whose identity is only `owned_page_facts=v8`. It MUST NOT overwrite or delete v1–v8 work JSON or existing source-review snapshots. Query MUST be able to keep reading those predecessor identities. The v8 repair observation remains recall 8/9, accuracy 8/8, critical numeric errors 0, and `expansion_gates_met=false`. The v6 and v7 source-review snapshots that reported accuracy 8/8 remain on disk and stay rejected by later review. This change MUST NOT alter the universe denominator, taxonomy, ordinary live-run sampling, publication scope, closure, or completed mode. It MUST NOT authorize the next expansion, a scale-quality claim, production, DCF, trading, or the legacy writer. Explicit named material input is specified by `explicit-material-input-role` and MUST NOT grow into a full raw-material industry package, airport throughput, compensation, vehicle volume, material-cost ratio, procurement mode, strategic reserve, supplier or customer chapter, net interest margin, cost-to-income ratio, or loan-structure projection.

#### Scenario: Predecessor work remains readable
- **WHEN** successor work with `material_input_facts=v1` is written for a report that already has v8 work
- **THEN** the v8 work JSON remains on disk and readable
- **AND** production authorization remains `not_authorized`
- **AND** the same v8 identity is not force-replayed
