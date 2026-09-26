## MODIFIED Requirements

### Requirement: Current published identity is owned_page_facts v8
The current published processing identity MUST be `{"rules":"company_profile_common_core.v1","owned_page_facts":"v8","material_input_facts":"v1"}`. `owned_page_facts` MUST remain `v8`. Query MUST prefer that full identity over an older identity that lacks `material_input_facts`. v1–v8 work JSON MUST remain readable and MUST NOT be overwritten. Owned overview projection and formal revenue-table unit scope remain defined by `common-core-owned-page-gap-repair`. Explicit named material input remains defined by `explicit-material-input-role`.

#### Scenario: Query prefers the material-input successor over v8-only work
- **WHEN** the same report has readable v8 work and successor work that adds `material_input_facts=v1`
- **THEN** query returns the successor accepted facts
- **AND** the v8 work JSON remains on disk
