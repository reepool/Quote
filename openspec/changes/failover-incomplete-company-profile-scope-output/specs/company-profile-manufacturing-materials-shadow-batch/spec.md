## ADDED Requirements

### Requirement: The completeness fix is validated with one small unseen canary
The manufacturing/materials research MVP MUST, after provider-free regression checks
pass, validate the current company-profile implementation with one previously unseen
annual-report identity and one new immutable run ID through the existing four-member
logical pool. The canary MUST use the current default-group subject convention and
finite dynamic token policy, MUST NOT use historical or comparison candidates, and MUST
stop after the single report completes as `usable`, `usable_with_caveats`, `hold`, or a
typed `failed` result. The stale Gemini-only twenty-report replay MUST NOT be executed
under its obsolete frozen contract.

#### Scenario: Unseen canary completes
- **WHEN** the one admitted report finishes all planned scopes
- **THEN** its immutable report, trace lineage, review workload, and status are recorded
- **AND** the result remains a research fixture with `production_authorization=not_authorized`

#### Scenario: Canary has a provider or semantic blocker
- **WHEN** the admitted report finishes as `hold` or a typed `failed` result
- **THEN** that result closes the canary honestly
- **AND** no targeted rerun, second model-comparison batch, or historical splice is authorized

#### Scenario: Stale broad replay is requested
- **WHEN** an operator attempts to execute `replay-company-profile-shadow-segment-financial-repair` under its Gemini-only old implementation contract
- **THEN** the request is rejected as superseded by the current MVP baseline
- **AND** no provider call is made for that obsolete replay identity
