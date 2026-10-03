## ADDED Requirements

### Requirement: The v7 unseen freeze excludes the latest observed pair
The freeze MUST keep the default same-identity exclusion unchanged. This round MUST pass the previously observed instrument ids through `delivered_ids`, including `600009.SH` and `600022.SH`. Selection MUST then take the first legal service company and the first legal manufacturing company in SSE, SZSE, BSE and instrument-id order. The stored plan MUST use cutoff `2026-09-17`, one shared 50000-token budget, the full v7 identity at run time, and an independent directory. Repeating the freeze MUST return the same plan.

#### Scenario: Omitting the latest pair selects those two companies
- **WHEN** delivered ids omit `600009.SH` and `600022.SH` and those two are the next legal service and manufacturing companies
- **THEN** the plan selects `600009.SH` then `600022.SH`

#### Scenario: Passing the full observed set selects companies outside that set
- **WHEN** the freeze receives the full observed set
- **THEN** neither selected report is in that set
- **AND** the plan lists service before manufacturing
- **AND** a second freeze reads back the same plan

### Requirement: Delivery and review use the frozen v7 identity
The run MUST use the existing owner, the full v7 processing identity, and the frozen directory. Service runs before manufacturing. Query and export MUST select that identity. Both companies MUST land in one merged observation. The independent review MUST score the new reports from their own text and the actual deliveries. A repeated disclosure of the same role counts only in accuracy. A checked absence of a company commodity role stays a negative with a review scope. The review MUST NOT prefill the prior 10/10 or 11/11. Production stays unauthorized and scale quality stays false.

#### Scenario: A failed delivery remains a failure
- **WHEN** a delivered fact does not match the source
- **THEN** the review records that failure
- **AND** the v7 10/10 and 11/11 review file is unchanged
