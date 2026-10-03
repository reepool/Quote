## ADDED Requirements

### Requirement: The unseen freeze excludes every previously observed company
The freeze MUST keep the default same-identity exclusion unchanged. This round MUST pass the previously observed instrument ids through `delivered_ids`. Those ids are `302132.SZ`, `600000.SH`, `600004.SH`, `600006.SH`, `600007.SH`, `600008.SH`, `600010.SH`, and `600019.SH`. Selection MUST then take the first legal service company and the first legal manufacturing company in SSE, SZSE, BSE and instrument-id order. The stored plan MUST use cutoff `2026-09-17`, one shared 50000-token budget, and an independent directory. Repeating the freeze MUST return the same plan. This task MUST NOT enqueue.

#### Scenario: v6 same-identity delivery would reuse the repaired pair
- **WHEN** delivered ids contain only `600008.SH` and `600019.SH`
- **THEN** the next legal pair is `600007.SH` then `600010.SH`

#### Scenario: Passing the observed set selects companies outside that set
- **WHEN** the freeze receives the full observed set
- **THEN** neither selected report is in that set
- **AND** the plan lists service before manufacturing
- **AND** a second freeze reads back the same plan

### Requirement: Delivery and review use the frozen v6 identity
The run MUST use the existing owner, the full v6 processing identity, and the frozen directory. Service runs before manufacturing. Query and export MUST select that identity. The second company MUST consume the remaining shared budget recorded in the merged observation. The independent review MUST score the new reports from their own text and the actual deliveries. It MUST NOT prefill the prior 14/14 or 16/16. Production stays unauthorized and scale quality stays false.

#### Scenario: A failed delivery remains a failure
- **WHEN** a company fails or a delivered fact does not match the source
- **THEN** the review records that failure
- **AND** the historical v6 review file is unchanged
