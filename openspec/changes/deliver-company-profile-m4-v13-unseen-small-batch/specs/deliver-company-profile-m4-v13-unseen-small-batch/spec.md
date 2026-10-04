## ADDED Requirements

### Requirement: The v13 unseen freeze excludes all fourteen observed companies
The freeze MUST keep the default same-identity exclusion unchanged and MUST pass all fourteen previously observed instrument ids through `delivered_ids`: `600000.SH`, `600004.SH`, `600006.SH`, `600007.SH`, `600008.SH`, `600009.SH`, `600010.SH`, `600011.SH`, `600012.SH`, `600019.SH`, `600022.SH`, `600028.SH`, `600031.SH`, and `302132.SZ`. Selection MUST take the first legal service company and the first legal manufacturing company in SSE, SZSE, BSE and instrument-id order. The stored plan MUST fix cutoff `2026-09-17`, the report identities, document versions, and PDF bindings, the shared 50000-token budget, the full v13 identity at run time, and the independent directory `reports/m4_v13_unseen_small_batch`. Repeating the freeze MUST return the same plan, and historical files MUST stay byte-identical.

#### Scenario: Passing the full observed set selects companies outside it
- **WHEN** the freeze receives the fourteen observed ids
- **THEN** neither selected report is in that set
- **AND** the plan lists service before manufacturing
- **AND** a second freeze reads back the same plan with unchanged historical bytes

### Requirement: Controlled delivery in one merged observation
The run MUST use the existing owner and the full v13 identity, service before manufacturing, with automatic evidence selection, acceptance, persistence, query, and export on the same identity. Both companies MUST land in one merged observation. The whole-round time MUST be measured from the first execute to the second export return; environment retries count and are retained, and a timeout or failure MUST be recorded honestly without deleting a failed result in favor of a faster one.

#### Scenario: A failure is kept
- **WHEN** either company times out or fails
- **THEN** the failure stays in the merged observation with the elapsed time recorded
- **AND** no faster substitute run replaces it

### Requirement: Independent review of the new sources
The review MUST rebuild the recall checklist from the two new reports and judge the six dimension answer texts plus every actually delivered Activity, Overview, Segment, and Measurement for subject, action, object, column, amount, and unit, with one finding per fact. A company without commodity roles MUST keep an explicit check scope. Passing requires recall 100%, accuracy 100%, zero critical numeric errors, at most 50000 shared tokens, and at most 300 measured seconds. Scores MUST NOT be prefilled and no historical round MAY be rewritten.

#### Scenario: A missed or unsupported delivery is kept
- **WHEN** an important disclosure is absent or a delivered record lacks local support
- **THEN** the review records the miss and the gates are computed from the honest counts
