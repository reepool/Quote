## ADDED Requirements

### Requirement: The v10 unseen freeze excludes every observed company
The freeze MUST keep the default same-identity exclusion unchanged. This round MUST pass the previously observed instrument ids through `delivered_ids`, explicitly including `600011.SH` and `600028.SH` and covering `600000.SH`, `600004.SH`, `600006.SH`, `600007.SH`, `600008.SH`, `600009.SH`, `600010.SH`, `600019.SH`, `600022.SH`, and `302132.SZ`. Selection MUST then take the first legal service company and the first legal manufacturing company in SSE, SZSE, BSE and instrument-id order. The stored plan MUST use cutoff `2026-09-17`, one shared 50000-token budget, the full v10 identity at run time, fixed report versions, and the independent directory `reports/m4_v10_unseen_small_batch`. Repeating the freeze MUST return the same plan, and historical files MUST stay byte-identical.

#### Scenario: Passing the full observed set selects companies outside that set
- **WHEN** the freeze receives the full observed set
- **THEN** neither selected report is in that set
- **AND** the plan lists service before manufacturing
- **AND** a second freeze reads back the same plan
- **AND** the historical first-expansion files keep their recorded bytes

### Requirement: Delivery uses the frozen v10 identity in one merged observation
The run MUST use the existing owner, the full v10 processing identity, and the frozen directory. Service runs before manufacturing, and a failure consumes the shared budget while the second company receives the remainder. Query and export MUST select that identity, and both companies MUST land in one merged observation. Each core dimension MUST carry a substantive answer or an explicit gap, and commodity roles MUST keep their subject, direction, and source. Elapsed time MUST be measured from the first execute start to the second export return. Errors found during the round stay in the observation for a successor.

#### Scenario: A failed first company does not block the second
- **WHEN** the service company fails or is refused
- **THEN** its failure stays in the merged observation with the consumed tokens
- **AND** the manufacturing company still runs with the remaining budget

### Requirement: Independent review under the fixed criteria
The independent review MUST rebuild the recall checklist from the two new reports' own text under the fixed criteria: column contexts judged separately, cross-page restatement counted once, Measurements repeating Segment values adding no recall while still being checked, and every six-dimension answer, role, and accepted fact mapped to its own finding. A checked absence of a company commodity role MUST stay a negative with a review scope. Acceptance stays recall 100%, accuracy 100%, zero critical numeric errors, at most 50000 shared tokens, and at most 300 measured seconds. The review MUST NOT prefill prior scores or rewrite the v9 or v10 repair observations. Production stays unauthorized and scale quality stays false.

#### Scenario: A missed source disclosure is kept as a miss
- **WHEN** an important disclosure in either new report is absent from delivery
- **THEN** the review records the miss with its source basis
- **AND** the gates are computed from the honest counts
