## ADDED Requirements

### Requirement: The unseen freeze excludes all eighteen observed companies
The freeze MUST pass all eighteen previously observed companies through delivered_ids: 302132.SZ, 600000.SH, 600004.SH, 600006.SH, 600007.SH, 600008.SH, 600009.SH, 600010.SH, 600011.SH, 600012.SH, 600017.SH, 600018.SH, 600019.SH, 600022.SH, 600028.SH, 600031.SH, 600038.SH and 600055.SH. It MUST choose one service and one manufacturing report in SSE, SZSE, BSE and within-exchange code ascending order under cutoff 2026-09-17. It MUST freeze the official report references, document versions, full v19 processing identity, shared 50000-token budget and independent m4_v19_unseen_small_batch directory. Repeated reading MUST return the same plan and historical files MUST retain their bytes. The freeze card MUST NOT enqueue.

#### Scenario: The new sample is frozen before execution
- **WHEN** the existing freeze owner receives the explicit eighteen-company exclusion set
- **THEN** both selected companies are outside that set, with service before manufacturing
- **AND** repeat reading matches the frozen reports and budget, no work is enqueued, and prior snapshots remain unchanged

### Requirement: The controlled round delivers both new companies under one budget
The existing owner MUST execute, query and export service first and manufacturing second under the same full v19 identity, with the second company receiving the remaining shared budget and both companies in one round observation. Acceptance MUST examine actual principal, products and revenue answer text, commodity roles and gaps. Scope reuse MUST follow actual runtime reused_scope_ids. Whole-round measured time MUST cover the first execute through the second export return, including retries and queries; an over-limit execution MUST remain a failed observation.

#### Scenario: The second company consumes only the remaining budget
- **WHEN** the first company finishes its execution, query and export
- **THEN** the second uses the remaining portion of the shared 50000-token budget
- **AND** both deliveries, actual scope reuse, any retries and total wall-clock duration are preserved together

### Requirement: Independent review rebuilds the unseen source and delivery denominators
The review MUST rebuild its recall checklist from the new original reports and uniquely evaluate six required dimension answers plus every accepted fact, without inheriting prior 26 or 51 denominators. Commodity-role gaps or absences MUST keep the actually checked source scope. Passing MUST require source recall and accuracy both 100%, zero critical numeric errors, at most 50000 shared tokens and at most 300 whole-round seconds. Failure MUST retain its observation and pause further batches; any repair MUST target actual business misses. Production authorization MUST remain not_authorized and scale quality claims MUST remain false.

#### Scenario: A new source disclosure is missing from delivery
- **WHEN** independent source reading identifies a required fact or answer that the delivery misses
- **THEN** recall records that miss and the computed gates fail
- **AND** the real observation is preserved, further batches stop, and the concrete business miss is recorded for repair

### Requirement: Concrete failed-round misses are repaired without replacing its observation
Local repair MUST address the actually observed long wrapped road labels, 其中-prefixed raw-drug row, internal-elimination row classification, company-owned business-block overview, multi-line industry revenue answer, subsidiary direct actor promotion and incomplete product fragment. Directed validation MUST use the frozen complete source pages and retain v19 counterexamples. The first formal failed observation and review MUST remain unchanged; local corrected tests MUST NOT be reported as a new passing formal round or allow another unseen batch.

#### Scenario: Local corrected delivery preserves the first failure
- **WHEN** a local repair passes directed source-page regressions
- **THEN** its local validation is reported separately from the original failed formal run
- **AND** original artifacts and scores remain unchanged and the next batch stays paused pending A-role instructions
