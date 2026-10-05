## ADDED Requirements

### Requirement: Freeze excludes all twenty observed companies before enqueue
The freeze MUST explicitly pass delivered_ids containing 302132.SZ, 600000.SH, 600004.SH, 600006.SH, 600007.SH, 600008.SH, 600009.SH, 600010.SH, 600011.SH, 600012.SH, 600017.SH, 600018.SH, 600019.SH, 600020.SH, 600022.SH, 600028.SH, 600031.SH, 600038.SH, 600055.SH and 600056.SH. It MUST preserve the full exclusion list and select one report per sampling category under SSE/SZSE/BSE and code ascending order. Both MUST be unseen. Official report versions, cutoff 2026-09-17, full cumulative v20 identity, shared 50000-token budget and independent m4_v20_unseen_small_batch directory MUST be frozen. Repeated reads MUST agree and historical snapshots MUST remain unchanged. This card MUST NOT enqueue.

#### Scenario: Frozen samples are outside every observed company
- **WHEN** the existing freeze owner receives the complete twenty-company delivered_ids
- **THEN** the selected pair is outside that set and repeated reads have identical report bindings and budget
- **AND** the full v20 identity and exclusion list are preserved without enqueuing work

### Requirement: First real owner delivery preserves actual runtime and whole-round cost
The existing owner MUST run, query and export both companies under the frozen identity and reports, with the second consuming the remaining shared budget and both in one observation. Review MUST compare three actual business answer texts, source evidence and commodity-role states. Runtime reused_scope_ids MUST be preserved directly. First genuine execution MUST be the formal round and measured time MUST include first execute through second export return and all retries. Failure or exceeding 300 seconds MUST retain the original observation and artifacts; they MUST NOT be removed to replace the result.

#### Scenario: First execution delivers the pair with one shared budget
- **WHEN** the first company's owner run-query-export returns
- **THEN** the second receives the remaining budget, actual runtime reuse and both deliveries are preserved together
- **AND** the complete first execute through second export duration determines the time gate

### Requirement: New PDF review rebuilds unique semantic denominators
The independent review MUST rebuild source disclosures for principal, products/services, revenue, important tables and explicit commodity roles from the new frozen PDFs. It MUST NOT inherit 25/63 denominators or previous checklists. Every required answer and accepted fact MUST be compared once; Measurement duplicates MUST count only in accuracy, and source rows MUST count recall once without repeated aggregation of child revenues. No-role findings MUST state their actual checked pages. Finite passing MUST require recall and accuracy both 100%, zero critical numeric errors, shared tokens at most 50000 and whole-round time at most 300 seconds. Failure MUST pause following batches and any repair MUST address the actual business miss. Passing MUST be submitted for A-role finite acceptance. Production MUST remain not_authorized and scale_quality_claim_allowed MUST remain false.

#### Scenario: New original disclosure exposes a missing delivery
- **WHEN** a required source fact or answer is absent or wrong
- **THEN** the recomputed semantic review records the real miss or error and preserves the formal observation
- **AND** following batches remain paused while local work addresses the concrete business gap

#### Scenario: A declared product matrix continues on the next owned page
- **WHEN** the frozen business section's next page contains the current company's product matrix or sales model
- **THEN** selection follows the real section boundary and delivered Overview, query and export preserve the original continuous context and matrix
- **AND** the first formal failed delivery remains unchanged by fixture verification

#### Scenario: Explicit native sales and inputs have no precise catalogue mapping
- **WHEN** a company-owned current production/sales clause, owned formal sales-volume table, own cost-risk input list or furnace-input disclosure names a product or material
- **THEN** existing native role projection may retain pending mapping without inventing a grade, procurement price or price-series link
- **AND** subsidiary, third-party, planned and negated actions do not become the reporting company's established role
