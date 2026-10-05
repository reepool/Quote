## ADDED Requirements

### Requirement: Freeze excludes all twenty-two observed companies before enqueue
The freeze MUST explicitly pass delivered_ids containing 302132.SZ, 600000.SH, 600004.SH, 600006.SH, 600007.SH, 600008.SH, 600009.SH, 600010.SH, 600011.SH, 600012.SH, 600017.SH, 600018.SH, 600019.SH, 600020.SH, 600021.SH, 600022.SH, 600028.SH, 600031.SH, 600038.SH, 600055.SH, 600056.SH and 600059.SH. It MUST preserve the full exclusion list and select one report per sampling category under SSE/SZSE/BSE and code ascending order. Both MUST be unseen. Official report versions, cutoff 2026-09-17, full cumulative v21 identity, shared 50000-token budget and independent m4_v21_unseen_small_batch directory MUST be frozen. Repeated reads MUST agree and historical snapshots MUST remain unchanged. This card MUST NOT enqueue.

#### Scenario: Frozen samples are outside every observed company
- **WHEN** the existing freeze owner receives the complete twenty-two-company delivered_ids
- **THEN** the selected pair is outside that set and repeated reads have identical report bindings and budget
- **AND** the full v21 identity and exclusion list are preserved without enqueuing work

### Requirement: First real owner delivery preserves actual runtime and whole-round cost
The existing owner MUST run, query and export both companies under the frozen identity and reports, with the second consuming the remaining shared budget and both in one observation. Review MUST compare three actual business answer texts, source evidence and commodity-role states. Runtime reused_scope_ids MUST be preserved directly. First genuine execution MUST be the formal round and measured time MUST include first execute through second export return and all retries. Failure or exceeding 300 seconds MUST retain the original observation and artifacts; they MUST NOT be removed to replace the result.

#### Scenario: First execution delivers the pair with one shared budget
- **WHEN** the first company's owner run-query-export returns
- **THEN** the second receives the remaining budget, actual runtime reuse and both deliveries are preserved together
- **AND** the complete first execute through second export duration determines the time gate

### Requirement: New PDF review rebuilds unique semantic denominators
The independent review MUST rebuild source disclosures for principal, products/services, revenue, important tables and explicit commodity roles from the new frozen PDFs. It MUST NOT inherit 38/66 denominators or previous checklists. Every required answer and accepted fact MUST be compared once; Measurement duplicates MUST count only in accuracy, and source rows MUST count recall once without repeated aggregation of child revenues. No-role findings MUST state their actual checked pages. Finite passing MUST require recall and accuracy both 100%, zero critical numeric errors, shared tokens at most 50000 and whole-round time at most 300 seconds. Failure MUST pause following batches and any repair MUST address the actual business miss. Passing MUST be submitted for A-role finite acceptance. Production MUST remain not_authorized and scale_quality_claim_allowed MUST remain false.

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

### Requirement: Cumulative v22 repairs restore substantive delivery before the formal round
The current change MUST restore the two frozen reports through existing selection, acceptance, query and export owners. The energy company's continuous principal/products narrative MUST retain the controlled photovoltaic company's subject and complete three-part business. Geographic management explanations MUST NOT become business Activity objects. Current insistence and ongoing business-development clauses MUST support the display company's five established businesses while vision and future layout retain their original state. Sixteen native revenue rows MUST retain labels, amounts, currencies and matching Segment/Measurement copies. Recognition timing MUST NOT be described as a sales channel; actual power customer and market-trading disclosure MUST support the revenue answer. Ten native sales/input roles MUST retain current company/group context, separated product/unit/numeric columns, pending or ambiguous unmapped names and no invented quantity, price, grade, panel input or future robot sale.

#### Scenario: Complete frozen pages pass before genuine execution
- **WHEN** full-page v22 acceptance, query and export regressions and company/external/planned boundary cases complete
- **THEN** the formal v22 identity activates all five cumulative repair switches without changing the default identity
- **AND** the first real round uses the original frozen plan and independent m4_v22_energy_display_repair directory

### Requirement: Substantive recall and preserved failure trail determine finite review
The v22 source conditions MUST be fixed before observation for six substantive answers, sixteen income rows and ten roles. An answered but wrong or incomplete text MUST NOT count as recalled. Every actual accepted fact and answered text MUST receive one accuracy judgment, with Measurement copies accuracy-only. The original 39/45 and corrected 38/45 observations and the limitation that 21/32 used answer-existence recall MUST remain preserved. First execute through second export and retries MUST determine the time gate, with actual runtime reused_scope_ids checked. Finite submission MUST require both semantic recall and accuracy 100%, critical numeric errors zero, shared tokens at most 50000 and whole-round time at most 300 seconds; otherwise following batches MUST remain paused. Current change MUST NOT be archived before A-role review, and production and scale-quality authorization MUST remain unchanged.

#### Scenario: An existing answer lacks required source substance
- **WHEN** a delivered answer is present but fails the independently fixed business substance
- **THEN** it remains one actual accuracy object and its missing substantive disclosure fails recall
- **AND** the answer-existence status cannot substitute for correct source coverage
