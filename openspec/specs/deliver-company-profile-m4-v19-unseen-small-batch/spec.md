# deliver-company-profile-m4-v19-unseen-small-batch Specification

## Purpose

Validate unseen official annual reports through the existing company-profile common core using an explicitly excluded observed-company set, a frozen two-company plan, real execution/query/export, and independent source review. The v19 first formal unseen round remains failed; the v20 corrected formal round is accepted only within the finite scope approved by A-role on 2026-10-05. The v19 requirements below preserve the historical intended contract and do not claim that its first formal round passed.

The accepted v20 scope covers the declared principal, products/services and revenue narratives, 12 current-period highway source-table rows and 7 pharma source-table rows, and all 57 accepted facts for frozen 600020.SH and 600056.SH. Recall is 25/25 and unique delivered-object accuracy is 63/63 (57 accepted facts plus six answers), critical numeric errors are zero, first formal whole-round time is 187.6268910896033 seconds, and shared tokens are 0/50000. Actual runtime reused_scope_ids are empty in every stage. Measurement copies are checked for accuracy only; the pharma child rows retain their hierarchy and are not added again to their parent.

Commodity-role absence is limited to the checked physical PDF pages: 600020.SH 10/11/17/18/19 and 600056.SH 9/10/11/13/14/15. This does not claim whole-report completeness. Preserve the original v19 failure (19/25 recall, 49/57 accuracy, zero critical numeric errors), all historical artifacts and the separate v18 review restrictions. Production remains not_authorized and scale_quality_claim_allowed remains false. The next two-company unseen group is authorized only through a separate change that explicitly excludes all twenty observed companies; this is not continuing batch expansion authorization.

## Requirements

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

### Requirement: The authorized v20 round validates the real corrected delivery
The v20 identity MUST enable all five cumulative repair switches and pass frozen-source end-to-end regression. The formal run MUST reuse the exact frozen 0bdc8dd3… plan, cutoff 2026-09-17 and shared 50000-token budget in the independent m4_v20_highway_pharma_repair directory, with service before the mixed pharma company and the second using the remaining budget. Actual runtime scopes MUST show no reuse. First genuine execute through second export including retries MUST be measured; failed or over-limit artifacts MUST remain unchanged. Six answers and every accepted fact MUST each receive one independent source comparison with denominators derived from actual source and delivery. Child rows marked 其中 MUST retain their source hierarchy and MUST NOT be added again to the parent amount. No-role statements MUST be limited to actually checked pages. Dual 100%, zero critical numeric errors, at most 50000 tokens and 300 seconds MUST precede submission for finite A-role acceptance. Production and scale quality MUST remain unauthorized and the next batch paused until reviewed.

#### Scenario: A corrected formal round is submitted without rewriting history
- **WHEN** the first real v20 execution and independent source review complete
- **THEN** the actual answers, facts, scope reuse, timing and recomputed scores are preserved in the new directory
- **AND** the v19 failure remains unchanged, manufacturing sampling labels do not decide the profile template, and no child revenue is counted twice
- **AND** finite acceptance and archive materials are submitted to A-role review without automatically expanding to another batch

### Requirement: The accepted v20 outcome preserves its finite scope and failed history
The archived outcome MUST retain the A-role limited acceptance of the declared source rows, narratives and accepted facts, with source recall 25/25, unique accuracy 63/63 from 57 accepted facts plus six answers, zero critical numeric errors, 187.6268910896033-second first formal whole-round time, 0/50000 shared tokens and no actual runtime scope reuse. It MUST retain the original v19 first failed observation and its 19/25 recall and 49/57 accuracy without rewriting its scores or artifacts. All historical artifacts and the historical v18 review restrictions MUST remain preserved. Commodity-role absence MUST stay limited to the actually checked pages and MUST NOT be generalized to whole-report completeness. Production MUST remain not_authorized and scale_quality_claim_allowed MUST remain false. The authorized next two-company unseen sample MUST use a separate change and explicitly exclude all twenty observed companies, including 600020.SH and 600056.SH; the finite decision MUST NOT grant continuing batch expansion.

#### Scenario: Limited acceptance closes only the reviewed v20 scope
- **WHEN** this change is synchronized and archived after A-role acceptance
- **THEN** its accepted outcome retains the declared source rows, narratives, 57 accepted facts and six answers with the measured v20 scores, timing, token use and empty runtime scope reuse
- **AND** the v19 first failed observation, historical artifacts and v18 restrictions remain unchanged and visible
- **AND** no-role conclusions retain their checked-page scope, production remains not_authorized and scale_quality_claim_allowed remains false
- **AND** the next two-company unseen validation proceeds only under its separately authorized change with the complete twenty-company exclusion set
