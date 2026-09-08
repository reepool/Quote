## ADDED Requirements

### Requirement: Closure validation uses a bounded non-composite rerun sequence
After offline tests and strict OpenSpec validation pass, the closure change MUST use at most one new preflight run containing exactly the Ningde segment scope, the Ningde top-five customer scope, and the Putailai segment-and-adjustment scope. If and only if that preflight demonstrates the required contracts, the operator MAY execute at most one complete four-report run under another new run ID. Neither run MAY copy, merge, backfill, or overwrite records from historical authoritative, targeted, interrupted, Gold, or fixture output.

#### Scenario: Three-scope preflight proves the local corrections
- **WHEN** the new preflight completes the Ningde segment scope, preserves accepted ranked top-five facts while blocking the aggregate Relationship, and accepts source-supported Putailai consolidation-adjustment rows
- **THEN** the operator may start one complete four-report run with a different new run ID
- **AND** no historical accepted record is used as runtime input

#### Scenario: Preflight reveals a remaining blocker
- **WHEN** any of the three preflight scopes has a required execution failure or frozen semantic blocker
- **THEN** the complete four-report run is not started under this change
- **AND** the result is reported without launching additional targeted run loops

### Requirement: The new complete run alone determines slice closure
The complete four-report run, if executed, MUST be the sole candidate authority for report statuses, research projections, Gold evaluation, real-report negative evaluation, and slice usability. Its committed bundle MUST retain every Gold failure or contract conflict and every untriggered negative case. The system MAY register `research_slice_usable` only when all four reports are `usable` or `usable_with_caveats`, all six core chapters per report are complete, no frozen §14 blocker or required execution failure remains, and every evaluated real-report negative case passes. This status MUST retain `production_authorization=not_authorized` and MUST NOT authorize stage six or any production consumer.

#### Scenario: Complete run satisfies the research acceptance policy
- **WHEN** one new complete four-report bundle satisfies all report usability, execution, blocker, and evaluated-negative conditions
- **THEN** that bundle may register `research_slice_usable`
- **AND** Gold non-passes remain disclosed rather than rewritten or silently accepted

#### Scenario: Complete run retains a blocking result
- **WHEN** any report in the new complete bundle retains a required execution failure or frozen §14 blocker
- **THEN** the bundle records the resulting `hold` or `failed` status and does not register `research_slice_usable`
- **AND** no historical or preflight result is spliced in to change that status

#### Scenario: Research slice becomes usable
- **WHEN** the new complete bundle is registered as `research_slice_usable`
- **THEN** all records remain research-only `accepted_for_review` results
- **AND** production authorization, legacy backfill, approved tables, CommodityExposure, ValueChainRole, DCF, and stage-six reset remain closed
