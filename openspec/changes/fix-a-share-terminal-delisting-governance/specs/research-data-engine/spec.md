## ADDED Requirements

### Requirement: Current financial maintenance excludes completed delistings without rewriting history
Current-universe financial incremental, reconciliation, and new-manifest selection SHALL use post-governance stock scope and reject formally delisted or already-effective-delisted records even if lifecycle fields are temporarily inconsistent. Scope exclusion SHALL NOT rewrite historical financial state or establish fact completeness.

#### Scenario: Konka remains a historical Q3 gap after master repair
- **WHEN** governance sets `000016.SZ` to completed delisting effective 2026-10-23 and its 2026-09-30 facts remain missing
- **THEN** subsequent current financial runs SHALL NOT select that target, invoke repair providers for it, or add it to new-run blockers
- **AND** the old `local-gap:000016.SZ:2026-09-30` record and original run results SHALL remain unchanged and queryable

#### Scenario: Historical state exists outside the current universe
- **WHEN** a delisted instrument has old pending, delisting-risk, expired, accepted, or blocking states
- **THEN** those records SHALL remain stored without deadline extension, success relabeling, or deletion
- **AND** they SHALL NOT contribute candidates, repair attempts, or pending/accepted/blocking counts to a new current run outside its target universe

#### Scenario: Current target filters name an inactive instrument
- **WHEN** a current maintenance task is given an explicit target instrument ID or symbol for an inactive confirmed-delisted stock
- **THEN** the target filter SHALL only narrow the current universe and SHALL NOT bypass current eligibility
- **AND** the result SHALL identify the empty or excluded target scope without claiming historical repair occurred

#### Scenario: Effective lifecycle fields disagree
- **WHEN** an instrument has `is_active=true` but a confirmed delisted status or an already-effective valid delisting date
- **THEN** current financial candidate selection SHALL reject it and make the inconsistency observable
- **AND** a future date alone SHALL NOT be treated as already-effective delisting

### Requirement: Delisting before a disclosure deadline is not sufficient evidence to accept a prior-period gap
Financial lifecycle classification SHALL NOT automatically accept missing facts for a report period ending on or before delisting solely because delisting occurred before that report's disclosure deadline. The rule SHALL apply to incremental/reconciliation maintenance and the full-import manifest lifecycle helper without retroactively rewriting frozen manifests or historic results.

#### Scenario: Q3 ended before Konka delisting
- **WHEN** the report period ends 2026-09-30, delisting is 2026-10-23, and no independently accepted disclosure-anomaly evidence exists
- **THEN** lifecycle classification SHALL NOT label the missing report `post_delisting_or_no_disclosure` solely from the deadline comparison
- **AND** explicit readiness for that instrument-period SHALL continue to report the missing facts

#### Scenario: Report period ends on delisting date
- **WHEN** period end equals the delisting date and the only proposed explanation is delisting before the disclosure deadline
- **THEN** the date comparison alone SHALL NOT establish an accepted gap or a legal disclosure exemption

#### Scenario: Report period is genuinely outside lifecycle
- **WHEN** period end is before listing or strictly after the confirmed actual delisting date
- **THEN** maintenance SHALL retain its existing lifecycle classification behavior
- **AND** it SHALL preserve missing-fact information rather than manufacture financial facts or disclosure timestamps

#### Scenario: Actual disclosure-anomaly evidence exists
- **WHEN** a report period has independently accepted official disclosure-anomaly evidence under the existing contract
- **THEN** maintenance SHALL retain that evidence-based explanation and missing-fact reporting
- **AND** SHALL NOT replace the evidence requirement with a generic deadline heuristic

### Requirement: Current scope changes preserve historical reads and targeted financial correctness
Delisting-driven current-scope changes SHALL preserve historical financial queries, explicit-target readiness, existing internal targeted-repair semantics, and original point-in-time scope limitations. They SHALL NOT apply today's active flag as a universal historical-universe filter.

#### Scenario: Existing historical facts are requested by instrument and period
- **WHEN** a caller queries pre-delisting financial facts for a delisted instrument
- **THEN** existing facts, source references, disclosure and availability metadata, and revisions SHALL remain available under their original read contract

#### Scenario: Explicit readiness requests a missing historical report
- **WHEN** readiness is requested explicitly for missing 2026-09-30 facts of 000016.SZ
- **THEN** readiness SHALL still show the missing facts, SHALL NOT become ready merely because the stock left current maintenance, and SHALL NOT invent a disclosure exemption

#### Scenario: Existing internal historical repair is invoked
- **WHEN** an authorized caller uses the existing internal explicit instrument-period repair capability for a delisted instrument
- **THEN** that capability SHALL retain its original target and source-availability semantics
- **AND** this change SHALL NOT require automatic reactivation or add a new public inactive-target mode

#### Scenario: Frozen manifest and point-in-time results predate the fix
- **WHEN** the fix is deployed with existing frozen manifests, first-run scores, or point-in-time outputs
- **THEN** they SHALL NOT be silently rewritten or advertised as newly completed delisted-stock historical coverage
- **AND** rollout SHALL identify relevant frozen resume tasks and handle their continued execution under their original contracts
