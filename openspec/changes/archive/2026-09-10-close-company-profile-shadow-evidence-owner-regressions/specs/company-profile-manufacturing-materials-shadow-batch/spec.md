## ADDED Requirements

### Requirement: External precision source review is corrected additively
The system MUST preserve the archived external replay batch and v1 review/readiness/comparison artifacts unchanged and MUST publish a hash-bound correction when a reviewed outcome is disproved by its own source page. The `920033.BJ` material-input row MUST be classified as correct because the owner page explicitly marks the main-material/energy disclosure as not applicable. The corrected review chain MUST change no other outcome, MUST retain `hold`, and MUST retain `production_authorization=not_authorized`.

#### Scenario: Explicit material-section applicability disproves an owner error
- **WHEN** the frozen source page contains the governed `主要原材料及能源情况` heading and explicitly marks it not applicable
- **THEN** the material legal-empty result is treated as owner-bound and correct
- **AND** the correction is recorded outside the immutable v1 audit with all prior hashes preserved

### Requirement: Confirmed owner and overview regressions are closed provider-free
Before another cohort replay is proposed, the system MUST close the seven confirmed external-review errors through the existing shadow planner and Stage 5 acceptance owners without invoking an LLM. Operating-quantity/capacity legal-empty conclusions MUST use issuer quantity/capacity owners and MUST NOT use industry background, production-planning, or generic overview/cost context; an explicit issuer capacity disclosure in a governed overview passage MUST remain positive capacity Evidence. Material legal-empty conclusions MUST use procurement/material owners and MUST NOT use controlling-shareholder context. Segment-dimension legal-empty conclusions MUST use segment or revenue/cost owners and MUST NOT use industry/business background. A BusinessOverview value containing only a cross-reference MUST NOT be accepted as substantive overview content.

#### Scenario: Non-owning page attempts to close a field
- **WHEN** industry, shareholder, planning, generic cost, or adjacent context Evidence is the only support for an operating-quantity, material, or segment legal-empty conclusion
- **THEN** the planner or acceptance owner rejects that legal empty or leaves the field unresolved
- **AND** it does not infer non-disclosure from absence on the non-owning page

#### Scenario: Overview explicitly discloses issuer capacity
- **WHEN** a governed issuer overview passage directly states a production-capacity value or scale
- **THEN** that passage may support positive `production_capacity` Evidence
- **AND** industry capacity or planning language remains insufficient

#### Scenario: Overview target is only a cross-reference
- **WHEN** the proposed BusinessOverview value only directs the reader to another section without stating substantive issuer business information
- **THEN** the target is rejected or retained for human review
- **AND** substantive business text in owner Evidence may still be proposed separately

#### Scenario: Seven frozen regressions are evaluated
- **WHEN** the corrected frozen review rows are run through provider-free fixtures
- **THEN** all seven confirmed errors produce the required owner rejection, positive capacity preservation, or cross-reference rejection
- **AND** the `920033.BJ` control remains a valid legal empty

### Requirement: Owner-regression closure remains research-only
All correction artifacts, fixtures, tests, and closure audits produced by this change MUST record zero provider calls and `production_authorization=not_authorized`. Provider-free closure MUST NOT change the external replay's `hold`, write approved data, or enable Stage 6, scheduler/backfill, commodity exposure, value-chain publication, DCF, or another production consumer.

#### Scenario: Every provider-free regression passes
- **WHEN** the corrected baseline and all seven confirmed fixtures pass
- **THEN** the change may authorize only a later separate empirical replay proposal
- **AND** no current research record is promoted to production
