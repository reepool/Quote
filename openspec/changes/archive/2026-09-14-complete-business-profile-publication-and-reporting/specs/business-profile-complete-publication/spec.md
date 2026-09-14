## ADDED Requirements

### Requirement: Targeted expanded semantic backfill completes governed publication
The system SHALL automatically select the complete targeted publication workflow when an expanded backfill has explicit instruments and requests atomic activities, unless the operator explicitly selects another rollout phase.

#### Scenario: Existing targeted validation command runs
- **WHEN** an operator runs expanded backfill for explicit instruments with `atomic_activities` and `named_relationships`
- **THEN** the effective scope includes program-derived value-chain roles, commodity exposure facts, and commodity exposure publication
- **AND** confirmed records pass through the existing deterministic promotion gates without a separate manual approval command

#### Scenario: Full-market default backfill runs
- **WHEN** latest-annual backfill runs without explicit targeted complete phase selection
- **THEN** the configured active rollout phase remains authoritative
- **AND** targeted publication behavior does not activate full-market promotion

#### Scenario: A derived mapping remains unsupported
- **WHEN** a confirmed activity cannot satisfy deterministic role or commodity publication rules
- **THEN** supported independent records are still published
- **AND** the unsupported target remains a persisted machine-rework exception visible in the result

### Requirement: Verification and publication are distinct durable stages
The asynchronous queue MUST represent extraction, independent semantic verification, and database publication as separate ordered stages, and publication MUST use a single writer.

#### Scenario: A report requires model verification
- **WHEN** extraction completes with network-backed verification targets
- **THEN** the work item advances from `semantic` to `verify`
- **AND** LLM verification time and metrics are attributed to `verify`, not `publish`

#### Scenario: Verification completes
- **WHEN** every required target has a durable verification decision
- **THEN** the work item advances to `publish`
- **AND** publish performs promotion and program derivation with maximum concurrency one

#### Scenario: Legacy unfinished publish item is loaded
- **WHEN** a pre-change unfinished `publish` work item has no completed verify artifact
- **THEN** storage recovery moves it to `verify` without discarding its prior plan, select, or extract artifacts

### Requirement: Independent verification uses bounded concurrency and resume
The verifier SHALL execute independent network-backed target checks concurrently up to the configured semantic limit and SHALL persist target-addressed progress between bounded waves.

#### Scenario: Several unverified targets are ready
- **WHEN** a report has multiple independent targets and gateway capacity is available
- **THEN** verification submits a bounded concurrent wave rather than waiting for each prior target sequentially
- **AND** result order remains deterministic by target identity

#### Scenario: A verification wave is interrupted
- **WHEN** some target results were persisted before interruption or budget exhaustion
- **THEN** the next run skips those target identifiers
- **AND** calls only the unfinished targets

#### Scenario: Token usage reaches the guard
- **WHEN** completed verification usage for a field family reaches 50,000 tokens in the current stage run
- **THEN** no additional verification wave starts for that field family
- **AND** completed in-flight results remain reusable on resume

### Requirement: Publication outcome is explicit and evidence based
The result SHALL distinguish candidate-only completion from complete publication and SHALL expose the counts needed to prove which business tables were populated.

#### Scenario: Candidate extraction completes with promotion disabled
- **WHEN** a shadow workflow completes extraction and verification without promotion
- **THEN** the result reports `candidate_only`
- **AND** it does not claim end-to-end publication success

#### Scenario: Complete publication succeeds
- **WHEN** confirmed activities are promoted and all supported downstream roles and exposures are published
- **THEN** the result reports `complete_publication`
- **AND** includes candidate, verified, promoted, role, exposure-fact, and exposure-publication counts

#### Scenario: Publication has governed input gaps
- **WHEN** one or more downstream targets remain machine rework while other targets publish
- **THEN** the result reports degraded partial publication with the gap counts and reasons
- **AND** does not roll back independent successful publications

### Requirement: Processing-identity queue scope remains visible
Queue health and progress SHALL state that counts are scoped to the current processing identity when such filtering is active.

#### Scenario: Another policy has retry work
- **WHEN** the current targeted identity is complete but another valid policy identity remains retryable
- **THEN** the current report shows its identity-scoped queue as complete
- **AND** does not label the other policy's work as stale or silently supersede it
