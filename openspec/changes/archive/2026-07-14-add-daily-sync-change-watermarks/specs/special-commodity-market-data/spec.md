## ADDED Requirements

### Requirement: Commodity Observations Emit Change Records
Special commodity daily and monthly observation syncs SHALL emit changelog records for inserted and materially changed observations.

#### Scenario: Daily commodity observation changes
- **WHEN** a domestic or overseas commodity observation is refetched with a changed semantic hash
- **THEN** the commodity storage path SHALL append a commodity-domain change record
- **AND** the record SHALL identify series id, observation date or period, source, and source profile

### Requirement: Commodity Policy Discovery Is Domain-Isolated
Special commodity policy discovery, candidate review, and policy-event promotion SHALL emit policy-domain changes separately from price-observation changes.

#### Scenario: Policy candidate is promoted
- **WHEN** a policy candidate is approved and promoted into an event record
- **THEN** the changelog SHALL classify the change under the policy/event domain
- **AND** price-only commodity change queries SHALL NOT include the policy event

### Requirement: Commodity Dry Runs Do Not Emit Persistent Changes
Special commodity syncs running in dry-run mode SHALL report would-write counters but SHALL NOT persist changelog rows.

#### Scenario: Dry run finds changed rows
- **WHEN** a dry-run commodity sync detects observations that would be changed
- **THEN** the task result SHALL report would-write or changed estimates
- **AND** no persistent change watermark SHALL be advanced
