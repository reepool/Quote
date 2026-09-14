## ADDED Requirements

### Requirement: Semantic activities are independent of measurements
The system MUST represent semantically identical issuer action/object assertions for one reporting scope as one business activity, regardless of how many numeric measurements the filing discloses for that activity.

#### Scenario: Sales volume and revenue describe one activity
- **WHEN** an annual-report semantic response states both a sales volume and a sales revenue for the same issuer, report period, action, and product
- **THEN** the system persists one sales activity without a duplicate-primary-key conflict
- **AND** the activity metadata retains every contributing semantic assertion and evidence identity

### Requirement: Every disclosed measurement is preserved as an operating fact
The system MUST create a distinct operating fact for every atomic semantic assertion containing a raw numeric value and raw unit, and MUST link that fact to the grouped activity through existing metadata.

#### Scenario: Correct heterogeneous measurements are both retained
- **WHEN** the LLM returns `23,634,408 片` as sales volume and `1,392,187,620.56 元` as sales revenue for the same product activity
- **THEN** the system persists separate `sales_volume` and `sales_revenue` operating facts with their original values and units
- **AND** both facts reference the same grouped activity

#### Scenario: Unknown measurement label is not discarded
- **WHEN** a numeric assertion has an original Chinese metric label that no deterministic classifier recognizes
- **THEN** the system persists an `other_measurement` candidate containing the original label, raw value, raw unit, assertion identity, and evidence identity

#### Scenario: Unknown unit does not terminate unrelated conversion
- **WHEN** one numeric assertion contains an unresolved unit
- **THEN** the system preserves the raw operating fact as pending normalization
- **AND** still converts the grouped activity and all other resolvable facts in the bundle

### Requirement: Measurement classification and conversion are program-owned
The system MUST classify fact types from original Chinese source labels and MUST perform unit normalization in versioned program logic; model hints MUST NOT override source fields or program calculations.

#### Scenario: Program classifies common operating measurements
- **WHEN** source labels identify sales revenue, sales volume, production volume, inventory volume, purchase amount, or purchase volume
- **THEN** the corresponding program-defined fact types are used without asking the LLM to translate, calculate, or normalize the measurement

### Requirement: Existing activity consumers remain compatible
The system MUST retain at most one explicitly marked compatibility measurement projection on a grouped activity until downstream exposure consumers use linked operating facts directly.

#### Scenario: Physical measurement wins the compatibility projection
- **WHEN** a grouped activity has both a physical volume and a currency amount
- **THEN** the activity's compatibility `value` and `unit` use the physical measurement
- **AND** metadata identifies the projection source and states that linked operating facts are authoritative

#### Scenario: Existing approved profile remains usable
- **WHEN** an already-published profile such as `601088.SH` is read or its deterministic roles and exposures are derived after deployment
- **THEN** its approved activities, operating facts, value-chain roles, exposure facts, and exposures remain available without destructive migration or duplicate publication

### Requirement: Failed conversion replays without a new extraction call
The system MUST version the deterministic conversion contract independently of the unchanged LLM prompt/schema contract so an exact persisted semantic response can be replayed after this fix.

#### Scenario: 300708 semantic artifact is replayed
- **WHEN** the failed `300708.SZ` work is retried with the same annual-report evidence after deployment
- **THEN** the system reuses the persisted semantic artifact with zero extraction LLM tokens
- **AND** publishes the grouped activity and linked operating facts under the new processing identity

### Requirement: Bundle conflict protection remains strict
The system MUST continue rejecting different payloads presented under the same primary key after deterministic grouping and fact identity construction.

#### Scenario: Genuine primary-key conflict is rejected
- **WHEN** a bundle contains two different records with an identical primary key that are not representations of the same grouped activity
- **THEN** the entire bundle is rejected without partial writes
