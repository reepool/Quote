## ADDED Requirements

### Requirement: Resume control does not change checkpoint identity
The system SHALL derive A-share historical backfill checkpoint identity from data-affecting parameters and SHALL exclude the `resume` execution-control value.

#### Scenario: Resume is enabled after an initial write run
- **WHEN** two otherwise identical backfill requests differ only in `resume=false` versus `resume=true`
- **THEN** the system resolves them to the same canonical checkpoint identity

#### Scenario: Data-affecting policy changes
- **WHEN** the requested range, exchanges, scopes, chunk size, or repair policy changes
- **THEN** the system resolves a different checkpoint identity

### Requirement: Legacy checkpoints remain resumable
The system SHALL discover and validate checkpoints created before resume-neutral identity was introduced when their stored parameters are otherwise compatible.

#### Scenario: Compatible legacy checkpoint exists
- **WHEN** resume is requested, the canonical checkpoint file is absent, and a legacy checkpoint differs only by the historical inclusion of `resume` in its hash
- **THEN** the system selects the most recently updated compatible legacy checkpoint and reuses its progress

#### Scenario: Legacy checkpoint parameters are incompatible
- **WHEN** a legacy checkpoint differs in a data-affecting parameter
- **THEN** the system SHALL NOT reuse it

### Requirement: Explicit checkpoint selection remains authoritative
The system SHALL preserve an operator-provided checkpoint ID as the selected checkpoint identity.

#### Scenario: Explicit checkpoint ID is supplied
- **WHEN** the operator supplies a valid `checkpoint_id`
- **THEN** the system uses that ID without replacing it through canonical or legacy discovery
