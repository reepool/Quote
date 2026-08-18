## ADDED Requirements

### Requirement: One authoritative current documentation index
The repository SHALL use `docs/README.md` as the only authoritative index of current architecture, interfaces, runbooks, development rules, and active requirements.

#### Scenario: Developer looks for current guidance
- **WHEN** a developer opens the documentation index
- **THEN** each production capability links to one current document or runbook and historical material is not presented as current guidance

### Requirement: Documents have an explicit lifecycle
Every retained document SHALL be classified as current, runbook, active requirements, or historical, and the classification SHALL determine how it is indexed and maintained.

#### Scenario: Completed requirement document is reviewed
- **WHEN** a requirement has been implemented and its durable rules exist in current documentation or OpenSpec specs
- **THEN** the old requirement document is removed from the current index and is merged or deleted according to the cleanup record

### Requirement: Same-capability documents are consolidated safely
Documents describing the same current capability SHALL be merged into one current document plus only the runbooks required for distinct operator workflows.

#### Scenario: Multiple factor documents overlap
- **WHEN** architecture, governance, operations, and completed requirement documents describe the same factor capability
- **THEN** current rules are consolidated, historical narrative is removed, and each deleted file records its replacement

### Requirement: Documentation deletion requires evidence
A document MUST NOT be deleted until its valid rules have a current replacement and repository references, commands, and active change dependencies have been checked.

#### Scenario: Candidate document still contains a unique command
- **WHEN** cleanup finds a command that is not documented by the proposed replacement
- **THEN** deletion is blocked until the command is validated and preserved or explicitly retired

### Requirement: Current architecture is verified from code and configuration
Architecture and scheduler documentation SHALL derive mutable facts such as job catalogs, database paths, and public entry points from current code or configuration rather than unsupported manual counts.

#### Scenario: Scheduler task count changes
- **WHEN** configured scheduler jobs are added or removed
- **THEN** the current documentation remains accurate through generated or verified catalog data without stale hard-coded totals

### Requirement: Documentation cleanup does not affect production
Documentation consolidation SHALL NOT modify production code, scheduler behavior, database files, public interfaces, or data collection.

#### Scenario: Documentation change is applied
- **WHEN** the documentation workstream is completed
- **THEN** production configuration and executable code are byte-for-byte unchanged except for explicitly documented development-governance references

### Requirement: Completed OpenSpec changes are archived deliberately
Status-complete OpenSpec changes SHALL be archived after confirming that no in-progress change requires their live artifacts and current specs contain the durable requirements.

#### Scenario: Complete change is still referenced
- **WHEN** an in-progress change references evidence or design files from a complete change
- **THEN** archive is deferred or the dependency is migrated before archival
