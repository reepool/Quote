## ADDED Requirements

### Requirement: Retirement candidates have explicit lifecycle classification
Every candidate entry point, compatibility method, script, probe, migration, or document SHALL be classified as production, operator, migration, compatibility, or obsolete before deletion.

#### Scenario: Dev-validation script is reviewed
- **WHEN** the script is still called by production or a release operation
- **THEN** it is not classified obsolete and its required logic/owner is documented

### Requirement: Deletion requires complete caller evidence
An obsolete candidate MUST NOT be deleted until imports, scheduler/config references, CLI/Telegram commands, docs, tests, and rollback dependencies have been checked and a replacement is verified where required.

#### Scenario: No Python import remains but a Telegram command references the script
- **WHEN** the caller inventory finds the command
- **THEN** deletion is blocked until the command migrates or is explicitly retired

### Requirement: Production cannot depend on scripts
All production imports from `scripts/` and `scripts/dev_validation` SHALL be eliminated by moving still-required logic into its owning production module before cleanup.

#### Scenario: Financial maintenance uses an audit helper
- **WHEN** the helper remains part of production acceptance
- **THEN** it moves to a financial production module and the script delegates to it

### Requirement: Retained operator tools use authoritative services
Every retained operator tool SHALL call the canonical application service, have a current runbook, and avoid copying business SQL, provider routing, or write loops.

#### Scenario: Manual repair tool remains necessary
- **WHEN** the tool is classified operator
- **THEN** its implementation is a parameter adapter to the authoritative repair command and its runbook identifies the owner

### Requirement: Obsolete paths are deleted rather than archived in-place
Candidates proven obsolete SHALL be removed from the repository; Git history SHALL provide archival recovery and no permanent `legacy` code/document area SHALL be created.

#### Scenario: Root probe is superseded by maintained tests
- **WHEN** its useful assertion exists in the maintained suite and no operator uses the file
- **THEN** the root probe is deleted

### Requirement: Compatibility facades exit after caller migration
DataManager, ResearchStorageManager, and ScheduledTasks compatibility methods SHALL be removed when caller inventory reaches zero and replacement behavior is accepted.

#### Scenario: Storage compatibility method has no callers
- **WHEN** repository search, static checks, and tests confirm zero callers
- **THEN** the method and its duplicate implementation are removed in the retirement change

### Requirement: Compatibility aliases have a bounded deprecation period
Compatibility methods with plausible external consumers MUST publish a replacement map and emit a deprecation warning for one documented transition cycle before physical deletion.

#### Scenario: Repository search finds no local callers
- **WHEN** a facade method may still be used by an external notebook or operator script
- **THEN** deletion waits for the documented transition cycle and warning evidence rather than treating local zero callers as sufficient

### Requirement: Completed OpenSpec changes are archived safely
Status-complete changes SHALL be archived after durable specs are current and no in-progress dependency requires the live change directory.

#### Scenario: Complete change has no active dependency
- **WHEN** status, cross-references, and current specs pass the archive check
- **THEN** the change is archived and the framework program matrix is updated

### Requirement: Final residue is measurable
The retirement workstream SHALL report remaining production script imports, Telegram subprocess production paths, compatibility callers, obsolete candidates, and unarchived complete changes.

#### Scenario: Framework program is proposed complete
- **WHEN** W1 through W8 acceptance is evaluated
- **THEN** each residue count is zero or has an explicit external blocker recorded in the program
