## ADDED Requirements

### Requirement: Deterministic SQLite connection closure
All SQLite connections opened by production code SHALL be deterministically closed at the end of their use scope, either through the repository/factory context managers that close in a finally block or by wrapping raw connections in contextlib.closing.

#### Scenario: Raw connections are wrapped for closure
- **WHEN** production code opens a SQLite connection directly with sqlite3.connect
- **THEN** the connection SHALL be wrapped so its file handle is released when the scope exits, regardless of success or exception

#### Scenario: Write semantics survive the wrapping
- **WHEN** a converted site previously relied on the bare `with connection:` auto-commit for writes
- **THEN** the converted site SHALL contain an explicit commit call preserving the original durability semantics
- **AND** each converted write site SHALL be verified against its pre-conversion behavior before being marked done

#### Scenario: Long-running paths get fd regression coverage
- **WHEN** a converted site sits on a long-running in-process path (semantic runtime, scheduled backup)
- **THEN** a regression test SHALL exercise the connection path repeatedly and assert the process file-descriptor count stays stable

### Requirement: Pattern guard against reintroduction
The test suite SHALL fail when a new bare `with sqlite3.connect(` usage appears in production code directories, with exemptions only through an explicit, justified allowlist.

#### Scenario: Guard fails on a new bare usage
- **WHEN** a source file under the production code directories contains a bare `with sqlite3.connect(` that is not registered in the allowlist
- **THEN** the guard test SHALL fail and name the file and line

#### Scenario: Wrapped and factory usages are not flagged
- **WHEN** a connection is opened through a closing() wrapper or a repository/factory context manager
- **THEN** the guard test SHALL NOT flag it

#### Scenario: Exemptions require justification
- **WHEN** a site is added to the allowlist
- **THEN** the entry SHALL record the file, line, and a justification comment
- **AND** the allowlist size SHALL be expected to shrink over time, never grow silently
