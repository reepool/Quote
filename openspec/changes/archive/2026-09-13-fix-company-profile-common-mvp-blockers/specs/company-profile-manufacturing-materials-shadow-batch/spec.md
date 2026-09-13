## REMOVED Requirements

### Requirement: Evidence-role shadow replay is single-use and hash-bound
**Reason**: The one-shot Evidence-role replay is complete and archived. Its executable proof is hash-bound to retired implementation code and has been removed.

**Migration**: Keep the archived audit artifacts as immutable historical evidence. Future empirical validation MUST use a newly versioned current application path rather than this replay contract.

#### Scenario: Obsolete Evidence-role replay is requested
- **WHEN** an operator attempts to select `evidence-role-semantic-replay` or invoke its retired proof validator
- **THEN** the current CLI exposes no such execution path and no provider request is made

### Requirement: Evidence-role replay is source-reviewed and research-only
**Reason**: The source review and readiness decision are already preserved in the archived change and batch outputs. Keeping their one-shot execution requirement in the active specification falsely implies the retired replay remains runnable.

**Migration**: Read the archived review, readiness, and comparison artifacts for audit purposes. New validation MUST create a new bounded contract and MUST retain `production_authorization=not_authorized` unless separately approved.

#### Scenario: Historical Evidence-role results are inspected
- **WHEN** a caller needs the completed replay result or source review
- **THEN** it reads the archived immutable artifacts without executing the retired replay or changing production authorization
