## REMOVED Requirements

### Requirement: Evidence-role semantic replay remains executable
**Reason**: The one-shot Evidence-role validation is complete and archived, while its hash-bound
implementation proof is intentionally incompatible with the current provider. Keeping a runnable
mode creates a dead second path and recurring false regression failures.

**Migration**: Use the current-MVP shadow mode for current-code validation. Read the archived
Evidence-role OpenSpec artifacts only as historical audit evidence; do not execute them.

#### Scenario: Obsolete Evidence-role mode is requested
- **WHEN** an operator attempts to select `evidence-role-semantic-replay` or its retired proof option
- **THEN** the current CLI exposes no such mode or proof validator and no provider request can be made through that path

### Requirement: Current-MVP semantic replay remains executable
**Reason**: The one-shot September 12 current-MVP batch is complete, and its implementation-hash
proof is intentionally incompatible with later provider fixes. Retaining the executable path
creates a second false historical regression gate instead of validating current behavior.

**Migration**: Use current application entry points and a newly versioned validation change for
future empirical runs. Keep the September 12 batch and OpenSpec artifacts as immutable evidence;
do not execute their proof against current code.

#### Scenario: Obsolete current-MVP mode is requested
- **WHEN** an operator attempts to select `current-mvp-semantic-replay` or its retired proof option
- **THEN** the current CLI exposes no such mode or proof validator and no provider request can be made through that path
