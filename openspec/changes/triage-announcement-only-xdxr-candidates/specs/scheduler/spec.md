## ADDED Requirements

### Requirement: Corporate-action daily job configures announcement-only triage
The existing `a_share_cninfo_corporate_action_daily_sync` job SHALL expose announcement-only mode, LLM profile, likelihood thresholds, confidence floor, case cap, and announcement bundle cap without changing its job identity or schedule.

#### Scenario: Disabled fallback is configured
- **WHEN** the configured announcement-only mode is `disabled`
- **THEN** the scheduler SHALL pass disabled mode to the maintenance command and the job SHALL continue with deterministic title routing

#### Scenario: Invalid thresholds are configured
- **WHEN** low likelihood is not below high likelihood or any threshold is outside the supported range
- **THEN** the maintenance command SHALL fail before announcement-only LLM calls with an explicit configuration error

### Requirement: Corporate-action daily report exposes event-centric triage
The daily report SHALL distinguish announcement-only mode, cases processed, active probable cases, uncertain cases, inactive watches, source reactivations, primary-evidence changes, and processing failures.

#### Scenario: Disabled mode completes normally
- **WHEN** announcement-only semantic mode is disabled and structured maintenance succeeds
- **THEN** the report SHALL show disabled mode without marking the operation partial solely because no announcement-only LLM ran

#### Scenario: Multiple announcements form one case
- **WHEN** two or more announcements are grouped into one provisional case
- **THEN** the report SHALL count one processed case and SHALL expose bounded announcement and primary-evidence counters without reporting duplicate events
