## ADDED Requirements

### Requirement: Financial Reports Shall Separate Source Collection From Readiness

Financial disclosure incremental and reconciliation reports SHALL expose separate counts for official requests, official structured responses parsed, official numeric facts written, strict canonical-ready targets, fallback-required targets, fallback successes, and unresolved blockers. The report SHALL not use `failed` as the sole label for a target that was parsed successfully but lacks a strict canonical field.

#### Scenario: CNInfo supplies partial official facts and fallback completes the target
- **WHEN** CNInfo parses an instrument-period but leaves `equity_parent` missing
- **AND** THS/Sina fallback fills that missing field
- **THEN** the report SHALL show official parse success and fallback-required/fallback-success counts separately
- **AND** the final target SHALL be successful unless another blocking defect remains

#### Scenario: Official acquisition fails and fallback also fails
- **WHEN** official transport or parsing fails for a target
- **AND** configured fallback cannot produce the required canonical facts
- **THEN** the report SHALL classify the target as unresolved/blocking
- **AND** SHALL include the bounded source diagnostics and missing canonical facts

### Requirement: Official Validation Shall Use Production Canonical Fact Names

The CNInfo official batch validation invoked by maintenance SHALL receive and evaluate the same profile-specific canonical required facts used by production readiness, including `net_income_parent` and `equity_parent`, rather than an unrelated legacy alias list.

#### Scenario: Bank and non-bank targets use the same canonical contract
- **WHEN** maintenance validates CNInfo data20 for a bank or non-bank instrument-period
- **THEN** the validator SHALL evaluate the configured profile-specific canonical fact list
- **AND** SHALL retain source-native `equity_total` as a non-parent fact when `equity_parent` is unavailable
