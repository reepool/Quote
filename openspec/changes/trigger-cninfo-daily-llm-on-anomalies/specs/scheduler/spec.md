## ADDED Requirements

### Requirement: Scheduled CNInfo daily maintenance configures anomaly semantics
The scheduler SHALL expose bounded anomaly semantic parameters on
`a_share_cninfo_corporate_action_daily_sync` without changing its task identity
or production isolation.

#### Scenario: Scheduled task starts
- **WHEN** the automatic CNInfo corporate-action daily task runs
- **THEN** it SHALL pass the configured anomaly enablement, event cap, document,
  title-classification, pipeline concurrency, and auto-promotion settings to the
  daily maintenance workflow

#### Scenario: Operator disables anomaly semantics
- **WHEN** `anomaly_llm_enabled` is false
- **THEN** structured refresh and targeted factor maintenance SHALL continue and
  semantic governance SHALL be reported as disabled

### Requirement: Daily report exposes anomaly workload
The scheduler report SHALL show anomaly candidates, reason counts, semantic
processing results, deferred unmatched announcements, promotions, failures, and
remaining review workload.

#### Scenario: No anomaly is selected
- **WHEN** the run contains only complete ordinary structured events
- **THEN** the report SHALL state that zero anomaly events were submitted and
  zero LLM calls were required

#### Scenario: Semantic review remains
- **WHEN** anomaly analysis completes with manual review items
- **THEN** the report SHALL distinguish review readiness from task execution
  failure
