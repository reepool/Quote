## ADDED Requirements

### Requirement: Period type governs the meaning of reported period
The common semantic model MUST keep duration, instant, event, and expected time semantics distinct by using the existing `period_type`. An annual duration fact MUST retain the disclosed annual period rather than a mechanically substituted report-end date. An instant fact MUST retain its evidenced as-of date. An event MUST keep its occurrence period separate from `event_date`, `regime_effective_at`, and `knowledge_time`. Normalization MUST NOT replace a source-supported narrower period or mutate historical records outside the current adapter output.

#### Scenario: Annual revenue uses a duration period
- **WHEN** an annual-report revenue Measurement has `period_type=duration` and its only date-shaped period is the same report-year end date
- **THEN** the bounded adapter records the annual `reported_period` as that year
- **AND** the original table header and Evidence remain source-native and unchanged

#### Scenario: Inventory remains an instant
- **WHEN** a report discloses inventory volume as of `2025-12-31` with `period_type=instant`
- **THEN** `reported_period` remains `2025-12-31`
- **AND** it is not reduced to an annual duration period

#### Scenario: Event time remains separated
- **WHEN** a restructuring event occurred on `2025-01-06` and was learned from a later annual report
- **THEN** the event keeps its occurrence period, `event_date`, `regime_effective_at`, and `knowledge_time` as separate fields
- **AND** none of those values overwrites another

### Requirement: Expected completion remains an Evidence-bound qualifier
When an approved Evidence bundle explicitly states an expected completion, completion, or commissioning period for a capacity project, the semantic record MUST preserve the exact source expression in `source_native.qualifier`. The expected period MUST NOT replace the record's report-period semantics, and it MUST NOT be inferred from Gold, project magnitude, industry custom, or a page outside the prepared Evidence scope.

#### Scenario: Continuation Evidence states expected completion
- **WHEN** a capacity row and its approved continuation page explicitly state an expected completion year
- **THEN** the capacity record retains the report period and copies the expected-completion wording into `source_native.qualifier`
- **AND** both physical Evidence anchors remain traceable

#### Scenario: Expected completion is absent
- **WHEN** the prepared Evidence reports new or under-construction capacity without an expected completion statement
- **THEN** the qualifier remains empty or preserves only other explicit source wording
- **AND** the system does not insert a completion year from Gold or convention
