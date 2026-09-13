## ADDED Requirements

### Requirement: Routed execution reserves bounded failover time
When a logical route has failover enabled and another hop remains, the gateway MUST bound the
current physical source attempt so it does not consume the entire logical execution deadline.
An attempt timeout caused by that bound MUST remain a transient, lineage-recorded source failure
eligible for the configured next member. All source attempts MUST continue sharing one absolute
logical deadline and MUST NOT exceed the configured hop limit.

#### Scenario: First source stalls within a multi-model route
- **WHEN** the first selected source does not respond before its bounded routed-attempt window and another eligible hop remains
- **THEN** the gateway records a transient attempt timeout and selects another eligible member with the remaining logical budget

#### Scenario: No alternate hop remains
- **WHEN** failover is disabled, the hop limit is exhausted, or the remaining time is below the configured useful-attempt minimum
- **THEN** the gateway does not start another source attempt and returns the authoritative typed failure
