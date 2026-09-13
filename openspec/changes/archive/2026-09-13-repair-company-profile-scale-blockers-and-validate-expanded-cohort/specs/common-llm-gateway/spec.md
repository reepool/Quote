## MODIFIED Requirements

### Requirement: Routed execution reserves bounded failover time
When a logical route has failover enabled, the gateway MUST count the current source and the
remaining eligible members permitted by the configured hop limit before every physical
attempt. A non-final attempt MUST receive a useful bounded share no greater than an equal
share of the remaining absolute logical deadline; the final eligible member MAY use the
remaining deadline. An attempt timeout caused by that bound MUST remain a transient,
lineage-recorded source failure eligible for the configured next member. All source attempts
MUST continue sharing one absolute logical deadline and MUST NOT exceed the configured hop
limit or start when their share is below the configured useful-attempt minimum.

#### Scenario: First source stalls within a four-model route
- **WHEN** four route members are eligible and the first selected source does not respond
  within its fair bounded share
- **THEN** the gateway records a transient attempt timeout and preserves meaningful logical
  deadline for the remaining eligible members
- **AND** it does not extend the caller's absolute request deadline.

#### Scenario: A later route member is selected
- **WHEN** one or more sources have failed and multiple eligible members remain within the
  configured hop budget
- **THEN** the next attempt window is recalculated from the current remaining deadline and
  eligible-member count
- **AND** earlier failed members cannot consume the recalculated share.

#### Scenario: No alternate hop remains
- **WHEN** failover is disabled, the hop limit is exhausted, only the current member remains,
  or the remaining time is below the configured useful-attempt minimum
- **THEN** the gateway does not reserve time for an unavailable hop or start a useless next
  attempt
- **AND** it returns the authoritative typed failure when the current/final attempt fails.
