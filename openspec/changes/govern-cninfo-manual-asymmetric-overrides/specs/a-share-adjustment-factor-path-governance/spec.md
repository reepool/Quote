## ADDED Requirements

### Requirement: Explicit no-effect corporate actions
The CNInfo-derived factor path SHALL retain resolved corporate-action lineage
while excluding any event whose active resolved-term overlay declares
`factor_effect=none` from factor aggregation.

#### Scenario: Recorded shares have no ex-right effect
- **WHEN** a resolved CNInfo event records shares used entirely for debt
  settlement and its active overlay declares `factor_effect=none`
- **THEN** the event does not change the event factor or cumulative factor and
  is reported as an explicitly excluded resolved event rather than pending
