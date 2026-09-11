## ADDED Requirements

### Requirement: Post-isolation readiness impact is bounded provider-free
After an item-isolation implementation changes future extract behavior, the system MUST quantify its relationship to an immutable reviewed shadow replay without mutating or rerunning that replay. The audit MUST bind the replay manifest and reports, source-review package and outcomes, empirical readiness result, and item-isolation evidence by content hash; distinguish observed metrics, deterministic optimistic bounds, and unavailable historical salvage; list report/scope exposure and hold reports with no applicable failed call; and retain the empirical readiness decision as authoritative. It MUST NOT reconstruct unavailable provider payloads, infer recovered facts, change report status, call an LLM, change request parameters, or authorize production.

#### Scenario: Item isolation cannot close every report hold
- **WHEN** one or more frozen hold reports have no failed call to which item isolation could apply
- **THEN** the audit reports those reports and computes an absolute usability upper bound that leaves them unchanged
- **AND** it does not claim that the implementation alone can satisfy the usable-report gate

#### Scenario: Historical provider payloads are absent
- **WHEN** the replay retained typed call failures but not the raw provider responses
- **THEN** actual sibling-candidate salvage and resulting report statuses are recorded as unavailable
- **AND** fixture success is not converted into historical accepted facts or readiness

#### Scenario: Unresolved workload is estimated optimistically
- **WHEN** unresolved review rows belong to scopes containing the historical failed calls
- **THEN** the audit MAY calculate a maximum-removal workload floor by excluding all such rows
- **AND** it labels the result as an optimistic bound while preserving the observed median and p90 as authoritative

#### Scenario: The frozen empirical gates remain unmet
- **WHEN** observed usable rate, sampled precision, or human-review workload misses a frozen gate
- **THEN** the post-isolation audit remains `hold` and identifies every unmet gate
- **AND** no replay, token-budget change, production write, Stage 6 activation, or downstream publication is authorized
