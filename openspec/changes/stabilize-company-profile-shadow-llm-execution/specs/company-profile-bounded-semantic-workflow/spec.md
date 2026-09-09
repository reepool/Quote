## ADDED Requirements

### Requirement: High-cardinality segment extraction is deterministically partitioned
The Stage 5 adapter MAY partition only `extract_segment_financials` requests that meet a frozen source-cardinality threshold and contain at least two unresolved output-bearing metric fields. Each partition MUST retain the same report, scope, Evidence identities, period and subject constraints, MUST include `segment_dimension`, and MUST request a disjoint metric-field subset. All other chapter tasks and non-eligible segment requests MUST remain single extract calls.

#### Scenario: A wide segment table meets the partition threshold
- **WHEN** a bound segment-financial scope meets the frozen numeric-cardinality threshold and requests revenue, cost, and reported margin
- **THEN** the adapter creates stable metric partitions with derived request IDs and identical Evidence
- **AND** the union of partition fields exactly equals the original requested field set

#### Scenario: Another chapter has large source text
- **WHEN** a business-overview, operating-quantity, material, counterparty, or regime scope is large
- **THEN** this partition policy does not split the request
- **AND** existing bounded execution and typed failure behavior remains authoritative

### Requirement: Partition results merge fail-closed before semantic expansion
Partition responses MUST be merged at the compact segment-row layer under the original request identity. Rows may merge only when their source dimension, label, period, subject fields, and Evidence IDs match; metric cell keys MUST be disjoint. Coverage MUST remain field-local. The combined result MUST pass the original minimal schema, full `ExtractResponse` validation, existing occurrence reconciliation, independent verification, and report projection without lowering any requirement.

#### Scenario: Revenue and cost partitions describe the same row
- **WHEN** two successful partitions return the same source row with disjoint revenue and cost cells
- **THEN** the adapter combines the cells into one compact row and expands it once under the original request ID
- **AND** the workflow does not create duplicate Segment records

#### Scenario: Partition rows conflict
- **WHEN** partitions disagree on a row identity field, Evidence IDs, subject semantics, period, or return the same metric cell twice
- **THEN** the extract ends with a typed schema/contract failure
- **AND** no partial partition result enters verification or the research projection

### Requirement: Physical partition calls retain truthful accounting
Every physical partition request MUST consume the existing provider-call budget before gateway invocation and MUST emit a trace with the parent semantic request ID and stable partition index/count. Consecutive physical partition traces MAY map to one logical workflow `extract`, but repair and verify order MUST remain unchanged. A failure in any required partition MUST fail the logical extract without importing another run or model.

#### Scenario: One of three partitions fails
- **WHEN** the second physical partition ends with a typed provider, deadline, truncation, parse, or schema failure
- **THEN** the provider budget and traces include both attempted physical calls and the logical extract fails with that typed error
- **AND** the successful first partition is not returned as a complete extract
