## MODIFIED Requirements

### Requirement: Segment-repair retry records the interrupted predecessor

The shadow batch audit MUST identify the prior operator-interrupted attempt by
batch identity and content hash, record its partial completion as an execution
failure, and prove that no report from it is included in the new retry result.

#### Scenario: Interrupted predecessor is preserved

- **WHEN** a retry is admitted after an operator cutoff
- **THEN** the audit records the predecessor path, available report count, and
  interruption reason
- **AND** the new batch comparison uses only the new batch and the complete frozen
  baseline
