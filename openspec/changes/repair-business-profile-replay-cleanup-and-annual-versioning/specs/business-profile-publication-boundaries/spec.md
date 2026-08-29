## ADDED Requirements

### Requirement: Candidate isolation after cleanup
The publication layer MUST publish only records that pass current identity, evidence, and semantic gates. Deleted or non-reusable candidate artifacts MUST not be eligible for publication or replay.

#### Scenario: Failed candidate is removed
- **WHEN** cleanup deletes a failed semantic receipt and its candidate descendants
- **THEN** publication and replay queries exclude those records

#### Scenario: Valid candidate remains reviewable
- **WHEN** a candidate has complete evidence and a current identity but is not yet approved
- **THEN** it remains available to the normal review path without being exposed as approved output

### Requirement: Publication failure transparency
The system MUST expose publication gaps and their deterministic reasons without converting them into successful publication or hiding them as empty data.

#### Scenario: Publication gate fails
- **WHEN** a derived exposure or role lacks a required approved component
- **THEN** publication remains unapproved and reports the missing gate reason

#### Scenario: Complete publication
- **WHEN** all required components and identity gates pass
- **THEN** the system publishes the derived record once and a replay does not duplicate it
