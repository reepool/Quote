## MODIFIED Requirements

### Requirement: Evidence and occurrence identity preserve the physical source

Every source-backed object MUST reference versioned Evidence containing report identity, one-based PDF physical page, section, and a stable table cell or normalized bounded-text anchor. A table Measurement identity MUST include `logical_slot + physical_anchor + source_row_or_measured_object` whenever the source Evidence contains multiple rows with otherwise equal logical values. Semantic interpretation, normalized names, evidence IDs, run IDs, and model artifacts remain outside the physical occurrence identity.

#### Scenario: Equal values occur on different source rows

- **WHEN** two rows have the same metric, period, and page but different measured objects
- **THEN** their source row or measured object keeps their occurrences distinct
- **AND** conflict detection does not merge them merely because numeric values match.
