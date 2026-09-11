## ADDED Requirements

### Requirement: Annual duration aliases are normalized only within the prepared report year

The segment-partition adapter MUST normalize a returned `reported_period` to the prepared report year only when `period_type=duration` and the value is the prepared report end date, `YYYY年`, or `YYYY年度` for that same year. It MUST leave narrower duration ranges, instant dates, event/expected periods, and unrecognized strings unchanged.

#### Scenario: Equivalent annual labels merge

- **WHEN** partitions for the same source row return `2025-12-31`, `2025年`, and `2025年度` as duration periods for a 2025 report
- **THEN** the adapter canonicalizes all three to `2025`
- **AND** the row's metric cells merge without changing source Evidence or source-native values.

#### Scenario: Narrower or different period remains distinct

- **WHEN** a partition returns `2025年1-6月`, `2024年度`, or an instant `2025-12-31`
- **THEN** the adapter does not normalize it as the 2025 annual duration
- **AND** a substantive identity conflict remains blocking.

### Requirement: The complete report-segment financial heading is schema-controlled

When controlled Evidence contains `报告分部的财务信息`, the segment extraction schema MUST expose that exact table-owning heading as a dimension choice. It MUST NOT add or accept the ambiguous `报告分部` prefix or the adjacent `报告分部的确定依据与会计政策` narrative heading as an equivalent table dimension, and existing full-heading validation MUST remain enforced.

#### Scenario: Exact report-segment financial heading is available

- **WHEN** Evidence contains both `报告分部的确定依据与会计政策` and `报告分部的财务信息`
- **THEN** `报告分部的财务信息` is available as a dimension choice
- **AND** neither `报告分部` nor the adjacent accounting-policy heading is an allowed choice.

#### Scenario: Existing substantive guards remain active

- **WHEN** partitions contain a duplicate metric cell, coverage disagreement, cost-component label, unsupported subject, or genuinely conflicting period/type
- **THEN** reconciliation keeps the existing blocking behavior
- **AND** the change does not convert that result into an accepted fact.

### Requirement: Historical shadow outputs remain immutable during offline proof

The change MUST validate the normalization behavior with provider-free fixtures and hash-bound audit inputs without modifying the September 11, 2026 batch, invoking an LLM, changing Gold, or granting production authorization.

#### Scenario: Offline proof completes

- **WHEN** focused fixtures and hash checks pass
- **THEN** the audit reports candidate normalization classes separately from retained unresolved classes and identifies where historical raw values are unavailable
- **AND** `production_authorization=not_authorized` remains unchanged.
