## MODIFIED Requirements

### Requirement: Segment extraction context is table-owned and compact

For `extract_segment_financials`, the prepared Evidence context MUST include the directly selected table pages and MUST NOT add an arbitrary preceding or following page solely because it is adjacent. A neighboring page MAY be added only when the selected table text explicitly requires continuation context and the page is available. Segment partition requests MUST require the exact source-native table heading when it is present and MUST request compact JSON rows/coverage without prose or repeated source text.

#### Scenario: A segment table ends before the next page

- **WHEN** a selected segment table page has no continuation marker and the next page is not table-owned
- **THEN** the next page is excluded from the segment request context
- **AND** source labels and Evidence validation continue to use the selected table page.

#### Scenario: A segment table explicitly continues

- **WHEN** a selected segment page contains `续表`, `续下表`, or `接下页` and the adjacent page is available
- **THEN** the adjacent page is included as bounded continuation context
- **AND** missing or unreadable continuation context remains a typed planning failure.

#### Scenario: Exact report-segment heading is present

- **WHEN** the controlled Evidence contains `报告分部的财务信息`
- **THEN** the segment extraction request requires that exact heading for non-adjustment rows
- **AND** the adapter continues to reject the ambiguous prefix `报告分部` or any other non-source heading.
