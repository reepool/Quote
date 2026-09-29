## ADDED Requirements

### Requirement: The repair stays on the existing chapter and the same four reports
The repair MUST reuse `extract_segment_financials` only. The sample MUST remain the four approved 2025 reports `300750.SZ`, `603659.SH`, `920015.BJ`, and `302132.SZ`. The change MUST NOT add an issuer, resample the manufacturing list, or reopen regime review. The original segment-financial change task 3.2 MUST remain an unchecked failed observation.

#### Scenario: The sample is unchanged
- **WHEN** the scope review names the reports for this repair
- **THEN** the set is exactly the four reports already used by `replay/20260929.2`
- **AND** no issuer outside that set is added

### Requirement: Page 139 keeps the two printed sections apart
On `920015.BJ` physical page 139, the region table MUST use the printed heading `地区分部` as its source dimension, and the business table MUST use the printed heading `业务分部`. Rows with the same amount MUST remain separate when their printed section or row label differs. The region total and the business total MUST both remain. Ordinary rows and elimination columns MUST NOT be merged because their amounts match.

#### Scenario: Two sections with the same total stay separate
- **WHEN** the region table and the business table each print a current total with the same amount
- **THEN** the successor stores one measurement under `地区分部` and one measurement under `业务分部`
- **AND** neither measurement uses the generic section name `报告分部`

#### Scenario: An elimination column stays with its own section
- **WHEN** a printed section has an elimination column with an empty amount
- **THEN** that column stays attached to that section
- **AND** the empty amount is not stored as zero

### Requirement: Page 178 uses the printed note title
On `302132.SZ` physical page 178, every current revenue and cost measurement MUST use the printed formal title `报告分部的财务信息` as its source dimension. The successor MUST NOT place those amounts under the generic section name `报告分部`. The `分部间抵销` row MUST keep `row_class=consolidation_adjustment`.

#### Scenario: The note title is the source dimension
- **WHEN** the successor reads the reportable-segment table on physical page 178
- **THEN** the five segments, the elimination column, and the total use `报告分部的财务信息`
- **AND** the elimination column keeps `consolidation_adjustment`

### Requirement: A printed heading binds the section before the previous table is reset
A numbered heading that is itself a printed section title MUST bind that title as the source dimension and then reset the previous table's column state. A later table MUST NOT inherit the previous table's section name. When the page prints a section title, the extractor MUST NOT substitute a generic default section name. The runtime extractor MUST NOT hard-code a security code, page number, company name, or product name.

#### Scenario: The next printed section replaces the previous section
- **WHEN** one page contains two formal tables with different printed section titles
- **THEN** each table uses its own printed title
- **AND** rows from the second table do not keep the first table's section name

### Requirement: Previously correct boundaries do not regress
The successor MUST keep the page-25 column-role repair: prior-year values `251,677,045`, `69.52%`, `110,335,509`, and `30.48%` stay absent, while domestic cost `223,497,885` with margin `24.00%` and overseas cost `88,885,412` with margin `31.44%` remain. The ten previously missing current-period cells MUST each remain one Measurement. `603659.SH` elimination amounts stay `consolidation_adjustment`, and the elimination margin stays `not_disclosed`. A total-row dash, an empty elimination amount, and the company-level `29.98%` MUST keep their current meanings. Report identity, unit, page, bounded quote, and page-text hash MUST remain consistent with the source page.

#### Scenario: The column repair and the ten cells remain
- **WHEN** the successor reads the four approved reports
- **THEN** the four page-25 misbinding values are absent and the ten repaired cells are present once
- **AND** `118.30`, `104.19`, a total-row dash, an empty elimination amount, and `29.98%` do not become segment amounts or zero

### Requirement: The successor replay is isolated from both failed observations
Implementation after scope approval MUST use plan version `manufacturing_materials_stage4_segment_financials.2026-09-29.3` and MUST write `accepted_for_review` output to a new isolation directory under this change. The bytes of `replay/20260929.2` enqueue, run, and result MUST remain unchanged, including enqueue SHA-256 `2a4c778790678197eda9472a907b8fc1c132be7f08fdbd5d95a3a0cc86c70f19`, run SHA-256 `b5aad9e75ea4200a385ad107f6c044d7d14787f231156b077888018a68708b5e`, and result SHA-256 `6ca3e2960e11f90a13aaaa75e71dc43452344864b78d10db51566f9bff6912b1`. The bytes of `replay/20260928` enqueue, run, result, and source-review MUST remain unchanged, including enqueue SHA-256 `935578cc64a58a84d84224443354c8a6e5f0d9a6167fd1753cbf156e7079ffe3`, run SHA-256 `974dbbfda8608cd1de535215390a9b1accf67b8e7810a9770966620e2e45634b`, result SHA-256 `7c2d5ea6e6433586b46545ad2c597a3c7cc60016ca1023559e7a137c8117d859`, and source-review SHA-256 `ff2f01fd4365cc621dcfeac2b2778d97b446bcf3d73c1d7f3bb06cb7b9035c2d`. The change MUST NOT preset recall, accuracy, critical numeric errors, or `expansion_gates_met`. `scale_quality_claim_allowed` MUST remain false and `production_authorization` MUST remain `not_authorized`. A later source review MUST reread the four reports and MUST NOT treat this dimension repair as proof that the gate passes.

#### Scenario: Scope approval does not fill the gate
- **WHEN** the 1.1 review accepts this repair scope
- **THEN** no successor metric or gate is filled
- **AND** the `replay/20260929.2` and `replay/20260928` artifact hashes remain unchanged
