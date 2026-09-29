## ADDED Requirements

### Requirement: The repair stays on the existing chapter and the same four reports
The repair MUST reuse `extract_segment_financials` only. The sample MUST remain the four approved 2025 reports `300750.SZ`, `603659.SH`, `920015.BJ`, and `302132.SZ`. The change MUST NOT add an issuer, resample the manufacturing list, or reopen regime review. Customer and supplier extraction, the six-chapter package, DCF, trading, price sensitivity, and the legacy writer MUST stay inactive.

#### Scenario: The sample is unchanged
- **WHEN** the scope review names the reports for this repair
- **THEN** the set is exactly the four reports already used by `replay/20260928`
- **AND** no issuer outside that set is added

### Requirement: Page 25 prior-year columns stay out of current cost and margin
On `300750.SZ` physical page 25, the revenue-composition continuation and the ten-percent table MUST remain separate formal tables. A prior-year revenue amount, a prior-year revenue share, and a year-over-year change MUST NOT be stored as current-period operating cost or reported gross margin. The four delivered records `manufacturing-materials-300750-2025:operating_cost:分地区:境内:251,677,045:25`, `manufacturing-materials-300750-2025:gross_margin_reported:分地区:境内:69.52%:25`, `manufacturing-materials-300750-2025:operating_cost:分地区:境外:110,335,509:25`, and `manufacturing-materials-300750-2025:gross_margin_reported:分地区:境外:30.48%:25` MUST be absent from the successor result. The successor MUST still retain the ten-percent table's current domestic cost `223,497,885` thousand yuan with reported margin `24.00%`, and the current overseas cost `88,885,412` thousand yuan with reported margin `31.44%`. The repair MUST NOT hide the four errors by editing the old Evidence, renaming the old records, or replacing `replay/20260928/result.json`.

#### Scenario: Prior-year composition values are not current facts
- **WHEN** a revenue-composition continuation states current revenue, a revenue share, prior-year revenue, and a prior-year share
- **THEN** the prior-year revenue is not stored as operating cost
- **AND** the prior-year share is not stored as gross margin

#### Scenario: The correct ten-percent region rows remain
- **WHEN** the successor reads the ten-percent table on the same page
- **THEN** domestic cost `223,497,885` thousand yuan and margin `24.00%` remain
- **AND** overseas cost `88,885,412` thousand yuan and margin `31.44%` remain

### Requirement: The ten missing current-period cells become measurements
The successor MUST deliver one Measurement for each current-period cell listed by the `replay/20260928` source review. `300750.SZ` MUST deliver six: composition total revenue `423,701,834` thousand yuan, product-row other-business revenue `16,916,612` thousand yuan, note revenue and cost for 主营业务 of `406,785,222` and `308,033,498` thousand yuan, note other-business cost `4,349,799` thousand yuan, and note total cost `312,383,297` thousand yuan. `920015.BJ` MUST deliver two: main-business revenue `1,014,715,549.55` yuan and main-business cost `708,496,694.98` yuan. `302132.SZ` MUST deliver two: composition total revenue `75,358,958,001.86` yuan and the other-row operating cost `1,062,261,143.96` yuan from the cost-composition table. Each Measurement MUST keep its own page, unit, source dimension, row label, and bounded quote. These names are source-review fixtures. The runtime extractor MUST NOT hard-code a security code, page number, company name, or product name.

#### Scenario: Each missing cell is delivered once
- **WHEN** the successor result is compared with the ten source-review cells
- **THEN** each cell has a matching Measurement
- **AND** the runtime rule that found it does not contain a hard-coded security code, page, company, or product name

### Requirement: Column roles come from the complete header and table boundary
A formal table MUST be read only after its header, column roles, and table boundary are bound. Current operating revenue, current operating cost, and source-reported gross margin MUST be taken from the columns the header assigns to those roles. A later table on the same page MUST NOT inherit the previous table's column roles. The extractor MUST NOT calculate gross margin from revenue and cost, MUST NOT turn an amount into an Activity, and MUST NOT let a summary sentence replace a formal table cell.

#### Scenario: A second table does not inherit the first header
- **WHEN** one physical page contains a revenue-composition continuation and a later revenue-cost-margin table
- **THEN** each table uses its own header and column roles
- **AND** values from the first table's comparative columns do not fill the second table's cost or margin fields

### Requirement: Previously correct boundaries do not regress
The successor MUST keep `118.30` and `104.19` out of reported gross margin. A total-row dash and an empty elimination amount MUST NOT be stored as zero. The company-level `29.98%` MUST NOT become a segment gross margin. An explicit elimination row or column MUST keep `row_class=consolidation_adjustment`. `not_applicable`, `not_disclosed`, and `unclear` MUST keep their existing meanings, and `legal_empty` MUST only wrap one of those coverage statuses.

#### Scenario: Change columns and empty cells stay unchanged
- **WHEN** the successor reads the Putailai elimination row, the Jinhua total dash, the blank elimination column, and the company-level margin
- **THEN** `118.30` and `104.19` are not gross margins
- **AND** the dash and the blank elimination amount are not stored as zero
- **AND** `29.98%` is not a segment gross margin

### Requirement: The successor replay is isolated from the failed observation
Implementation after scope approval MUST use plan version `manufacturing_materials_stage4_segment_financials.2026-09-29.2` and MUST write `accepted_for_review` output to a new isolation directory. The bytes of `replay/20260928` enqueue, run, result, and source-review MUST remain unchanged, including enqueue SHA-256 `935578cc64a58a84d84224443354c8a6e5f0d9a6167fd1753cbf156e7079ffe3`, run SHA-256 `974dbbfda8608cd1de535215390a9b1accf67b8e7810a9770966620e2e45634b`, result SHA-256 `7c2d5ea6e6433586b46545ad2c597a3c7cc60016ca1023559e7a137c8117d859`, and source-review SHA-256 `ff2f01fd4365cc621dcfeac2b2778d97b446bcf3d73c1d7f3bb06cb7b9035c2d`. The change MUST NOT preset recall `132/132`, accuracy, critical numeric errors, or `expansion_gates_met`. `scale_quality_claim_allowed` MUST remain false and `production_authorization` MUST remain `not_authorized`. A later source review MUST reread the four reports and MUST NOT treat delivery of these fourteen items as proof that the gate passes.

#### Scenario: Scope approval does not fill the gate
- **WHEN** the 1.1 review accepts this repair scope
- **THEN** no successor metric or gate is filled
- **AND** the `replay/20260928` artifact hashes remain unchanged
