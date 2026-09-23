## ADDED Requirements

### Requirement: As-of industry history exposes its stored Shenwan L1 name
The official company-profile registry MUST read industry membership as of the requested `knowledge_cutoff`. When that history row has no top-level `sw_l1_name` but stores a Shenwan L1 name at `classification.levels.sw_l1.industry_name`, the registry classification MUST expose that stored name as `sw_l1_name`. A top-level `sw_l1_name` already present on the same row MUST be kept. The reader MUST NOT invent a name, use `unknown`, or hard-code an instrument id.

#### Scenario: Nested L1 name becomes the classification name
- **WHEN** the as-of history row has an empty top-level `sw_l1_name` and `classification.levels.sw_l1.industry_name` is `商贸零售`
- **THEN** the registry classification `sw_l1_name` is `商贸零售`
- **AND** the name comes from that history row

#### Scenario: Existing top-level name is unchanged
- **WHEN** the as-of history row already has top-level `sw_l1_name` `银行`
- **THEN** the registry classification keeps `银行`

### Requirement: Disclosure form still uses only the closed Shenwan table
After the stored L1 name is exposed, disclosure form MUST be assigned only by the existing closed Shenwan L1 table. A name already listed as service, finance, or manufacturing MUST receive that existing form. A name absent from the table, an empty stored name, and a missing classification MUST remain `other`. This change MUST NOT add, remove, or rename any Shenwan L1 entry.

#### Scenario: Stored service name uses the existing service form
- **WHEN** the as-of history row stores `社会服务` only inside `classification.levels.sw_l1.industry_name`
- **THEN** the disclosure form is `service`
- **AND** the closed Shenwan L1 table is unchanged

#### Scenario: Missing stored L1 name stays other
- **WHEN** the as-of history row has `official_industry_code` but no stored Shenwan L1 name
- **THEN** the disclosure form is `other`

### Requirement: Later current membership cannot replace the as-of row
When an as-of history row exists, the registry MUST NOT fill its Shenwan L1 name from a current membership row. When industry classification history exists for the instrument but no row is effective on or before the requested cutoff, the registry MUST NOT use a current membership either. A current membership may be used only when that instrument has no industry classification history.

#### Scenario: Later membership does not override history
- **WHEN** the as-of history row stores no Shenwan L1 name and a current membership row has `sw_l1_name` `商贸零售`
- **THEN** the registry does not copy `商贸零售` from that current row
- **AND** the disclosure form stays `other`

#### Scenario: History before cutoff is absent
- **WHEN** industry classification history exists only after the cutoff and the current membership `sw_l1_name` is `商贸零售`
- **THEN** the registry does not use that current membership
- **AND** the disclosure form is `other`

### Requirement: This read does not move the first-expansion budget
This change MUST NOT alter the first-expansion two-company budget, MUST NOT record or activate a first-expansion plan, and MUST NOT write a first-expansion observation or closure. Production MUST remain `not_authorized`. `scale_quality_claim_allowed` MUST remain false.

#### Scenario: Restored service name does not activate first expansion
- **WHEN** the as-of read exposes a stored service L1 name
- **THEN** no first-expansion plan, mode, live-run snapshot, source-review snapshot, or closure v2 is written by this change
- **AND** production authorization remains `not_authorized`
