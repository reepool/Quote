## ADDED Requirements

### Requirement: As-of industry history resolves Shenwan L1 from the taxonomy parent chain
The official company-profile registry MUST read industry membership as of the requested `knowledge_cutoff`. When that history row has no top-level `sw_l1_name`, the registry MUST resolve the L1 name by walking the existing `industry_taxonomy` parent chain for that row's `taxonomy_system`, `taxonomy_version`, and `official_industry_code`. The official history payload, which stores only the stock code, industry code, inclusion date, and update time, MUST be sufficient for that walk. A top-level `sw_l1_name` already present on the same row MUST be kept. The reader MUST NOT read `classification.levels.sw_l1.industry_name`, invent a name, use `unknown`, hard-code an instrument id, or write the resolved name back into `industry_classification_history`.

#### Scenario: Official L3 code resolves through the stored parent chain
- **WHEN** the as-of history row stores `official_industry_code` `480301` with `taxonomy_system` `sw` and `taxonomy_version` `sw_2021`, and the active taxonomy parent chain is `480301` → `480300` → `480000` named `银行`
- **THEN** the registry classification `sw_l1_name` is `银行`
- **AND** the history row is not rewritten

#### Scenario: Existing top-level name is unchanged
- **WHEN** the as-of history row already has top-level `sw_l1_name` `银行`
- **THEN** the registry classification keeps `银行`

### Requirement: Disclosure form still uses only the closed Shenwan table
After the stored L1 name is exposed, disclosure form MUST be assigned only by the existing closed Shenwan L1 table. A name already listed as service, finance, or manufacturing MUST receive that existing form. A name absent from the table, an empty stored name, and a missing classification MUST remain `other`. This change MUST NOT add, remove, or rename any Shenwan L1 entry.

#### Scenario: Official service code uses the existing service form
- **WHEN** the as-of history row's official industry code walks the stored taxonomy parent chain to `商贸零售`
- **THEN** the disclosure form is `service`
- **AND** the closed Shenwan L1 table is unchanged

#### Scenario: Broken or missing taxonomy chain stays other
- **WHEN** the as-of history row has an official industry code whose taxonomy node or parent chain is missing
- **THEN** the disclosure form is `other`
- **AND** the registry does not copy a current membership name

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
